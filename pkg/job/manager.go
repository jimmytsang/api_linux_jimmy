// Package job runs commands as child processes ("jobs") and streams their
// combined output to any number of readers.
//
// TODO: out of scope - cgroups resource limits and guaranteed cleanup of child processes.
// TODO: out of scope - limits on output size, number of jobs, and how long finished jobs are kept.
package job

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"slices"
	"sync"
	"syscall"
	"time"
)

var (
	// ErrNotFound is returned for a job ID the manager doesn't know.
	ErrNotFound = errors.New("job not found")
	// ErrClosed is returned by Start once the manager has been closed.
	ErrClosed = errors.New("job manager closed")
)

// waitDelay is how long cmd.Wait waits for the output pipe to close after the
// process exits. A child that inherited the pipe would otherwise keep it open,
// and the output stream with it, until the child exits.
const waitDelay = time.Second

// State is the lifecycle state of a job.
type State int

const (
	Running State = iota + 1 // command in progress
	Exited                   // process exited by itself
	Stopped                  // process was killed by SIGKILL from Stop or Close
)

func (s State) String() string {
	switch s {
	case Running:
		return "RUNNING"
	case Exited:
		return "EXITED"
	case Stopped:
		return "STOPPED"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// Status is a snapshot of a job. Callers own it and may modify it freely.
type Status struct {
	ID, Owner, Command string
	Args               []string
	State              State
	ExitCode           *int // nil while running; -1 if killed by a signal
	// Signal is the signal that terminated the job, such as SIGKILL from Stop
	// or SIGSEGV from a crash. It is 0 while running and when the job exited
	// with a status code.
	Signal syscall.Signal
}

// Manager starts jobs and keeps track of them. It is safe for concurrent use.
type Manager struct {
	// lifecycle guards closed. Start holds it for reading while it forks, so
	// Starts run in parallel; Close takes it for writing, so it waits for them.
	lifecycle sync.RWMutex
	closed    bool

	mu   sync.Mutex // guards jobs; never held across a fork
	jobs map[string]*job
}

func NewManager() *Manager {
	return &Manager{jobs: make(map[string]*job)}
}

// Start runs command with args as a new job owned by owner. The command runs
// directly, without a shell, with no stdin and only PATH in its environment.
// If the command can't be started, no job is created.
//
// owner is an opaque label kept with the job and returned in its Status. The
// library makes no decisions with it; it lives here so a job and its owner are
// created, stored and (eventually) removed together.
func (m *Manager) Start(owner, command string, args []string) (Status, error) {
	// Hold lifecycle until the job is in the map, so Close can't run in between
	// and miss it.
	m.lifecycle.RLock()
	defer m.lifecycle.RUnlock()
	if m.closed {
		return Status{}, ErrClosed
	}

	out := newOutput()
	cmd := exec.Command(command, args...)
	// out is not an *os.File, so os/exec creates the pipe and a goroutine that
	// copies from it into out. Because Stdout and Stderr are the same
	// comparable writer, os/exec shares one pipe between them and at most one
	// goroutine calls Write at a time.
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	cmd.WaitDelay = waitDelay
	if err := cmd.Start(); err != nil {
		return Status{}, fmt.Errorf("start %q: %w", command, err)
	}

	j := &job{
		id:      rand.Text(),
		owner:   owner,
		command: command,
		args:    slices.Clone(args),
		cmd:     cmd,
		out:     out,
		done:    make(chan struct{}),
		state:   Running,
	}
	go j.wait()

	m.mu.Lock()
	m.jobs[j.id] = j
	m.mu.Unlock()

	return j.status(), nil
}

// Stop kills a running job with SIGKILL and waits for it to exit. Stopping a
// job that has already finished just returns its final status. If the process
// exits by itself before the signal lands, the job is Exited, not Stopped.
//
// ctx bounds only the wait: once Stop has sent the signal, cancelling ctx
// doesn't save the job. Its final status is available later through Status.
//
// TODO: out of scope - send SIGTERM first and SIGKILL only after a grace period.
func (m *Manager) Stop(ctx context.Context, id string) (Status, error) {
	j, err := m.get(id)
	if err != nil {
		return Status{}, err
	}
	if err := j.kill(); err != nil {
		return Status{}, err
	}
	select {
	case <-j.done:
		return j.status(), nil
	case <-ctx.Done():
		// select picks at random when both are ready, so check done again:
		// if the job has finished, its final status wins over ctx.
		select {
		case <-j.done:
			return j.status(), nil
		default:
			return Status{}, ctx.Err()
		}
	}
}

// Status returns a snapshot of the job's current status.
func (m *Manager) Status(id string) (Status, error) {
	j, err := m.get(id)
	if err != nil {
		return Status{}, err
	}
	return j.status(), nil
}

// Output returns a reader over the job's combined stdout and stderr, starting
// at the first byte. Read blocks until there is more output, and returns
// io.EOF once the job has exited and everything has been read. Closing the
// reader unblocks a pending Read.
func (m *Manager) Output(id string) (io.ReadCloser, error) {
	j, err := m.get(id)
	if err != nil {
		return nil, err
	}
	return j.out.newReader(), nil
}

// Close kills every running job and waits for them all to exit, which ends
// every output stream. Start fails with ErrClosed afterwards; the other methods
// keep working on the jobs that exist.
func (m *Manager) Close() error {
	m.lifecycle.Lock() // waits for any Start in progress
	m.closed = true
	m.lifecycle.Unlock()

	m.mu.Lock()
	jobs := slices.Collect(maps.Values(m.jobs))
	m.mu.Unlock()

	// Signal every job before waiting on any, so the waits overlap.
	var errs []error
	for _, j := range jobs {
		if err := j.kill(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, j := range jobs {
		<-j.done
	}
	return errors.Join(errs...)
}

func (m *Manager) get(id string) (*job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}

// job is one started process. The fields above mu never change after Start.
type job struct {
	id, owner, command string
	args               []string
	cmd                *exec.Cmd
	out                *output
	done               chan struct{} // closed once the final status is recorded

	mu            sync.Mutex
	state         State
	exitCode      int            // valid once state is not Running
	signal        syscall.Signal // valid once state is not Running; 0 unless signalled
	stopRequested bool           // set by kill; Stopped only if SIGKILL then ends it
}

// wait runs on its own goroutine for the life of the job. It is the only
// place that moves a job out of Running.
func (j *job) wait() {
	// Wait's error is either an *ExitError, whose information is already in
	// ProcessState, or ErrWaitDelay, meaning a surviving child still held the
	// pipe and its later output was dropped. Neither changes the job's status.
	_ = j.cmd.Wait()

	code, sig := -1, syscall.Signal(0)
	if ps := j.cmd.ProcessState; ps != nil {
		code = ps.ExitCode() // -1 if terminated by a signal
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			sig = ws.Signal()
		}
	}

	j.mu.Lock()
	j.exitCode = code
	j.signal = sig
	// Stopped only if our SIGKILL is what ended it. Stop can land after the
	// process already exited by itself, e.g. while Wait sits in WaitDelay for
	// a child holding the pipe; that job Exited, and its status must say so.
	if j.stopRequested && sig == syscall.SIGKILL {
		j.state = Stopped
	} else {
		j.state = Exited
	}
	j.mu.Unlock()

	// Record the final status before ending the output, so a client that sees
	// its stream end and then asks for the status gets the final one.
	j.out.finish()
	close(j.done)
}

// kill sends SIGKILL to a running job. A job that has already finished is
// never signalled, which makes Stop safe to retry.
func (j *job) kill() error {
	j.mu.Lock()
	if j.state != Running {
		j.mu.Unlock()
		return nil
	}
	j.stopRequested = true
	j.mu.Unlock()

	// ErrProcessDone means the process already exited and was reaped, so the
	// waiter will record it as Exited. os.Process never signals a reaped PID,
	// so there is no risk of hitting a reused one.
	if err := j.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill job %s: %w", j.id, err)
	}
	return nil
}

func (j *job) status() Status {
	j.mu.Lock()
	defer j.mu.Unlock()
	s := Status{
		ID:      j.id,
		Owner:   j.owner,
		Command: j.command,
		Args:    slices.Clone(j.args),
		State:   j.state,
	}
	if j.state != Running {
		s.ExitCode = new(j.exitCode)
		s.Signal = j.signal
	}
	return s
}
