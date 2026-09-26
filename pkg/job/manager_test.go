package job

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// testTimeout bounds every wait in these tests, so a bug hangs a test for a
// few seconds instead of until the go test deadline.
const testTimeout = 10 * time.Second

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager()
	t.Cleanup(func() { m.Close() })
	return m
}

func start(t *testing.T, m *Manager, command string, args ...string) Status {
	t.Helper()
	s, err := m.Start("alice", command, args)
	if err != nil {
		t.Fatalf("Start(%q, %q): %v", command, args, err)
	}

	// Kill the process directly rather than through the manager, so a bug in
	// Stop or Close can't leak it. Best effort: once the process has been
	// reaped, Kill fails harmlessly with os.ErrProcessDone.
	j, err := m.get(s.ID)
	if err != nil {
		t.Fatalf("get(%s): %v", s.ID, err)
	}
	t.Cleanup(func() { _ = j.cmd.Process.Kill() })
	return s
}

// readOutput reads the job's output until EOF and returns it along with the
// job's status. The status is final, because the waiter records it before
// ending the output.
func readOutput(t *testing.T, m *Manager, id string) (string, Status) {
	t.Helper()
	r, err := m.Output(id)
	if err != nil {
		t.Fatalf("Output(%s): %v", id, err)
	}
	defer r.Close()

	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(r)
		ch <- result{data, err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("reading output of %s: %v", id, res.err)
		}
		s, err := m.Status(id)
		if err != nil {
			t.Fatalf("Status(%s): %v", id, err)
		}
		return string(res.data), s
	case <-time.After(testTimeout):
		t.Fatalf("output of %s did not end within %v", id, testTimeout)
		return "", Status{}
	}
}

// waitExit waits for the job to finish, without reading its output, and
// returns its final status. The waiter closes done only after recording it.
func waitExit(t *testing.T, m *Manager, id string) Status {
	t.Helper()
	j, err := m.get(id)
	if err != nil {
		t.Fatalf("get(%s): %v", id, err)
	}
	select {
	case <-j.done:
		return j.status()
	case <-time.After(testTimeout):
		t.Fatalf("job %s did not exit within %v", id, testTimeout)
		return Status{}
	}
}

func checkFinal(t *testing.T, s Status, state State, code int, sig syscall.Signal) {
	t.Helper()
	if s.State != state {
		t.Errorf("state = %v, want %v", s.State, state)
	}
	if s.Signal != sig {
		t.Errorf("signal = %d (%v), want %d (%v)", s.Signal, s.Signal, sig, sig)
	}
	if s.ExitCode == nil {
		t.Fatalf("exit code = nil, want %d", code)
	}
	if *s.ExitCode != code {
		t.Errorf("exit code = %d, want %d", *s.ExitCode, code)
	}
}

func TestStartReturnsRunningStatus(t *testing.T) {
	m := newTestManager(t)
	args := []string{"30"}
	s := start(t, m, "sleep", args...)

	if s.ID == "" || s.Owner != "alice" || s.Command != "sleep" || s.State != Running {
		t.Errorf("unexpected status %+v", s)
	}
	if s.ExitCode != nil {
		t.Errorf("exit code = %d while running, want nil", *s.ExitCode)
	}
	if s.Signal != 0 {
		t.Errorf("signal = %v while running, want 0", s.Signal)
	}

	// The caller's args and the returned Status are copies of the job's.
	args[0] = "changed"
	s.Args[0] = "changed"
	if got, _ := m.Status(s.ID); got.Args[0] != "30" {
		t.Errorf("job args changed to %q through a caller's slice", got.Args[0])
	}
}

func TestExitCodes(t *testing.T) {
	tests := []struct {
		name    string
		command string
		args    []string
		code    int
		sig     syscall.Signal
	}{
		{"success", "true", nil, 0, 0},
		{"failure", "sh", []string{"-c", "exit 3"}, 3, 0},
		// Killed by signals it didn't get from Stop: still Exited, code -1,
		// and Signal tells an OOM-style SIGKILL apart from a crash.
		{"sigkill", "sh", []string{"-c", "kill -KILL $$"}, -1, syscall.SIGKILL},
		{"sigsegv", "sh", []string{"-c", "kill -SEGV $$"}, -1, syscall.SIGSEGV},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			s := start(t, m, tt.command, tt.args...)
			checkFinal(t, waitExit(t, m, s.ID), Exited, tt.code, tt.sig)
		})
	}
}

func TestOutputCombinesStdoutAndStderr(t *testing.T) {
	m := newTestManager(t)
	s := start(t, m, "sh", "-c", "echo out; echo err >&2; echo out again")
	got, _ := readOutput(t, m, s.ID)
	if want := "out\nerr\nout again\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestMinimalEnvironment(t *testing.T) {
	t.Setenv("JOB_TEST_SECRET", "hunter2")
	m := newTestManager(t)
	s := start(t, m, "env")
	got, _ := readOutput(t, m, s.ID)

	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "PATH=") {
		t.Errorf("job environment = %q, want only PATH", got)
	}
}

func TestStartMissingExecutable(t *testing.T) {
	m := newTestManager(t)
	for _, command := range []string{"no-such-command-jobtest", "/no/such/path", ""} {
		s, err := m.Start("alice", command, nil)
		if err == nil {
			t.Errorf("Start(%q) succeeded with status %+v, want an error", command, s)
		}
	}

	// The server maps this to InvalidArgument, so the cause must survive.
	_, err := m.Start("alice", "no-such-command-jobtest", nil)
	if !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("Start err = %v, want it to wrap exec.ErrNotFound", err)
	}
}

func TestStop(t *testing.T) {
	m := newTestManager(t)
	s := start(t, m, "sleep", "30")

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stopped, err := m.Stop(ctx, s.ID)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	checkFinal(t, stopped, Stopped, -1, syscall.SIGKILL)

	// The stream ends once the job is stopped.
	_, final := readOutput(t, m, s.ID)
	checkFinal(t, final, Stopped, -1, syscall.SIGKILL)
}

func TestStopFinishedJob(t *testing.T) {
	m := newTestManager(t)
	s := start(t, m, "sh", "-c", "exit 7")
	waitExit(t, m, s.ID)

	// Never signalled, stays Exited, and repeated Stops agree.
	for range 2 {
		got, err := m.Stop(t.Context(), s.ID)
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
		checkFinal(t, got, Exited, 7, 0)
	}
}

func TestStopFinishedJobCancelledContext(t *testing.T) {
	m := newTestManager(t)
	s := start(t, m, "sh", "-c", "exit 7")
	waitExit(t, m, s.ID)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A finished job has nothing to wait for, so the cancelled context must
	// not matter. Repeat, since a select choosing between two ready cases
	// would get it right half the time.
	for range 20 {
		got, err := m.Stop(ctx, s.ID)
		if err != nil {
			t.Fatalf("Stop with cancelled ctx on a finished job: %v", err)
		}
		checkFinal(t, got, Exited, 7, 0)
	}
}

func TestStopAfterProcessExitedIsExited(t *testing.T) {
	m := newTestManager(t)
	// sh exits at once, but the backgrounded sleep holds the pipe, so Wait
	// sits in WaitDelay and the job still reads Running for about a second.
	s := start(t, m, "sh", "-c", "sleep 5 & exit 0")
	j, err := m.get(s.ID)
	if err != nil {
		t.Fatalf("get(%s): %v", s.ID, err)
	}

	// Wait until sh has exited and been reaped, which is when Signal starts
	// returning ErrProcessDone, while the job is still Running.
	deadline := time.Now().Add(testTimeout)
	for !errors.Is(j.cmd.Process.Signal(syscall.Signal(0)), os.ErrProcessDone) {
		if time.Now().After(deadline) {
			t.Fatalf("sh did not exit within %v", testTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := j.status(); got.State != Running {
		t.Fatalf("state = %v before Stop, want RUNNING (still in WaitDelay)", got.State)
	}

	// Stop lands after the process ended by itself, so it isn't Stopped.
	got, err := m.Stop(t.Context(), s.ID)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	checkFinal(t, got, Exited, 0, 0)
}

func TestStopCancelledContextStillKills(t *testing.T) {
	m := newTestManager(t)
	s := start(t, m, "sleep", "30")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// Stop may see either the cancelled context or the job's exit first; both
	// are correct. Either way the signal has been sent.
	if _, err := m.Stop(ctx, s.ID); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop: %v", err)
	}
	checkFinal(t, waitExit(t, m, s.ID), Stopped, -1, syscall.SIGKILL)
}

func TestUnknownID(t *testing.T) {
	m := newTestManager(t)
	if _, err := m.Status("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Status err = %v, want ErrNotFound", err)
	}
	if _, err := m.Stop(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Stop err = %v, want ErrNotFound", err)
	}
	if _, err := m.Output("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Output err = %v, want ErrNotFound", err)
	}
}

func TestChildHoldingPipeDoesNotHoldStream(t *testing.T) {
	m := newTestManager(t)
	// The backgrounded sleep inherits the output pipe and outlives sh. Without
	// WaitDelay the stream would stay open until the sleep exits.
	const childLife = 5 * time.Second
	begin := time.Now()
	s := start(t, m, "sh", "-c", "sleep 5 & echo hi")

	got, final := readOutput(t, m, s.ID)
	if elapsed := time.Since(begin); elapsed >= childLife-time.Second {
		t.Errorf("stream took %v to end, want about waitDelay (%v)", elapsed, waitDelay)
	}
	if got != "hi\n" {
		t.Errorf("output = %q, want %q", got, "hi\n")
	}
	checkFinal(t, final, Exited, 0, 0)
}

func TestClose(t *testing.T) {
	m := newTestManager(t)
	s := start(t, m, "sleep", "30")

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close waits for every job, so the status is already final.
	got, err := m.Status(s.ID)
	if err != nil {
		t.Fatalf("Status after Close: %v", err)
	}
	checkFinal(t, got, Stopped, -1, syscall.SIGKILL)

	if _, err := m.Start("alice", "true", nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Start after Close err = %v, want ErrClosed", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestStartRacingClose(t *testing.T) {
	m := newTestManager(t)

	// Start jobs from many goroutines while Close runs.
	const starts = 20
	var wg sync.WaitGroup
	ids := make(chan string, starts)
	for range starts {
		wg.Go(func() {
			s, err := m.Start("alice", "sleep", []string{"30"})
			switch {
			case err == nil:
				ids <- s.ID
			case !errors.Is(err, ErrClosed):
				t.Errorf("Start: %v", err)
			}
		})
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	close(ids)

	// Every Start either failed with ErrClosed or got its job into the map
	// before Close took the lock, so Close killed it and waited for it. No
	// job may still be running.
	for id := range ids {
		j, err := m.get(id)
		if err != nil {
			t.Fatalf("get(%s): %v", id, err)
		}
		t.Cleanup(func() { _ = j.cmd.Process.Kill() })
		if s := j.status(); s.State != Stopped {
			t.Errorf("job %s is %v after Close, want STOPPED", id, s.State)
		}
	}
}
