package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
	"github.com/jimmytsang/api_linux_jimmy/internal/certgen"
	"github.com/jimmytsang/api_linux_jimmy/internal/server"
	"github.com/jimmytsang/api_linux_jimmy/pkg/job"
)

func TestWriteStatus(t *testing.T) {
	tests := []struct {
		name string
		st   *pb.JobStatus
		want string
	}{
		{
			name: "running has no exit code",
			st:   &pb.JobStatus{JobId: "ID1", Owner: "jimmy", Command: "ping", Args: []string{"-c", "3", "localhost"}, State: pb.JobState_JOB_STATE_RUNNING},
			want: "" +
				"ID:        ID1\n" +
				"Owner:     jimmy\n" +
				"Command:   ping -c 3 localhost\n" +
				"State:     RUNNING\n",
		},
		{
			name: "exited 0",
			st:   &pb.JobStatus{JobId: "ID1", Owner: "jimmy", Command: "true", State: pb.JobState_JOB_STATE_EXITED, ExitCode: new(int32(0))},
			want: "" +
				"ID:        ID1\n" +
				"Owner:     jimmy\n" +
				"Command:   true\n" +
				"State:     EXITED\n" +
				"Exit code: 0\n",
		},
		{
			name: "crashed",
			st:   &pb.JobStatus{JobId: "ID1", Owner: "jimbob", Command: "./crash", State: pb.JobState_JOB_STATE_EXITED, ExitCode: new(int32(-1)), Signal: new(int32(11))},
			want: "" +
				"ID:        ID1\n" +
				"Owner:     jimbob\n" +
				"Command:   ./crash\n" +
				"State:     EXITED\n" +
				"Exit code: -1 (signal 11: segmentation fault)\n",
		},
		{
			name: "stopped",
			st:   &pb.JobStatus{JobId: "ID1", Owner: "jimmy", Command: "sleep", Args: []string{"60"}, State: pb.JobState_JOB_STATE_STOPPED, ExitCode: new(int32(-1)), Signal: new(int32(9))},
			want: "" +
				"ID:        ID1\n" +
				"Owner:     jimmy\n" +
				"Command:   sleep 60\n" +
				"State:     STOPPED\n" +
				"Exit code: -1 (signal 9: killed)\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b strings.Builder
			writeStatus(&b, tt.st)
			if got := b.String(); got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestCommandLine(t *testing.T) {
	tests := []struct {
		command string
		args    []string
		want    string
	}{
		{"ping", []string{"-c", "3", "localhost"}, `ping -c 3 localhost`},
		{"bash", []string{"-c", "sleep 300; echo done"}, `bash -c "sleep 300; echo done"`},
		{"echo", []string{""}, `echo ""`},
		{"echo", []string{`say "hi"`}, `echo "say \"hi\""`},
		{"echo", []string{`it's`}, `echo "it's"`},
		{"echo", []string{`a\b`}, `echo "a\\b"`},
		{"echo", []string{"a\tb"}, `echo "a\tb"`},
		// Escape sequences from the job's owner never reach the terminal raw.
		{"echo", []string{"\x1b[2J"}, `echo "\x1b[2J"`},
		{"/opt/my tools/run", nil, `"/opt/my tools/run"`},
		{"echo", []string{"héllo;|&*"}, `echo héllo;|&*`},
	}
	for _, tt := range tests {
		if got := commandLine(tt.command, tt.args); got != tt.want {
			t.Errorf("commandLine(%q, %q) = %q, want %q", tt.command, tt.args, got, tt.want)
		}
	}
}

func TestTerminalWriter(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{"text, newlines and tabs pass through", []string{"hi\tthere\n"}, "hi\tthere\n"},
		{"printable text in any language passes through", []string{"héllo 日本\n"}, "héllo 日本\n"},
		{"clear screen", []string{"\x1b[2J"}, `\x1b[2J`},
		{"set title and write clipboard", []string{"\x1b]0;hi\x07\x1b]52;c;aGk=\x07"}, `\x1b]0;hi\a\x1b]52;c;aGk=\a`},
		{"carriage return can't overwrite a line", []string{"real\rfake\n"}, `real\rfake` + "\n"},
		{"NUL and DEL", []string{"\x00\x7f"}, `\x00\x7f`},
		{"C1 control as UTF-8", []string{"\u009b2J"}, `\u009b2J`},
		{"bytes that aren't UTF-8", []string{"\xff\x9b2J"}, `\xff\x9b2J`},
		{"character split across writes", []string{"h\xc3", "\xa9llo"}, "héllo"},
		{"escape split from its sequence", []string{"\x1b", "[2J"}, `\x1b[2J`},
		{"incomplete character at the end", []string{"a\xe6\x97"}, `a\xe6\x97`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			w := &terminalWriter{w: &b}
			for _, s := range tt.writes {
				if n, err := w.Write([]byte(s)); n != len(s) || err != nil {
					t.Fatalf("Write(%q) = %d, %v; want %d, nil", s, n, err, len(s))
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := b.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseArgs(t *testing.T) {
	defaults := invocation{server: "127.0.0.1:50051", cert: "certs/jimbob.crt", key: "certs/jimbob.key", ca: "certs/ca.crt"}
	with := func(f func(*invocation)) invocation {
		inv := defaults
		f(&inv)
		return inv
	}
	tests := []struct {
		name    string
		args    []string
		want    invocation
		wantErr string
	}{
		{
			name: "start",
			args: []string{"start", "--", "ping", "-c", "3", "localhost"},
			want: with(func(i *invocation) { i.command = "start"; i.job = []string{"ping", "-c", "3", "localhost"} }),
		},
		{
			name: "job args that look like our flags belong to the job",
			args: []string{"start", "--", "ls", "--server", "x", "--", "-l"},
			want: with(func(i *invocation) { i.command = "start"; i.job = []string{"ls", "--server", "x", "--", "-l"} }),
		},
		{
			name: "flags before the command",
			args: []string{"--server", "127.0.0.1:1", "--cert", "c.crt", "-key", "k.key", "--ca", "ca.crt", "status", "ID1"},
			want: invocation{server: "127.0.0.1:1", cert: "c.crt", key: "k.key", ca: "ca.crt", command: "status", jobID: "ID1"},
		},
		{name: "status", args: []string{"status", "ID1"}, want: with(func(i *invocation) { i.command = "status"; i.jobID = "ID1" })},
		{name: "output", args: []string{"output", "ID1"}, want: with(func(i *invocation) { i.command = "output"; i.jobID = "ID1" })},
		{name: "stop", args: []string{"stop", "ID1"}, want: with(func(i *invocation) { i.command = "stop"; i.jobID = "ID1" })},

		{name: "no command", args: nil, wantErr: "missing command"},
		{name: "unknown command", args: []string{"list"}, wantErr: `unknown command "list"`},
		{name: "unknown flag", args: []string{"--user", "jimbob", "status", "ID1"}, wantErr: "flag provided but not defined: -user"},
		{name: "start without --", args: []string{"start", "ls", "-l"}, wantErr: "usage: worker [flags] start -- <command> [args...]"},
		{name: "start with nothing after --", args: []string{"start", "--"}, wantErr: "usage: worker [flags] start"},
		{name: "flag after start", args: []string{"start", "--cert", "c.crt", "--", "ls"}, wantErr: "usage: worker [flags] start"},
		{name: "status without ID", args: []string{"status"}, wantErr: "usage: worker [flags] status <job-id>"},
		{name: "stop with two IDs", args: []string{"stop", "ID1", "ID2"}, wantErr: "usage: worker [flags] stop <job-id>"},
		{name: "flag after status", args: []string{"status", "--cert", "c.crt", "ID1"}, wantErr: "usage: worker [flags] status <job-id>"},
		// A lone flag after the command would otherwise be sent as the job ID.
		{name: "help after status", args: []string{"status", "-h"}, wantErr: "usage: worker [flags] status <job-id>"},
		{name: "help after stop", args: []string{"stop", "--help"}, wantErr: "usage: worker [flags] stop <job-id>"},
		{name: "help after output", args: []string{"output", "-h"}, wantErr: "usage: worker [flags] output <job-id>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseArgs(%q) error = %v, want one containing %q", tt.args, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%q): %v", tt.args, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseArgs(%q) =\n%+v\nwant\n%+v", tt.args, got, tt.want)
			}
		})
	}
}

// TestUsage checks run's exit codes and output for bad command lines. None of
// these reach a server.
func TestUsage(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string // prefix
		wantStderr string // prefix
	}{
		{"help", []string{"-h"}, exitOK, "usage: worker [flags] <command>", ""},
		{"no command", nil, exitUsage, "", "error: missing command\n\nusage: worker"},
		{"unknown flag", []string{"--nope", "status", "ID1"}, exitUsage, "", "error: flag provided but not defined: -nope\n\nusage: worker"},
		{"start without --", []string{"start", "ls"}, exitUsage, "", "error: usage: worker [flags] start -- <command> [args...]\n\nusage: worker"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			code := run(t.Context(), tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if !strings.HasPrefix(stdout.String(), tt.wantStdout) || (tt.wantStdout == "" && stdout.Len() > 0) {
				t.Errorf("stdout = %q, want it to start with %q", stdout.String(), tt.wantStdout)
			}
			if !strings.HasPrefix(stderr.String(), tt.wantStderr) || (tt.wantStderr == "" && stderr.Len() > 0) {
				t.Errorf("stderr = %q, want it to start with %q", stderr.String(), tt.wantStderr)
			}
		})
	}
}

// TestEndToEnd drives the CLI against a real server over mTLS, the way the
// README demo does: jimmy is an admin, jimbob a user.
func TestEndToEnd(t *testing.T) {
	env := newTestEnv(t)

	id := env.start(t, "jimmy", "sleep", "60")

	t.Run("status of a running job", func(t *testing.T) {
		env.expect(t, env.worker(t, "jimmy", "status", id), exitOK, ""+
			"ID:        "+id+"\n"+
			"Owner:     jimmy\n"+
			"Command:   sleep 60\n"+
			"State:     RUNNING\n", "")
	})

	t.Run("another user's job is not found", func(t *testing.T) {
		for _, cmd := range []string{"status", "stop", "output"} {
			env.expect(t, env.worker(t, "jimbob", cmd, id), exitError, "", "error: job not found\n")
		}
		// jimbob's stop didn't touch it.
		if out := env.worker(t, "jimmy", "status", id).stdout; !strings.Contains(out, "State:     RUNNING\n") {
			t.Errorf("after jimbob's stop, status = \n%s\nwant RUNNING", out)
		}
	})

	t.Run("admin sees other users' jobs", func(t *testing.T) {
		bobs := env.start(t, "jimbob", "sleep", "60")
		res := env.worker(t, "jimmy", "status", bobs)
		env.expect(t, res, exitOK, "", "")
		if !strings.Contains(res.stdout, "Owner:     jimbob\n") {
			t.Errorf("admin status of jimbob's job = \n%s\nwant Owner jimbob", res.stdout)
		}
	})

	t.Run("stop", func(t *testing.T) {
		want := "" +
			"State:     STOPPED\n" +
			"Exit code: -1 (signal 9: killed)\n"
		env.expect(t, env.worker(t, "jimmy", "stop", id), exitOK, want, "")
		// Stopping a finished job just reports the final status again.
		env.expect(t, env.worker(t, "jimmy", "stop", id), exitOK, want, "")
		// A stopped job didn't succeed, so output of it fails too.
		env.expect(t, env.worker(t, "jimmy", "output", id), exitError, "", "error: job stopped, exit code -1 (signal 9: killed)\n")
	})

	t.Run("another user's escape codes don't reach the admin's terminal", func(t *testing.T) {
		// jimbob's job tries to clear the screen of whoever watches it. main
		// wraps a terminal stdout the same way.
		bobs := env.start(t, "jimbob", "printf", `\033[2Jgotcha\n`)
		var screen bytes.Buffer
		var stderr strings.Builder
		code := run(t.Context(), env.flags("jimmy", "output", bobs), &terminalWriter{w: &screen}, &stderr)
		if code != exitOK || stderr.String() != "" {
			t.Fatalf("output = exit %d, stderr %q; want exit 0 and no error", code, stderr.String())
		}
		if got, want := screen.String(), `\x1b[2Jgotcha`+"\n"; got != want {
			t.Errorf("admin's screen got %q, want %q", got, want)
		}
	})

	t.Run("output of a job that succeeded", func(t *testing.T) {
		id := env.start(t, "jimmy", "echo", "hi")
		env.expect(t, env.worker(t, "jimmy", "output", id), exitOK, "hi\n", "")
	})

	t.Run("output and final status", func(t *testing.T) {
		// Binary-safe: a NUL and a byte that isn't valid UTF-8 come back as
		// is. The job exits 3, so output does too: the output comes first,
		// then an error, so `worker output $id && next` stops.
		script := `printf 'one\n'; printf 'two\000\377\n' >&2; exit 3`
		id := env.start(t, "jimmy", "sh", "-c", script)
		env.expect(t, env.worker(t, "jimmy", "output", id), exitError, "one\ntwo\x00\xff\n", "error: job exited, exit code 3\n")
		// The stream ends only once the final status is recorded.
		env.expect(t, env.worker(t, "jimmy", "status", id), exitOK, ""+
			"ID:        "+id+"\n"+
			"Owner:     jimmy\n"+
			"Command:   sh -c "+fmt.Sprintf("%q", script)+"\n"+
			"State:     EXITED\n"+
			"Exit code: 3\n", "")
	})
}

// TestOutputCtrlC checks that Ctrl-C while watching a job ends the CLI
// cleanly and leaves the job running. Stdout presses Ctrl-C (cancels ctx) as
// soon as the job's first line arrives, while the job is still running, so
// run needs no goroutine or timer. A CLI that held output back until the job
// ended would only write once the job exited, and fail the RUNNING check.
func TestOutputCtrlC(t *testing.T) {
	env := newTestEnv(t)
	id := env.start(t, "jimmy", "sh", "-c", "echo ready; sleep 60")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stdout := &onWrite{fn: cancel}
	var stderr bytes.Buffer
	code := run(ctx, env.flags("jimmy", "output", id), stdout, &stderr)

	if code != exitOK {
		t.Errorf("exit code after Ctrl-C = %d, want %d", code, exitOK)
	}
	if got := stdout.String(); got != "ready\n" {
		t.Errorf("stdout = %q, want %q", got, "ready\n")
	}
	if stderr.Len() > 0 {
		t.Errorf("stderr after Ctrl-C = %q, want nothing", stderr.String())
	}
	if out := env.worker(t, "jimmy", "status", id).stdout; !strings.Contains(out, "State:     RUNNING\n") {
		t.Errorf("status after Ctrl-C =\n%s\nwant the job still RUNNING", out)
	}
}

// onWrite records what the CLI writes and calls fn on every write, to make
// something happen once output appears, such as the user pressing Ctrl-C.
type onWrite struct {
	bytes.Buffer
	fn func()
}

func (w *onWrite) Write(p []byte) (int, error) {
	w.fn()
	return w.Buffer.Write(p)
}

// TestOutputServerVanishes checks that output gives up on a server that has
// vanished without closing the connection, instead of waiting forever on a job
// that prints nothing. Nothing is closed, so only the client's keepalive ping
// going unanswered can tell.
func TestOutputServerVanishes(t *testing.T) {
	env := newTestEnv(t)
	id := env.start(t, "jimmy", "sh", "-c", "echo ready; sleep 60")
	// gRPC won't ping more often than every 10s; a 1s ping timeout keeps the
	// test at about 11s.
	saved := server.ClientKeepalive
	server.ClientKeepalive.Time, server.ClientKeepalive.Timeout = 10*time.Second, time.Second
	t.Cleanup(func() { server.ClientKeepalive = saved })

	proxy := newSilentProxy(t, env.addr)
	args := env.flags("jimmy", "output", id)
	args[1] = proxy.addr // the value of --server
	// The server vanishes once output is streaming: the job's first line is in.
	stdout := &onWrite{fn: func() { proxy.silent.Store(true) }}
	var stderr bytes.Buffer
	code := run(t.Context(), args, stdout, &stderr)

	if code != exitError {
		t.Errorf("exit code = %d, want %d", code, exitError)
	}
	if got := stdout.String(); got != "ready\n" {
		t.Errorf("stdout = %q, want %q", got, "ready\n")
	}
	if !strings.HasPrefix(stderr.String(), "error: ") {
		t.Errorf("stderr = %q, want an error", stderr.String())
	}
}

// silentProxy forwards TCP connections to a target until silent is set. From
// then on it keeps every connection open but drops what either side sends,
// like a server whose machine has dropped off the network.
type silentProxy struct {
	addr   string
	silent atomic.Bool
}

func newSilentProxy(t *testing.T, target string) *silentProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &silentProxy{addr: lis.Addr().String()}
	var mu sync.Mutex
	var conns []net.Conn
	// Closing the listener and every connection ends the proxy's goroutines.
	t.Cleanup(func() {
		lis.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			client, err := lis.Accept()
			if err != nil {
				return
			}
			srv, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			mu.Lock()
			conns = append(conns, client, srv)
			mu.Unlock()
			go p.forward(srv, client)
			go p.forward(client, srv)
		}
	}()
	return p
}

func (p *silentProxy) forward(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if p.silent.Load() {
			continue // dropped
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

// TestErrors checks that failures print "error: <message>" to stderr, never
// the raw "rpc error: code = ..." text, and exit non-zero.
func TestErrors(t *testing.T) {
	env := newTestEnv(t)

	// A socket path in a fresh temp directory, so nothing can be listening.
	// A just-closed TCP port could be taken by another test's listener
	// before the CLI dials it.
	notRunning := env.flags("jimmy", "status", "ID1")
	notRunning[1] = "unix://" + filepath.Join(t.TempDir(), "no-server.sock") // the value of --server
	missingCert := env.flags("jimmy", "status", "ID1")
	missingCert[3] = "/no/such.crt" // the value of --cert

	tests := []struct {
		name       string
		args       []string
		wantStderr string // prefix
		notStderr  string // must not appear
	}{
		{"unknown job", env.flags("jimmy", "status", "NOSUCHJOB"), "error: job not found\n", ""},
		{"stop unknown job", env.flags("jimmy", "stop", "NOSUCHJOB"), "error: job not found\n", ""},
		{"output unknown job", env.flags("jimmy", "output", "NOSUCHJOB"), "error: job not found\n", ""},
		{"executable not found", env.flags("jimmy", "start", "--", "no-such-command-8f3a"), `error: executable "no-such-command-8f3a" not found` + "\n", ""},
		// The server closes an unknown user's connection right after the
		// TLS handshake, so the call never reaches a handler, which would
		// say "job not found". The client only sees the connection close, so
		// the wording isn't checked.
		{"unknown user", env.flags("mallory", "status", "NOSUCHJOB"), "error: ", "job not found"},
		{"missing certificate", missingCert, "error: load key pair: open /no/such.crt", ""},
		{"server not running", notRunning, "error: ", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), tt.args, &stdout, &stderr)
			if code != exitError {
				t.Errorf("exit code = %d, want %d", code, exitError)
			}
			if stdout.Len() > 0 {
				t.Errorf("stdout = %q, want nothing", stdout.String())
			}
			if !strings.HasPrefix(stderr.String(), tt.wantStderr) || strings.Contains(stderr.String(), "rpc error") {
				t.Errorf("stderr = %q, want it to start with %q and not contain the raw rpc error", stderr.String(), tt.wantStderr)
			}
			if tt.notStderr != "" && strings.Contains(stderr.String(), tt.notStderr) {
				t.Errorf("stderr = %q, want it not to contain %q", stderr.String(), tt.notStderr)
			}
		})
	}
}

// testEnv is a real worker-server on a loopback port, with its own CA and a
// certificate per user in dir.
type testEnv struct {
	addr string
	dir  string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	ca, err := certgen.NewCA("test CA")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "ca.crt", ca.CertPEM)
	leaves := map[string]certgen.Leaf{
		"server":  {CommonName: "worker-server", Hosts: []string{"localhost", "127.0.0.1"}, Usage: x509.ExtKeyUsageServerAuth},
		"jimmy":   {CommonName: "jimmy", Usage: x509.ExtKeyUsageClientAuth},
		"jimbob":  {CommonName: "jimbob", Usage: x509.ExtKeyUsageClientAuth},
		"mallory": {CommonName: "mallory", Usage: x509.ExtKeyUsageClientAuth}, // signed by our CA, but not in the roles table
	}
	for name, leaf := range leaves {
		certPEM, keyPEM, err := ca.Issue(leaf)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, dir, name+".crt", certPEM)
		writeFile(t, dir, name+".key", keyPEM)
	}

	tlsConfig, err := server.ServerTLSConfig(filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key"), filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(tlsConfig, job.NewManager(), slog.New(slog.NewTextHandler(t.Output(), nil)))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	// Shutdown also kills every job the test started.
	t.Cleanup(func() {
		srv.Shutdown()
		if err := <-serveErr; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	// Dial the address the listener bound, as the CLI's default does.
	return &testEnv{addr: lis.Addr().String(), dir: dir}
}

// flags returns a command line that runs as user against the test server.
func (e *testEnv) flags(user string, args ...string) []string {
	return append([]string{
		"--server", e.addr,
		"--cert", filepath.Join(e.dir, user+".crt"),
		"--key", filepath.Join(e.dir, user+".key"),
		"--ca", filepath.Join(e.dir, "ca.crt"),
	}, args...)
}

type result struct {
	code           int
	stdout, stderr string
}

// worker runs the CLI as user and returns what it printed.
func (e *testEnv) worker(t *testing.T, user string, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), e.flags(user, args...), &stdout, &stderr)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

var jobIDLine = regexp.MustCompile(`^[A-Z0-9]+\n$`)

// start starts a job as user and returns its ID, checking the CLI printed
// nothing but the ID.
func (e *testEnv) start(t *testing.T, user string, job ...string) string {
	t.Helper()
	res := e.worker(t, user, append([]string{"start", "--"}, job...)...)
	if res.code != exitOK || res.stderr != "" || !jobIDLine.MatchString(res.stdout) {
		t.Fatalf("start %q = %+v, want exit 0 and only a job ID on stdout", job, res)
	}
	return strings.TrimSuffix(res.stdout, "\n")
}

// expect checks a result. An empty wantStdout on success skips the stdout
// check; everywhere else "" means nothing was printed.
func (e *testEnv) expect(t *testing.T, res result, wantCode int, wantStdout, wantStderr string) {
	t.Helper()
	if res.code != wantCode {
		t.Errorf("exit code = %d, want %d (stderr %q)", res.code, wantCode, res.stderr)
	}
	if (wantStdout != "" || wantCode != exitOK) && res.stdout != wantStdout {
		t.Errorf("stdout:\n%q\nwant:\n%q", res.stdout, wantStdout)
	}
	if res.stderr != wantStderr {
		t.Errorf("stderr = %q, want %q", res.stderr, wantStderr)
	}
}

func writeFile(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}
