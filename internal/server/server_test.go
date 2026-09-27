package server

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
	"github.com/jimmytsang/api_linux_jimmy/internal/certgen"
	"github.com/jimmytsang/api_linux_jimmy/pkg/job"
)

// timeout bounds every wait in these tests, so a broken close or shutdown
// fails the test instead of hanging it.
const timeout = 5 * time.Second

// testEnv is a real server on a loopback port with its own CA, serving over
// mTLS exactly as worker-server does.
type testEnv struct {
	ca     *certgen.CA
	caFile string
	addr   string
	srv    *Server
	jobs   *spyManager
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ca := newCA(t)
	caFile := writeFile(t, "ca.crt", ca.CertPEM)
	certFile, keyFile := issue(t, ca, certgen.Leaf{
		CommonName: "worker-server",
		Hosts:      []string{"localhost", "127.0.0.1"},
		Usage:      x509.ExtKeyUsageServerAuth,
	})
	tlsConfig, err := ServerTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	jobs := &spyManager{Manager: job.NewManager(), readers: make(chan *spyReader, 100)}
	srv := newServer(tlsConfig, jobs, slog.New(slog.NewTextHandler(t.Output(), nil)))

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
	return &testEnv{ca: ca, caFile: caFile, addr: lis.Addr().String(), srv: srv, jobs: jobs}
}

// dial connects with a fresh client certificate for commonName.
func (e *testEnv) dial(t *testing.T, commonName string) *grpc.ClientConn {
	t.Helper()
	certFile, keyFile := issue(t, e.ca, certgen.Leaf{CommonName: commonName, Usage: x509.ExtKeyUsageClientAuth})
	tlsConfig, err := ClientTLSConfig(certFile, keyFile, e.caFile)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(e.addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func (e *testEnv) client(t *testing.T, commonName string) pb.JobWorkerClient {
	return pb.NewJobWorkerClient(e.dial(t, commonName))
}

// spyManager is the real manager, except that it hands the test every output
// reader the server opens, so the test can see when it is read and closed.
type spyManager struct {
	*job.Manager
	readers chan *spyReader
}

func (m *spyManager) Output(id string) (io.ReadCloser, error) {
	r, err := m.Manager.Output(id)
	if err != nil {
		return nil, err
	}
	spy := &spyReader{ReadCloser: r, reading: make(chan struct{}), closed: make(chan struct{})}
	select {
	case m.readers <- spy:
	default: // the test isn't watching this many readers
	}
	return spy, nil
}

type spyReader struct {
	io.ReadCloser
	readOnce, closeOnce sync.Once
	reading             chan struct{} // closed on the first Read
	closed              chan struct{} // closed on the first Close
}

func (r *spyReader) Read(p []byte) (int, error) {
	r.readOnce.Do(func() { close(r.reading) })
	return r.ReadCloser.Read(p)
}

func (r *spyReader) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return r.ReadCloser.Close()
}

// TestStreamJobOutputClientGoesAway checks that a stream handler blocked in
// Read ends when its client leaves. The job prints nothing, so the handler
// has nothing to send and only the context hook closing the reader can get it
// out of Read: if the hook is broken, the handler hangs and the test fails.
func TestStreamJobOutputClientGoesAway(t *testing.T) {
	tests := []struct {
		name  string
		leave func(cancel context.CancelFunc, conn *grpc.ClientConn)
	}{
		{"cancel", func(cancel context.CancelFunc, _ *grpc.ClientConn) { cancel() }},
		{"close connection", func(_ context.CancelFunc, conn *grpc.ClientConn) { conn.Close() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			conn := env.dial(t, "jimmy")
			c := pb.NewJobWorkerClient(conn)
			id := startJob(t, c, "sleep", "60").GetJobId()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if _, err := c.StreamJobOutput(ctx, &pb.StreamJobOutputRequest{JobId: id}); err != nil {
				t.Fatal(err)
			}
			r := within(t, env.jobs.readers, "the handler to open the output")
			within(t, r.reading, "the handler to start reading")

			tt.leave(cancel, conn)

			within(t, r.closed, "the reader to be closed")
			waitGoroutinesGone(t, "(*service).StreamJobOutput") // the handler and its AfterFunc
			waitGoroutinesGone(t, "job.(*reader).Read")

			// Leaving stops the watching, not the job.
			st, err := env.jobs.Status(id)
			if err != nil {
				t.Fatal(err)
			}
			if st.State != job.Running {
				t.Errorf("job state = %v, want %v", st.State, job.Running)
			}
		})
	}
}

func TestStreamJobOutput(t *testing.T) {
	env := newTestEnv(t)
	c := env.client(t, "jimmy")
	// About 170 KiB, several chunks' worth.
	id := startJob(t, c, "seq", "1", "30000").GetJobId()

	stream, err := c.StreamJobOutput(t.Context(), &pb.StreamJobOutputRequest{JobId: id})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	var chunks int
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if n := len(msg.GetData()); n == 0 || n > chunkSize {
			t.Errorf("chunk of %d bytes, want 1 to %d", n, chunkSize)
		}
		chunks++
		got = append(got, msg.GetData()...)
	}

	var want bytes.Buffer
	for i := 1; i <= 30000; i++ {
		fmt.Fprintln(&want, i)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Errorf("got %d bytes of output, want %d", len(got), want.Len())
	}
	if chunks < 2 {
		t.Errorf("got %d chunks, want the output split into several", chunks)
	}
	r := within(t, env.jobs.readers, "the handler to open the output")
	within(t, r.closed, "the reader to be closed after the output ended")

	// The stream only ends after the final status is recorded.
	resp, err := c.GetJobStatus(t.Context(), &pb.GetJobStatusRequest{JobId: id})
	if err != nil {
		t.Fatal(err)
	}
	if st := resp.GetStatus(); st.GetState() != pb.JobState_JOB_STATE_EXITED || st.ExitCode == nil || st.GetExitCode() != 0 {
		t.Errorf("status after the stream ended = %v, want EXITED with exit code 0", st)
	}
}

// TestShutdown checks that Shutdown kills jobs before stopping gRPC: a stream
// on a running job then ends cleanly, which lets GracefulStop finish. Stopping
// gRPC first would wait on that stream until shutdownTimeout.
func TestShutdown(t *testing.T) {
	env := newTestEnv(t)
	c := env.client(t, "jimmy")
	id := startJob(t, c, "sleep", "60").GetJobId()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	stream, err := c.StreamJobOutput(ctx, &pb.StreamJobOutputRequest{JobId: id})
	if err != nil {
		t.Fatal(err)
	}
	r := within(t, env.jobs.readers, "the handler to open the output")
	within(t, r.reading, "the handler to start reading")

	done := make(chan struct{})
	go func() {
		env.srv.Shutdown()
		close(done)
	}()
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv during shutdown = %v, want io.EOF", err)
	}
	within(t, done, "Shutdown to return")

	st, err := env.jobs.Status(id)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != job.Stopped {
		t.Errorf("job state after shutdown = %v, want %v", st.State, job.Stopped)
	}
}

func TestInvalidRequests(t *testing.T) {
	env := newTestEnv(t)
	c := env.client(t, "jimmy")
	ctx := t.Context()
	start := func(command string) func() error {
		return func() error {
			_, err := c.StartJob(ctx, &pb.StartJobRequest{Command: command})
			return err
		}
	}
	stat := func(id string) func() error {
		return func() error {
			_, err := c.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: id})
			return err
		}
	}
	stop := func(id string) func() error {
		return func() error {
			_, err := c.StopJob(ctx, &pb.StopJobRequest{JobId: id})
			return err
		}
	}
	stream := func(id string) func() error {
		return func() error {
			_, err := streamAll(ctx, c, id)
			return err
		}
	}

	tests := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"start with empty command", start(""), codes.InvalidArgument},
		{"start command not in PATH", start("no-such-command-8f3a"), codes.InvalidArgument},
		{"start path that doesn't exist", start("/no/such/dir/cmd"), codes.InvalidArgument},
		{"status with empty ID", stat(""), codes.InvalidArgument},
		{"stop with empty ID", stop(""), codes.InvalidArgument},
		{"stream with empty ID", stream(""), codes.InvalidArgument},
		{"status of unknown job", stat("NOSUCHJOB"), codes.NotFound},
		{"stop unknown job", stop("NOSUCHJOB"), codes.NotFound},
		{"stream unknown job", stream("NOSUCHJOB"), codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.Code(tt.call()); got != tt.want {
				t.Errorf("code = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestJobStatus checks each state's fields make it through the API, in
// particular that exit_code and signal are absent rather than 0 when unset.
func TestJobStatus(t *testing.T) {
	env := newTestEnv(t)
	c := env.client(t, "jimmy")
	ctx := t.Context()

	t.Run("running then stopped", func(t *testing.T) {
		started := startJob(t, c, "sleep", "60")
		want := &pb.JobStatus{
			JobId:   started.GetJobId(),
			Owner:   "jimmy",
			Command: "sleep",
			Args:    []string{"60"},
			State:   pb.JobState_JOB_STATE_RUNNING,
		}
		if !proto.Equal(started, want) {
			t.Errorf("StartJob status = %v, want %v", started, want)
		}

		resp, err := c.StopJob(ctx, &pb.StopJobRequest{JobId: started.GetJobId()})
		if err != nil {
			t.Fatal(err)
		}
		want.State = pb.JobState_JOB_STATE_STOPPED
		want.ExitCode = new(int32(-1))
		want.Signal = new(int32(9))
		if !proto.Equal(resp.GetStatus(), want) {
			t.Errorf("StopJob status = %v, want %v", resp.GetStatus(), want)
		}
	})

	tests := []struct {
		name     string
		args     []string
		exitCode int32
		signal   *int32
	}{
		{"exit code", []string{"-c", "exit 3"}, 3, nil},
		{"crash", []string{"-c", "kill -SEGV $$"}, -1, new(int32(11))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := startJob(t, c, "sh", tt.args...).GetJobId()
			if _, err := streamAll(ctx, c, id); err != nil { // returns once the job has exited
				t.Fatal(err)
			}
			resp, err := c.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: id})
			if err != nil {
				t.Fatal(err)
			}
			want := &pb.JobStatus{
				JobId:    id,
				Owner:    "jimmy",
				Command:  "sh",
				Args:     tt.args,
				State:    pb.JobState_JOB_STATE_EXITED,
				ExitCode: new(tt.exitCode),
				Signal:   tt.signal,
			}
			if !proto.Equal(resp.GetStatus(), want) {
				t.Errorf("status = %v, want %v", resp.GetStatus(), want)
			}
		})
	}
}

// TestToRPCError covers the mappings the API tests can't reach reliably, such
// as a Stop whose caller gives up before SIGKILL has ended the job.
func TestToRPCError(t *testing.T) {
	s := &service{log: slog.New(slog.NewTextHandler(t.Output(), nil))}
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"unknown job", job.ErrNotFound, codes.NotFound},
		{"shutting down", job.ErrClosed, codes.Unavailable},
		{"caller cancelled", context.Canceled, codes.Canceled},
		{"caller timed out", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"wrapped", fmt.Errorf("stop: %w", context.Canceled), codes.Canceled},
		{"unexpected", errors.New("kill job X: operation not permitted"), codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.toRPCError(tt.err)
			if status.Code(got) != tt.want {
				t.Errorf("toRPCError(%v) code = %v, want %v", tt.err, status.Code(got), tt.want)
			}
			// Details of an unexpected error are logged, never sent.
			if tt.want == codes.Internal && strings.Contains(status.Convert(got).Message(), "not permitted") {
				t.Errorf("Internal error leaks details: %v", got)
			}
		})
	}
}

func startJob(t *testing.T, c pb.JobWorkerClient, command string, args ...string) *pb.JobStatus {
	t.Helper()
	resp, err := c.StartJob(t.Context(), &pb.StartJobRequest{Command: command, Args: args})
	if err != nil {
		t.Fatalf("StartJob(%s %v): %v", command, args, err)
	}
	return resp.GetStatus()
}

// streamAll reads a job's output until the stream ends. It returns a nil
// error if the stream ended normally.
func streamAll(ctx context.Context, c pb.JobWorkerClient, id string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := c.StreamJobOutput(ctx, &pb.StreamJobOutputRequest{JobId: id})
	if err != nil {
		return nil, err
	}
	var out []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, msg.GetData()...)
	}
}

// within waits for a value from ch, failing the test after timeout.
func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// waitGoroutinesGone fails the test if some goroutine is still running fn
// after timeout. fn is matched against the stacks of every goroutine.
func waitGoroutinesGone(t *testing.T, fn string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		if !bytes.Contains(buf, []byte(fn)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine still running %s:\n%s", fn, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newCA(t *testing.T) *certgen.CA {
	t.Helper()
	ca, err := certgen.NewCA("test CA")
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

// issue writes a new certificate and key from ca to files and returns their
// paths.
func issue(t *testing.T, ca *certgen.CA, leaf certgen.Leaf) (certFile, keyFile string) {
	t.Helper()
	certPEM, keyPEM, err := ca.Issue(leaf)
	if err != nil {
		t.Fatal(err)
	}
	return writeFile(t, "leaf.crt", certPEM), writeFile(t, "leaf.key", keyPEM)
}

// writeFile writes data to a new file in its own temp directory, so names
// never collide.
func writeFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
