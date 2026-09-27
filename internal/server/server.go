// Package server serves the JobWorker gRPC API over mTLS. It works out the
// user from the client certificate, checks they may act on the job, and hands
// the call to a pkg/job Manager.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os/exec"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
	"github.com/jimmytsang/api_linux_jimmy/pkg/job"
)

const (
	// chunkSize caps each StreamJobOutput message.
	chunkSize = 32 << 10

	// The server pings a client after keepaliveTime without any traffic and
	// drops the connection if the ping isn't answered within keepaliveTimeout.
	// That is how a stream on a job that prints nothing notices its client
	// has vanished: the stream's context is cancelled, which closes its reader.
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second

	// shutdownTimeout bounds how long Shutdown waits for calls in flight.
	shutdownTimeout = 10 * time.Second
)

// jobManager is the part of *job.Manager the server uses. Tests wrap the real
// manager to watch the readers the stream handler opens and closes.
type jobManager interface {
	Start(owner, command string, args []string) (job.Status, error)
	Stop(ctx context.Context, id string) (job.Status, error)
	Status(id string) (job.Status, error)
	Output(id string) (io.ReadCloser, error)
	Close() error
}

// Server is a gRPC server for the JobWorker service.
type Server struct {
	gs   *grpc.Server
	jobs jobManager
	log  *slog.Logger
}

// New returns a server that runs jobs with m. tlsConfig should come from
// ServerTLSConfig. The server takes ownership of m and closes it on Shutdown.
func New(tlsConfig *tls.Config, m *job.Manager, logger *slog.Logger) *Server {
	return newServer(tlsConfig, m, logger)
}

func newServer(tlsConfig *tls.Config, jobs jobManager, logger *slog.Logger) *Server {
	svc := &service{jobs: jobs, log: logger}
	gs := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.UnaryInterceptor(svc.unaryAuth),
		grpc.StreamInterceptor(svc.streamAuth),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	)
	pb.RegisterJobWorkerServer(gs, svc)
	return &Server{gs: gs, jobs: jobs, log: logger}
}

// Serve accepts connections on lis until Shutdown. It returns nil after a
// Shutdown.
func (s *Server) Serve(lis net.Listener) error {
	return s.gs.Serve(lis)
}

// Shutdown kills every job, which ends every output stream, and then stops the
// gRPC server gracefully, letting calls in flight finish.
//
// The order matters: GracefulStop waits for every stream, and a stream on a
// running job only ends when the job does. If calls are still running after
// shutdownTimeout, such as a stream stuck in Send to a client that stopped
// reading, Shutdown closes every connection.
func (s *Server) Shutdown() {
	if err := s.jobs.Close(); err != nil {
		s.log.Error("kill jobs", "err", err)
	}
	stopped := make(chan struct{})
	go func() {
		s.gs.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		s.log.Warn("graceful stop timed out, closing connections")
		s.gs.Stop()
		<-stopped
	}
}

// service implements the JobWorker RPCs. Every call reaches it through the
// auth interceptors, which put the user in the context.
type service struct {
	pb.UnimplementedJobWorkerServer
	jobs jobManager
	log  *slog.Logger
}

// errJobNotFound is returned both for an unknown job ID and for another user's
// job, so the two can't be told apart.
var errJobNotFound = status.Error(codes.NotFound, "job not found")

func (s *service) StartJob(ctx context.Context, req *pb.StartJobRequest) (_ *pb.StartJobResponse, err error) {
	u, err := userFrom(ctx)
	if err != nil {
		return nil, err
	}
	var id string
	defer func() { s.logCall(u, "start", id, err) }()

	if req.GetCommand() == "" {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	st, err := s.jobs.Start(u.name, req.GetCommand(), req.GetArgs())
	// exec.ErrNotFound: a bare name that isn't in PATH. fs.ErrNotExist: a
	// path that doesn't exist.
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist) {
		return nil, status.Errorf(codes.InvalidArgument, "executable %q not found", req.GetCommand())
	}
	if err != nil {
		return nil, s.toRPCError(err)
	}
	id = st.ID
	return &pb.StartJobResponse{Status: toProto(st)}, nil
}

func (s *service) StopJob(ctx context.Context, req *pb.StopJobRequest) (_ *pb.StopJobResponse, err error) {
	u, err := userFrom(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { s.logCall(u, "stop", req.GetJobId(), err) }()

	if _, err := s.authorize(u, req.GetJobId()); err != nil {
		return nil, err
	}
	st, err := s.jobs.Stop(ctx, req.GetJobId())
	if err != nil {
		return nil, s.toRPCError(err)
	}
	return &pb.StopJobResponse{Status: toProto(st)}, nil
}

func (s *service) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (_ *pb.GetJobStatusResponse, err error) {
	u, err := userFrom(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { s.logCall(u, "status", req.GetJobId(), err) }()

	st, err := s.authorize(u, req.GetJobId())
	if err != nil {
		return nil, err
	}
	return &pb.GetJobStatusResponse{Status: toProto(st)}, nil
}

func (s *service) StreamJobOutput(req *pb.StreamJobOutputRequest, stream grpc.ServerStreamingServer[pb.StreamJobOutputResponse]) (err error) {
	ctx := stream.Context()
	u, err := userFrom(ctx)
	if err != nil {
		return err
	}
	defer func() { s.logCall(u, "stream_output", req.GetJobId(), err) }()

	if _, err := s.authorize(u, req.GetJobId()); err != nil {
		return err
	}
	r, err := s.jobs.Output(req.GetJobId())
	if err != nil {
		return s.toRPCError(err)
	}
	defer r.Close() // the output ended, or Send failed
	// Read blocks until the job prints something or exits, so this goroutine
	// can't notice the client leaving. When it leaves (cancel, disconnect or
	// a failed keepalive), gRPC cancels ctx and AfterFunc calls Close on a
	// goroutine of its own, which makes the blocked Read return.
	stop := context.AfterFunc(ctx, func() { r.Close() })
	defer stop()

	buf := make([]byte, chunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			// gRPC may keep a reference to a sent message (stats handlers can
			// read it later), so each message gets its own copy, not buf.
			if err := stream.Send(&pb.StreamJobOutputResponse{Data: slices.Clone(buf[:n])}); err != nil {
				return err
			}
		}
		switch {
		case err == io.EOF:
			return nil
		case errors.Is(err, io.ErrClosedPipe):
			// Only AfterFunc closes r while the loop runs, so the client has
			// gone and ctx says why.
			return status.FromContextError(ctx.Err()).Err()
		case err != nil:
			return s.toRPCError(err)
		}
	}
}

// authorize returns the job's status if u may access it. Another user's job
// fails exactly like a missing one, with errJobNotFound, so users can't probe
// for job IDs. A job's owner never changes, so the check still holds for the
// Stop or Output call that follows it.
func (s *service) authorize(u user, id string) (job.Status, error) {
	if id == "" {
		return job.Status{}, status.Error(codes.InvalidArgument, "job ID is required")
	}
	st, err := s.jobs.Status(id)
	if err != nil {
		return job.Status{}, s.toRPCError(err)
	}
	if !u.canAccess(st.Owner) {
		return job.Status{}, errJobNotFound
	}
	return st, nil
}

// toRPCError maps an error from the job manager to a gRPC status. Details of
// unexpected errors are logged and never sent to the client.
func (s *service) toRPCError(err error) error {
	switch {
	case errors.Is(err, job.ErrNotFound):
		return errJobNotFound
	case errors.Is(err, job.ErrClosed):
		return status.Error(codes.Unavailable, "server is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		s.log.Error("internal error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// logCall logs who did what to which job. Job output is never logged.
func (s *service) logCall(u user, action, id string, err error) {
	s.log.Info("rpc", "user", u.name, "action", action, "job_id", id, "code", status.Code(err))
}

func toProto(st job.Status) *pb.JobStatus {
	p := &pb.JobStatus{
		JobId:   st.ID,
		Owner:   st.Owner,
		Command: st.Command,
		Args:    st.Args,
		State:   stateToProto(st.State),
	}
	if st.ExitCode != nil {
		p.ExitCode = new(int32(*st.ExitCode))
	}
	if st.Signal != 0 {
		p.Signal = new(int32(st.Signal))
	}
	return p
}

func stateToProto(s job.State) pb.JobState {
	switch s {
	case job.Running:
		return pb.JobState_JOB_STATE_RUNNING
	case job.Exited:
		return pb.JobState_JOB_STATE_EXITED
	case job.Stopped:
		return pb.JobState_JOB_STATE_STOPPED
	default:
		return pb.JobState_JOB_STATE_UNSPECIFIED
	}
}
