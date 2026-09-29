// Command worker is the CLI for worker-server. Flags go before the command:
//
//	worker [flags] start -- <command> [args...]
//	worker [flags] status <job-id>
//	worker [flags] output <job-id>
//	worker [flags] stop <job-id>
//
// The certificate flags default to the dev certificates in certs/, so run it
// from the repo root or point the flags elsewhere. The user is the common name
// of the client certificate: switch user with
// --cert certs/jimbob.crt --key certs/jimbob.key.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
	"github.com/jimmytsang/api_linux_jimmy/internal/server"
)

const (
	exitOK    = 0
	exitError = 1 // the call failed
	exitUsage = 2 // bad flags or arguments, as with the flag package
)

func main() {
	// Ctrl-C cancels ctx. For output that means stop watching: the job keeps
	// running on the server.
	ctx, stop := interruptContext()
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// interruptContext returns a context that the first Ctrl-C cancels. Only the
// first is caught: after it, Ctrl-C gets its default behaviour back, so a
// second one kills the CLI even if it is stuck, e.g. writing to a paused
// terminal.
func interruptContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	context.AfterFunc(ctx, stop)
	return ctx, stop
}

// run is the whole CLI minus the process: it parses args, makes the call and
// returns the exit code. Errors go to stderr as "error: <message>".
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	inv, err := parseArgs(args)
	if errors.Is(err, flag.ErrHelp) {
		printUsage(stdout)
		return exitOK
	}
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n\n", err)
		printUsage(stderr)
		return exitUsage
	}
	if err := execute(ctx, inv, stdout); err != nil {
		// Message drops the "rpc error: code = ... desc =" wrapping. For an
		// error that isn't a gRPC status, it is err.Error().
		fmt.Fprintf(stderr, "error: %s\n", status.Convert(err).Message())
		return exitError
	}
	return exitOK
}

// invocation is a parsed command line.
type invocation struct {
	server, cert, key, ca string

	command string   // start, status, output or stop
	jobID   string   // status, output and stop
	job     []string // start: the job's command and its args
}

func newFlagSet(inv *invocation) *flag.FlagSet {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // run prints errors and usage itself
	// The default is the exact address worker-server binds, not "localhost":
	// that can resolve to ::1 first, and when the server then rejects our
	// certificate, gRPC reports the ::1 "connection refused" instead.
	fs.StringVar(&inv.server, "server", "127.0.0.1:50051", "server address")
	fs.StringVar(&inv.cert, "cert", "certs/jimmy.crt", "client certificate; its common name is the user")
	fs.StringVar(&inv.key, "key", "certs/jimmy.key", "client certificate's private key")
	fs.StringVar(&inv.ca, "ca", "certs/ca.crt", "CA that must have signed the server certificate")
	return fs
}

func parseArgs(args []string) (invocation, error) {
	var inv invocation
	fs := newFlagSet(&inv)
	if err := fs.Parse(args); err != nil {
		return inv, err
	}
	// Parse stops at the first argument that isn't a flag: the command.
	rest := fs.Args()
	if len(rest) == 0 {
		return inv, errors.New("missing command")
	}
	inv.command, rest = rest[0], rest[1:]
	switch inv.command {
	case "start":
		// Everything after -- belongs to the job, including arguments that
		// look like flags, as in: worker start -- ls -l
		if len(rest) < 2 || rest[0] != "--" {
			return inv, errors.New("usage: worker [flags] start -- <command> [args...]")
		}
		inv.job = rest[1:]
	case "status", "output", "stop":
		// Job IDs never start with "-", so one that does is a flag placed
		// after the command, e.g. `worker status -h`, not an ID to send.
		if len(rest) != 1 || strings.HasPrefix(rest[0], "-") {
			return inv, fmt.Errorf("usage: worker [flags] %s <job-id>", inv.command)
		}
		inv.jobID = rest[0]
	default:
		return inv, fmt.Errorf("unknown command %q", inv.command)
	}
	return inv, nil
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `usage: worker [flags] <command>

commands:
  start -- <command> [args...]  start a job and print its ID
  status <job-id>               show a job's status
  output <job-id>               stream a job's output from the start until it ends
                                (Ctrl-C stops watching, not the job)
  stop <job-id>                 stop a job and show its final status

flags:
`)
	fs := newFlagSet(&invocation{})
	fs.SetOutput(w)
	fs.PrintDefaults()
}

func execute(ctx context.Context, inv invocation, stdout io.Writer) error {
	tlsConfig, err := server.ClientTLSConfig(inv.cert, inv.key, inv.ca)
	if err != nil {
		return err
	}
	// No client keepalive on purpose: the server already pings idle
	// connections, and gRPC's default enforcement on the server drops clients
	// that ping more often than every 5 minutes (GOAWAY too_many_pings).
	conn, err := grpc.NewClient(inv.server, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := pb.NewJobWorkerClient(conn)

	switch inv.command {
	case "start":
		resp, err := c.StartJob(ctx, &pb.StartJobRequest{Command: inv.job[0], Args: inv.job[1:]})
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, resp.GetStatus().GetJobId())
	case "status":
		resp, err := c.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: inv.jobID})
		if err != nil {
			return err
		}
		writeStatus(stdout, resp.GetStatus())
	case "stop":
		resp, err := c.StopJob(ctx, &pb.StopJobRequest{JobId: inv.jobID})
		if err != nil && ctx.Err() != nil {
			// Ctrl-C while waiting for the job to exit. If the request
			// reached the server, SIGKILL has already been sent, so don't
			// claim the stop failed.
			return fmt.Errorf("stopped waiting; the job may still be stopping, check: worker status %s", inv.jobID)
		}
		if err != nil {
			return err
		}
		writeResult(stdout, resp.GetStatus())
	case "output":
		return streamOutput(ctx, c, inv.jobID, stdout)
	}
	return nil
}

// streamOutput copies a job's output to stdout, byte for byte, from the first
// byte until the job ends.
func streamOutput(ctx context.Context, c pb.JobWorkerClient, id string, stdout io.Writer) error {
	stream, err := c.StreamJobOutput(ctx, &pb.StreamJobOutputRequest{JobId: id})
	if err != nil {
		return streamEnd(ctx, err)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return streamEnd(ctx, err)
		}
		if _, err := stdout.Write(msg.GetData()); err != nil {
			return err
		}
	}
}

// streamEnd turns the error that ended a stream into the command's result.
func streamEnd(ctx context.Context, err error) error {
	switch {
	case err == io.EOF: // the job ended
		return nil
	case ctx.Err() != nil:
		// Ctrl-C: the user stopped watching, which isn't an error. The server
		// sees the stream cancelled and leaves the job running.
		return nil
	default:
		return err
	}
}

// field writes one "Label:  value" line. Values start in the same column on
// every line, one space past the longest label, "Exit code:", so they don't
// shift once a job finishes.
func field(w io.Writer, label, value string) {
	fmt.Fprintf(w, "%-11s%s\n", label+":", value)
}

func writeStatus(w io.Writer, st *pb.JobStatus) {
	field(w, "ID", st.GetJobId())
	field(w, "Owner", st.GetOwner())
	field(w, "Command", commandLine(st.GetCommand(), st.GetArgs()))
	writeResult(w, st)
}

// writeResult writes the State line and, once the job has finished, the Exit
// code line.
func writeResult(w io.Writer, st *pb.JobStatus) {
	field(w, "State", strings.TrimPrefix(st.GetState().String(), "JOB_STATE_"))
	if st.ExitCode == nil {
		return // still running: there is no exit code yet, and 0 would be wrong
	}
	code := strconv.Itoa(int(st.GetExitCode()))
	if st.Signal != nil {
		// The API sends the signal's number, and the standard library has no
		// table of names like SIGKILL, so show the number with its description.
		code += fmt.Sprintf(" (signal %d: %s)", st.GetSignal(), syscall.Signal(st.GetSignal()))
	}
	field(w, "Exit code", code)
}

// commandLine joins a command and its args with spaces, so the line shows
// exactly what ran. A part that couldn't be split back out, or that holds
// characters a terminal would act on, is quoted Go-style.
func commandLine(command string, args []string) string {
	parts := make([]string, 0, 1+len(args))
	for _, s := range append([]string{command}, args...) {
		parts = append(parts, quoteIfNeeded(s))
	}
	return strings.Join(parts, " ")
}

// quoteIfNeeded quotes s if it is empty or contains whitespace, a quote, a
// backslash, or anything unprintable. The last matters because args are the
// job owner's input shown on an admin's terminal: quoting escapes control
// characters, so they can't clear the screen or rewrite the output.
func quoteIfNeeded(s string) string {
	if s != "" && !strings.ContainsFunc(s, needsQuote) {
		return s
	}
	return strconv.Quote(s)
}

func needsQuote(r rune) bool {
	return unicode.IsSpace(r) || r == '"' || r == '\'' || r == '\\' || !unicode.IsPrint(r)
}
