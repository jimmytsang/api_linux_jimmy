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
	"bytes"
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
	"unicode/utf8"

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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	var stdout io.Writer = os.Stdout
	var tw *terminalWriter
	if isTerminal(os.Stdout) {
		// A job's output is the job owner's bytes shown on someone else's
		// screen, possibly an admin's. Redirected to a file or pipe, it stays
		// byte for byte.
		tw = &terminalWriter{w: os.Stdout}
		stdout = tw
	}
	code := run(ctx, os.Args[1:], stdout, os.Stderr)
	if tw != nil {
		tw.Flush()
	}
	stop()
	os.Exit(code)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
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
	// The default user is the least privileged one. Acting as the admin takes
	// asking for it: --cert certs/jimmy.crt --key certs/jimmy.key.
	fs.StringVar(&inv.cert, "cert", "certs/jimbob.crt", "client certificate; its common name is the user")
	fs.StringVar(&inv.key, "key", "certs/jimbob.key", "client certificate's private key")
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
	// Ping the server when the connection goes quiet. On a job that prints
	// nothing, `output` would otherwise wait forever if the server vanished
	// without closing the connection: only an unanswered ping notices.
	conn, err := grpc.NewClient(inv.server,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithKeepaliveParams(server.ClientKeepalive),
	)
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
// byte until the job ends. It then fails unless the job exited 0, so scripts
// like `worker output $id && deploy` stop when the job failed.
func streamOutput(ctx context.Context, c pb.JobWorkerClient, id string, stdout io.Writer) error {
	stream, err := c.StreamJobOutput(ctx, &pb.StreamJobOutputRequest{JobId: id})
	if err != nil {
		return streamEnd(ctx, err)
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return streamEnd(ctx, err)
		}
		if _, err := stdout.Write(msg.GetData()); err != nil {
			return err
		}
	}
	// The stream only ends once the final status is recorded, so this is it.
	resp, err := c.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: id})
	if err != nil {
		if ctx.Err() != nil {
			return nil // Ctrl-C right as the output ended
		}
		// E.g. the server shut down, killing the job and ending the stream,
		// and was gone before we could ask how the job ended.
		return fmt.Errorf("output ended, but the job's final status is unavailable: %s", status.Convert(err).Message())
	}
	st := resp.GetStatus()
	if st.GetState() == pb.JobState_JOB_STATE_EXITED && st.GetExitCode() == 0 {
		return nil
	}
	return fmt.Errorf("job %s, exit code %s", strings.ToLower(stateName(st)), exitCode(st))
}

// streamEnd turns the error that ended a stream early into the command's
// result.
func streamEnd(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		// Ctrl-C: the user stopped watching, which isn't an error. The server
		// sees the stream cancelled and leaves the job running.
		return nil
	}
	return err
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
	field(w, "State", stateName(st))
	if st.ExitCode == nil {
		return // still running: there is no exit code yet, and 0 would be wrong
	}
	field(w, "Exit code", exitCode(st))
}

func stateName(st *pb.JobStatus) string {
	return strings.TrimPrefix(st.GetState().String(), "JOB_STATE_")
}

// exitCode formats a finished job's exit code, e.g. "0" or
// "-1 (signal 9: killed)".
func exitCode(st *pb.JobStatus) string {
	code := strconv.Itoa(int(st.GetExitCode()))
	if st.Signal != nil {
		// The API sends the signal's number, and the standard library has no
		// table of names like SIGKILL, so show the number with its description.
		code += fmt.Sprintf(" (signal %d: %s)", st.GetSignal(), syscall.Signal(st.GetSignal()))
	}
	return code
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

// terminalWriter shows control characters as text, e.g. \x1b or \r, instead
// of letting a terminal act on them, so the bytes written can't clear the
// screen, move the cursor, set the title or write the clipboard. Newlines and
// tabs pass through, and so does printable text in any language. Bytes that
// aren't valid UTF-8 are shown as \xNN, since some terminals read a lone byte
// such as 0x9b as a control.
type terminalWriter struct {
	w io.Writer
	// rest is the start of a UTF-8 character split across writes: output
	// arrives in chunks that can cut one in half.
	rest []byte
}

func (t *terminalWriter) Write(p []byte) (int, error) {
	buf := append(t.rest, p...)
	var out []byte
	for len(buf) > 0 && utf8.FullRune(buf) {
		r, size := utf8.DecodeRune(buf)
		switch {
		case r == utf8.RuneError && size == 1: // not valid UTF-8
			out = fmt.Appendf(out, `\x%02x`, buf[0])
		case r == '\n' || r == '\t' || !unicode.IsControl(r):
			out = append(out, buf[:size]...)
		default: // C0 controls such as ESC, DEL, and C1 controls
			q := strconv.QuoteRune(r)
			out = append(out, q[1:len(q)-1]...) // drop the quotes: \x1b, \u009b
		}
		buf = buf[size:]
	}
	t.rest = bytes.Clone(buf)
	if _, err := t.w.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Flush writes a character left incomplete when the output ended, escaped.
func (t *terminalWriter) Flush() error {
	var out []byte
	for _, b := range t.rest {
		out = fmt.Appendf(out, `\x%02x`, b)
	}
	t.rest = nil
	_, err := t.w.Write(out)
	return err
}
