package server

import (
	"crypto/tls"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
)

func TestAuthorization(t *testing.T) {
	env := newTestEnv(t)
	jimmy := env.client(t, "jimmy")   // admin
	jimbob := env.client(t, "jimbob") // user
	ctx := t.Context()

	// Jobs that finish straight away, so an allowed stream reads to the end
	// instead of blocking.
	jimmysJob := startJob(t, jimmy, "echo", "hi").GetJobId()
	jimbobsJob := startJob(t, jimbob, "echo", "hi").GetJobId()

	tests := []struct {
		name   string
		client pb.JobWorkerClient
		id     string
		want   codes.Code
	}{
		{"owner", jimbob, jimbobsJob, codes.OK},
		{"admin, another user's job", jimmy, jimbobsJob, codes.OK},
		{"user, another user's job", jimbob, jimmysJob, codes.NotFound},
		{"user, unknown job", jimbob, "NOSUCHJOB", codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: tt.id})
			if got := status.Code(err); got != tt.want {
				t.Errorf("GetJobStatus code = %v, want %v", got, tt.want)
			}
			_, err = streamAll(ctx, tt.client, tt.id)
			if got := status.Code(err); got != tt.want {
				t.Errorf("StreamJobOutput code = %v, want %v", got, tt.want)
			}
			// Last, since an allowed Stop ends the job. They have all
			// finished anyway, so Stop only returns the final status.
			_, err = tt.client.StopJob(ctx, &pb.StopJobRequest{JobId: tt.id})
			if got := status.Code(err); got != tt.want {
				t.Errorf("StopJob code = %v, want %v", got, tt.want)
			}
		})
	}

	// Another user's job must be indistinguishable from one that doesn't
	// exist, message included.
	_, errOther := jimbob.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: jimmysJob})
	_, errUnknown := jimbob.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: "NOSUCHJOB"})
	if a, b := status.Convert(errOther).Message(), status.Convert(errUnknown).Message(); a != b {
		t.Errorf("another user's job says %q, unknown job says %q; want the same", a, b)
	}
}

// TestAuthentication covers certificates signed by our CA that still don't
// identify a known user. They are refused when the connection is made, so no
// call of any kind reaches a handler, and the refusal is logged.
func TestAuthentication(t *testing.T) {
	tests := []struct {
		name       string
		commonName string
		wantLog    string // "" means the user is let in
	}{
		{"known user", "jimbob", ""},
		{"unknown user", "mallory", `unknown user \"mallory\"`},
		{"no common name", "", "no common name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			checkAuthentication(t, env, env.client(t, tt.commonName), tt.wantLog)
		})
	}
}

// TestAuthenticationFailsClosed checks serverCreds refuse an unknown user even
// if the TLS config lets the handshake through, i.e. without verifyUser.
func TestAuthenticationFailsClosed(t *testing.T) {
	env := newTestEnv(t, func(c *tls.Config) { c.VerifyConnection = nil })
	// Without verifyUser the handshake itself succeeds, so the client only
	// sees the connection close; what matters is that it is still refused.
	checkAuthentication(t, env, env.client(t, "mallory"), `unknown user \"mallory\"`)
}

// checkAuthentication makes a unary call and a streaming call and checks both
// succeed, or, if wantLog is set, that the connection is refused and the
// server logs why.
//
// Only the server knows the reason for certain. In TLS 1.3 the client's
// handshake is done before the server checks its certificate, so the client
// may read the "bad certificate" alert, or first hit the connection the server
// closed ("broken pipe", "connection reset"). Which one it sees is timing.
func checkAuthentication(t *testing.T, env *testEnv, c pb.JobWorkerClient, wantLog string) {
	t.Helper()
	ctx := t.Context()
	resp, errStart := c.StartJob(ctx, &pb.StartJobRequest{Command: "true"})
	_, errStream := streamAll(ctx, c, resp.GetStatus().GetJobId())
	if wantLog == "" {
		if errStart != nil || errStream != nil {
			t.Fatalf("StartJob: %v, StreamJobOutput: %v; want both to succeed", errStart, errStream)
		}
		return
	}
	for name, err := range map[string]error{"StartJob": errStart, "StreamJobOutput": errStream} {
		if got := status.Code(err); got != codes.Unavailable {
			t.Errorf("%s code = %v (%v), want %v", name, got, err, codes.Unavailable)
		}
	}
	env.waitLog(t, "connection rejected", wantLog)
}
