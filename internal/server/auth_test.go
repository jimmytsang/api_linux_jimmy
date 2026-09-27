package server

import (
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
// identify a known user, through both interceptors: unary and stream.
func TestAuthentication(t *testing.T) {
	env := newTestEnv(t)
	ctx := t.Context()
	tests := []struct {
		name       string
		commonName string
		want       codes.Code
	}{
		{"known user", "jimbob", codes.OK},
		{"unknown user", "mallory", codes.PermissionDenied},
		{"no common name", "", codes.Unauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := env.client(t, tt.commonName)
			resp, err := c.StartJob(ctx, &pb.StartJobRequest{Command: "true"})
			if got := status.Code(err); got != tt.want {
				t.Errorf("StartJob code = %v, want %v", got, tt.want)
			}
			// The interceptor rejects before the handler looks at the ID, so a
			// rejected user's empty ID doesn't matter; a known user streams
			// the job just started.
			_, err = streamAll(ctx, c, resp.GetStatus().GetJobId())
			if got := status.Code(err); got != tt.want {
				t.Errorf("StreamJobOutput code = %v, want %v", got, tt.want)
			}
		})
	}
}
