package server

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
	"github.com/jimmytsang/api_linux_jimmy/internal/certgen"
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
// identify a known user. serverCreds close the connection right after the TLS
// handshake, so no call of either kind reaches a handler. The client can't
// tell why, it just sees the connection close; TestLookupUser covers the
// reasons.
func TestAuthentication(t *testing.T) {
	tests := []struct {
		name       string
		commonName string
		refused    bool
	}{
		{"known user", "jimbob", false},
		{"unknown user", "mallory", true},
		{"no common name", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			c := env.client(t, tt.commonName)
			ctx := t.Context()
			resp, errStart := c.StartJob(ctx, &pb.StartJobRequest{Command: "true"})
			_, errStream := streamAll(ctx, c, resp.GetStatus().GetJobId())
			want := codes.OK
			if tt.refused {
				want = codes.Unavailable
			}
			for name, err := range map[string]error{"StartJob": errStart, "StreamJobOutput": errStream} {
				if got := status.Code(err); got != want {
					t.Errorf("%s code = %v (%v), want %v", name, got, err, want)
				}
			}
		})
	}
}

func TestLookupUser(t *testing.T) {
	ca := newCA(t)
	verified := func(commonName string) tls.ConnectionState {
		certPEM, _, err := ca.Issue(certgen.Leaf{CommonName: commonName, Usage: x509.ExtKeyUsageClientAuth})
		if err != nil {
			t.Fatal(err)
		}
		block, _ := pem.Decode(certPEM)
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	}
	tests := []struct {
		name    string
		state   tls.ConnectionState
		want    user
		wantErr error
	}{
		{"admin", verified("jimmy"), user{name: "jimmy", role: roleAdmin}, nil},
		{"user", verified("jimbob"), user{name: "jimbob", role: roleUser}, nil},
		{"unknown user", verified("mallory"), user{}, errUnknownUser},
		{"no common name", verified(""), user{}, errNoCommonName},
		{"not verified", tls.ConnectionState{}, user{}, errNoVerifiedCert},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lookupUser(tt.state)
			if !errors.Is(err, tt.wantErr) || got != tt.want {
				t.Errorf("lookupUser = %+v, %v; want %+v, %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
