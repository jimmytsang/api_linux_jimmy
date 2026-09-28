package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/jimmytsang/api_linux_jimmy/gen/jobworker/v1"
	"github.com/jimmytsang/api_linux_jimmy/internal/certgen"
)

// TestServerTLSConfig checks which clients the server's handshake accepts.
func TestServerTLSConfig(t *testing.T) {
	ca, untrusted := newCA(t), newCA(t)
	caFile := writeFile(t, "ca.crt", ca.CertPEM)
	certFile, keyFile := serverCert(t, ca, "localhost")
	serverConfig, err := ServerTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	client := certgen.Leaf{CommonName: "jimmy", Usage: x509.ExtKeyUsageClientAuth}
	expired := client
	expired.NotBefore = time.Now().Add(-2 * time.Hour)
	expired.NotAfter = time.Now().Add(-time.Hour)
	serverUsage := client
	serverUsage.Usage = x509.ExtKeyUsageServerAuth
	unknownUser := client
	unknownUser.CommonName = "mallory"
	noCommonName := client
	noCommonName.CommonName = ""

	tests := []struct {
		name       string
		config     *tls.Config
		wantReject func(error) bool // nil: the handshake must succeed
	}{
		{"valid", clientConfig(t, ca, client, caFile), nil},
		{"no certificate", &tls.Config{RootCAs: pool(t, ca), ServerName: "localhost"},
			errContains("didn't provide a certificate")},
		{"untrusted CA", clientConfig(t, untrusted, client, caFile), isUnknownAuthority},
		{"expired", clientConfig(t, ca, expired, caFile), isInvalid(x509.Expired)},
		{"server certificate as client", clientConfig(t, ca, serverUsage, caFile), isInvalid(x509.IncompatibleUsage)},
		{"TLS 1.2 only", maxTLS12(clientConfig(t, ca, client, caFile)), errContains("unsupported versions")},
		{"unknown user", clientConfig(t, ca, unknownUser, caFile), isErr(errUnknownUser)},
		{"no common name", clientConfig(t, ca, noCommonName, caFile), isErr(errNoCommonName)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, serverErr := handshake(t, serverConfig, tt.config)
			checkHandshake(t, "server", serverErr, tt.wantReject)
		})
	}
}

// TestClientTLSConfig checks which servers the client's handshake accepts.
func TestClientTLSConfig(t *testing.T) {
	ca, untrusted := newCA(t), newCA(t)
	caFile := writeFile(t, "ca.crt", ca.CertPEM)
	certFile, keyFile := issue(t, ca, certgen.Leaf{CommonName: "jimmy", Usage: x509.ExtKeyUsageClientAuth})
	clientConfig, err := ClientTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	clientConfig.ServerName = "localhost" // gRPC sets this from the address

	tests := []struct {
		name       string
		server     certgen.Leaf
		ca         *certgen.CA
		wantReject func(error) bool
	}{
		{"valid", certgen.Leaf{Hosts: []string{"localhost"}, Usage: x509.ExtKeyUsageServerAuth}, ca, nil},
		{"untrusted CA", certgen.Leaf{Hosts: []string{"localhost"}, Usage: x509.ExtKeyUsageServerAuth}, untrusted, isUnknownAuthority},
		{"wrong hostname", certgen.Leaf{Hosts: []string{"example.com"}, Usage: x509.ExtKeyUsageServerAuth}, ca, isHostnameError},
		{"client certificate as server", certgen.Leaf{Hosts: []string{"localhost"}, Usage: x509.ExtKeyUsageClientAuth}, ca, isInvalid(x509.IncompatibleUsage)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			certPEM, keyPEM, err := tt.ca.Issue(tt.server)
			if err != nil {
				t.Fatal(err)
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			serverConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
			clientErr, _ := handshake(t, serverConfig, clientConfig)
			checkHandshake(t, "client", clientErr, tt.wantReject)
		})
	}
}

// TestServerRequiresClientCert checks the gRPC server really uses the mTLS
// config: a client without a certificate never reaches a handler.
func TestServerRequiresClientCert(t *testing.T) {
	env := newTestEnv(t)
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool(t, env.ca)}
	conn, err := grpc.NewClient(env.addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = pb.NewJobWorkerClient(conn).StartJob(t.Context(), &pb.StartJobRequest{Command: "true"})
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("StartJob without a client certificate: code = %v (%v), want %v", got, err, codes.Unavailable)
	}
}

func TestLoadCAWithoutCertificates(t *testing.T) {
	ca := newCA(t)
	certFile, keyFile := issue(t, ca, certgen.Leaf{Hosts: []string{"localhost"}, Usage: x509.ExtKeyUsageServerAuth})
	notACA := writeFile(t, "ca.crt", []byte("not a certificate"))
	if _, err := ServerTLSConfig(certFile, keyFile, notACA); err == nil {
		t.Error("ServerTLSConfig with an empty CA file succeeded, want an error")
	}
	if _, err := ClientTLSConfig(certFile, keyFile, notACA); err == nil {
		t.Error("ClientTLSConfig with an empty CA file succeeded, want an error")
	}
}

// handshake runs a TLS handshake between the two configs over loopback TCP and
// returns each side's error. In TLS 1.3 the client finishes its handshake
// before the server has checked the client's certificate, so a server-side
// rejection only shows up in serverErr.
func handshake(t *testing.T, serverConfig, clientConfig *tls.Config) (clientErr, serverErr error) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(timeout))
		serverDone <- tls.Server(conn, serverConfig).Handshake()
	}()

	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	clientErr = tls.Client(conn, clientConfig).Handshake()
	return clientErr, within(t, serverDone, "the server handshake")
}

func checkHandshake(t *testing.T, side string, err error, wantReject func(error) bool) {
	t.Helper()
	switch {
	case wantReject == nil && err != nil:
		t.Errorf("%s rejected the handshake: %v", side, err)
	case wantReject != nil && err == nil:
		t.Errorf("%s accepted the handshake, want it rejected", side)
	case wantReject != nil && !wantReject(err):
		t.Errorf("%s rejected the handshake for the wrong reason: %v", side, err)
	}
}

func serverCert(t *testing.T, ca *certgen.CA, hosts ...string) (certFile, keyFile string) {
	t.Helper()
	return issue(t, ca, certgen.Leaf{CommonName: "worker-server", Hosts: hosts, Usage: x509.ExtKeyUsageServerAuth})
}

func clientConfig(t *testing.T, ca *certgen.CA, leaf certgen.Leaf, caFile string) *tls.Config {
	t.Helper()
	certFile, keyFile := issue(t, ca, leaf)
	c, err := ClientTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	c.ServerName = "localhost"
	return c
}

func maxTLS12(c *tls.Config) *tls.Config {
	c.MinVersion = tls.VersionTLS12
	c.MaxVersion = tls.VersionTLS12
	return c
}

func pool(t *testing.T, ca *certgen.CA) *x509.CertPool {
	t.Helper()
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("bad CA PEM")
	}
	return p
}

func isUnknownAuthority(err error) bool {
	var e x509.UnknownAuthorityError
	return errors.As(err, &e)
}

func isHostnameError(err error) bool {
	var e x509.HostnameError
	return errors.As(err, &e)
}

func isInvalid(reason x509.InvalidReason) func(error) bool {
	return func(err error) bool {
		var e x509.CertificateInvalidError
		return errors.As(err, &e) && e.Reason == reason
	}
}

func isErr(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}

// errContains is for the handshake failures crypto/tls has no error type for.
func errContains(s string) func(error) bool {
	return func(err error) bool { return strings.Contains(err.Error(), s) }
}
