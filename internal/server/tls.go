package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc/credentials"
)

// ServerTLSConfig returns the server's mTLS config: TLS 1.3 only, and every
// client must present a certificate signed by the CA in caFile and marked for
// client use. A client that doesn't fails the handshake, before any RPC runs.
func ServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, pool, err := loadKeyPairAndCA(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// Verification checks the chain up to ClientCAs and that the
		// certificate is marked for client auth (ExtKeyUsageClientAuth).
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
	}, nil
}

// ClientTLSConfig returns the client's mTLS config: TLS 1.3 only, and the
// server must present a certificate signed by the CA in caFile (not the
// system's trusted CAs), marked for server use and valid for the host dialled.
// gRPC fills in ServerName from the address being dialled.
func ClientTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, pool, err := loadKeyPairAndCA(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

// loggingCreds logs every failed server handshake. A client with a bad
// certificate is dropped during the handshake, before any interceptor runs,
// and gRPC only reports that at Info level, which its default logger discards.
// Without this, someone probing with bad certificates leaves no trace.
type loggingCreds struct {
	credentials.TransportCredentials
	log *slog.Logger
}

func (c loggingCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	out, info, err := c.TransportCredentials.ServerHandshake(conn)
	if err != nil {
		c.log.Warn("tls handshake failed", "remote", conn.RemoteAddr().String(), "err", err)
	}
	return out, info, err
}

// Clone keeps the logging; the embedded Clone would return the bare creds.
func (c loggingCreds) Clone() credentials.TransportCredentials {
	return loggingCreds{TransportCredentials: c.TransportCredentials.Clone(), log: c.log}
}

func loadKeyPairAndCA(certFile, keyFile, caFile string) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load key pair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("read CA: no certificates in %s", caFile)
	}
	return cert, pool, nil
}
