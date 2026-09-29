package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type role int

const (
	roleUser  role = iota + 1 // only jobs they started
	roleAdmin                 // all jobs
)

// roles maps a client certificate's common name to a role. Roles live here
// rather than in the certificate, so they can change without reissuing it.
//
// TODO: out of scope - load roles from a config file. A connection's user is
// fixed when it is made, so a role change would apply from the next connection.
var roles = map[string]role{
	"jimmy":  roleAdmin,
	"jimbob": roleUser,
}

// user is who a client is, as the server knows them.
type user struct {
	name string
	role role
}

var (
	errNoVerifiedCert = errors.New("no verified client certificate")
	errNoCommonName   = errors.New("client certificate has no common name")
	errUnknownUser    = errors.New("unknown user")
)

// lookupUser works out the user from the client certificate the TLS handshake
// verified.
func lookupUser(cs tls.ConnectionState) (user, error) {
	// VerifiedChains is only set if the handshake verified the certificate
	// against our CA; [0][0] is the client's own certificate in that chain.
	if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return user{}, errNoVerifiedCert
	}
	name := cs.VerifiedChains[0][0].Subject.CommonName
	if name == "" {
		return user{}, errNoCommonName
	}
	r, ok := roles[name]
	if !ok {
		return user{}, fmt.Errorf("%w %q", errUnknownUser, name)
	}
	return user{name: name, role: r}, nil
}

// serverCreds are gRPC's TLS credentials plus authentication. Right after the
// TLS handshake they look up the user once for the connection, since a
// certificate can't change during a connection: a known user is attached to
// the connection for handlers to read with userFrom, and anyone else has the
// connection closed before it carries a call. Authorization stays per call,
// because it depends on the job.
//
// serverCreds also log every rejected connection. Those fail during the
// handshake, before any handler runs, and gRPC only reports them at Info
// level, which its default logger discards.
type serverCreds struct {
	credentials.TransportCredentials
	log *slog.Logger
}

func (c serverCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.authenticate(rawConn)
	if err != nil {
		c.log.Warn("connection rejected", "remote", rawConn.RemoteAddr().String(), "err", err)
	}
	return conn, info, err
}

func (c serverCreds) authenticate(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(rawConn)
	if err != nil {
		return nil, nil, err
	}
	tlsInfo, ok := info.(credentials.TLSInfo)
	if !ok {
		conn.Close()
		return nil, nil, fmt.Errorf("unexpected auth info %T", info)
	}
	u, err := lookupUser(tlsInfo.State)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, authInfo{TLSInfo: tlsInfo, user: u}, nil
}

// Clone keeps the authentication; the embedded Clone would return bare TLS
// credentials.
func (c serverCreds) Clone() credentials.TransportCredentials {
	return serverCreds{TransportCredentials: c.TransportCredentials.Clone(), log: c.log}
}

// authInfo is what serverCreds attach to a connection: the TLS details gRPC
// expects, plus the user.
type authInfo struct {
	credentials.TLSInfo
	user user
}

// userFrom returns the user of the connection a call arrived on. It fails
// closed: a connection that didn't come through serverCreds has no user.
func userFrom(ctx context.Context) (user, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return user{}, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	info, ok := p.AuthInfo.(authInfo)
	if !ok {
		return user{}, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return info.user, nil
}
