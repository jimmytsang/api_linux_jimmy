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

type user struct {
	name string
	role role
}

// canAccess reports whether u may see and act on a job owned by owner.
func (u user) canAccess(owner string) bool {
	return u.role == roleAdmin || u.name == owner
}

var (
	errNoCommonName = errors.New("client certificate has no common name")
	errUnknownUser  = errors.New("unknown user")
)

// lookupUser works out the user from the client certificate the TLS handshake
// verified.
func lookupUser(cs tls.ConnectionState) (user, error) {
	// VerifiedChains is only set if the handshake verified the certificate
	// against our CA; [0][0] is the client's own certificate in that chain.
	if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return user{}, errors.New("no verified client certificate")
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

// verifyUser is the server's tls.Config.VerifyConnection. It runs during the
// handshake, after the certificate chain is verified, so a certificate from
// our CA that doesn't name a known user is rejected like any other bad
// certificate: the server sends a "bad certificate" alert and closes the
// connection, which never carries a call. In TLS 1.3 the client can hit the
// closed connection before it reads the alert, so it may see a broken pipe
// instead; the server's log always has the reason.
func verifyUser(cs tls.ConnectionState) error {
	_, err := lookupUser(cs)
	return err
}

// serverCreds are gRPC's TLS credentials plus authentication: each connection
// learns its user once, when it is made, since a certificate can't change
// during a connection. Handlers read the user from the connection with
// userFrom; authorization stays per call, because it depends on the job.
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
	// verifyUser has already rejected unknown users during the handshake.
	// Looking up again here is what attaches the user, and it fails closed
	// if the TLS config didn't come from ServerTLSConfig.
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
