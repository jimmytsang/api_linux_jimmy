package server

import (
	"context"

	"google.golang.org/grpc"
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
// TODO: out of scope - load roles from a config file.
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

type userKey struct{}

// authenticate works out the user from the verified client certificate.
func authenticate(ctx context.Context) (user, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return user{}, status.Error(codes.Unauthenticated, "no peer")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	// VerifiedChains is only set if the handshake verified the certificate
	// against our CA; [0][0] is the client's own certificate in that chain.
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return user{}, status.Error(codes.Unauthenticated, "no verified client certificate")
	}
	name := info.State.VerifiedChains[0][0].Subject.CommonName
	if name == "" {
		return user{}, status.Error(codes.Unauthenticated, "client certificate has no common name")
	}
	r, ok := roles[name]
	if !ok {
		return user{}, status.Errorf(codes.PermissionDenied, "unknown user %q", name)
	}
	return user{name: name, role: r}, nil
}

// userFrom returns the user the interceptor stored in ctx. It fails closed:
// a handler reached without going through the interceptor handles nothing.
func userFrom(ctx context.Context) (user, error) {
	u, ok := ctx.Value(userKey{}).(user)
	if !ok {
		return user{}, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return u, nil
}

func (s *service) unaryAuth(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	u, err := authenticate(ctx)
	if err != nil {
		s.log.Warn("rejected", "method", info.FullMethod, "err", err)
		return nil, err
	}
	return handler(context.WithValue(ctx, userKey{}, u), req)
}

func (s *service) streamAuth(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	u, err := authenticate(ss.Context())
	if err != nil {
		s.log.Warn("rejected", "method", info.FullMethod, "err", err)
		return err
	}
	return handler(srv, &authedStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), userKey{}, u)})
}

// authedStream swaps in a context that carries the user.
type authedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authedStream) Context() context.Context { return s.ctx }
