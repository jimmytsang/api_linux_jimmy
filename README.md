# api_linux_jimmy

Prototype job worker service that provides an API to run arbitrary Linux
processes. These processes can be any executable program that is available on
the machine running the service.

The project has three components:

1. `pkg/job` — a reusable library to start, stop, query and stream the output of jobs.
2. `worker-server` — a gRPC API server, secured with mTLS, that wraps the library.
3. `worker` — a CLI that talks to the API server.

See [docs/design.md](docs/design.md) for the design: what's in scope, how
output streaming works, the security model, and the trade-offs.

## Prerequisites

- 64-bit Linux for the server. The CLI runs on the same host: the server
  listens on loopback only, and the dev certificates only cover `localhost`
  and `127.0.0.1`.
- Go 1.27 (pinned in `go.mod`).
- `make`.
- Only to regenerate the protobuf code: `protoc` v36.2.

## Build and test

```bash
make build   # bin/worker-server and bin/worker
make test    # go test -race ./...
```

## Run

Run everything from the repo root: the server and the CLI default to the dev
certificates in `certs/`.

In one terminal, start the server on `127.0.0.1:50051`:

```bash
./bin/worker-server
```

It logs every call with the user, action and job ID, and every rejected TLS
handshake. Ctrl-C kills all jobs and shuts it down.

In another terminal, use the CLI. Flags go before the command:

```
worker [flags] start -- <command> [args...]   start a job and print its ID
worker [flags] status <job-id>                show a job's status
worker [flags] output <job-id>                stream a job's output until it ends
worker [flags] stop <job-id>                  stop a job and show its final status

--server  server address (default 127.0.0.1:50051)
--cert    client certificate; its common name is the user (default certs/jimmy.crt)
--key     client certificate's private key (default certs/jimmy.key)
--ca      CA that must have signed the server certificate (default certs/ca.crt)
```

Errors go to stderr as `error: <message>`, with exit code 1 (2 for bad usage).

### Users

The user is the common name of the client certificate. Roles are hardcoded on
the server:

| User | Certificate | Role |
| --- | --- | --- |
| `jimmy` | `certs/jimmy.crt`, `certs/jimmy.key` (the CLI's default) | admin: all jobs |
| `jimbob` | `certs/jimbob.crt`, `certs/jimbob.key` | user: only jobs they started |

## Demo

Start a job as jimmy. Everything after `--` belongs to the job, and `start`
prints only the job ID:

```console
$ ./bin/worker start -- ping -c 3 localhost
JAPLPGN2E2QMTEYX7RB47CUGIV

$ ./bin/worker status JAPLPGN2E2QMTEYX7RB47CUGIV
ID:        JAPLPGN2E2QMTEYX7RB47CUGIV
Owner:     jimmy
Command:   ping -c 3 localhost
State:     RUNNING
```

jimbob can't see jimmy's job. It looks exactly like a job that doesn't exist,
so job IDs can't be probed:

```console
$ ./bin/worker --cert certs/jimbob.crt --key certs/jimbob.key status JAPLPGN2E2QMTEYX7RB47CUGIV
error: job not found
```

Stream the output. It starts from the first byte, even after the job has
finished, and ends when the job does. Ctrl-C stops watching, not the job.
Several clients can watch the same job at once:

```console
$ ./bin/worker output JAPLPGN2E2QMTEYX7RB47CUGIV
PING localhost (127.0.0.1) 56(84) bytes of data.
64 bytes from localhost (127.0.0.1): icmp_seq=1 ttl=64 time=0.041 ms
...

$ ./bin/worker status JAPLPGN2E2QMTEYX7RB47CUGIV
ID:        JAPLPGN2E2QMTEYX7RB47CUGIV
Owner:     jimmy
Command:   ping -c 3 localhost
State:     EXITED
Exit code: 0
```

jimbob starts a job of their own. There is no shell, so pipes and `;` need
one as the job itself. jimmy is an admin and can see it:

```console
$ ./bin/worker --cert certs/jimbob.crt --key certs/jimbob.key start -- bash -c "sleep 300; echo done"
YG553VOG5UYGVLU4NABF4TCQXT

$ ./bin/worker status YG553VOG5UYGVLU4NABF4TCQXT
ID:        YG553VOG5UYGVLU4NABF4TCQXT
Owner:     jimbob
Command:   bash -c "sleep 300; echo done"
State:     RUNNING
```

Stop it. The job is killed with SIGKILL:

```console
$ ./bin/worker stop YG553VOG5UYGVLU4NABF4TCQXT
State:     STOPPED
Exit code: -1 (signal 9: killed)
```

Stop kills only the job's own process. Here `bash` dies, but its child
`sleep 300` keeps running on its own until it finishes. Cleaning up children
needs cgroups, which is out of scope (see the design doc).

## Regenerating

**Certificates.** The dev certificates in `certs/` are committed so the demo
works out of the box. Client and server certificates expire after 90 days
(the current ones on 2026-12-25). To replace them all:

```bash
go run certs/gen.go
```

This creates a new CA each time and never writes its private key to disk, so
no one can issue more certificates from the committed CA. Tests generate their
own certificates and never use these.

**Protobuf.** The generated code in `gen/` is committed. After changing
`proto/jobworker/v1/jobworker.proto`, with `protoc` v36.2 installed:

```bash
make proto-tools   # protoc-gen-go v1.36.12, protoc-gen-go-grpc v1.6.2
make proto
```
