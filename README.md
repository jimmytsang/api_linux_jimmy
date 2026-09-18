# api_linux_jimmy

Prototype job worker service that provides an API to run arbitrary Linux
processes. These processes can be any executable program that is available on
the machine running the service.

The project has three components:

1. `pkg/job` — a reusable library to start, stop, query and stream the output of jobs.
2. `worker-server` — a gRPC API server, secured with mTLS, that wraps the library.
3. `worker` — a CLI that talks to the API server.

Requires 64-bit Linux.

See [docs/design.md](docs/design.md) for the design.
