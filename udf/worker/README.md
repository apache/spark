# udf/worker -- Language-agnostic UDF Worker Framework

Package structure for the UDF worker framework described in
[SPIP SPARK-55278](https://issues.apache.org/jira/browse/SPARK-55278).

## Overview

Spark processes a UDF by obtaining a **WorkerDispatcher** from a worker
specification. The dispatcher manages workers behind the scenes. From
the dispatcher, Spark gets a **WorkerSession** -- one per UDF invocation --
with an Iterator-to-Iterator `process` API that streams input batches
through the worker and returns result batches.

```
UDFWorkerSpecification   -- how to create and configure workers
    |
    v
WorkerDispatcher      -- manages workers, creates sessions
    |
    v
WorkerSession         -- one UDF execution
    |  1. session.init(Init proto)
    |  2. val results = session.process(inputBatches)
    |  3. session.close()
```

How workers are created depends on the dispatcher implementation. The
framework currently provides **direct worker creation** (local OS
processes) and is designed for future **indirect creation** (via a
provisioning service or daemon).

## Architecture

The framework factors into three layers, separating *what the engine sees*
(transport-agnostic API), *how a worker is provisioned* (local subprocess
today, pooled / remote later), and *how bytes are moved* (gRPC over UDS
today).

```
                       Engine
                          │
                          ▼
              ┌──────────────────────┐
              │  UDFDispatcherManager│   caches one dispatcher per worker spec
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐   engine-facing API
              │  WorkerDispatcher    │   (transport- and provisioning-agnostic)
              └──────────┬───────────┘
                         │ creates
                         ▼
              ┌──────────────────────┐   holds    ┌─────────────────┐
              │   WorkerSession      │ ─────────▶ │  WorkerHandle   │
              │  (one UDF execution) │            │ (release hook)  │
              └──────────┬───────────┘            └─────────────────┘
                         │ uses
                         ▼
              ┌──────────────────────┐   1 per worker process;
              │   WorkerConnection   │   shared across all sessions on that worker
              └──────────────────────┘

   ── Direct mode (core.direct) ─────────────────────────────────────────────
              DirectWorkerDispatcher   (abstract: spawn / wait / SIGTERM-SIGKILL)
                   │
                   └─▶ DirectWorkerProcess  (live subprocess; implements WorkerHandle)

   ── gRPC over UDS (grpc) ─────────────────────────────────────────────
              DirectGrpcDispatcher     (extends DirectWorkerDispatcher)
                   │
                   ├─▶ GrpcWorkerSession    (drives bidi Execute stream)
                   ├─▶ GrpcWorkerChannel    (ManagedChannel + Netty event loop)
                   └─▶ UnixDomainSocketTransport  (epoll / kqueue detection)
```

### Cardinality

| Relationship | Cardinality | Notes |
|---|---|---|
| `UDFDispatcherManager` ↔ `WorkerDispatcher` | 1 ↔ N | One dispatcher per worker spec, cached |
| `WorkerDispatcher` ↔ `WorkerSession` | 1 ↔ N | Sessions are short-lived; dispatcher outlives them |
| `WorkerDispatcher` ↔ worker process | 1 ↔ N | In direct mode the dispatcher manages a fleet of subprocesses |
| worker process ↔ `WorkerConnection` | 1 ↔ 1 | One transport per worker |
| worker process ↔ `WorkerSession` | 1 ↔ N | Future via gRPC stream multiplexing; today effectively 1 ↔ 1 (no pooling yet) |
| `WorkerSession` ↔ `WorkerHandle` | 1 ↔ 1 | Each session holds one handle; released exactly once on session close |
| `GrpcWorkerSession` ↔ `Execute` stream | 1 ↔ 1 | One bidi gRPC stream per UDF execution |

### Key ideas

- **Three independent lifecycle scopes.** Dispatcher (per spec), connection
  (per worker process), session (per UDF execution). The split lets a worker
  be reused across many UDFs without conflating the transport with the
  per-execution state machine.
- **Engine doesn't know the transport.** It sees only `WorkerSession`. A new
  transport (TCP, named pipe, future remote workers) plugs in via fresh
  `WorkerDispatcher` / `WorkerConnection` / `WorkerSession` implementations;
  engine code is unchanged.
- **Engine doesn't know how the worker was provisioned.** Sessions hold a
  `WorkerHandle`, not a worker. A future pooled or remotely-leased worker
  model supplies a different `WorkerHandle` without touching session code.
- **Lifecycle guards live in base classes; protocol lives in subclasses.**
  `WorkerSession.init / process / close` are `final` and own the AtomicBoolean
  guards; subclasses fill `doInit / doProcess / doClose` and do not re-check
  the contract.
- **The wire protocol is language-agnostic.** `udf_message.proto` (message
  types) and `udf_service.proto` (the gRPC service) define it; any worker
  (Python, Scala, Rust, ...) implementing the proto is consumable by this
  stack.

## Sub-packages

```
udf/worker/
├── proto/                        -- protobuf message classes only (protobuf-java)
│     worker_spec.proto           -- UDFWorkerSpecification protobuf
│     udf_message.proto           -- UDF execution protocol messages (Init, UdfPayload, ...)
│     udf_service.proto           -- UdfWorker gRPC service (Execute, Manage)
│     common.proto                -- shared enums (UDFWorkerDataFormat, etc.)
│
├── core/                         -- abstract interfaces (no gRPC dependency)
│     WorkerDispatcher.scala      -- creates sessions, manages worker lifecycle
│     WorkerSession.scala         -- per-UDF init/process/cancel/close
│     WorkerConnection.scala      -- transport channel abstraction
│     WorkerHandle.scala          -- release hook used by sessions
│     WorkerSecurityScope.scala   -- security boundary for worker pooling
│     UDFDispatcherManager.scala  -- caches one dispatcher per worker spec
│     │
│     └── direct/                 -- "direct" creation: local OS processes
│           DirectWorkerDispatcher.scala  -- spawns processes, env lifecycle
│           DirectWorkerProcess.scala     -- OS process + connection + WorkerHandle
│
└── grpc/                         -- gRPC over UDS transport (gRPC runtime confined here)
      (generated)                    -- UdfWorkerGrpc service stubs from proto/udf_service.proto
      DirectGrpcDispatcher.scala     -- direct dispatcher speaking gRPC/UDS
      GrpcWorkerSession.scala        -- drives one bidi Execute stream
      GrpcWorkerChannel.scala        -- ManagedChannel + Netty event loop
      UnixDomainSocketTransport.scala -- epoll/kqueue detection
```

The `core/` package defines abstract interfaces that are independent of how
workers are created and what transport carries the protocol. `core/direct/`
implements "direct" worker creation where Spark spawns local OS processes.
The `grpc/` module provides the gRPC-over-UDS transport on top of the direct
provisioning model. Future packages (e.g., `core/indirect/`) can implement
alternative creation modes such as obtaining workers from a provisioning
service or daemon, and additional transports can be added alongside `grpc/`
without touching the engine-facing API.

The `grpc/` module owns the gRPC service-stub generation (from
`proto/`'s `udf_service.proto`) and the gRPC runtime dependencies. Keeping
gRPC here means `proto/`, `core/`, and their consumers (`core`, `catalyst`,
`sql/core`) carry no gRPC dependency on their classpath.

## Wire protocol

Each UDF execution uses a single bidirectional `Execute` gRPC stream.

```
Engine -> Worker:  Init -> PayloadChunk* -> (DataRequest)* -> Finish (Cancel)?
                                                            | Cancel
Worker -> Engine:          InitResponse  -> (DataResponse)* -> (ErrorResponse)? -> (FinishResponse | CancelResponse)
```

See `udf/worker/proto/src/main/protobuf/udf_message.proto` for the complete
message definitions, ordering invariants, and error contract, and
`udf_service.proto` for the gRPC service.

### Direct worker creation

`DirectWorkerDispatcher` spawns worker processes locally. On the first
session, it runs the optional environment lifecycle callables from the
`UDFWorkerSpecification`:

- **`environmentVerification`** -- checks if the environment is ready
  (exit 0 = ready). When it succeeds, installation is skipped.
- **`installation`** -- prepares the environment (installs runtime,
  dependencies, worker binaries). Only runs when verification is absent
  or fails.
- **`environmentCleanup`** -- runs after the dispatcher is closed or on
  JVM shutdown to clean up temporary resources.

Environment setup runs **once per dispatcher** (not per session).
Workers are terminated via SIGTERM/SIGKILL when the dispatcher is closed.

## Basic usage (Scala)

```scala
import org.apache.spark.udf.worker.{
  DirectWorker, Init, ProcessCallable, UdfPayload,
  UDFProtoCommunicationPattern, UDFWorkerDataFormat, UDFWorkerProperties,
  UDFWorkerSpecification, UnixDomainSocket, WorkerCapabilities,
  WorkerConnectionSpec, WorkerEnvironment}
import org.apache.spark.udf.worker.core._
import com.google.protobuf.ByteString

// 1. Define a worker spec (direct creation mode).
val spec = UDFWorkerSpecification.newBuilder()
  .setEnvironment(WorkerEnvironment.newBuilder()
    .setEnvironmentVerification(ProcessCallable.newBuilder()
      .addCommand("python").addCommand("-c").addCommand("import my_udf_worker").build())
    .setInstallation(ProcessCallable.newBuilder()
      .addCommand("pip").addCommand("install").addCommand("my_udf_worker").build())
    .build())
  .setCapabilities(WorkerCapabilities.newBuilder()
    .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
    .addSupportedCommunicationPatterns(
      UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)
    .build())
  .setDirect(DirectWorker.newBuilder()
    .setRunner(ProcessCallable.newBuilder()
      .addCommand("python").addCommand("-m").addCommand("my_udf_worker").build())
    .setProperties(UDFWorkerProperties.newBuilder()
      .setConnection(WorkerConnectionSpec.newBuilder()
        .setUnixDomainSocket(UnixDomainSocket.getDefaultInstance).build())
      .build())
    .build())
  .build()

// 2. Create a dispatcher. DirectGrpcDispatcher is the shipped concrete
//    implementation (gRPC over UDS).
val dispatcher: WorkerDispatcher =
  new org.apache.spark.udf.worker.grpc.DirectGrpcDispatcher(spec)

// 3. Create a session for one UDF execution.
val session = dispatcher.createSession(securityScope = None)
try {
  // 4. Initialize with the serialized function and schemas.
  session.init(Init.newBuilder()
    .setProtocolVersion(1)
    .setUdf(UdfPayload.newBuilder()
      .setPayload(ByteString.copyFrom(serializedFunction))
      .setFormat(payloadFormat)   // worker-recognised tag
      .build())
    .setDataFormat(UDFWorkerDataFormat.ARROW)
    .setInputSchema(ByteString.copyFrom(arrowInputSchema))
    .setOutputSchema(ByteString.copyFrom(arrowOutputSchema))
    .build())

  // 5. Process data -- Iterator in, Iterator out.
  val results: Iterator[Array[Byte]] =
    session.process(inputBatches)

  // Consume results lazily.
  results.foreach(processResultBatch)
} finally {
  session.close()
}

// 6. Shut down all workers.
dispatcher.close()
```

## Build

SBT:
```
build/sbt "udf-worker-proto/compile" "udf-worker-core/compile" "udf-worker-grpc/compile"
```

Maven:
```
build/mvn compile -pl udf/worker/proto,udf/worker/core,udf/worker/grpc -am
```

## Test

SBT:
```
build/sbt "udf-worker-core/test" "udf-worker-grpc/test"
```

## Current status

The framework ships the core abstraction layer, the direct worker dispatcher,
and the first concrete transport (**gRPC over UDS** via `DirectGrpcDispatcher`).
The following are left as TODOs:

- **Connection pooling** -- reuse workers across sessions
- **Security scope isolation** -- partition pools by `WorkerSecurityScope`
- **Indirect worker creation** -- obtain workers from a service or daemon
- **Protocol-level flow control** -- complement gRPC's transport-level flow
  control with worker-side acks so the engine can bound in-flight input
  batches (useful for reduce-style UDFs that hold input without producing
  output)

## Design references

* [SPIP Language-agnostic UDF Protocol for Spark](https://docs.google.com/document/d/19Whzq127QxVt2Luk0EClgaDtcpBsFUp67NcVdKKyPF8/edit?tab=t.0)
* [SPIP Language-agnostic UDF Protocol for Spark -- Worker Specification](https://docs.google.com/document/d/1Dx9NqHRNuUpatH9DYoFF9cmvUl2fqHT4Rjbyw4EGLHs/edit?tab=t.0#heading=h.4h01j4b8rjzv)
