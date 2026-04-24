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
    |  1. session.init(InitMessage(payload, inputSchema, outputSchema))
    |  2. val results = session.process(inputBatches)
    |  3. session.close()
```

How workers are created depends on the dispatcher implementation. The
framework currently provides **direct worker creation** (local OS
processes) and is designed for future **indirect creation** (via a
provisioning service or daemon).

## Sub-packages

```
udf/worker/
├── proto/
│     worker_spec.proto           -- UDFWorkerSpecification protobuf (+ generated Java classes)
│     common.proto                -- shared enums (UDFWorkerDataFormat, etc.)
│
└── core/                         -- abstract interfaces
      WorkerDispatcher.scala      -- creates sessions, manages worker lifecycle
      WorkerSession.scala         -- per-UDF init/process/cancel/close + InitMessage
      WorkerConnection.scala      -- transport channel abstraction
      WorkerSecurityScope.scala   -- security boundary for worker pooling
      │
      └── direct/                 -- "direct" creation: local OS processes
            DirectWorkerDispatcher.scala  -- spawns processes, env lifecycle
            DirectWorkerProcess.scala     -- OS process + connection + UDS socket
            DirectWorkerSession.scala     -- session backed by a direct process
```

The `core/` package defines abstract interfaces that are independent of how
workers are created. The `core/direct/` sub-package implements "direct"
worker creation where Spark spawns local OS processes. Future packages
(e.g., `core/indirect/`) can implement alternative creation modes such as
obtaining workers from a provisioning service or daemon.

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
  DirectWorker, ProcessCallable, UDFProtoCommunicationPattern,
  UDFWorkerDataFormat, UDFWorkerProperties, UDFWorkerSpecification,
  UnixDomainSocket, WorkerCapabilities, WorkerConnectionSpec, WorkerEnvironment}
import org.apache.spark.udf.worker.core._

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

// 2. Create a dispatcher. Use a protocol-specific subclass of
//    DirectWorkerDispatcher (e.g., gRPC over UDS).
val dispatcher: WorkerDispatcher = ...

// 3. Create a session for one UDF execution.
val session = dispatcher.createSession(securityScope = None)
try {
  // 4. Initialize with the serialized function and schemas.
  session.init(InitMessage(
    functionPayload = serializedFunction,
    inputSchema = arrowInputSchema,
    outputSchema = arrowOutputSchema))

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
build/sbt "udf-worker-proto/compile" "udf-worker-core/compile"
```

Maven:
```
build/mvn compile -pl udf/worker/proto,udf/worker/core -am
```

## Test

SBT:
```
build/sbt "udf-worker-core/test"
```

## Current status

This is the **first MVP** providing the core abstraction layer and the
direct worker dispatcher.
The following are left as TODOs:

- **Connection pooling** -- reuse workers across sessions
- **Security scope isolation** -- partition pools by `WorkerSecurityScope`
- **Indirect worker creation** -- obtain workers from a service or daemon
- **Protocol-specific implementations** -- e.g., gRPC over UDS

## Design references

* [SPIP Language-agnostic UDF Protocol for Spark](https://docs.google.com/document/d/19Whzq127QxVt2Luk0EClgaDtcpBsFUp67NcVdKKyPF8/edit?tab=t.0)
* [SPIP Language-agnostic UDF Protocol for Spark -- Worker Specification](https://docs.google.com/document/d/1Dx9NqHRNuUpatH9DYoFF9cmvUl2fqHT4Rjbyw4EGLHs/edit?tab=t.0#heading=h.4h01j4b8rjzv)
