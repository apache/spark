# udf/worker -- Language-agnostic UDF Worker Framework

Package structure for the UDF worker framework described in
[SPIP SPARK-55278](https://issues.apache.org/jira/browse/SPARK-55278).

## Overview

Spark processes a UDF by first obtaining a **WorkerDispatcher** from the worker
specification (plus context such as security scope). The dispatcher manages the
actual worker processes behind the scenes -- pooling, reuse, and termination are
all invisible to Spark.

From the dispatcher, Spark gets a **WorkerSession**, which represents one single
UDF execution and can carry per-execution state. A WorkerSession is not 1-to-1
mapped to an actual worker -- multiple sessions may share the same underlying
worker when it is reused. Worker reuse is managed by each dispatcher
implementation based on the worker specification.

## Sub-packages

```
udf/worker/
├── proto/  Protobuf definition of the worker specification
│           (UDFWorkerSpecification).
│           WorkerSpecification   -- typed Scala wrapper around the protobuf spec.
└── core/   Engine-side APIs (all @Experimental):
              WorkerDispatcher      -- manages workers for one spec; creates sessions.
              WorkerSession         -- represents one single UDF execution.
              WorkerSecurityScope   -- security boundary for connection pooling.
```

## Build

SBT:
```
build/sbt "udf-worker-core/compile"
build/sbt "udf-worker-core/test"
```

Maven:
```
./build/mvn -pl udf/worker/proto,udf/worker/core -am compile
./build/mvn -pl udf/worker/proto,udf/worker/core -am test
```

## Design references

* [SPIP Language-agnostic UDF Protocol for Spark](https://docs.google.com/document/d/19Whzq127QxVt2Luk0EClgaDtcpBsFUp67NcVdKKyPF8/edit?tab=t.0)
* [SPIP Language-agnostic UDF Protocol for Spark -- Worker Specification](https://docs.google.com/document/d/1Dx9NqHRNuUpatH9DYoFF9cmvUl2fqHT4Rjbyw4EGLHs/edit?tab=t.0#heading=h.4h01j4b8rjzv)
