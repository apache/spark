<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Rust native data source SDK prototype

This crate demonstrates a Rust API for **batch reads** over
the JNI ABI proposed in [Spark PR #59313](https://github.com/apache/spark/pull/59313),
at commit `ff7b1ae0150fbcb3b6a7c67bea8accf6aa4ad9bd`. It does not include that PR's
Spark implementation. Running the Spark example requires a Spark distribution
with that native data source framework. Rust and JNI tests run independently of
a Spark build.

This is an API prototype, not a published crate or a compatibility commitment.
It is not part of the Maven/SBT build and does not add a Rust requirement to
building Spark.

The `spark-native-source` SDK crate and Cargo workspace live in this directory.
The [Range connector][range] is a separate workspace member under
`examples/src/main/rust/native-data-source`, with its JNI integration test,
packaging helper, and Spark smoke script. The example depends on the SDK through
a Cargo path dependency. Both crates share the lockfile and `target` directory
here. By default, Cargo commands in this directory select only the SDK; use
`--workspace` to include the example.

There are no write or Structured Streaming exports.

## Connector API

A connector implements two traits from [`src/lib.rs`](src/lib.rs):

* `BatchSource`: parse options, return the schema, create a driver-side scan
  builder, and open an Arrow reader on an executor.
* `ScanBuilder`: negotiate filters, limit and output columns, then build a plan
  containing typed state and typed partitions.

The state and partition types derive `serde::Serialize` and `serde::Deserialize`.
The SDK encodes and restores them. Connections, pointers and reader objects stay
inside the process that created them. Include snapshot identifiers in the plan
when reads must be repeatable across task retries.

Register one provider in the connector's `cdylib` crate:

```rust
spark_native_source::export_data_source!(RangeSource);
```

The macro exports the batch-read JNI functions of
`org.apache.spark.sql.datasource.NativeBridge`. It handles Java strings and
arrays, native handles, error conversion, plan serialization, and Arrow C Stream
export. The connector does not use `JNIEnv`, raw pointers, or `unsafe` code. Rust
traits remain inside the plugin and are not used as a cross-library Rust ABI.

`BatchSource::open` returns `Box<dyn RecordBatchReader + Send>`. Existing Arrow
readers can be used when their types and layouts are supported by the pinned
Spark ABI. That implementation requires zero array offsets at every nesting
level and does not support dictionary encoding or every Arrow type. Connectors
must normalize sliced arrays when necessary; the SDK preserves their layout.
Return batches lazily so a partition does not need to fit in memory. The
[Range implementation][range-source] demonstrates
multiple bounded batches, disjoint splits, and an empty output projection that
preserves row counts.

## Pushdown contracts

Filter results correspond to the input predicates in order:

| Result | Connector responsibility | Spark behavior |
| --- | --- | --- |
| `Exact` | Evaluate completely with Spark SQL semantics | May remove the filter |
| `PruningOnly` | Only skip data that cannot match | Keep the residual filter |
| `Unsupported` | No evaluation | Keep the residual filter |

Unknown expression names and unsupported data types must remain residual. The
SDK preserves literal JSON values, including exact 64-bit integers, instead of
converting every number to floating point. It does not implement a generic
Spark expression evaluator.

Limit pushdown distinguishes a per-partition cap from a cap on the whole scan:

| Result | JNI `pushLimit` return | Meaning |
| --- | --- | --- |
| `Unsupported` | `false` | No limit applied |
| `PerPartition` | `false` | A local cap; the total may exceed the limit |
| `Global` | `true` | The entire scan returns at most the limit |

The boolean mapping follows #59313's existing contract. Spark retains its final
limit in either case. Spark's optimizer decides whether a query allows limit
pushdown; the SDK additionally declines it if any filter it received was not
`Exact`. Capping rows before a residual filter is unsafe and cannot be repaired
by retaining the final Spark limit.

Projection returns `true` only when the reader will produce exactly the
requested top-level columns, in order. Keep columns needed internally for exact
filters even if they are absent from the output projection. An empty projection
must still produce the correct number of rows.

The Range example accepts integer comparisons on `id`, including reversed
operands and conjunctions. It intersects the range with the exact predicates,
applies a global limit, and then creates disjoint partitions. Other predicates
remain with Spark. Pushdown methods have conservative defaults in the SDK, so
connectors can add these capabilities incrementally.

## Build and test without Spark

Use a Rust toolchain compatible with Arrow 58 and a JDK (17 or newer). Set
`JAVA_HOME` if the installed JDK is not otherwise discoverable. From this
directory:

```bash
cargo build --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

Build before testing so the JNI integration test can load the actual connector
dynamic library. The test compiles a small Java fixture containing the batch
ABI declarations, starts a real JVM, invokes the JNI exports, and reads the
resulting Arrow C Stream. This checks the binary boundary independently of
Spark; it does not substitute for testing Spark's planner and task lifecycle.

The crate unit tests cover filter negotiation, integer boundaries, global
limits across partitions, projections, and serialized plans. The SDK also
tests Arrow stream and JNI lifecycle behavior.

## Package and run with Spark

See the [Range example guide][range] for packaging and `spark-submit` commands.
The example's `package.py` creates a `.sparkpkg` artifact for one platform, and
`smoke.py` checks reading and pushdown against a Spark distribution with #59313.
The compiled example library is under this workspace's `target` directory.

## Scope and remaining work

The JNI ABI and this demo both run trusted native code inside the driver and
executors. Arrow buffers cross the language boundary by reference. Files still
need decoding, and downstream Spark operators can convert batches to rows.

The SDK centralizes resource cleanup and translates provider errors and
unwinding panics from JNI calls and reader callbacks. Process aborts and panics
in custom Arrow buffer destructors cannot be recovered.
Exported buffers must remain valid until their consumer releases them; releasing
the stream does not release batches already handed to its consumer.

This demo does not introduce task cancellation, Spark memory reservations,
custom metrics, nested projection, a generic filter evaluator, C++ bindings,
or a new C ABI. Network readers still need their own I/O timeouts. It does not
fix the implementation issues tracked on #59313 or claim full end-to-end
compatibility with later revisions of that PR.

[range]: ../../../examples/src/main/rust/native-data-source/README.md
[range-source]: ../../../examples/src/main/rust/native-data-source/src/lib.rs
