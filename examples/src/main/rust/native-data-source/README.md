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

# Rust native range data source

This example implements batch reads using the [Rust native data source SDK][sdk].
The connector in [src/lib.rs](src/lib.rs) exports `rust_sdk_range` through the SDK's
`export_data_source!(RangeSource)` macro. The macro exports the C entry point
`spark_ds_init`; the connector does not use JNI, raw pointers, or unsafe code.

The example targets C ABI version 1 from [Spark PR #59424][native-source-pr], pinned to commit
`74aa0dffc53463034fa6549796c4dd599e9705de`. The SDK and example build without Spark or a JVM.
Running the Spark smoke script requires a distribution built with that native data source
framework and Java 22 or later on the driver and executors. The JNI SDK prototype in PR #59442
targets a different interface.

Naming `RangeScan` as the `ScanBuilder`, with `type BatchWrite = Unsupported`, derives the
`BatchRead` capability and the native mask `SPARK_DS_BATCH_READ` (`1`); the connector does not
maintain a separate capability list. Connectors can narrow their operations through
`DataSource::capabilities()` based on options. This example does not implement writes or
Structured Streaming. The companion [file sink](../native-data-sink/README.md) demonstrates batch
append and overwrite.
During analysis, Spark creates and closes a source with the table options to inspect its
capabilities. It exposes `BATCH_WRITE` and `TRUNCATE` to Data Source V2 only
when declared, so this example rejects batch append and overwrite before evaluating their input.
The framework still reports the Data Source V2 flags `BATCH_READ`, `MICRO_BATCH_READ`, and
`STREAMING_WRITE` to keep operations on the V2 path instead of falling back to V1. These routing
flags do not add native capabilities: each operation checks the native mask again with its own
options. A source created just for capability discovery is closed without creating a scan.

## Data and options

| Column | Spark type | Value |
| --- | --- | --- |
| `id` | `BIGINT`, non-null | Integers in `[start, end)` |
| `label` | `STRING`, non-null | `row-{id}`, such as `row-13` |

| Option | Default | Meaning |
| --- | --- | --- |
| `start` | `0` | Inclusive start, as a signed 64-bit integer |
| `end` | `100` | Exclusive end; must be at least `start` |
| `numpartitions` | `2` | Maximum number of partitions; must be positive |
| `batchsize` | `1024` | Maximum rows per Arrow batch; must be positive |

Spark normalizes option names to lower case. Unknown options are rejected.

The source evaluates `=`, `<=>`, `<`, `<=`, `>`, and `>=` comparisons between `id` and non-null
integer literals, which `Predicate::literal_value` decodes and checks against the range of their
type, including conjunctions of supported predicates. It reports these predicates as `Exact`;
Spark still evaluates all predicates again after reading. Unknown expressions and unsupported
literal types remain `Unsupported`.

Spark pushes a limit only when no residual filter remains, which excludes filtered scans in
the pinned framework. The connector can apply a global limit by shortening the selected range
before splitting it into partitions. Column pruning preserves the requested order and supports
zero-column batches for operations such as `COUNT(*)`.

## Planning and reading

`RangeSource::new_scan` creates a driver-side `RangeScan`. Pushdown methods update that builder,
and `plan(self)` consumes it to produce a `ScanPlan<RangeReaderFactory>`. The plan contains:

- a serializable `RangeReaderFactory` with the batch size;
- disjoint `RangePartition` values with inclusive starts and exclusive ends.

The SDK serializes the negotiated schema with the factory, so the reader takes its output
columns, in order, from `ReaderContext::schema` instead of carrying them in the factory.

This API matches the framework's separation of driver planning and executor reading. Spark's
`NativeBatchPlanner` owns the native handle; its `NativePartitionReaderFactory` carries the
serialized Rust factory and schema to executors. Java `ScanBuilder.build()` caches a scan but
does not call Rust `plan()`. The first request for batch partitions or a reader factory triggers
native `partitions` and then `serialize_reader`, which share the SDK's cached plan. Spark then
closes the native driver handle. The Rust API and C ABI layout remain unchanged.

The SDK serializes this data for the executors. Each factory opens a lazy Arrow reader using a
partition and a `ReaderContext`, which carries the negotiated schema and a lifetime-bound
`TaskContext`. The example checks task cancellation before generating each batch and keeps
memory bounded by the configured batch size. Neither the factory
nor a partition contains a driver handle or open connection. A storage connector can also put a
snapshot identifier in its factory to make retried reads refer to the same data.

## Build and test

Run from the repository root with Rust and a C compiler:

```sh
cargo build --manifest-path sql/native/rust/Cargo.toml --workspace --locked
cargo test --manifest-path sql/native/rust/Cargo.toml --workspace --locked
```

Build before testing so the `c_abi_smoke` integration test can load the actual connector library.
That test calls its function table through the C header and consumes Arrow C streams without a
JVM. The Rust tests cover filters, global limits, projections, serialization, bounded batches,
and empty output projections. These checks do not substitute for the Spark smoke script.

## Package and run with Spark

Set `SPARK_HOME` to the distribution with #59424, then run:

```sh
python3 examples/src/main/rust/native-data-source/package.py \
  --library sql/native/rust/target/debug/libspark_datasource_rust_sdk_range.so \
  --output sql/native/rust/target/rust_sdk_range.sparkpkg

"$SPARK_HOME/bin/spark-submit" --master 'local[2]' \
  examples/src/main/rust/native-data-source/smoke.py \
  --package sql/native/rust/target/rust_sdk_range.sparkpkg
```

The library name above is for Linux. On macOS use `libspark_datasource_rust_sdk_range.dylib`;
on Windows use `spark_datasource_rust_sdk_range.dll`. The packaging script detects the host
platform. For a cross-compiled library, supply `--platform`, for example `linux-aarch64`.

The package can also be used from a Python session backed by the same Spark build:

```python
spark.addArtifact("sql/native/rust/target/rust_sdk_range.sparkpkg")
df = (
    spark.read.format("rust_sdk_range")
    .option("end", 100)
    .option("numpartitions", 4)
    .load()
)
df.where("id >= 10 AND id < 20").select("label", "id").show()
```

[smoke.py](smoke.py) checks rows, projection, unfiltered and filtered limits, zero-column counts,
and residual predicates against Spark. It also checks that batch append and overwrite fail during
analysis before evaluating an input containing `raise_error` and a shuffle. Run it against the
pinned framework before relying on end-to-end compatibility.

[sdk]: ../../../../../sql/native/rust/README.md
[native-source-pr]: https://github.com/apache/spark/pull/59424
