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

This example implements batch and micro-batch streaming reads using the
[Rust native data source SDK][sdk].
The connector in [src/lib.rs](src/lib.rs) exports `rust_sdk_range` through the SDK's
`export_data_source!(RangeSource)` macro. The macro exports the C entry point
`spark_ds_init`; the connector does not use JNI, raw pointers, or unsafe code.

The example targets version 1 of the C ABI of native data sources, `spark_datasource.h`, which
Spark 4.4.0 introduces. The SDK and example build without Spark or a JVM. Running the Spark smoke
script requires a distribution with native data sources and Java 22 or later on the driver and
executors. The JNI SDK prototype in PR #59442 targets a different interface.

Naming `RangeScan` as the `ScanBuilder` and `RangeStream` as the `MicroBatchStream`, with
`Unsupported` writes, derives the `BATCH_READ` and `MICRO_BATCH_READ` capabilities and the native
mask `SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ | SPARK_DS_ADMISSION_CONTROL` (`67`),
since every stream supports admission control; the connector does not maintain a separate
capability list. Connectors can narrow their operations through `DataSource::capabilities()`
based on options. This example does not implement writes. The companion
[file sink](../native-data-sink/README.md) demonstrates batch append and overwrite, and
exactly-once streaming writes.
During analysis, Spark creates and closes a source with the table options to inspect its
capabilities. It exposes `BATCH_WRITE` and `TRUNCATE` to Data Source V2 only when the source
declares `BATCH_WRITE` and `BATCH_TRUNCATE`, so this example rejects batch append and overwrite
before evaluating their input.
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
| `numpartitions` | `2` | Maximum number of partitions; from 1 to 100000 |
| `batchsize` | `1024` | Maximum rows per Arrow batch; from 1 to 1000000 |
| `maxRowsPerTrigger` | none | Maximum rows per micro-batch of a streaming read; must be positive |

Spark normalizes option names to lower case. Unknown options are rejected.

Spark pushes a limit only to a scan whose rows it does not filter, and evaluates filters after
reading. The connector uses none of the domains of the filtered columns that Spark pushes to
`ScanBuilder::push_domains`, so it reads all its rows. It applies a global
limit by shortening the range before splitting it into partitions. The readers return the
columns of the schema to read, in its order, including zero-column batches for operations such
as `COUNT(*)`.

## Planning and reading

`RangeSource::new_scan` creates a driver-side `RangeScan`. Pushdown methods update that builder,
and `plan(self)` consumes it to produce a `ScanPlan<RangeReaderFactory>`. The plan contains:

- a serializable `RangeReaderFactory` with the batch size;
- disjoint `RangePartition` values with inclusive starts and exclusive ends.

Spark passes the schema to read to each reader, so the reader takes its output columns, in
order, from `ReaderContext::schema` instead of carrying them in the factory, and the scan does
not implement `prune_columns`.

This API matches the framework's separation of driver planning and executor reading. Spark's
`NativeBatchPlanner` owns the native handle; its `NativePartitionReaderFactory` carries the
serialized Rust factory and the schema to read to executors. Java `ScanBuilder.build()` caches
a scan but does not call Rust `plan()`. The first request for batch partitions or a reader
factory triggers native `partitions`, which returns the partitions and the serialized factory.
Spark then closes the native driver handle.

The SDK serializes this data for the executors. Each factory opens a lazy iterator of Arrow
batches using a partition and a `ReaderContext`, which carries the schema to read and a
lifetime-bound `TaskContext`. The example checks task cancellation before generating each batch
and keeps memory bounded by the configured batch size. Neither the factory
nor a partition contains a driver handle or open connection. A storage connector can also put a
snapshot identifier in its factory to make retried reads refer to the same data.

A streaming read creates a `RangeStream`, whose offsets are the next row to read, such as
`{"next":13}`. The range is fixed, so all its rows are available when the query starts: the
initial offset is the start of the range, the latest offset its end, and the first micro-batch
reads every row. Later micro-batches find no new data. With `maxRowsPerTrigger`,
`default_read_limit` returns `ReadLimit::MaxRows`, and `latest_offset` ends each micro-batch
after at most that many rows, and not beyond the end of the run of `Trigger.AvailableNow`, which
then reads the range in several micro-batches. `RangeStream::plan` splits the rows between two
offsets into partitions with the `RangeReaderFactory` of batch scans, so the executors read a
micro-batch exactly as a batch scan. A restarted query may plan its last range again, from the
offsets of its checkpoint.

## Build and test

The example builds only in the Spark source tree, not from the copy in a binary distribution:
it depends on the SDK under `sql/native/rust`, and its C test on the header under `sql/core`.
Run from the repository root with Rust and a C compiler:

```sh
cargo build --manifest-path sql/native/rust/Cargo.toml --workspace --locked
cargo test --manifest-path sql/native/rust/Cargo.toml --workspace --locked
```

The build produces the library to package below. The `c_abi_smoke` integration test loads the
library that `cargo test` builds, and calls its function table through the C header and
consumes Arrow C streams without a JVM, including streaming reads through the stream reader
functions, with and without a read limit. The Rust tests cover global limits, projections,
serialization, bounded batches, empty output projections, and the micro-batches of a stream,
within a limit and the end of a run. These checks do not substitute for the Spark smoke script.

## Package and run with Spark

Set `SPARK_HOME` to a distribution with native data sources, then run:

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
filters, and streaming reads with `Trigger.AvailableNow` against Spark: one micro-batch without a
limit, and micro-batches of at most `maxRowsPerTrigger` rows with it. It also checks that batch
append and overwrite fail during analysis before evaluating an input containing `raise_error` and
a shuffle.

[sdk]: ../../../../../sql/native/rust/README.md
