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

# C++ native range data source

This example implements batch and micro-batch streaming reads using the
[C++ native data source SDK][sdk]. The connector in [range_source.h](range_source.h) and
[range_source.cc](range_source.cc) exports `cpp_sdk_range` through the SDK's
`SPARK_EXPORT_DATA_SOURCE(cpp_sdk_range::RangeSource)` macro. The macro exports the C entry point
`spark_ds_init`; the connector does not use JNI, C structs, or raw pointers.

The example targets C ABI version 1 from [Spark PR #59424][native-source-pr], pinned to commit
`74aa0dffc53463034fa6549796c4dd599e9705de`. The SDK and example build without Spark or a JVM.
Running the Spark smoke script requires a distribution built with that native data source
framework and Java 22 or later on the driver and executors.

Naming `RangeScan` as the `ScanBuilder` and `RangeStream` as the `MicroBatchStream`, without
writes, derives the `kBatchRead` and `kMicroBatchRead` capabilities and the native mask
`SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ` (`3`); the connector does not maintain a
separate capability list. Connectors can narrow their operations through
`DataSource::capabilities()` based on options. This example does not implement writes. The
companion [file sink](../native-data-sink/README.md) demonstrates batch append and overwrite, and
exactly-once streaming writes. During analysis, Spark creates and closes a source with the table
options to inspect its capabilities. It exposes `BATCH_WRITE` and `TRUNCATE` to Data Source V2
only when declared, so this example rejects batch append and overwrite before evaluating their
input. A source created just for capability discovery is closed without creating a scan.

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
integer literals, which `Predicate::literal_value()` decodes and checks against the range of
their type, including conjunctions of supported predicates. It reports these predicates as
`kExact`; Spark still evaluates all predicates again after reading. Unknown expressions and
unsupported literal types remain `kUnsupported`. The selected interval stays within the range,
so its bounds never overflow, also at the limits of 64-bit integers.

Spark pushes a limit only when no residual filter remains, which excludes filtered scans in the
pinned framework. The connector can apply a global limit by shortening the selected range before
splitting it into partitions. Column pruning preserves the requested order and supports
zero-column batches for operations such as `COUNT(*)`.

## Planning and reading

`RangeSource::NewScan` creates a driver-side `RangeScan`. Pushdown methods update that builder,
and `Plan()` produces a `ScanPlan<RangeReaderFactory>`. The plan contains:

- a serializable `RangeReaderFactory` with the batch size;
- disjoint `RangePartition` values with inclusive starts and exclusive ends.

`NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE` makes the factory, the partitions, and the offsets
serializable. The SDK serializes the negotiated schema with the factory, so the reader takes its
output columns, in order, from `ReaderContext::schema` instead of carrying them in the factory.

The SDK serializes this data for the executors. Each factory opens a lazy
`arrow::RecordBatchReader`, built with `RecordBatchReader::MakeFromIterator` from a generator of
batches, using a partition and a `ReaderContext`, which carries the negotiated schema and the
`TaskContext`. The example checks task cancellation before generating each batch and keeps memory
bounded by the configured batch size. Neither the factory nor a partition contains a driver
handle or open connection. A storage connector can also put a snapshot identifier in its factory
to make retried reads refer to the same data.

A streaming read creates a `RangeStream`, whose offsets are the next row to read, such as
`{"next":13}`. The range is fixed, so all its rows are available when the query starts: the
initial offset is the start of the range, the latest offset its end, and the first micro-batch
reads every row. Later micro-batches find no new data. `RangeStream::Plan` splits the rows between
two offsets into partitions with the `RangeReaderFactory` of batch scans, so the executors read a
micro-batch exactly as a batch scan, with all the columns of the schema: Spark pushes nothing down
into a streaming scan. A restarted query may plan its last range again, from the offsets of its
checkpoint.

## Build and test

From the repository root, with the dependencies of the [SDK][sdk]:

```sh
cmake -S sql/native/cpp -B sql/native/cpp/build
cmake --build sql/native/cpp/build
(cd sql/native/cpp/build && ctest --output-on-failure -R range)
```

`range_source_test` covers filters, global limits, projections, serialization, bounded batches,
empty output projections, 64-bit boundaries, and the micro-batches of a stream.
`range_c_abi_smoke` compiles [tests/c_abi_smoke.c](tests/c_abi_smoke.c) against the C header
only, loads the actual connector library, calls its function table, and consumes Arrow C streams
without a JVM, including a streaming read through the stream reader functions. These checks do
not substitute for the Spark smoke script.

## Package and run with Spark

Set `SPARK_HOME` to the distribution with #59424, then run:

```sh
python3 examples/src/main/cpp/native-data-source/package.py \
  --library sql/native/cpp/build/examples/native-data-source/libspark_datasource_cpp_sdk_range.so \
  --output sql/native/cpp/build/cpp_sdk_range.sparkpkg

"$SPARK_HOME/bin/spark-submit" --master 'local[2]' \
  examples/src/main/cpp/native-data-source/smoke.py \
  --package sql/native/cpp/build/cpp_sdk_range.sparkpkg
```

The library name above is for Linux. On macOS use `libspark_datasource_cpp_sdk_range.dylib`. The
packaging script detects the host platform. For a cross-compiled library, supply `--platform`,
for example `linux-aarch64`. The library links Arrow C++ dynamically, so the driver and the
executors must find the Arrow libraries that it was built with.

The package can also be used from a Python session backed by the same Spark build:

```python
spark.addArtifact("sql/native/cpp/build/cpp_sdk_range.sparkpkg")
df = (
    spark.read.format("cpp_sdk_range")
    .option("end", 100)
    .option("numpartitions", 4)
    .load()
)
df.where("id >= 10 AND id < 20").select("label", "id").show()
```

[smoke.py](smoke.py) checks rows, projection, unfiltered and filtered limits, zero-column counts,
residual predicates, and a streaming read with `Trigger.AvailableNow` against Spark. It also
checks that batch append and overwrite fail during analysis before evaluating an input containing
`raise_error` and a shuffle. Run it against the pinned framework before relying on end-to-end
compatibility.

[sdk]: ../../../../../sql/native/cpp/README.md
[native-source-pr]: https://github.com/apache/spark/pull/59424
