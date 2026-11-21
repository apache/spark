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

The example targets version 1 of the C ABI of native data sources, `spark_datasource.h`, which
Spark 4.4.0 introduces. The SDK and example build without Spark or a JVM. Running the Spark smoke
script requires a distribution with native data sources and Java 22 or later on the driver and
executors.

Naming `RangeScan` as the `ScanBuilder` and `RangeStream` as the `MicroBatchStream`, without
writes, derives the `kBatchRead` and `kMicroBatchRead` capabilities and the native mask
`SPARK_DS_BATCH_READ | SPARK_DS_MICRO_BATCH_READ | SPARK_DS_ADMISSION_CONTROL` (`67`), since every
stream supports admission control; the connector does not maintain a separate capability list.
Connectors can narrow their operations through `DataSource::capabilities()` based on options.
This example does not implement writes. The companion [file sink](../native-data-sink/README.md)
demonstrates batch append and overwrite, and exactly-once streaming writes. During analysis,
Spark creates and closes a source with the table options to inspect its capabilities. It exposes
`BATCH_WRITE` and `TRUNCATE` to Data Source V2 only when the source declares `BATCH_WRITE` and
`BATCH_TRUNCATE`, so this example rejects batch append and overwrite before evaluating their
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
| `maxRowsPerTrigger` | none | Maximum rows per micro-batch of a streaming read; must be positive |

Spark normalizes option names to lower case. Unknown options are rejected.

Spark pushes a limit only to a scan whose rows it does not filter, and evaluates filters after
reading, since version 1 of the ABI does not push them down. The connector applies a global limit
by shortening the range before splitting it into partitions, with 64-bit arithmetic that never
overflows, also at the limits of 64-bit integers. The readers return the columns of the schema to
read, in its order, including zero-column batches for operations such as `COUNT(*)`.

## Planning and reading

`RangeSource::NewScan` creates a driver-side `RangeScan`. Pushdown methods update that builder,
and `Plan()` produces a `ScanPlan<RangeReaderFactory>`. The plan contains:

- a serializable `RangeReaderFactory` with the batch size;
- disjoint `RangePartition` values with inclusive starts and exclusive ends.

`NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE` makes the factory, the partitions, and the offsets
serializable. Spark passes the schema to read to each reader, so the reader takes its output
columns, in order, from `ReaderContext::schema` instead of carrying them in the factory, and the
scan does not implement `PruneColumns`.

The SDK serializes this data for the executors. Each factory opens a lazy
`arrow::RecordBatchReader`, built with `RecordBatchReader::MakeFromIterator` from a generator of
batches, using a partition and a `ReaderContext`, which carries the schema to read and the
`TaskContext`. The example checks task cancellation before generating each batch and keeps memory
bounded by the configured batch size. Neither the factory nor a partition contains a driver
handle or open connection. A storage connector can also put a snapshot identifier in its factory
to make retried reads refer to the same data.

A streaming read creates a `RangeStream`, whose offsets are the next row to read, such as
`{"next":13}`. The range is fixed, so all its rows are available when the query starts: the
initial offset is the start of the range, the latest offset its end, and the first micro-batch
reads every row. Later micro-batches find no new data. With `maxRowsPerTrigger`,
`DefaultReadLimit` returns `ReadLimit::MaxRows`, and `LatestOffset` ends each micro-batch after at
most that many rows, and not beyond the end of the run of `Trigger.AvailableNow`, which then reads
the range in several micro-batches. `RangeStream::Plan` splits the rows between two offsets into
partitions with the `RangeReaderFactory` of batch scans, so the executors read a micro-batch
exactly as a batch scan. A restarted query may plan its last range again, from the offsets of its
checkpoint.

## Build and test

From the repository root, with the dependencies of the [SDK][sdk]:

```sh
cmake -S sql/native/cpp -B sql/native/cpp/build
cmake --build sql/native/cpp/build
(cd sql/native/cpp/build && ctest --output-on-failure -R range)
```

`range_source_test` covers global limits, projections, serialization, bounded batches, empty
output projections, 64-bit boundaries, and the micro-batches of a stream, within a limit and the
end of a run. `range_c_abi_smoke` compiles [tests/c_abi_smoke.c](tests/c_abi_smoke.c) against the
C header only, loads the actual connector library, calls its function table, and consumes Arrow C
streams without a JVM, including streaming reads through the stream reader functions, with and
without a read limit. These checks do not substitute for the Spark smoke script.

## Package and run with Spark

Set `SPARK_HOME` to a distribution with native data sources, then run:

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
filters, and streaming reads with `Trigger.AvailableNow` against Spark: one micro-batch without a
limit, and micro-batches of at most `maxRowsPerTrigger` rows with it. It also checks that batch
append and overwrite fail during analysis before evaluating an input containing `raise_error` and
a shuffle.

[sdk]: ../../../../../sql/native/cpp/README.md
