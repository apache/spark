<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information regarding copyright
ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy of the
License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.
-->

# Rust native Parquet source

This batch-read example implements `DataSource`, `ScanBuilder`, and `PartitionReaderFactory` from
the [Rust native data source SDK][sdk] on the Parquet reader of [arrow-rs][arrow-rs]. Its
`DataSource` sets `MicroBatchStream`, `BatchWrite`, and `StreamingWrite` to `Unsupported`, and the
library exports `export_data_source!(ParquetSource)` under the name `rust_sdk_parquet`.

The example targets C ABI version 1 from [Spark PR #59424][native-source-pr], pinned to
`74aa0dffc53463034fa6549796c4dd599e9705de`. Running it in Spark requires that framework and Java 22
or later on the driver and executors. The standalone Rust tests do not need Spark or a JVM.

## How it reads

The driver lists the files of the path and splits each one into byte ranges of at most
`maxPartitionBytes`, as Spark splits the files of its own sources. A task reads the footer of its
file, and the row groups whose middle falls in its range, so that each row group belongs to exactly
one task. The reader decodes only the columns that Spark reads, and returns them in the order of
`ReaderContext::schema`.

The pushed predicates that the reader can evaluate as Spark does become an arrow-rs
[`RowFilter`][row-filter]. For the row groups of a task, the reader decodes the columns of the first
predicate and evaluates it, then decodes the columns of the next predicate only for the rows that
passed, and the other columns only for the rows that pass every predicate. Spark's own vectorized
reader can skip row groups and pages by their statistics, but decodes every row of the pages that it
reads, and filters afterwards. Spark evaluates the pushed predicates again on the rows that the
source returns.

The reader evaluates these predicates on top-level columns, with literals of the type of their
column:

- `=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`, and `IN`, on boolean, integer, string, binary, date,
  timestamp, and decimal columns, the latter with literals of the same scale;
- `STARTS_WITH`, `ENDS_WITH`, and `CONTAINS`, on string columns, which Spark pushes for some
  `LIKE` patterns;
- `IS_NULL` and `IS_NOT_NULL`, on any column;
- `AND`, `OR`, and `NOT` of these.

They evaluate to what Spark computes, including null in three-valued logic, so the reader drops
exactly the rows that Spark drops. Other predicates remain for Spark, in particular comparisons of
floating-point columns: Arrow orders floats by the IEEE 754 total order, in which -0.0 is less than
0.0 and some NaNs are less than all numbers, while Spark equates the zeros and orders NaN above all
numbers. Spark pushes a limit only to a scan without predicates; each task then stops after the
limit, which the scan as a whole may exceed, so the source does not report the limit as pushed.

## Supported data and options

As Spark does without `mergeSchema`, the source takes the schema of one file, the first by path. Its
columns are nullable at every level, as Spark reads the columns of files. The types are those that
arrow-rs reads from Parquet, without any Arrow schema embedded in the files: booleans, signed
integers, floats, strings, binary, decimals, dates, microsecond timestamps with or without a time
zone, structs, lists, and maps. INT96 columns, Spark's default encoding of timestamps, are read as
microsecond timestamps in UTC. The source rejects the other types when Spark asks for the schema,
among them timestamps in milliseconds or nanoseconds, unsigned integers, and fixed-size binary,
which Spark converts or rejects.

| Option | Default | Meaning |
| --- | --- | --- |
| `path` | Required | Absolute path of a Parquet file, or of a directory of Parquet files |
| `batchSize` | `8192` | Maximum number of rows of a batch |
| `maxPartitionBytes` | `134217728` | Maximum number of bytes of a file that one task reads |
| `rowFilter` | `true` | Whether to evaluate the pushed predicates in a `RowFilter` |

In a directory, the source skips the files whose names start with `_` or `.`, such as `_SUCCESS` and
checksums, and empty files, and rejects subdirectories. Unknown options are rejected.

## Build, package, and run

From the repository root:

```sh
cargo build --manifest-path sql/native/rust/Cargo.toml --release --locked \
  -p spark-native-parquet-demo
cargo test --manifest-path sql/native/rust/Cargo.toml -p spark-native-parquet-demo --locked

python3 examples/src/main/rust/native-data-source/package.py \
  --source rust_sdk_parquet \
  --library sql/native/rust/target/release/libspark_datasource_rust_sdk_parquet.so \
  --output sql/native/rust/target/rust_sdk_parquet.sparkpkg

"$SPARK_HOME/bin/spark-submit" --master 'local[2]' \
  examples/src/main/rust/native-parquet/smoke.py \
  --package sql/native/rust/target/rust_sdk_parquet.sparkpkg
```

Use `libspark_datasource_rust_sdk_parquet.dylib` for macOS. The smoke script writes Parquet files
with Spark, including INT96 and microsecond timestamps, small row groups, NaN, -0.0, and nested
types, and checks that each of its queries returns what Spark's own reader returns, with and without
the row filter. It also checks the pushed predicates in the plan, a limit, and the rejection of
millisecond timestamps.

An application can read with:

```python
spark.addArtifact("sql/native/rust/target/rust_sdk_parquet.sparkpkg")
df = spark.read.format("rust_sdk_parquet").load("/absolute/shared/path/table")
df.where("k = 7").select("id", "payload").show()
```

[benchmark.py](benchmark.py) compares queries with Spark's own reader, with and without the row
filter, on data that it writes if the given path does not exist:

```sh
"$SPARK_HOME/bin/spark-submit" --master 'local[4]' --driver-memory 4g \
  examples/src/main/rust/native-parquet/benchmark.py \
  --package sql/native/rust/target/rust_sdk_parquet.sparkpkg \
  --data /tmp/rust-sdk-parquet-benchmark
```

Package a release build for the benchmark: a debug build is many times slower.

## Limits of this example

The files must be on a local POSIX filesystem, or on a shared filesystem at the same absolute path
on all hosts. Object-store URLs are unsupported. The source does not discover partition directories,
merge schemas, rebase dates and timestamps that Spark wrote in the legacy hybrid calendar, or skip
row groups and pages by their statistics. It matches the names of columns by case, and requires
valid UTF-8 in string columns, which arrow-rs checks and Spark does not. It reports no statistics,
so Spark does not plan to broadcast its tables in joins without a hint. The reader evaluates the row
filter with the synchronous reader of arrow-rs, which decodes the columns of the predicates again,
for the rows that pass, when Spark also reads them; the predicate cache of arrow-rs serves only its
asynchronous reader. Native memory is not reserved through Spark's memory manager.

[sdk]: ../../../../../sql/native/rust/README.md
[native-source-pr]: https://github.com/apache/spark/pull/59424
[arrow-rs]: https://github.com/apache/arrow-rs
[row-filter]: https://docs.rs/parquet/58.4.0/parquet/arrow/arrow_reader/struct.RowFilter.html
