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

The example targets version 1 of the C ABI of native data sources, `spark_datasource.h`, which
Spark 4.4.0 introduces, with `push_domains`, which SPARK-61200 added to it. Running it in Spark
requires a distribution with native data sources and Java 22 or later on the driver and
executors. The standalone Rust tests do not need Spark or a JVM.

## How it reads

The driver lists the files of the path and splits each one into byte ranges of at most
`maxPartitionBytes`, which it packs into tasks as Spark packs the files of its own sources: by
decreasing size, a task takes ranges while their bytes, with `openCostInBytes` for each, fit in
`maxPartitionBytes`, so that small files share tasks. For each of its ranges, a task reads the
footer of the file, and the row groups whose middle falls in the range, so that each row group
belongs to exactly one task. The reader decodes only the columns that Spark reads, and returns
them in the order of `ReaderContext::schema`.

Spark pushes down the domains of the columns that the filters of a query constrain: for each
column, whether null and NaN are in its domain, and the ranges of its other values, which Spark
derives from comparisons of the column with literals, `IN`, `IS NULL`, `IS NOT NULL`, and
`STARTS_WITH`, under `AND`, `OR`, and `NOT`. A domain has every value that its column has in the
rows that the filters keep, so a row whose value is outside the domain of its column matches no
filter, and Spark evaluates all the filters after reading anyway. The source uses the domains of
top-level columns whose values Arrow compares as Spark does: boolean, integer, floating-point,
string, binary, date, timestamp, and decimal columns. When the filters are comparisons of single
columns with literals, combined with `AND`, such as those of TPC-H Q6, the rows whose values are in
the domains are exactly the rows that Spark keeps. From other filters, such as
`k = 3 OR label = 'x'`, which constrains two columns, or `ENDS_WITH`, Spark derives no domain, and
filters the rows that the source returns.

The reader evaluates the domains on each batch that it decodes, and returns only the rows whose
values are in them, which the [`BatchCoalescer`][coalescer] of arrow-rs gathers into batches of
`batchSize` rows. Spark evaluates the filters again on the rows that the source returns, so that it
also reads the columns of the domains. An arrow-rs [`RowFilter`][row-filter], which decodes the
columns of the filters first, and the other columns only for the rows that pass, would decode these
columns twice, and when the rows that pass are scattered over the pages, the selections of rows
that it builds cost more than decoding every row. Spark's own vectorized reader also decodes every
row of the pages that it reads, and filters afterwards.

The reader returns strings and binary values as Arrow views, which refer to the values in the pages
that arrow-rs decompresses, or in their dictionaries, rather than copying each value into a buffer
of its batch; short values are in the views themselves. A value of at most 12 bytes is in its view,
followed by zeros, so the domains of single values with such strings or binary values, as `=`,
`<=>`, and `IN` give, compare the views of a column as 128-bit integers, without reading the bytes
of the column. The coalescer copies the values of the rows that pass when their pages hold mostly
the values of other rows, so that a batch does not keep the pages of the rows that the domains
dropped. The reader decodes the decimal columns that Parquet stores as INT32 or INT64 as 64-bit
decimals, which the domains compare and the coalescer copies, and widens only the rows that it
returns to the 128 bits that Spark reads. Floats compare as Spark compares them: a domain holds NaN
apart from its ranges, whose bounds are never NaN, and Arrow orders floats by the IEEE 754 total
order, in which -0.0 is less than 0.0, which Spark equates, so the reader changes -0.0 to 0.0 in the
values and in the bounds before Arrow compares them. The library allocates with
[mimalloc][mimalloc], which reuses the memory of each thread whatever the malloc settings of the
JVM's process: with a fixed mmap threshold, as `MALLOC_TRIM_THRESHOLD_` and other settings make it,
glibc maps the buffer of each page and batch, and unmaps it when it is freed.

Before it decodes, the reader skips the row groups and pages that, by their statistics, have no
value in the domain of a column, as Spark's reader does with the statistics of row groups and the
column indexes of pages: a range of rows may have a value in a domain if it may have nulls and the
domain has null, or if it has other values and its bounds meet one of the ranges of the domain. It
does not use the bounds of INT96 columns, which have no defined order, nor of floating-point
columns, whose bounds writers compute without NaN, nor of string, binary, and decimal columns in
files that define no column order, whose bounds old writers ordered by signed bytes. The page
indexes select ranges of rows, which arrow-rs reads only from the pages that hold them, also in the
other columns.

Spark pushes a limit only to a scan whose rows it does not filter; each task then stops after the
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
| `maxPartitionBytes` | `134217728` | Maximum number of bytes of files that one task reads |
| `openCostInBytes` | `4194304` | Cost of opening a file, in bytes, when packing files into tasks |
| `filterPushdown` | `true` | Whether to use the pushed domains, by statistics and per row |

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
filter pushdown. It also checks the pushed filters in the plan, a limit, and the rejection of
millisecond timestamps.

An application can read with:

```python
spark.addArtifact("sql/native/rust/target/rust_sdk_parquet.sparkpkg")
df = spark.read.format("rust_sdk_parquet").load("/absolute/shared/path/table")
df.where("k = 7").select("id", "payload").show()
```

[benchmark.py](benchmark.py) compares queries with Spark's own reader, with and without filter
pushdown, on data that it writes if the given path does not exist:

```sh
"$SPARK_HOME/bin/spark-submit" --master 'local[4]' --driver-memory 4g \
  examples/src/main/rust/native-parquet/benchmark.py \
  --package sql/native/rust/target/rust_sdk_parquet.sparkpkg \
  --data /tmp/rust-sdk-parquet-benchmark
```

With `--dataset lineitem`, it writes a table with the columns of TPC-H lineitem, whose decimals,
dates, and dictionary-encoded strings Spark computes from the row number, and runs TPC-H Q1 and Q6,
and other queries on it. Package a release build for the benchmark: a debug build is many times
slower.

## Limits of this example

The files must be on a local POSIX filesystem, or on a shared filesystem at the same absolute path
on all hosts. Object-store URLs are unsupported. The source does not discover partition directories,
merge schemas, or rebase dates and timestamps that Spark wrote in the legacy hybrid calendar. It
skips no pages by bloom filters, which Spark's reader can. It matches the names of columns by case,
and requires valid UTF-8 in string columns, which arrow-rs checks and Spark does not. It reports no
statistics to Spark, so Spark does not plan to broadcast its tables in joins without a hint. The
reader decodes the columns of a batch also for the rows that the domains drop, except in the pages
that statistics skip. Native memory is not reserved through Spark's memory manager.

[sdk]: ../../../../../sql/native/rust/README.md
[arrow-rs]: https://github.com/apache/arrow-rs
[row-filter]: https://docs.rs/parquet/58.4.0/parquet/arrow/arrow_reader/struct.RowFilter.html
[coalescer]: https://docs.rs/arrow-select/58.4.0/arrow_select/coalesce/struct.BatchCoalescer.html
[mimalloc]: https://github.com/microsoft/mimalloc
