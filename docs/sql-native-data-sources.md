---
layout: global
title: Native Data Sources
displayTitle: Native Data Sources
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## Overview

A native data source is a data source implemented in native code, such as Rust or C++, without
any JVM code. It is a shared library that implements the C interface declared in the header
[`spark_datasource.h`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h). Spark finds the library
automatically when it is installed like other native libraries, or when a session adds it in a
package, and loads it when a query uses the data source. Spark adapts it to
[Data Source V2](sql-v2-data-sources.html), so it supports:

- batch and micro-batch streaming reads,
- batch and streaming writes, appending to or overwriting the existing data,
- column and limit pushdown.

Data is exchanged only as columnar batches, through the
[Arrow C data interface](https://arrow.apache.org/docs/format/CDataInterface.html), so Spark reads
the data that a library returns without copying it.

Spark calls native data sources with the
[Foreign Function and Memory API](https://openjdk.org/jeps/454) of Java, which is final from Java
22, so they require Java 22 or later on the driver and the executors. With an earlier version of
Java, a query that uses a native data source fails with
`NATIVE_DATA_SOURCE_UNSUPPORTED_JAVA_VERSION`. Spark exchanges the data with the C data interface
of Arrow Java, whose JNI library is available for Linux with glibc and macOS on x86_64 and aarch64,
and for Windows on x86_64. On other platforms, such as Linux on ppc64le or s390x, or Linux with
musl, as Alpine Linux, a query that uses a native data source fails with
`NATIVE_DATA_SOURCE_UNSUPPORTED_PLATFORM`.

### When to Use a Native Data Source

A native data source fits a source whose client exists only in native code, such as an in-house
storage system written in C++ or the C SDK of a vendor, and that Spark needs to read and write.
Such a source then needs no JVM connector: the same library works with all the releases of Spark
that support its version of the interface, and with every Spark API, including Spark Connect
clients in any language.

A native data source provides what Data Source V2 offers to data sources, except for the features
that version 1 of the interface does not cover yet, such as row-level operations: see
[Limitations](#limitations). Extensions of Spark outside of Data Source V2, such as table
functions, SQL commands and optimizer rules, are out of the scope of this interface. A source that
needs them is better served by a JVM connector, which can still read and write through a native
library with the Arrow C stream interface.

## How It Works

A native data source library exports a single C function, `spark_ds_init`, which returns a table
of function pointers, a `SparkDsLibrary`, that implements the data source: it needs no JVM headers
or bindings. Each library has its own table, so any number of native data sources can be loaded
side by side. When Spark loads a library, it calls `spark_ds_init` with a `SparkDsHost`, which
gives the library the version of Spark and a function to log messages to the log of Spark.

A Spark distribution contains the header in its `include` directory, as
`$SPARK_HOME/include/spark_datasource.h`, and the `spark-sql` JAR contains it as
`org/apache/spark/sql/datasource/spark_datasource.h`. The header only depends on the C standard
library, so a library can also copy it into its source tree, to build without Spark, for example
from the source of a release:

```bash
curl -LO {{site.SPARK_GITHUB_URL}}/raw/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h
```

A library built against the header works with all the releases of Spark that support its version,
`SPARK_DS_ABI_VERSION`: see [Compatibility](#compatibility).

The schemas are Arrow schemas, with the types of version 1 that `spark_datasource.h` lists, such
as `int64` for `bigint`, `utf8` for `string` and `timestamp[us]` for `timestamp`: Spark fails an
operation whose schema has another type, such as an interval, before it calls the library. The data
that a library returns must agree with the nullability of the schema to read.

Spark calls the functions of the table as follows:

- **Data sources**: for each schema inference, scan and write, Spark creates a data source on the
  driver with `create`, which receives the name of the data source and the options of the user in
  a `SparkDsCreateInfo`, and returns the capabilities of the data source, such as
  `SPARK_DS_BATCH_READ` or `SPARK_DS_STREAMING_WRITE`, and closes it right after with `close`.
  Spark reports the operations that a data source does not declare as unsupported. It also creates
  a data source with the options of the table while it analyzes a query, to reject a batch write
  that the data source does not support before it computes the data to write. When the user does
  not specify a schema, Spark asks for it with `schema`.
- **Batch reads**: on the driver, Spark creates a reader with `create_reader`, pushes operations
  down with `push_limit` and `prune_columns`, and plans the partitions with `partitions`, which
  also returns the state of the reader. Then it calls `read` on the executors, in one task for each
  partition, with a `SparkDsReadInfo` that has the partition, the schema to read and the state of
  the reader, and `read` exports an `ArrowArrayStream`. Its callbacks can be called from any
  thread, but never concurrently.
- **Streaming reads**: on the driver, Spark creates a stream reader with `create_stream_reader`,
  which gets the columns to read, which may be only those that the query needs, and the location of
  the checkpoint of the source in a `SparkDsStreamReaderInfo`. When the checkpoint does not have the
  initial offset of the source yet, Spark asks for it with `initial_offset`, and keeps it in the
  checkpoint, so a query that restarts before its first micro-batch is committed starts from the
  same offset. For each micro-batch, or only once, before the first micro-batch that it plans, with
  `Trigger.AvailableNow`, it asks for the `latest_offset`, with the offset that the micro-batch
  starts after, plans the partitions with `stream_partitions`, which also returns the state of the
  stream reader, and calls `read` on the executors. Offsets are defined by the library, as compact
  JSON on a single line: Spark stores each offset as a line of the offset log of the checkpoint,
  and rejects an offset that is not valid JSON or contains a line break, such as pretty-printed
  JSON. Spark keeps what it needs in the checkpoint of the source under the names that start with
  `_spark`, which the library must not use.
- **Writes**: on the driver, right before the tasks of the write (or of a micro-batch) start, Spark
  creates a writer with `create_writer`, which gets a `SparkDsWriteInfo` with the mode of the write,
  `SPARK_DS_WRITE_APPEND` or `SPARK_DS_WRITE_TRUNCATE`, and the ID of the query of a streaming
  write, and returns the state of the writer. With adaptive query execution, it first runs the
  shuffle stages of the data to write, and creates no writer if they fail, but the last stage runs
  in the tasks of the write, so a writer can still be aborted because computing the data failed: do
  irreversible work, such as removing the existing data for an overwrite, in `commit`. On the
  executors, each task creates a data writer with `create_data_writer`, which gets the schema of the
  data and the state of the writer in a `SparkDsDataWriterInfo`, writes the batches of its data with
  `write`, and calls `commit_data_writer`, or `abort_data_writer` if the task or the commit fails.
  Then the driver calls `commit` with the messages of the tasks, or `abort` if the write failed.
  When Spark rejects what `create_writer` returned, such as a state set more than once, it calls
  `abort` with the field `rejected` of `SparkDsFinishWriteInfo` set to 1, and an epoch ID of -1 also
  for a streaming write, before `close_writer`. A streaming write commits each micro-batch with its
  epoch ID, and the ID of the query stays the same when the query restarts from its checkpoint:
  together, they identify the output of a micro-batch, so that a library can skip an epoch that it
  already committed. A micro-batch streaming query creates a new stream writer for each micro-batch,
  and Spark closes the previous one when it creates the next one. Because Spark is not told when a
  streaming query stops, the last stream writer of a query is closed once it is no longer
  referenced, or when the query is restarted.

The library defines the format of the partitions, of the state of its readers and writers, and of
the commit messages, which Spark sends to the executors and back as byte arrays. The library
returns them, and the offsets, by passing them to a `SparkDsSink` callback, which copies them. A
function reports an error by returning a non-zero value, with the message returned by
`last_error`, and Spark reports it as a `NATIVE_DATA_SOURCE_ERROR`. A C++ exception or a Rust panic
must not cross the interface. Each handle that a `create` function returns is released exactly
once, by the `close` function of its kind. The close functions cannot fail, and they can run after
the query, so do the work that can fail, such as flushing data, in `commit`. Strings are UTF-8.

A library only sets the functions of the operations it supports, and leaves the others `NULL`. A
data source must not declare a capability whose functions are `NULL`:

| Capability | Required functions | Optional functions |
|-----------|--------------------|--------------------|
| All | `create`, `close` | `last_error`, `schema` |
| `SPARK_DS_BATCH_READ` | `create_reader`, `partitions`, `read`, `close_reader` | `push_limit`, `prune_columns` |
| `SPARK_DS_MICRO_BATCH_READ` | `create_stream_reader`, `initial_offset`, `latest_offset`, `stream_partitions`, `read`, `close_stream_reader` | `commit_offset` |
| `SPARK_DS_BATCH_WRITE`, `SPARK_DS_STREAMING_WRITE` | `create_writer`, `commit`, `abort`, `close_writer`, `create_data_writer`, `write`, `commit_data_writer`, `abort_data_writer`, `close_data_writer` | |

`SPARK_DS_BATCH_TRUNCATE` declares that a data source supports `SPARK_DS_WRITE_TRUNCATE` in batch
writes, for the save mode `overwrite`, and `SPARK_DS_STREAMING_TRUNCATE` in streaming writes, for
the output mode `complete`.

The comments of [`spark_datasource.h`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h)
specify each function, including the ownership of the Arrow structs.

The arrays that a library exports must have an offset of 0: copy a sliced array, such as the
result of `RecordBatch::slice` in arrow-rs, before exporting it. A batch must have no null rows,
since Arrow Java ignores the validity of the struct of a batch: mark the values of its columns as
null instead. Spark also checks that each array has at least as many values as its parent refers
to, but it cannot check that the buffers of the arrays hold those values, because the C data
interface does not carry the sizes of buffers: a library that exports a buffer that is too short
makes Spark read past its end.

`read` and `create_data_writer` get a `SparkDsTaskContext`, with the IDs of the task, its
attempt, and the partition: for `read`, the index of the partition in the list that the planning
returned, which is unique within a scan but not across the scans that a task reads, and for
`create_data_writer`, the partition of the task. It also has an `is_interrupted` function that
returns whether the task was killed, for example because its query was canceled. Spark cannot
interrupt native code: when a task is killed, Spark stops reading only once the `get_next` function
of the stream returns. So a read or a write that runs for long, or blocks, for example on network
I/O, should check `is_interrupted` regularly and use timeouts. The task reaper,
`spark.task.reaper.enabled` and `spark.task.reaper.killTimeout`, kills an executor whose killed
task does not end.

### Compatibility

The function table, `SparkDsLibrary`, and the structs that Spark passes to the library,
`SparkDsHost`, `SparkDsTaskContext` and those whose names end with `Info`, such as
`SparkDsCreateInfo`, start with their size, so that they can grow. The version of the interface,
1, is `SPARK_DS_ABI_VERSION`, which the table and the manifest of a package declare. Within a
version:

- fields are only appended to these structs, and the existing fields and the semantics of the
  existing functions never change;
- Spark only reads the functions that fit in the size of the table of a library, so a library built
  against an earlier header keeps working with later releases of Spark, with the functions that it
  does not know as `NULL`. A library must only read the fields that fit in the size of a struct
  that Spark passes, so that it also runs on earlier releases of Spark that support its version;
- Spark only uses a new capability, write mode or kind of read with a data source that declares
  it.

An incompatible change comes with a new version: `spark_ds_init` gets the newest version that Spark
supports, in `SparkDsHost`, and returns a table of a version that the library implements. Spark
supports version 1 at least in all the releases of Spark 4 from 4.4.0.

### Limitations

Version 1 of the interface does not cover these features of Data Source V2 yet. They can be added
later as functions or fields appended to the structs of the interface:

- predicate pushdown: Spark filters the rows of a native scan after reading them, so a library
  cannot use the filters of a query to skip data;
- statistics: Spark estimates the size of a native scan with `spark.sql.defaultSizeInBytes`, so it
  does not plan a native scan as the broadcast side of a join, unless the query has a broadcast
  hint. Adaptive query execution can still broadcast it at run time, from the size of a shuffle of
  the scan;
- custom metrics, shown in the Spark UI;
- admission control for streaming reads, such as a maximum number of rows for each micro-batch;
- continuous processing (`Trigger.Continuous`): a continuous query that reads from or writes to a
  native data source fails when it starts;
- the pushdown of aggregates and of top-N queries (`ORDER BY` with `LIMIT`), and reporting the
  partitioning and the sort order of the data, with which Spark avoids shuffles and sorts, as in
  storage-partitioned joins;
- row-level operations, `UPDATE`, `DELETE` and `MERGE INTO`, and metadata columns;
- memory accounting: see below.

Extensions of Spark outside of Data Source V2, such as table functions, SQL commands and optimizer
rules, are out of the scope of this interface: see
[When to Use a Native Data Source](#when-to-use-a-native-data-source).

### Memory

Spark does not manage the memory of native data sources. The memory that a library allocates, and
the Arrow buffers that it exports, which Spark reads without copying them, are not accounted by the
memory manager of Spark, nor limited by `spark.memory.offHeap.size`. Neither are the Arrow batches
that Spark writes, which end at `spark.sql.execution.arrow.maxRecordsPerBatch` rows or once they
hold `spark.sql.execution.arrow.maxBytesPerBatch` bytes, while the size of the batches that a
library reads is up to the library. This memory comes out of the memory overhead of the driver and
the executors, `spark.driver.memoryOverhead` and `spark.executor.memoryOverhead`, which are 10% of
their memory, and at least 384 MiB, by default. Increase it for libraries that use much memory, and
bound the memory that a library uses, for example with the size of its batches: on YARN or
Kubernetes, an executor that uses more memory than it requested is killed.

### Native Execution Engines

An engine that runs queries in native code within Spark, such as Apache Gluten or Apache
DataFusion Comet, can read a native data source without Spark importing its data on the JVM. The
reader factory of a batch or micro-batch scan of a native data source implements
`org.apache.spark.sql.connector.read.SupportsArrowStreamRead`: in the task that reads a partition,
`openArrowStream` initializes an Arrow C stream that the engine allocated with the stream that
`read` returns. The engine reads the stream in the task that opened it, and owns and releases the
stream, which it may do after the task ended: Spark frees the `SparkDsTaskContext` of the read
once the library released its stream. Once the task ended, `is_interrupted` returns true, so the
library may stop reading then. The callbacks of the stream can be called from any thread, but
never concurrently. They call into the JVM, so the engine releases the stream before the JVM
exits. Spark does not check the arrays of such a stream, unlike the streams that it reads itself,
so the engine checks what its import of the arrays requires.

## Finding Native Data Sources

When no Java or Python data source has the name that a query uses, Spark looks for a native data
source with that name: in the packages of the session, and then in the installed libraries. A Java
data source with the same name takes precedence over a native one, and so does a Python data
source.

### Installed Libraries

Like a Python data source installed in the Python path, a native data source library installed
like other native libraries is found automatically, without any configuration. The library is
named after the data source, in lower case, with the prefix `spark_datasource_`: the library of
`rust_range` is `libspark_datasource_rust_range.so` on Linux,
`libspark_datasource_rust_range.dylib` on macOS and `spark_datasource_rust_range.dll` on Windows.
Spark passes the name of the data source to the `create` function of the library. A library that
implements several data sources is installed under each of their names, for example with symbolic
links.

Spark looks for the library in the native library path, which consists of, in order:

1. the directories of `java.library.path`, where the JVM finds native libraries. They include
   `LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH` on macOS and `PATH` on Windows, which also
   contain the directories set with `spark.driver.extraLibraryPath` and
   `spark.executor.extraLibraryPath`.
2. the `lib` directory of each installation prefix in `PATH`: `<prefix>/lib` for each directory
   `<prefix>/bin` in `PATH`.

So Spark finds the libraries where native libraries are usually installed:

- `/usr/local/lib`, the default directory of `make install`, CMake (`cmake --install`) and
  [cargo-c](https://github.com/lu-zero/cargo-c) (`cargo cinstall`), and of Homebrew on Intel macs;
- `/opt/homebrew/lib`, for Homebrew on Apple silicon;
- `$CONDA_PREFIX/lib` and `$VIRTUAL_ENV/lib`, in an activated conda environment or Python virtual
  environment;
- `/usr/lib`, for the packages of a Linux distribution.

Cargo does not install libraries: copy the library that `cargo build --release` builds into one of
these directories. A library in any other directory is found with `LD_LIBRARY_PATH`, or with
`spark.driver.extraLibraryPath` and `spark.executor.extraLibraryPath`.

The executors load the library from the same path as the driver, or else find it in their own
native library path, which they get from their own environment, such as the `PATH` of the
executor processes. So install the library on every node, for example in the container image of
the cluster. An executor only loads the same build of the library as the driver, which it checks
with the SHA-256 checksum of the file, because the partitions and the state that the driver
serializes are only meant for that build: a query fails with
`NATIVE_DATA_SOURCE_LIBRARY_NOT_FOUND` on a node where a different build is installed.

A JVM loads a library from a path only once. So after a new build is installed at the same path,
the queries of an application that loaded the previous build fail with
`INVALID_NATIVE_DATA_SOURCE_LIBRARY.REPLACED`, on the driver or on the executors that loaded it,
rather than running mixed builds: restart the application to use the new build.

### Packages

A native data source package distributes a library with a session, for one or more platforms. It
is a zip file with the extension `.sparkpkg`, which contains a manifest named
`spark-native-datasource.json` and the libraries:

```json
{
  "abiVersion": 1,
  "dataSources": ["rust_range"],
  "libraries": {
    "linux-x86_64": "linux-x86_64/libspark_datasource_rust_range.so",
    "linux-aarch64": "linux-aarch64/libspark_datasource_rust_range.so",
    "osx-aarch64": "osx-aarch64/libspark_datasource_rust_range.dylib"
  }
}
```

- `abiVersion` is the oldest version of `spark_datasource.h` that the library implements,
  currently 1. Spark uses a package whose `abiVersion` it supports, or is older than the newest
  version that it supports, and `spark_ds_init` returns a table of a version that both support.
  Spark ignores the fields of the manifest that it does not know, so that later releases can add
  fields.
- `dataSources` lists the names of the data sources that the library implements. The names are
  case-insensitive, and Spark passes the name to `create`.
- `libraries` maps each platform, `<os>-<arch>` with an `os` of `linux`, `osx` or `windows` and an
  `arch` of `x86_64` or `aarch64`, to the path of the library in the package. `linux` is Linux with
  glibc: Linux with musl, as Alpine Linux, is `linux-<arch>-musl`, such as `linux-x86_64-musl`, on
  which native data sources do not run yet.

A session finds the packages in two places, and the executors get them automatically:

- **Artifacts of a session**: `spark.addArtifact` adds a package to a session, in Scala, Java and
  Python, and with Spark Connect:

  ```python
  spark.addArtifact("/path/to/rust_range.sparkpkg")
  spark.read.format("rust_range").option("end", 100).load().show()
  ```

- **Configured paths**: the packages listed in `spark.sql.dataSource.native.paths`, or in the
  directories it lists, on the driver. A package can be replaced in place: the next queries use
  the new version.

The packages of a session take precedence over the installed libraries, and the package that a
session added last over its other packages, also the configured ones: a session cannot remove an
artifact, or add a file with the same name again, so it upgrades a package by adding the new
version under another file name, such as `rust_range-1.1.sparkpkg`. Distinct file names are also
needed because the sessions that are not isolated share the files they add. A query fails if more
than one configured package provides its data source. Spark ignores, and logs, a package that it
cannot read or whose version it does not support, unless its manifest lists the data source that a
query uses and no other package or installed library provides it.

## Example in Rust

This data source reads the numbers `[0, end)` as a column `id`, in two partitions. It only depends
on the `arrow-array` and `arrow-schema` crates of [arrow-rs](https://github.com/apache/arrow-rs),
which export schemas and streams with `FFI_ArrowSchema` and `FFI_ArrowArrayStream`. It declares
`SparkDsLibrary` as a `#[repr(C)]` struct up to `read`, the last function that it implements, since
Spark only reads the functions that fit in the size of the table, and leaves the functions that it
does not implement `None`. The fields must follow the order of `spark_datasource.h`, which
[bindgen](https://github.com/rust-lang/rust-bindgen) can also generate from the header.
`Cargo.toml`, where the name of the library is the name that Spark finds when it is installed:

```toml
[package]
name = "rust_range"
version = "0.1.0"
edition = "2021"

[lib]
name = "spark_datasource_rust_range"
crate-type = ["cdylib"]

[dependencies]
arrow-array = { version = "58", features = ["ffi"] }
arrow-schema = { version = "58", features = ["ffi"] }
```

`src/lib.rs`:

{% raw %}
```rust
//! A native data source for Apache Spark, written in Rust. It reads the numbers [0, end) as a
//! column `id`, in two partitions. The option `end` defaults to 10.

use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// `SparkDsSink`: receives a byte array returned to Spark, and copies it.
type Sink = unsafe extern "C" fn(sink: *mut c_void, data: *const u8, length: i64);

/// A function of `SparkDsLibrary` that this data source does not implement, left NULL.
type Unused = Option<unsafe extern "C" fn()>;

/// `SparkDsCreateInfo` of `spark_datasource.h`: the name and the options of a data source.
#[repr(C)]
pub struct SparkDsCreateInfo {
    size: i64,
    name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    num_options: i64,
}

/// `SparkDsReadInfo` of `spark_datasource.h`: a partition to read, with the schema to read and
/// the state of the reader.
#[repr(C)]
pub struct SparkDsReadInfo {
    size: i64,
    kind: i32,
    schema: *mut FFI_ArrowSchema,
    reader_state: *const u8,
    reader_state_length: i64,
    partition: *const u8,
    partition_length: i64,
}

/// `SparkDsLibrary` of `spark_datasource.h`, up to `read`, the last function that this data
/// source implements: Spark only reads the functions that fit in `size`.
#[repr(C)]
pub struct SparkDsLibrary {
    size: i64,
    abi_version: i32,
    last_error: unsafe extern "C" fn() -> *const c_char,
    create: unsafe extern "C" fn(
        info: *const SparkDsCreateInfo,
        capabilities: *mut u64,
        source: *mut *mut c_void,
    ) -> c_int,
    schema: unsafe extern "C" fn(source: *mut c_void, out: *mut FFI_ArrowSchema) -> c_int,
    create_reader: unsafe extern "C" fn(
        source: *mut c_void,
        schema: *mut FFI_ArrowSchema,
        info: *const c_void,
        reader: *mut *mut c_void,
    ) -> c_int,
    create_stream_reader: Unused,
    create_writer: Unused,
    close: unsafe extern "C" fn(source: *mut c_void),
    push_limit: Unused,
    prune_columns: Unused,
    partitions: unsafe extern "C" fn(
        reader: *mut c_void,
        info: *const c_void,
        add_partition: Sink,
        set_state: Sink,
        sink: *mut c_void,
    ) -> c_int,
    close_reader: unsafe extern "C" fn(reader: *mut c_void),
    initial_offset: Unused,
    latest_offset: Unused,
    stream_partitions: Unused,
    commit_offset: Unused,
    close_stream_reader: Unused,
    read: unsafe extern "C" fn(
        task: *const c_void,
        info: *const SparkDsReadInfo,
        out: *mut FFI_ArrowArrayStream,
    ) -> c_int,
}

const SPARK_DS_BATCH_READ: u64 = 1;

static LIBRARY: SparkDsLibrary = SparkDsLibrary {
    size: std::mem::size_of::<SparkDsLibrary>() as i64,
    abi_version: 1,
    last_error,
    create,
    schema,
    create_reader,
    create_stream_reader: None,
    create_writer: None,
    close,
    push_limit: None,
    prune_columns: None,
    partitions,
    close_reader,
    initial_offset: None,
    latest_offset: None,
    stream_partitions: None,
    commit_offset: None,
    close_stream_reader: None,
    read,
};

/// The function that Spark calls when it loads the library.
#[no_mangle]
pub unsafe extern "C" fn spark_ds_init(
    _host: *const c_void,
    library: *mut *const SparkDsLibrary,
) -> c_int {
    *library = &LIBRARY;
    0
}

struct RangeSource {
    end: i64,
}

struct RangeReader {
    end: i64,
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// Runs `f`, and reports its error or panic to Spark: neither may cross into the JVM.
fn call(f: impl FnOnce() -> Result<()>) -> c_int {
    let message = match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => return 0,
        Ok(Err(e)) => e.to_string(),
        Err(_) => "the data source panicked".to_string(),
    };
    LAST_ERROR.with(|error| *error.borrow_mut() = CString::new(message).unwrap_or_default());
    1
}

unsafe extern "C" fn last_error() -> *const c_char {
    LAST_ERROR.with(|error| error.borrow().as_ptr())
}

unsafe extern "C" fn create(
    info: *const SparkDsCreateInfo,
    capabilities: *mut u64,
    source: *mut *mut c_void,
) -> c_int {
    call(|| {
        let info = &*info;
        let mut range = RangeSource { end: 10 };
        for i in 0..info.num_options as usize {
            if CStr::from_ptr(*info.option_keys.add(i)).to_str()? == "end" {
                range.end = CStr::from_ptr(*info.option_values.add(i)).to_str()?.parse()?;
            }
        }
        *capabilities = SPARK_DS_BATCH_READ;
        *source = Box::into_raw(Box::new(range)) as *mut c_void;
        Ok(())
    })
}

unsafe extern "C" fn schema(_source: *mut c_void, out: *mut FFI_ArrowSchema) -> c_int {
    call(|| {
        std::ptr::write(out, FFI_ArrowSchema::try_from(arrow_schema().as_ref())?);
        Ok(())
    })
}

unsafe extern "C" fn create_reader(
    source: *mut c_void,
    _schema: *mut FFI_ArrowSchema,
    _info: *const c_void,
    reader: *mut *mut c_void,
) -> c_int {
    call(|| {
        let end = (*(source as *const RangeSource)).end;
        *reader = Box::into_raw(Box::new(RangeReader { end })) as *mut c_void;
        Ok(())
    })
}

unsafe extern "C" fn close(source: *mut c_void) {
    drop(Box::from_raw(source as *mut RangeSource));
}

/// Plans two partitions. It needs nothing from its `SparkDsPartitionsInfo`, and the reader has no
/// state, so it does not call `set_state`.
unsafe extern "C" fn partitions(
    reader: *mut c_void,
    _info: *const c_void,
    add_partition: Sink,
    _set_state: Sink,
    sink: *mut c_void,
) -> c_int {
    call(|| {
        let end = (*(reader as *const RangeReader)).end;
        for (start, end) in [(0, end / 2), (end / 2, end)] {
            let bytes = [start.to_le_bytes(), end.to_le_bytes()].concat();
            add_partition(sink, bytes.as_ptr(), bytes.len() as i64);
        }
        Ok(())
    })
}

unsafe extern "C" fn close_reader(reader: *mut c_void) {
    drop(Box::from_raw(reader as *mut RangeReader));
}

unsafe extern "C" fn read(
    _task: *const c_void,
    info: *const SparkDsReadInfo,
    out: *mut FFI_ArrowArrayStream,
) -> c_int {
    call(|| {
        let info = &*info;
        let bytes = std::slice::from_raw_parts(info.partition, info.partition_length as usize);
        let start = i64::from_le_bytes(bytes[0..8].try_into()?);
        let end = i64::from_le_bytes(bytes[8..16].try_into()?);
        let ids = Int64Array::from_iter_values(start..end);
        let batch = RecordBatch::try_new(arrow_schema(), vec![Arc::new(ids)])?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema());
        std::ptr::write(out, FFI_ArrowArrayStream::new(Box::new(batches)));
        Ok(())
    })
}
```
{% endraw %}

Build the library, here on macOS, and install it:

```bash
cargo build --release
cp target/release/libspark_datasource_rust_range.dylib /usr/local/lib/
```

Spark then finds the data source `rust_range` automatically:

```python
spark.read.format("rust_range").option("end", 100).load().show()
```

Or package it, to add it to a session with `spark.addArtifact`:

```bash
mkdir -p package/osx-aarch64
cp target/release/libspark_datasource_rust_range.dylib package/osx-aarch64/
cat > package/spark-native-datasource.json <<EOF
{
  "abiVersion": 1,
  "dataSources": ["rust_range"],
  "libraries": {"osx-aarch64": "osx-aarch64/libspark_datasource_rust_range.dylib"}
}
EOF
(cd package && zip -r ../rust_range.sparkpkg .)
```

## Example in C++

The same data source in C++, with [Arrow C++](https://arrow.apache.org/docs/cpp/), which exports
schemas and streams with `arrow::ExportSchema` and `arrow::ExportRecordBatchReader`. It includes
`spark_datasource.h`, here from the `include` directory of a Spark distribution, and fills
`SparkDsLibrary` with designated initializers, which leave the other functions `NULL`.

`cpp_range.cc`:

{% raw %}
```cpp
// A native data source for Apache Spark, written in C++ with Arrow C++. It reads the numbers
// [0, end) as a column `id`, in two partitions. The option `end` defaults to 10.

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "spark_datasource.h"

namespace {

struct RangeSource {
  int64_t end = 10;
};

struct RangeReader {
  int64_t end;
};

thread_local std::string last_error;

std::shared_ptr<arrow::Schema> Schema() {
  return arrow::schema({arrow::field("id", arrow::int64(), /*nullable=*/false)});
}

// Runs `f`, and reports its error to Spark: no C++ exception may cross into the JVM.
template <typename F>
int Call(F&& f) {
  try {
    f();
    return 0;
  } catch (const std::exception& e) {
    last_error = e.what();
    return 1;
  } catch (...) {
    last_error = "unknown error";
    return 1;
  }
}

void Check(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

const char* LastError() { return last_error.c_str(); }

int Create(const SparkDsCreateInfo* info, uint64_t* capabilities, void** source) {
  return Call([&] {
    auto range = std::make_unique<RangeSource>();
    for (int64_t i = 0; i < info->num_options; i++) {
      if (std::string(info->option_keys[i]) == "end") {
        range->end = std::stoll(info->option_values[i]);
      }
    }
    *capabilities = SPARK_DS_BATCH_READ;
    *source = range.release();
  });
}

int GetSchema(void*, struct ArrowSchema* out) {
  return Call([&] { Check(arrow::ExportSchema(*Schema(), out)); });
}

int CreateReader(void* source, struct ArrowSchema*, const SparkDsReaderInfo*, void** reader) {
  return Call([&] { *reader = new RangeReader{static_cast<RangeSource*>(source)->end}; });
}

void Close(void* source) { delete static_cast<RangeSource*>(source); }

// Plans two partitions. It needs nothing from its info, and the reader has no state, so it does
// not call set_state.
int Partitions(void* reader, const SparkDsPartitionsInfo*, SparkDsSink add_partition, SparkDsSink,
               void* sink) {
  return Call([&] {
    int64_t end = static_cast<RangeReader*>(reader)->end;
    int64_t bounds[2][2] = {{0, end / 2}, {end / 2, end}};
    for (const auto& partition : bounds) {
      add_partition(sink, reinterpret_cast<const uint8_t*>(partition), sizeof(partition));
    }
  });
}

void CloseReader(void* reader) { delete static_cast<RangeReader*>(reader); }

int Read(const SparkDsTaskContext*, const SparkDsReadInfo* info, struct ArrowArrayStream* out) {
  return Call([&] {
    int64_t bounds[2];
    std::memcpy(bounds, info->partition, sizeof(bounds));
    arrow::Int64Builder ids;
    for (int64_t id = bounds[0]; id < bounds[1]; id++) Check(ids.Append(id));
    std::shared_ptr<arrow::Array> array;
    Check(ids.Finish(&array));
    auto batch = arrow::RecordBatch::Make(Schema(), array->length(), {array});
    auto batches = arrow::RecordBatchReader::Make({batch}, Schema()).ValueOrDie();
    Check(arrow::ExportRecordBatchReader(batches, out));
  });
}

// The functions of the data source. The ones that it does not implement are NULL.
const SparkDsLibrary kLibrary = {
    .size = sizeof(SparkDsLibrary),
    .abi_version = SPARK_DS_ABI_VERSION,
    .last_error = LastError,
    .create = Create,
    .schema = GetSchema,
    .create_reader = CreateReader,
    .close = Close,
    .partitions = Partitions,
    .close_reader = CloseReader,
    .read = Read,
};

}  // namespace

extern "C" SPARK_DS_EXPORT int spark_ds_init(const SparkDsHost*, const SparkDsLibrary** library) {
  *library = &kLibrary;
  return 0;
}
```
{% endraw %}

Build the library, here on macOS with Arrow C++ 24, which requires C++20, and install it, or
package it as above:

```bash
c++ -std=c++20 -O2 -shared -fPIC -I"$SPARK_HOME/include" -I"$ARROW_HOME/include" \
  -o libspark_datasource_cpp_range.dylib cpp_range.cc -L"$ARROW_HOME/lib" -larrow
cp libspark_datasource_cpp_range.dylib /usr/local/lib/
```

A library loaded by Spark must find its dependencies, such as `libarrow`, on every node: link them
statically, or install them on the nodes.

## Configuration

| Property Name | Default | Meaning |
|---------------|---------|---------|
| `spark.sql.dataSource.native.enabled` | true | Whether Spark loads native data sources. It is a static configuration, so it can only be set when the Spark application starts. |
| `spark.sql.dataSource.native.paths` | (none) | Comma-separated list of native data source packages, and of directories that contain them, on the local file system of the driver. They take precedence over the installed libraries. |

## Security

A native data source runs native code in the driver and executor processes, with their
privileges, and is not sandboxed. Spark finds installed libraries by their file name, in the
directories of the native library path, and loads one when a query uses its name and no Java or
Python data source has that name, much like it imports the Python data sources installed as
`pyspark_*` packages. So only install libraries and add packages that you trust, and do not let
untrusted users write to the directories of the native library path, or to the directories in
`spark.sql.dataSource.native.paths`. A user who can add artifacts to a session, for example a Spark
Connect client, can also add a package, as they can add JAR files. Set
`spark.sql.dataSource.native.enabled` to false to prevent loading native data sources, for example
on a server shared by several users.
