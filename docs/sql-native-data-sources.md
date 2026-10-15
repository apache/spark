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
[`spark_datasource.h`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h), with the
messages of `spark_datasource.proto`. Spark finds the library
automatically when it is installed like other native libraries, or when a session adds it in a
package, and loads it when a query uses the data source. Spark adapts it to
[Data Source V2](sql-v2-data-sources.html), so it supports:

- batch and micro-batch streaming reads,
- batch and streaming writes, appending to or overwriting the existing data,
- predicate, column and limit pushdown.

Data is exchanged only as columnar batches, through the
[Arrow C data interface](https://arrow.apache.org/docs/format/CDataInterface.html), so Spark reads
the data that a library returns without copying it.

Spark calls native data sources with the
[Foreign Function and Memory API](https://openjdk.org/jeps/454) of Java, which is final from Java
22, so they require Java 22 or later on the driver and the executors. With an earlier version of
Java, a query that uses a native data source fails with
`NATIVE_DATA_SOURCE_UNSUPPORTED_JAVA_VERSION`.

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

A native data source library exports the six C functions of `spark_datasource.h`, which never
change: it needs no JVM headers or bindings. Everything else is in the messages of
[`spark_datasource.proto`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/protobuf/org/apache/spark/sql/datasource/spark_datasource.proto),
which Spark and the library exchange as [Protocol Buffers](https://protobuf.dev) bytes: the
library generates their code with a Protocol Buffers compiler, such as `protoc` or `prost`. The
data itself is exchanged without copying it, as Arrow C streams. Spark looks the functions up in
each library separately, so any number of native data sources can be loaded side by side.

| Function | What it does |
|----------|--------------|
| `spark_ds_open` | Opens an object, with an `OpenRequest` that says which object and carries everything that it needs, and replies with an `OpenReply` |
| `spark_ds_call` | Runs a control operation on an object, with a `CallRequest`, and replies with a `CallReply` |
| `spark_ds_stream` | Exchanges the data of an object, as Arrow C streams, with a `StreamRequest` |
| `spark_ds_close` | Releases an object |
| `spark_ds_abi_version` | Returns the version of the interface, `SPARK_DS_ABI_VERSION` |
| `spark_ds_last_error` | Returns the message of the last error on the calling thread |

A Spark distribution contains the header and the messages in its `include` directory, as
`$SPARK_HOME/include/spark_datasource.h` and `$SPARK_HOME/include/spark_datasource.proto`, and the
`spark-sql` JAR contains the header as `org/apache/spark/sql/datasource/spark_datasource.h`. They
only depend on the C standard library and on Protocol Buffers, so a library can also copy them
into its source tree, to build without Spark, for example from the source of a release:

```bash
curl -LO {{site.SPARK_GITHUB_URL}}/raw/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h
curl -LO {{site.SPARK_GITHUB_URL}}/raw/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/protobuf/org/apache/spark/sql/datasource/spark_datasource.proto
```

A library built against them works with all the releases of Spark that support its version,
`SPARK_DS_ABI_VERSION`: see [Compatibility](#compatibility).

Spark opens and uses the objects of a library as follows:

- **Data sources**: for each schema inference, scan and write, Spark opens a data source on the
  driver, with the name of the data source and the options of the user. Its `OpenReply` declares
  the capabilities of the data source, such as `CAPABILITY_BATCH_READ` or
  `CAPABILITY_STREAMING_WRITE`, and Spark reports the operations that a data source does not
  declare as unsupported. When the user does not specify a schema, Spark asks for it with
  `InferSchema`. Spark closes the data source right after.
- **Batch reads**: on the driver, Spark opens a batch reader, with the schema to read, pushes
  operations down with `PushPredicates`, `PushLimit` and `PruneColumns`, plans the partitions with
  `PlanPartitions`, and serializes the state of the reader with `Serialize`. Then, on the
  executors, it opens a partition reader for each partition, in a task, and `Read` initializes an
  `ArrowArrayStream` with the batches of the partition.
- **Streaming reads**: on the driver, Spark opens a stream reader. For each micro-batch, it asks
  for the `LatestOffset`, plans the partitions with `PlanStreamPartitions`, and reads them on the
  executors. Offsets are defined by the library, as compact JSON on a single line: Spark stores
  each offset as a line of the offset log of the checkpoint, and rejects an offset that is not
  valid JSON or contains a line break, such as pretty-printed JSON.
- **Writes**: on the driver, right before the tasks of the write (or of a micro-batch) start, Spark
  opens a writer, with the mode of the write, `WRITE_MODE_APPEND` or `WRITE_MODE_TRUNCATE`, and the
  ID of the query of a streaming write. With adaptive query execution, it first runs the shuffle
  stages of the data to write, and opens no writer if they fail, but the last stage runs in the
  tasks of the write, so a writer can still be aborted because computing the data failed: do
  irreversible work, such as removing the existing data for an overwrite, in `Commit`. On the
  executors, each task opens a data writer, and `Write` passes it an `ArrowArrayStream` with the
  data of the task, which the library reads during the call. Then the task calls
  `CommitDataWriter`, or `AbortDataWriter` if the task or the commit fails, and the driver calls
  `Commit` with the messages of the tasks, or `Abort` if the write failed. A streaming write
  commits each micro-batch with its epoch ID, and the ID of the query stays the same when the query
  restarts from its checkpoint: together, they identify the output of a micro-batch, so that a
  library can skip an epoch that it already committed. A micro-batch streaming query opens a new
  writer for each micro-batch, and Spark closes the previous one when it opens the next one.
  Because Spark is not told when a streaming query stops, the last writer of a query is closed once
  it is no longer referenced, or when the query is restarted.

The library defines the format of the partitions, of the state of its readers and writers, and of
the commit messages, which Spark sends to the executors and back as bytes. A library replies to a
function by passing the encoded reply to a `SparkDsSink` callback, which copies it, or not at all
for an empty reply, which is valid for every request: for example, a reader that does not support
pushdown replies to `PushPredicates` with nothing, and Spark pushes nothing down. A function reports
an error by returning a non-zero value, with the message returned by `spark_ds_last_error`, and
Spark reports it as a `NATIVE_DATA_SOURCE_ERROR`. A C++ exception or a Rust panic must not cross the
interface. Each object is released exactly once, with `spark_ds_close`, which cannot fail and can
run after the query, so do the work that can fail, such as flushing data, in a commit. Strings are
UTF-8, and the JSON of the predicates is ASCII, with the other characters escaped.

`CAPABILITY_TRUNCATE` declares that a writer supports `WRITE_MODE_TRUNCATE`: the save mode
`overwrite` of a batch write, and the output mode `complete` of a streaming write.

The comments of [`spark_datasource.h`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h)
and `spark_datasource.proto` specify each function and message, including the ownership of the
Arrow structs and the JSON format of the pushed predicates. `PushPredicatesReply` tells Spark how
the reader uses each predicate: not at all, only to skip data (`PREDICATE_USE_PARTIAL`), or to
only return the rows for which it is true (`PREDICATE_USE_COMPLETE`). Spark shows the predicates
that the reader uses as pushed in the plan of the query, and evaluates all the pushed predicates
again after reading either way, so a library can use them to skip data, for example files or row
groups whose statistics show that no row matches, but it must never skip a row for which the
predicates are true under the semantics of Spark SQL: NaN values are equal to each other and
greater than any other value, `-0.0` is equal to `0.0`, comparisons with null are null, and strings
compare by their UTF-8 bytes. The comparisons of IEEE 754 and the total orders of Arrow libraries,
such as the comparison kernels of arrow-rs, differ for some of these values. The jar of Spark SQL
contains test cases for a library, `org/apache/spark/sql/datasource/predicate-cases.json`:
predicates as Spark pushes them, with the rows of a table for which they are true:

```bash
unzip -p "$SPARK_HOME"/jars/spark-sql_*.jar org/apache/spark/sql/datasource/predicate-cases.json
```

The arrays that a library exports must have an offset of 0: copy a sliced array, such as the
result of `RecordBatch::slice` in arrow-rs, before exporting it. Spark also checks that each array
has at least as many values as its parent refers to, but it cannot check that the buffers of the
arrays hold those values, because the C data interface does not carry the sizes of buffers: a
library that exports a buffer that is too short makes Spark read past its end.

Partition readers and data writers get the `TaskContext` of their task, with its IDs, and the
`is_interrupted` function of `SparkDsHost` returns whether a task was killed, for example because
its query was canceled. Spark cannot interrupt native code: when a task is killed, Spark stops
reading only once the `get_next` function of the stream returns. So a read or a write that runs
for long, or blocks, for example on network I/O, should check `is_interrupted` regularly and use
timeouts. The task reaper, `spark.task.reaper.enabled` and `spark.task.reaper.killTimeout`, kills
an executor whose killed task does not end. `SparkDsHost` also has a `log` function, which writes
to the log of the driver or of the executor.

### Compatibility

The six functions of `spark_datasource.h` never change, nor do the existing fields of
`SparkDsHost`, which starts with its size and only gets fields appended: a library must only read
the fields that fit in the size of the struct that Spark passes. Everything else grows in the
messages, as Protocol Buffers messages do:

- a later release of Spark may add fields, messages and values of enums, but never changes or
  reuses the number of a field. Both sides ignore the fields that they do not know, so a library
  built against an earlier version of the messages keeps working with later releases of Spark, and
  a library built against a later version works with earlier releases, without the features that
  they do not know;
- Spark only sends a new kind of request to an object whose `OpenReply` declares that it supports
  it, so a new request never reaches an older library;
- new values may appear where the messages say so: for example, later releases may push down
  predicates with other names, which a library must ignore if it does not know them.

The version of the interface, 1, is `SPARK_DS_ABI_VERSION`, which `spark_ds_abi_version` returns
and the manifest of a package declares. Spark supports version 1 at least in all the releases of
Spark 4 from 4.4.0.

### Limitations

Version 1 of the interface does not cover these features of Data Source V2 yet. They can be added
later as fields and messages of `spark_datasource.proto`:

- statistics: Spark estimates the size of a native scan with `spark.sql.defaultSizeInBytes`, so it
  does not plan a native scan as the broadcast side of a join, unless the query has a broadcast
  hint. Adaptive query execution can still broadcast it at run time, from the size of a shuffle of
  the scan;
- custom metrics, shown in the Spark UI;
- admission control for streaming reads, such as a maximum number of rows for each micro-batch,
  and continuous streaming;
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
Spark passes the name of the data source in the requests of the library. A library that
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

- `abiVersion` is the version of `spark_datasource.h` that the library implements, currently 1.
- `dataSources` lists the names of the data sources that the library implements. The names are
  case-insensitive, and Spark passes the name in the requests of the library.
- `libraries` maps each platform, `<os>-<arch>` with an `os` of `linux`, `osx` or `windows` and an
  `arch` of `x86_64` or `aarch64`, to the path of the library in the package.

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

The packages of a session take precedence over the installed libraries. Give packages distinct
file names, for example with their version: sessions that are not isolated share the files they
add. Spark ignores, and logs, a package that it cannot read or whose version it does not
support, unless its manifest lists the data source that a query uses and no other package or
installed library provides it.

## Example in Rust

This data source reads the numbers `[0, end)` as a column `id`, in two partitions. It depends on
the `arrow-array` and `arrow-schema` crates of [arrow-rs](https://github.com/apache/arrow-rs),
which export streams with `FFI_ArrowArrayStream`, and on [prost](https://github.com/tokio-rs/prost),
whose `prost-build` generates the code of the messages from `spark_datasource.proto` when the
library is built, with `protoc`. It only opens data sources, batch readers and partition readers,
and replies to the requests that it does not support with empty replies. `Cargo.toml`, where the
name of the library is the name that Spark finds when it is installed:

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
prost = "0.14"

[build-dependencies]
prost-build = "0.14"
```

`build.rs`, which generates the messages from the `include` directory of a Spark distribution:

```rust
// Generates the code of the messages from spark_datasource.proto, in the include directory of a
// Spark distribution. prost-build runs protoc, which must be installed.
fn main() -> std::io::Result<()> {
    let include = format!("{}/include", std::env::var("SPARK_HOME").expect("SPARK_HOME"));
    prost_build::compile_protos(&[format!("{include}/spark_datasource.proto")], &[include])
}
```

`src/lib.rs`:

{% raw %}
```rust
//! A native data source for Apache Spark, written in Rust. It reads the numbers [0, end) as a
//! column `id`, in two partitions. The option `end` defaults to 10.

use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use prost::Message;

/// The messages of `spark_datasource.proto`.
mod proto {
    include!(concat!(env!("OUT_DIR"), "/spark.datasource.rs"));
}

use proto::{call_reply, call_request, data_type, open_request, stream_request};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// `SparkDsSink`: receives the reply of a function, and copies it.
type Sink = unsafe extern "C" fn(sink: *mut c_void, data: *const u8, length: i64);

/// What a handle refers to.
enum Object {
    DataSource,
    Reader { end: i64 },
    Partition { start: i64, end: i64 },
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// The schema of the data, as the Spark SQL types of the messages.
fn spark_schema() -> proto::StructType {
    let long = data_type::Kind::Long(data_type::Long {});
    proto::StructType {
        fields: vec![proto::StructField {
            name: "id".to_string(),
            data_type: Some(proto::DataType { kind: Some(long) }),
            nullable: false,
        }],
    }
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

unsafe fn decode<M: Message + Default>(request: *const u8, length: i64) -> Result<M> {
    Ok(M::decode(std::slice::from_raw_parts(request, length as usize))?)
}

unsafe fn reply(reply: Sink, sink: *mut c_void, message: &impl Message) {
    let bytes = message.encode_to_vec();
    reply(sink, bytes.as_ptr(), bytes.len() as i64);
}

#[no_mangle]
pub extern "C" fn spark_ds_abi_version() -> i32 {
    1
}

#[no_mangle]
pub extern "C" fn spark_ds_last_error() -> *const c_char {
    LAST_ERROR.with(|error| error.borrow().as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_open(
    _host: *const c_void,
    request: *const u8,
    length: i64,
    sink_function: Sink,
    sink: *mut c_void,
    handle: *mut *mut c_void,
) -> c_int {
    call(|| {
        let object = match decode::<proto::OpenRequest>(request, length)?.object {
            Some(open_request::Object::DataSource(_)) => {
                let capabilities = vec![proto::Capability::BatchRead as i32];
                reply(sink_function, sink, &proto::OpenReply { capabilities });
                Object::DataSource
            }
            Some(open_request::Object::BatchReader(reader)) => {
                let options = reader.data_source.unwrap_or_default().options;
                let end = options.get("end").map(|end| end.parse()).transpose()?;
                Object::Reader { end: end.unwrap_or(10) }
            }
            Some(open_request::Object::PartitionReader(reader)) => {
                let bytes = reader.partition;
                let start = i64::from_le_bytes(bytes[0..8].try_into()?);
                let end = i64::from_le_bytes(bytes[8..16].try_into()?);
                Object::Partition { start, end }
            }
            _ => return Err("this data source only supports batch reads".into()),
        };
        *handle = Box::into_raw(Box::new(object)) as *mut c_void;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_call(
    handle: *mut c_void,
    request: *const u8,
    length: i64,
    sink_function: Sink,
    sink: *mut c_void,
) -> c_int {
    call(|| {
        let object = &*(handle as *const Object);
        let result = match (decode::<proto::CallRequest>(request, length)?.call, object) {
            (Some(call_request::Call::InferSchema(_)), _) => {
                let schema = Some(spark_schema());
                call_reply::Reply::InferSchema(proto::InferSchemaReply { schema })
            }
            (Some(call_request::Call::PlanPartitions(_)), Object::Reader { end }) => {
                let partitions = [(0, end / 2), (end / 2, *end)]
                    .iter()
                    .map(|(start, end)| [start.to_le_bytes(), end.to_le_bytes()].concat())
                    .collect();
                call_reply::Reply::PlanPartitions(proto::PlanPartitionsReply { partitions })
            }
            // The other requests, such as the pushdown, have empty replies.
            _ => return Ok(()),
        };
        reply(sink_function, sink, &proto::CallReply { reply: Some(result) });
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_stream(
    handle: *mut c_void,
    request: *const u8,
    length: i64,
    _input: *mut FFI_ArrowArrayStream,
    output: *mut FFI_ArrowArrayStream,
) -> c_int {
    call(|| {
        let Object::Partition { start, end } = &*(handle as *const Object) else {
            return Err("not a partition reader".into());
        };
        let stream = decode::<proto::StreamRequest>(request, length)?.stream;
        let Some(stream_request::Stream::Read(_)) = stream else {
            return Err("this data source only supports reads".into());
        };
        let ids = Int64Array::from_iter_values(*start..*end);
        let batch = RecordBatch::try_new(arrow_schema(), vec![Arc::new(ids)])?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema());
        std::ptr::write(output, FFI_ArrowArrayStream::new(Box::new(batches)));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_close(handle: *mut c_void) {
    drop(Box::from_raw(handle as *mut Object));
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
streams with `arrow::ExportRecordBatchReader`, and with the code that `protoc` generates from
`spark_datasource.proto`. It includes `spark_datasource.h` and the generated messages, here from
the `include` directory of a Spark distribution.

`cpp_range.cc`:

{% raw %}
```cpp
// A native data source for Apache Spark, written in C++ with Arrow C++ and Protocol Buffers. It
// reads the numbers [0, end) as a column `id`, in two partitions. The option `end` defaults to 10.

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "spark_datasource.h"
#include "spark_datasource.pb.h"

namespace {

namespace sds = spark::datasource;

// What a handle refers to.
struct DataSource {};
struct Reader {
  int64_t end;
};
struct Partition {
  int64_t start;
  int64_t end;
};
using Object = std::variant<DataSource, Reader, Partition>;

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

template <typename M>
M Decode(const uint8_t* request, int64_t length) {
  M message;
  if (!message.ParseFromArray(request, static_cast<int>(length))) {
    throw std::runtime_error("invalid request");
  }
  return message;
}

void Reply(SparkDsSink reply, void* sink, const google::protobuf::MessageLite& message) {
  std::string bytes = message.SerializeAsString();
  reply(sink, reinterpret_cast<const uint8_t*>(bytes.data()), static_cast<int64_t>(bytes.size()));
}

}  // namespace

extern "C" {

int32_t spark_ds_abi_version(void) { return SPARK_DS_ABI_VERSION; }

const char* spark_ds_last_error(void) { return last_error.c_str(); }

int spark_ds_open(const SparkDsHost*, const uint8_t* request, int64_t length, SparkDsSink reply,
                  void* sink, void** handle) {
  return Call([&] {
    auto open = Decode<sds::OpenRequest>(request, length);
    std::unique_ptr<Object> object;
    if (open.has_data_source()) {
      sds::OpenReply capabilities;
      capabilities.add_capabilities(sds::CAPABILITY_BATCH_READ);
      Reply(reply, sink, capabilities);
      object = std::make_unique<Object>(DataSource{});
    } else if (open.has_batch_reader()) {
      const auto& options = open.batch_reader().data_source().options();
      auto end = options.find("end");
      object = std::make_unique<Object>(
          Reader{end == options.end() ? 10 : std::stoll(end->second)});
    } else if (open.has_partition_reader()) {
      int64_t bounds[2];
      std::memcpy(bounds, open.partition_reader().partition().data(), sizeof(bounds));
      object = std::make_unique<Object>(Partition{bounds[0], bounds[1]});
    } else {
      throw std::runtime_error("this data source only supports batch reads");
    }
    *handle = object.release();
  });
}

int spark_ds_call(void* handle, const uint8_t* request, int64_t length, SparkDsSink reply,
                  void* sink) {
  return Call([&] {
    auto call = Decode<sds::CallRequest>(request, length);
    sds::CallReply result;
    if (call.has_infer_schema()) {
      auto* field = result.mutable_infer_schema()->mutable_schema()->add_fields();
      field->set_name("id");
      field->mutable_data_type()->mutable_long_();
      field->set_nullable(false);
    } else if (call.has_plan_partitions()) {
      int64_t end = std::get<Reader>(*static_cast<Object*>(handle)).end;
      int64_t bounds[2][2] = {{0, end / 2}, {end / 2, end}};
      for (const auto& partition : bounds) {
        result.mutable_plan_partitions()->add_partitions(partition, sizeof(partition));
      }
    } else {
      return;  // The other requests, such as the pushdown, have empty replies.
    }
    Reply(reply, sink, result);
  });
}

int spark_ds_stream(void* handle, const uint8_t* request, int64_t length, struct ArrowArrayStream*,
                    struct ArrowArrayStream* out) {
  return Call([&] {
    if (!Decode<sds::StreamRequest>(request, length).has_read()) {
      throw std::runtime_error("this data source only supports reads");
    }
    const auto& partition = std::get<Partition>(*static_cast<Object*>(handle));
    arrow::Int64Builder ids;
    for (int64_t id = partition.start; id < partition.end; id++) Check(ids.Append(id));
    std::shared_ptr<arrow::Array> array;
    Check(ids.Finish(&array));
    auto batch = arrow::RecordBatch::Make(Schema(), array->length(), {array});
    auto batches = arrow::RecordBatchReader::Make({batch}, Schema()).ValueOrDie();
    Check(arrow::ExportRecordBatchReader(batches, out));
  });
}

void spark_ds_close(void* handle) { delete static_cast<Object*>(handle); }

}  // extern "C"
```
{% endraw %}

Generate the messages and build the library, here on macOS with Arrow C++ 24, which requires
C++20, and install it, or package it as above:

```bash
protoc --cpp_out=. -I"$SPARK_HOME/include" "$SPARK_HOME/include/spark_datasource.proto"
c++ -std=c++20 -O2 -shared -fPIC -I. -I"$SPARK_HOME/include" -I"$ARROW_HOME/include" \
  cpp_range.cc spark_datasource.pb.cc $(pkg-config --cflags --libs protobuf-lite) \
  -L"$ARROW_HOME/lib" -larrow -o libspark_datasource_cpp_range.dylib
cp libspark_datasource_cpp_range.dylib /usr/local/lib/
```

A library loaded by Spark must find its dependencies, such as `libarrow` and `libprotobuf-lite`, on
every node: link them statically, or install them on the nodes. Several native data source
libraries, or several copies of one, such as an installed library and the library of a package, can
share a process. The messages use the lite runtime of Protocol Buffers, which, unlike the full one,
does not register them in a pool of the process, where a second copy would abort the process. But
libraries built against different versions of Protocol Buffers conflict if they link it
dynamically, so link it statically, with hidden symbols.

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
