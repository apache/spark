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
any JVM code. It is a shared library that exports the C functions declared in the header
[`spark_datasource.h`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h). Spark finds the library
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

## How It Works

A native data source library exports the functions of `spark_datasource.h`, such as
`spark_ds_create`, as plain C functions: it needs no JVM headers or bindings. Spark looks the
functions up in each library separately, so any number of native data sources can be loaded side
by side, even though they export the same functions.

Spark calls the functions of the library as follows:

- **Data sources**: for each schema inference, scan and write, Spark creates a data source on the
  driver with `spark_ds_create`, which receives the options of the user, and closes it right after
  with `spark_ds_close`. When the user does not specify a schema, Spark asks for it with
  `spark_ds_schema`.
- **Batch reads**: on the driver, Spark creates a reader with `spark_ds_create_reader`, pushes
  operations down with `spark_ds_push_predicates`, `spark_ds_push_limit` and
  `spark_ds_prune_columns`, plans the partitions with `spark_ds_partitions`, and serializes the
  state of the reader with `spark_ds_serialize_reader`. Then it calls `spark_ds_read` on the
  executors, in one task for each partition, and `spark_ds_read` exports an `ArrowArrayStream`.
- **Streaming reads**: on the driver, Spark creates a stream reader with
  `spark_ds_create_stream_reader`. For each micro-batch, it asks for the `spark_ds_latest_offset`,
  plans the partitions with `spark_ds_stream_partitions`, and calls `spark_ds_read` on the
  executors. Offsets are defined by the library, as compact JSON on a single line: Spark stores
  each offset as a line of the offset log of the checkpoint, and rejects an offset that is not
  valid JSON or contains a line break, such as pretty-printed JSON.
- **Writes**: on the driver, right before the tasks of the write (or of a micro-batch) start, Spark
  creates a writer with `spark_ds_create_writer` or `spark_ds_create_stream_writer`. With adaptive
  query execution, it first runs the shuffle stages of the data to write, and creates no writer if
  they fail, but the last stage runs in the tasks of the write, so a writer can still be aborted
  because computing the data failed: do irreversible work, such as removing the existing data for
  an overwrite, in `spark_ds_commit`. On the executors, each task creates a data writer with
  `spark_ds_create_data_writer`, writes the batches of its data with `spark_ds_write`, and calls
  `spark_ds_commit_data_writer`, or `spark_ds_abort_data_writer` if the task or the commit fails.
  Then the driver calls `spark_ds_commit` with the messages of the tasks, or `spark_ds_abort` if
  the write failed. A streaming write commits each micro-batch with its epoch ID, and
  `spark_ds_create_stream_writer` gets the ID of the query, which stays the same when the query
  restarts from its checkpoint: together, they identify the output of a micro-batch, so that a
  library can skip an epoch that it already committed. A micro-batch streaming query creates a new
  stream writer for each micro-batch, and Spark closes the previous one when it creates the next
  one. Because Spark is not told when a streaming query stops, the last stream writer of a query is
  closed once it is no longer referenced, or when the query is restarted.

The library defines the format of the partitions, of the state of its readers and writers, and of
the commit messages, which Spark sends to the executors and back as byte arrays. The library
returns them, and the offsets, by passing them to a `SparkDsSink` callback, which copies them. A
function reports an error by returning a non-zero value, with the message returned by
`spark_ds_last_error`, and Spark reports it as a `NATIVE_DATA_SOURCE_ERROR`. A C++ exception or a
Rust panic must not cross the interface. The close functions cannot fail, and they can run after
the query, so do the work that can fail, such as flushing data, in `spark_ds_commit`. Strings are
UTF-8, and the JSON of the predicates is ASCII, with the other characters escaped.

A library only exports the functions of the operations it supports. If a function is missing,
Spark treats the operation as unsupported, except for the optional functions:

| Operation | Required functions | Optional functions |
|-----------|--------------------|--------------------|
| All | `spark_ds_abi_version`, `spark_ds_create`, `spark_ds_close` | `spark_ds_last_error`, `spark_ds_schema` |
| Batch read | `spark_ds_create_reader`, `spark_ds_partitions`, `spark_ds_read`, `spark_ds_close_reader` | `spark_ds_push_predicates`, `spark_ds_push_limit`, `spark_ds_prune_columns`, `spark_ds_serialize_reader` |
| Streaming read | `spark_ds_create_stream_reader`, `spark_ds_initial_offset`, `spark_ds_latest_offset`, `spark_ds_stream_partitions`, `spark_ds_read`, `spark_ds_close_stream_reader` | `spark_ds_serialize_stream_reader`, `spark_ds_commit_offset` |
| Batch write | `spark_ds_create_writer`, `spark_ds_commit`, `spark_ds_abort`, `spark_ds_close_writer`, `spark_ds_create_data_writer`, `spark_ds_write`, `spark_ds_commit_data_writer`, `spark_ds_abort_data_writer` | `spark_ds_serialize_writer` |
| Streaming write | `spark_ds_create_stream_writer`, `spark_ds_commit`, `spark_ds_abort`, `spark_ds_close_writer`, `spark_ds_create_data_writer`, `spark_ds_write`, `spark_ds_commit_data_writer`, `spark_ds_abort_data_writer` | `spark_ds_serialize_writer` |

The comments of [`spark_datasource.h`]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/sql/core/src/main/resources/org/apache/spark/sql/datasource/spark_datasource.h)
specify each function, including the ownership of the Arrow structs and the JSON format of the
pushed predicates. Spark evaluates all the pushed predicates again after reading, so a library can
use them to skip data, for example files or row groups whose statistics show that no row matches,
but it must never skip a row for which the predicates are true under the semantics of Spark SQL:
NaN values are equal to each other and greater than any other value, `-0.0` is equal to `0.0`,
comparisons with null are null, and strings compare by their UTF-8 bytes. The comparisons of IEEE
754 and the total orders of Arrow libraries, such as the comparison kernels of arrow-rs, differ for
some of these values. The jar of Spark SQL contains test cases for a library,
`org/apache/spark/sql/datasource/predicate-cases.json`: predicates as Spark pushes them, with the
rows of a table for which they are true:

```bash
unzip -p "$SPARK_HOME"/jars/spark-sql_*.jar org/apache/spark/sql/datasource/predicate-cases.json
```

The arrays that a library exports must have an offset of 0: copy a sliced array, such as the
result of `RecordBatch::slice` in arrow-rs, before exporting it. Spark also checks that each array
has at least as many values as its parent refers to, but it cannot check that the buffers of the
arrays hold those values, because the C data interface does not carry the sizes of buffers: a
library that exports a buffer that is too short makes Spark read past its end.

The `get_next` function of the stream that `spark_ds_read` exports may block, for example on
network I/O, but Spark cannot interrupt native code: when a task is killed, Spark stops reading
only once `get_next` returns. Use timeouts for blocking operations, or enable the task reaper with
`spark.task.reaper.enabled` and `spark.task.reaper.killTimeout`, which kills an executor whose
killed task does not end.

### Compatibility

Spark binds the functions of a library by their names, so the names and the signatures of the
functions of `spark_datasource.h` are the interface. Its version, 1, is `SPARK_DS_ABI_VERSION`,
which `spark_ds_abi_version` returns and the manifest of a package declares. Within a version, the
signature and the semantics of an existing function never change, and new functions are only
added as optional functions, so a library keeps working with later releases of Spark. New values
may appear where the header says so: for example, later releases may push down predicates with
other names, which a library must ignore if it does not know them. An incompatible change comes
with a new version, with new names. Spark supports version 1 at least in all the releases of Spark
4 from 4.4.0.

### Limitations

Version 1 of the interface does not cover these features of Data Source V2 yet. They can be added
later as optional functions:

- statistics: Spark estimates the size of a native scan with `spark.sql.defaultSizeInBytes`, so it
  does not plan a native scan as the broadcast side of a join, unless the query has a broadcast
  hint. Adaptive query execution can still broadcast it at run time, from the size of a shuffle of
  the scan;
- custom metrics, shown in the Spark UI;
- admission control for streaming reads, such as a maximum number of rows for each micro-batch,
  and continuous streaming;
- the pushdown of aggregates, of the sort order and of the partitioning of the data;
- a function to cancel a read in progress;
- memory accounting: see below.

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
Spark passes the name of the data source to `spark_ds_create`. A library that implements several
data sources is installed under each of their names, for example with symbolic links.

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
  case-insensitive, and Spark passes the name to `spark_ds_create`.
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

This data source reads the numbers `[0, end)` as a column `id`, in two partitions. It only depends
on the `arrow-array` and `arrow-schema` crates of [arrow-rs](https://github.com/apache/arrow-rs),
which export schemas and streams with `FFI_ArrowSchema` and `FFI_ArrowArrayStream`, and declares
the functions of `spark_datasource.h` that it implements as `extern "C"` functions. `Cargo.toml`,
where the name of the library is the name that Spark finds when it is installed:

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
type Sink = extern "C" fn(sink: *mut c_void, data: *const u8, length: i64);

struct RangeSource {
    end: i64,
}

struct RangeReader {
    end: i64,
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn schema() -> SchemaRef {
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

#[no_mangle]
pub extern "C" fn spark_ds_last_error() -> *const c_char {
    LAST_ERROR.with(|error| error.borrow().as_ptr())
}

#[no_mangle]
pub extern "C" fn spark_ds_abi_version() -> i32 {
    1
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_create(
    _name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    num_options: i64,
    source: *mut *mut c_void,
) -> c_int {
    call(|| {
        let mut range = RangeSource { end: 10 };
        for i in 0..num_options as usize {
            if CStr::from_ptr(*option_keys.add(i)).to_str()? == "end" {
                range.end = CStr::from_ptr(*option_values.add(i)).to_str()?.parse()?;
            }
        }
        *source = Box::into_raw(Box::new(range)) as *mut c_void;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_schema(_source: *mut c_void, out: *mut FFI_ArrowSchema) -> c_int {
    call(|| {
        std::ptr::write(out, FFI_ArrowSchema::try_from(schema().as_ref())?);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_create_reader(
    source: *mut c_void,
    _schema: *mut FFI_ArrowSchema,
    reader: *mut *mut c_void,
) -> c_int {
    call(|| {
        let end = (*(source as *const RangeSource)).end;
        *reader = Box::into_raw(Box::new(RangeReader { end })) as *mut c_void;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_close(source: *mut c_void) {
    drop(Box::from_raw(source as *mut RangeSource));
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_partitions(
    reader: *mut c_void,
    add_partition: Sink,
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

#[no_mangle]
pub unsafe extern "C" fn spark_ds_close_reader(reader: *mut c_void) {
    drop(Box::from_raw(reader as *mut RangeReader));
}

#[no_mangle]
pub unsafe extern "C" fn spark_ds_read(
    _reader_state: *const u8,
    _reader_state_length: i64,
    partition: *const u8,
    partition_length: i64,
    out: *mut FFI_ArrowArrayStream,
) -> c_int {
    call(|| {
        let bytes = std::slice::from_raw_parts(partition, partition_length as usize);
        let start = i64::from_le_bytes(bytes[0..8].try_into()?);
        let end = i64::from_le_bytes(bytes[8..16].try_into()?);
        let ids = Int64Array::from_iter_values(start..end);
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(ids)])?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema());
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
`spark_datasource.h`, which is also in the `spark-sql` JAR of a Spark distribution:

```bash
unzip -j "$SPARK_HOME"/jars/spark-sql_*.jar org/apache/spark/sql/datasource/spark_datasource.h
```

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

}  // namespace

extern "C" {

const char* spark_ds_last_error(void) { return last_error.c_str(); }

int32_t spark_ds_abi_version(void) { return SPARK_DS_ABI_VERSION; }

int spark_ds_create(const char*, const char* const* option_keys,
                    const char* const* option_values, int64_t num_options, void** source) {
  return Call([&] {
    auto range = std::make_unique<RangeSource>();
    for (int64_t i = 0; i < num_options; i++) {
      if (std::string(option_keys[i]) == "end") range->end = std::stoll(option_values[i]);
    }
    *source = range.release();
  });
}

int spark_ds_schema(void*, struct ArrowSchema* out) {
  return Call([&] { Check(arrow::ExportSchema(*Schema(), out)); });
}

int spark_ds_create_reader(void* source, struct ArrowSchema*, void** reader) {
  return Call([&] { *reader = new RangeReader{static_cast<RangeSource*>(source)->end}; });
}

void spark_ds_close(void* source) { delete static_cast<RangeSource*>(source); }

int spark_ds_partitions(void* reader, SparkDsSink add_partition, void* sink) {
  return Call([&] {
    int64_t end = static_cast<RangeReader*>(reader)->end;
    int64_t bounds[2][2] = {{0, end / 2}, {end / 2, end}};
    for (const auto& partition : bounds) {
      add_partition(sink, reinterpret_cast<const uint8_t*>(partition), sizeof(partition));
    }
  });
}

void spark_ds_close_reader(void* reader) { delete static_cast<RangeReader*>(reader); }

int spark_ds_read(const uint8_t*, int64_t, const uint8_t* partition, int64_t,
                  struct ArrowArrayStream* out) {
  return Call([&] {
    int64_t bounds[2];
    std::memcpy(bounds, partition, sizeof(bounds));
    arrow::Int64Builder ids;
    for (int64_t id = bounds[0]; id < bounds[1]; id++) Check(ids.Append(id));
    std::shared_ptr<arrow::Array> array;
    Check(ids.Finish(&array));
    auto batch = arrow::RecordBatch::Make(Schema(), array->length(), {array});
    auto batches = arrow::RecordBatchReader::Make({batch}, Schema()).ValueOrDie();
    Check(arrow::ExportRecordBatchReader(batches, out));
  });
}

}  // extern "C"
```
{% endraw %}

Build the library, here on macOS with Arrow C++ 24, which requires C++20, and install it, or
package it as above:

```bash
c++ -std=c++20 -O2 -shared -fPIC -I. -I"$ARROW_HOME/include" -L"$ARROW_HOME/lib" -larrow \
  -o libspark_datasource_cpp_range.dylib cpp_range.cc
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
