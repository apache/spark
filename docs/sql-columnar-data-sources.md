---
layout: global
title: Columnar Data Source API
displayTitle: Columnar Data Source API
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

The columnar data source API is a simpler way to plug a data source into Spark than implementing
the [Data Source V2](sql-v2-data-sources.html) interfaces directly. It is meant for data sources
that identify their data by options, such as a path or a URL. It follows the
[Python Data Source API](api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html),
but data is exchanged only as columnar batches: a data source returns
[`ColumnarBatch`](api/java/org/apache/spark/sql/vectorized/ColumnarBatch.html)es when reading, and
receives them when writing. Spark adapts a data source to Data Source V2, so it supports:

- batch and micro-batch streaming reads,
- batch and streaming writes, appending to or overwriting the existing data,
- predicate, column and limit pushdown.

A data source can be implemented in two ways:

- **On the JVM**, in Java or Scala, with the interfaces of the
  [`org.apache.spark.sql.datasource`](api/java/org/apache/spark/sql/datasource/package-summary.html)
  package.
- **In native code**, such as Rust or C++, as a shared library that implements the same API
  through the [`NativeBridge`](api/java/org/apache/spark/sql/datasource/NativeBridge.html) binary
  interface. A native data source needs no JVM code: it is distributed as a native data source
  package, and Spark loads it when a query uses it.

## The API

| Interface | Methods | Description |
|-----------|---------|-------------|
| `DataSource` | `schema`, `reader`, `streamReader`, `writer`, `streamWriter` | Created on the driver with the options of the user, for each schema inference, scan and write. A data source only implements the operations it supports. |
| `DataSourceReader` | `pushPredicates`, `pushLimit`, `pruneColumns`, `partitions`, `read` | Reads the data for a batch scan. Spark pushes operations down and plans the partitions on the driver, then serializes the reader to the executors, where `read` returns the batches of a partition. |
| `DataSourceStreamReader` | `initialOffset`, `latestOffset`, `partitions`, `read`, `commit`, `stop` | Reads the data for a micro-batch streaming scan. Offsets are JSON strings defined by the data source. |
| `DataSourceWriter` | `createWriter`, `commit`, `abort` | Writes the data for a batch write. `createWriter` returns a `DataWriter<ColumnarBatch>` for each task, and Spark commits or aborts the write on the driver with the messages of the tasks. |
| `DataSourceStreamWriter` | `createWriter`, `commit`, `abort` | Writes the data for a streaming write, one micro-batch (epoch) at a time. |

The batches that a writer receives are backed by Apache Arrow: every column is an
[`ArrowColumnVector`](api/java/org/apache/spark/sql/vectorized/ArrowColumnVector.html). A batch has
at most `spark.sql.execution.arrow.maxRecordsPerBatch` rows, and is only valid during the call.

## JVM Data Sources

A JVM data source extends
[`DataSourceProvider`](api/java/org/apache/spark/sql/datasource/DataSourceProvider.html), which
creates the `DataSource` with the options given by the user. The following data source reads the
numbers `[0, end)` as a column `id`, in two partitions:

```scala
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.datasource._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

class RangeProvider extends DataSourceProvider {
  override def shortName(): String = "scala_range"

  override def createDataSource(options: CaseInsensitiveStringMap): DataSource = {
    new RangeDataSource(options.getLong("end", 10))
  }
}

class RangeDataSource(end: Long) extends DataSource {
  override def schema(): StructType = new StructType().add("id", LongType, nullable = false)

  override def reader(schema: StructType): DataSourceReader = new RangeReader(end)
}

case class RangePartition(start: Long, end: Long) extends InputPartition

class RangeReader(end: Long) extends DataSourceReader {
  override def partitions(): Array[InputPartition] = {
    Array(RangePartition(0, end / 2), RangePartition(end / 2, end))
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val range = partition.asInstanceOf[RangePartition]
    new RangePartitionReader(range.start, range.end)
  }
}

// Returns the ids [start, end) in a single batch, backed by an Arrow vector.
class RangePartitionReader(start: Long, end: Long) extends PartitionReader[ColumnarBatch] {
  private val allocator = new RootAllocator()
  private val ids = new BigIntVector("id", allocator)
  private var done = false

  override def next(): Boolean = {
    if (done) return false
    val count = (end - start).toInt
    ids.allocateNew(count)
    (0 until count).foreach(i => ids.set(i, start + i))
    ids.setValueCount(count)
    done = true
    true
  }

  override def get(): ColumnarBatch = {
    new ColumnarBatch(Array[ColumnVector](new ArrowColumnVector(ids)), ids.getValueCount)
  }

  override def close(): Unit = {
    ids.close()
    allocator.close()
  }
}
```

To use the data source by its short name, list the provider in
`META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` in its JAR, like any other data
source. Its fully qualified class name can also be used as the format:

```scala
spark.read.format("scala_range").option("end", 100).load()
```

## Native Data Sources

### How It Works

A native data source is a shared library that implements the `native` methods of
[`NativeBridge`](api/java/org/apache/spark/sql/datasource/NativeBridge.html) as JNI functions,
named after the class, such as `Java_org_apache_spark_sql_datasource_NativeBridge_createDataSource`.
Spark loads each library into its own copy of the class, so any number of native data sources can
be loaded side by side. The methods follow the API above: Spark implements `DataSource`,
`DataSourceReader` and the other interfaces by calling them.

- **Handles**: the `create` functions return a pointer to a native object, which Spark releases with
  the matching `close` function.
- **Arrow**: schemas and data are exchanged with the
  [Arrow C data interface](https://arrow.apache.org/docs/format/CDataInterface.html). A partition
  is read by exporting an `ArrowArrayStream`, and a writer receives an `ArrowArray` for each batch.
- **State**: partitions, the state of readers and writers, and commit messages are byte arrays in a
  format defined by the library. Spark sends them to the executors and back.
- **Errors**: a library reports an error by throwing a Java exception, for example with the JNI
  function `ThrowNew`. Spark reports it as a `NATIVE_DATA_SOURCE_ERROR`.

A library only exports the functions of the operations it supports. If a function is missing,
Spark treats the operation as unsupported, except for the pushdown and `serialize` functions, which
are optional:

| Operation | Required functions | Optional functions |
|-----------|--------------------|--------------------|
| All | `abiVersion`, `createDataSource`, `closeDataSource` | `schema` |
| Batch read | `createReader`, `partitions`, `read`, `closeReader` | `pushPredicates`, `pushLimit`, `pruneColumns`, `serializeReader` |
| Streaming read | `createStreamReader`, `initialOffset`, `latestOffset`, `streamPartitions`, `read`, `closeStreamReader` | `serializeStreamReader`, `commitOffset` |
| Batch write | `createWriter`, `commit`, `abort`, `closeWriter`, `createDataWriter`, `write`, `commitDataWriter`, `abortDataWriter` | `serializeWriter` |
| Streaming write | `createStreamWriter`, `commit`, `abort`, `closeWriter`, `createDataWriter`, `write`, `commitDataWriter`, `abortDataWriter` | `serializeWriter` |

The Javadoc of `NativeBridge` specifies each function, including the JSON format of the pushed
predicates.

### Packages

A native data source package is a zip file with the extension `.sparkpkg`. It contains a manifest
named `spark-native-datasource.json`, and the library built for one or more platforms:

```json
{
  "abiVersion": 1,
  "dataSources": ["rust_range"],
  "libraries": {
    "linux-x86_64": "linux-x86_64/librust_range.so",
    "linux-aarch64": "linux-aarch64/librust_range.so",
    "osx-aarch64": "osx-aarch64/librust_range.dylib"
  }
}
```

- `abiVersion` is the version of `NativeBridge` that the library implements, currently 1.
- `dataSources` lists the names of the data sources that the library implements. The names are
  case-insensitive, and Spark passes the name to `createDataSource`.
- `libraries` maps each platform, `<os>-<arch>` with an `os` of `linux`, `osx` or `windows` and an
  `arch` of `x86_64` or `aarch64`, to the path of the library in the package.

Add a package to a session with `spark.addArtifact`, which works in Scala, Java and Python, and with
Spark Connect:

```python
spark.addArtifact("/path/to/rust_range.sparkpkg")
spark.read.format("rust_range").option("end", 100).load().show()
```

Spark can also find packages under `spark.sql.dataSource.native.paths`, for example in a container
image. In both cases, the executors get the package automatically. Give packages distinct file
names, for example with their version: sessions that are not isolated share the files they add.

A Java data source with the same name takes precedence over a native one, and so does a Python
data source.

### Example in Rust

This data source reads the numbers `[0, end)` as a column `id`, in two partitions. It uses the
[`jni`](https://crates.io/crates/jni) crate, and the `arrow-array` and `arrow-schema` crates of
[arrow-rs](https://github.com/apache/arrow-rs), which export schemas and streams with
`FFI_ArrowSchema` and `FFI_ArrowArrayStream`. `Cargo.toml`:

```toml
[package]
name = "rust_range"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
arrow-array = { version = "58", features = ["ffi"] }
arrow-schema = { version = "58", features = ["ffi"] }
jni = "0.21"
```

`src/lib.rs`:

```rust
//! A native data source for Apache Spark, written in Rust. It reads the numbers [0, end) as a
//! column `id`, in two partitions. The option `end` defaults to 10.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong, jobjectArray};
use jni::JNIEnv;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct RangeSource {
    end: i64,
}

struct RangeReader {
    end: i64,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// Runs `f`, and reports its error or panic as a Java exception: neither may cross into the JVM.
fn call<T>(env: &mut JNIEnv, default: T, f: impl FnOnce(&mut JNIEnv) -> Result<T>) -> T {
    let message = match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => return value,
        Ok(Err(e)) => e.to_string(),
        Err(_) => "the data source panicked".to_string(),
    };
    if !env.exception_check().unwrap_or(true) {
        let _ = env.throw_new("java/lang/RuntimeException", message);
    }
    default
}

fn strings(env: &mut JNIEnv, array: &JObjectArray) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for i in 0..env.get_array_length(array)? {
        let value = JString::from(env.get_object_array_element(array, i)?);
        result.push(env.get_string(&value)?.into());
    }
    Ok(result)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_abiVersion(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    1
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_createDataSource(
    mut env: JNIEnv,
    _class: JClass,
    _name: JString,
    keys: JObjectArray,
    values: JObjectArray,
) -> jlong {
    call(&mut env, 0, |env| {
        let mut source = RangeSource { end: 10 };
        for (key, value) in strings(env, &keys)?.into_iter().zip(strings(env, &values)?) {
            if key == "end" {
                source.end = value.parse()?;
            }
        }
        Ok(Box::into_raw(Box::new(source)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_schema(
    mut env: JNIEnv,
    _class: JClass,
    _source: jlong,
    schema_address: jlong,
) {
    call(&mut env, (), |_| {
        let schema = FFI_ArrowSchema::try_from(schema().as_ref())?;
        unsafe { std::ptr::write(schema_address as *mut FFI_ArrowSchema, schema) };
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_createReader(
    mut env: JNIEnv,
    _class: JClass,
    source: jlong,
    _schema_address: jlong,
) -> jlong {
    call(&mut env, 0, |_| {
        let source = unsafe { &*(source as *const RangeSource) };
        Ok(Box::into_raw(Box::new(RangeReader { end: source.end })) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_closeDataSource(
    _env: JNIEnv,
    _class: JClass,
    source: jlong,
) {
    drop(unsafe { Box::from_raw(source as *mut RangeSource) });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_partitions(
    mut env: JNIEnv,
    _class: JClass,
    reader: jlong,
) -> jobjectArray {
    call(&mut env, std::ptr::null_mut(), |env| {
        let end = unsafe { &*(reader as *const RangeReader) }.end;
        let bounds = [(0, end / 2), (end / 2, end)];
        let partitions = env.new_object_array(bounds.len() as i32, "[B", JObject::null())?;
        for (i, (start, end)) in bounds.into_iter().enumerate() {
            let bytes = [start.to_le_bytes(), end.to_le_bytes()].concat();
            let partition = env.byte_array_from_slice(&bytes)?;
            env.set_object_array_element(&partitions, i as i32, partition)?;
        }
        Ok(partitions.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_closeReader(
    _env: JNIEnv,
    _class: JClass,
    reader: jlong,
) {
    drop(unsafe { Box::from_raw(reader as *mut RangeReader) });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_datasource_NativeBridge_read(
    mut env: JNIEnv,
    _class: JClass,
    _reader_state: JByteArray,
    partition: JByteArray,
    stream_address: jlong,
) {
    call(&mut env, (), |env| {
        let bytes = env.convert_byte_array(&partition)?;
        let start = i64::from_le_bytes(bytes[0..8].try_into()?);
        let end = i64::from_le_bytes(bytes[8..16].try_into()?);
        let ids = Int64Array::from_iter_values(start..end);
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(ids)])?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema());
        let stream = FFI_ArrowArrayStream::new(Box::new(batches));
        unsafe { std::ptr::write(stream_address as *mut FFI_ArrowArrayStream, stream) };
        Ok(())
    })
}
```

Build the library, and package it:

```bash
cargo build --release
mkdir -p package/osx-aarch64
cp target/release/librust_range.dylib package/osx-aarch64/
cat > package/spark-native-datasource.json <<EOF
{
  "abiVersion": 1,
  "dataSources": ["rust_range"],
  "libraries": {"osx-aarch64": "osx-aarch64/librust_range.dylib"}
}
EOF
(cd package && zip -r ../rust_range.sparkpkg .)
```

### Example in C++

The same data source in C++, with [Arrow C++](https://arrow.apache.org/docs/cpp/), which exports
schemas and streams with `arrow::ExportSchema` and `arrow::ExportRecordBatchReader`:

```cpp
// A native data source for Apache Spark, written in C++ with Arrow C++. It reads the numbers
// [0, end) as a column `id`, in two partitions. The option `end` defaults to 10.

#include <jni.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#define SPARK_JNI(ret, method) \
  extern "C" JNIEXPORT ret JNICALL Java_org_apache_spark_sql_datasource_NativeBridge_##method

namespace {

struct RangeSource {
  int64_t end = 10;
};

struct RangeReader {
  int64_t end;
};

std::shared_ptr<arrow::Schema> Schema() {
  return arrow::schema({arrow::field("id", arrow::int64(), /*nullable=*/false)});
}

// Runs `f`, and reports its error as a Java exception: no C++ exception may cross into the JVM.
template <typename F>
auto Call(JNIEnv* env, F&& f) -> decltype(f()) {
  try {
    return f();
  } catch (const std::exception& e) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
    if constexpr (!std::is_void_v<decltype(f())>) return {};
  }
}

void Check(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

std::string ToString(JNIEnv* env, jobject value) {
  const char* chars = env->GetStringUTFChars(static_cast<jstring>(value), nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(static_cast<jstring>(value), chars);
  return result;
}

}  // namespace

SPARK_JNI(jint, abiVersion)(JNIEnv*, jclass) { return 1; }

SPARK_JNI(jlong, createDataSource)(JNIEnv* env, jclass, jstring, jobjectArray keys,
                                   jobjectArray values) {
  return Call(env, [&] {
    auto source = std::make_unique<RangeSource>();
    for (jsize i = 0; i < env->GetArrayLength(keys); i++) {
      if (ToString(env, env->GetObjectArrayElement(keys, i)) == "end") {
        source->end = std::stoll(ToString(env, env->GetObjectArrayElement(values, i)));
      }
    }
    return reinterpret_cast<jlong>(source.release());
  });
}

SPARK_JNI(void, schema)(JNIEnv* env, jclass, jlong, jlong schema_address) {
  Call(env, [&] {
    Check(arrow::ExportSchema(*Schema(), reinterpret_cast<ArrowSchema*>(schema_address)));
  });
}

SPARK_JNI(jlong, createReader)(JNIEnv* env, jclass, jlong source, jlong) {
  return Call(env, [&] {
    auto* reader = new RangeReader{reinterpret_cast<RangeSource*>(source)->end};
    return reinterpret_cast<jlong>(reader);
  });
}

SPARK_JNI(void, closeDataSource)(JNIEnv*, jclass, jlong source) {
  delete reinterpret_cast<RangeSource*>(source);
}

SPARK_JNI(jobjectArray, partitions)(JNIEnv* env, jclass, jlong reader) {
  return Call(env, [&] {
    int64_t end = reinterpret_cast<RangeReader*>(reader)->end;
    int64_t bounds[2][2] = {{0, end / 2}, {end / 2, end}};
    jobjectArray partitions = env->NewObjectArray(2, env->FindClass("[B"), nullptr);
    for (jsize i = 0; i < 2; i++) {
      jbyteArray partition = env->NewByteArray(sizeof(bounds[i]));
      env->SetByteArrayRegion(partition, 0, sizeof(bounds[i]),
                              reinterpret_cast<const jbyte*>(bounds[i]));
      env->SetObjectArrayElement(partitions, i, partition);
    }
    return partitions;
  });
}

SPARK_JNI(void, closeReader)(JNIEnv*, jclass, jlong reader) {
  delete reinterpret_cast<RangeReader*>(reader);
}

SPARK_JNI(void, read)(JNIEnv* env, jclass, jbyteArray, jbyteArray partition,
                      jlong stream_address) {
  Call(env, [&] {
    int64_t bounds[2];
    env->GetByteArrayRegion(partition, 0, sizeof(bounds), reinterpret_cast<jbyte*>(bounds));
    arrow::Int64Builder ids;
    for (int64_t id = bounds[0]; id < bounds[1]; id++) Check(ids.Append(id));
    std::shared_ptr<arrow::Array> array;
    Check(ids.Finish(&array));
    auto batch = arrow::RecordBatch::Make(Schema(), array->length(), {array});
    auto batches = arrow::RecordBatchReader::Make({batch}, Schema()).ValueOrDie();
    Check(arrow::ExportRecordBatchReader(batches,
                                         reinterpret_cast<ArrowArrayStream*>(stream_address)));
  });
}
```

Build the library, here on macOS with Arrow C++ 24, which requires C++20, and package it as above:

```bash
c++ -std=c++20 -O2 -shared -fPIC \
  -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/darwin" \
  -I"$ARROW_HOME/include" -L"$ARROW_HOME/lib" -larrow \
  -o libcpp_range.dylib cpp_range.cc
```

A library loaded by Spark must find its dependencies, such as `libarrow`, on every node: link them
statically, or install them on the nodes.

### Configuration

| Property Name | Default | Meaning |
|---------------|---------|---------|
| `spark.sql.dataSource.native.enabled` | true | Whether Spark loads native data sources. It is a static configuration, so it can only be set when the Spark application starts. |
| `spark.sql.dataSource.native.paths` | (none) | Comma-separated list of native data source packages, and of directories that contain them, on the local file system of the driver. |

### Security

A native data source runs native code in the driver and executor processes, with their
privileges, and is not sandboxed. Only add packages that you trust. Set
`spark.sql.dataSource.native.enabled` to false to prevent loading native data sources, for example
on a server shared by several users.
