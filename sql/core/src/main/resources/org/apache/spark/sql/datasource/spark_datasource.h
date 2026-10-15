/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * spark_datasource.h: the binary interface between Spark and data sources implemented in native
 * code, such as Rust or C++. Since Spark 4.4.0.
 *
 * A native data source is a shared library that exports the six functions declared below. They
 * never change. Everything that the interface grows with is in the messages of
 * spark_datasource.proto, which Spark and the library exchange as Protocol Buffers bytes: which
 * object to open, and every control operation on it, such as the pushdown of predicates, the
 * planning of partitions, offsets and commits. The data itself is exchanged without copying, as
 * Arrow C streams. Spark calls the functions with the Foreign Function and Memory API of Java, so
 * native data sources require Java 22 or later. Spark adapts a native data source to Data Source
 * V2, with batch and micro-batch streaming reads, batch and streaming writes, and predicate,
 * column and limit pushdown.
 *
 * This header is self-contained: it only depends on the C standard library, and defines the
 * structs of the Arrow C data interface unless they are already defined, as by Arrow C++. A library
 * can copy it and spark_datasource.proto into its source tree, or use the copies in the include
 * directory of a Spark distribution, and generate the code of the messages with any Protocol
 * Buffers compiler, such as protoc or prost. A library built against them works with all the
 * releases of Spark that support its SPARK_DS_ABI_VERSION.
 *
 * Spark finds the library of a data source automatically when it is installed like other native
 * libraries and named after the data source: the library of my_source is
 * spark_datasource_my_source, such as libspark_datasource_my_source.so in /usr/local/lib on Linux.
 * Spark looks for it in the directories of java.library.path, which include LD_LIBRARY_PATH on
 * Linux, and in the lib directory of each installation prefix in PATH. A library can also be
 * distributed in a native data source package: a zip file with the extension .sparkpkg that
 * contains a manifest named spark-native-datasource.json and the library built for one or more
 * platforms, added to a session with spark.addArtifact or under the paths in
 * spark.sql.dataSource.native.paths. See the "Native Data Sources" page of the Spark SQL guide for
 * details and examples.
 *
 * Lifecycle
 * ---------
 * Spark loads a library once in each JVM. It opens objects with spark_ds_open, whose OpenRequest
 * says which object and carries everything that it needs, calls control operations on them with
 * spark_ds_call, exchanges their data with spark_ds_stream, and releases them with spark_ds_close.
 * The messages are those of spark_datasource.proto.
 *
 * - Data sources. On the driver, for each schema inference, scan and write, Spark opens a data
 *   source, whose OpenReply declares its capabilities: Spark reports the operations that a data
 *   source does not declare as unsupported, without opening a reader or a writer. When the user
 *   does not specify a schema, Spark calls InferSchema. Spark closes the data source right after.
 * - Batch reads. On the driver, Spark opens a batch reader, calls, each at most once and in this
 *   order, PushPredicates, PushLimit and PruneColumns, then PlanPartitions and Serialize, and
 *   closes it. Then, on the executors, it opens a partition reader for each partition, in a task,
 *   initializes its stream with a Read, reads the stream, releases it and closes the partition
 *   reader. If Spark never plans the partitions, for example to only explain the plan of the query,
 *   it closes the batch reader once the reader is no longer referenced.
 * - Streaming reads. On the driver, Spark opens a stream reader and keeps it until the streaming
 *   query stops. For each micro-batch, it calls LatestOffset, PlanStreamPartitions and Serialize,
 *   then reads the partitions on the executors as for a batch read, and calls CommitOffset once it
 *   has processed the micro-batch. Offsets are defined by the library, as compact JSON on a single
 *   line. When a streaming query restarts, from its checkpoint, Spark opens a new stream reader and
 *   may plan the last range that it planned before again, so the partitions of a range must read
 *   the same data each time.
 * - Writes. On the driver, right before it starts the tasks of the write (or of a micro-batch),
 *   Spark opens a writer and calls Serialize. With adaptive query execution, Spark first runs the
 *   stages of the data to write that end with a shuffle, and opens no writer if they fail. The
 *   rest of the data, such as its last stage, is computed in the tasks of the write, so a writer
 *   may still be aborted because computing the data failed: do irreversible work, such as removing
 *   the existing data for an overwrite, in Commit. On the executors, each task opens a data writer,
 *   passes its data in one or more calls of Write, each with a stream of batches of at most
 *   spark.sql.execution.arrow.maxRecordsPerBatch rows, which also end once they hold
 *   spark.sql.execution.arrow.maxBytesPerBatch bytes, and calls CommitDataWriter, or
 *   AbortDataWriter if the task or the commit fails, before it closes the data writer. Then the
 *   driver calls Commit with the messages of the tasks if they all succeeded, or Abort otherwise,
 *   including when Commit fails, and closes the writer. Several attempts of a task may write the
 *   same partition, after a failure or with speculative execution, each with a data writer of its
 *   own. Spark lets the attempts of a partition commit one at a time, and passes to Commit the
 *   message of one committed attempt for each partition, so the write consists of the data of
 *   these attempts only. It aborts the data writers of the other attempts, except on an executor
 *   that is lost, where a data writer is neither committed nor aborted, and may even have
 *   committed before its message reached the driver. A streaming write commits or aborts each
 *   micro-batch (epoch), which the query ID of the writer and the epoch ID identify, also when the
 *   query restarts from its checkpoint and when other queries write to the same destination. A
 *   micro-batch streaming query opens a new writer for each micro-batch, and Spark closes the
 *   previous writer of the query when the query opens a new one. Spark is not told when a
 *   streaming query stops, so it closes the last writer of the query once the writer is no longer
 *   referenced, or when the query is restarted.
 *
 * Conventions
 * -----------
 * - Errors. The functions that return an int return 0 on success, or any other value on failure.
 *   They then set the message that spark_ds_last_error returns, and Spark reports it as a
 *   NATIVE_DATA_SOURCE_ERROR. spark_ds_close cannot fail, and it may run after the query, from
 *   another thread: do the work that can fail a query, such as flushing data, in a commit. A C++
 *   exception or a Rust panic must not cross the interface: catch it and return an error instead.
 * - Handles. spark_ds_open returns an opaque, non-NULL handle in its last argument, typically a
 *   pointer: Spark reports a NULL handle as an error. Spark releases each handle exactly once,
 *   with spark_ds_close, and no other function releases a handle. A handle is not always released
 *   right after its last use: a batch reader whose partitions are never planned and the last
 *   writer of a streaming query are released once they are no longer referenced (see Lifecycle
 *   above). A handle does not depend on another one: each OpenRequest carries everything that the
 *   object needs.
 * - Messages. A request is valid only during the call. The library replies by calling the
 *   SparkDsSink that it gets once, with the encoded reply, on the thread of the call and during the
 *   call. Not calling it is an empty reply, which is valid for every request. Both sides ignore the
 *   fields that they do not know.
 * - Memory. What Spark passes to a function is only valid during the call, except for SparkDsHost,
 *   which stays valid until the JVM exits.
 * - Arrow data. Data is exchanged with the Arrow C stream interface
 *   (https://arrow.apache.org/docs/format/CStreamInterface.html), as structs allocated by Spark.
 *   For an output stream, the library initializes it and Spark takes ownership: Spark reads it
 *   from the thread of the task, and releases it before it closes the object. For an input stream,
 *   the library reads it during the call, on the thread of the call, and may move it, or the arrays
 *   that it returns, to take ownership; otherwise, Spark releases it after the call returns. The
 *   arrays that the library exports must have an offset of 0, at every level: Arrow Java ignores
 *   the offsets of the arrays it imports, so Spark rejects an array with another offset, such as a
 *   zero-copy slice. Copy a sliced array before exporting it. Spark also rejects an array with
 *   fewer values than its parent refers to, such as a column shorter than its batch. The buffers of
 *   an array must hold all of its values, as the C data interface requires: it does not carry the
 *   sizes of the buffers, so Spark cannot check them, and reads past the end of a buffer that is
 *   too short.
 * - Schemas. The messages describe schemas with Spark SQL types, which map to Arrow types as in
 *   spark.sql.execution.arrow: for example, bigint is int64, string is utf8, timestamp is
 *   timestamp[us] with a time zone and timestamp_ntz is timestamp[us] without one. Data read from a
 *   library may also use large_utf8 and utf8_view for strings, large_binary and binary_view for
 *   binary data, and any names for the fields of the entries of a map. Spark rejects the other
 *   Arrow types, such as unsigned integers, decimal256, large lists, fixed-size lists and list
 *   views, and dictionary encoding.
 * - Threads. Spark calls the library from multiple threads, and calls on different handles may
 *   run concurrently. Calls on the same handle never run concurrently. A handle may be released
 *   from a thread other than the threads that used it, such as a JVM Cleaner thread when Spark
 *   releases a handle once it is no longer referenced, for example the last writer of a streaming
 *   query. The functions of SparkDsHost can be called from any thread.
 *
 * Compatibility
 * -------------
 * The six functions of this header and the fields of SparkDsHost never change. SparkDsHost starts
 * with its size, and only gets fields appended: a library must only read the fields that fit in
 * the size of the struct that Spark passes. The messages only grow, as spark_datasource.proto
 * describes, and Spark only sends a new kind of request to an object whose OpenReply declares that
 * it supports it. So a library built against an earlier version of these files keeps working with
 * later releases of Spark, and a library built against a later version works with earlier
 * releases, without the features that they do not know. SPARK_DS_ABI_VERSION is the version of
 * the functions of this header, and Spark rejects a library of a version that it does not support.
 * Spark supports version 1 at least in all the releases of Spark 4 from 4.4.0.
 */
#ifndef SPARK_DATASOURCE_H
#define SPARK_DATASOURCE_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The version of this interface, which spark_ds_abi_version returns. */
#define SPARK_DS_ABI_VERSION 1

/* Exports the functions of this header, also when the library hides its other symbols. */
#if defined(_WIN32)
#define SPARK_DS_EXPORT __declspec(dllexport)
#else
#define SPARK_DS_EXPORT __attribute__((visibility("default")))
#endif

/* The Arrow C data interface: https://arrow.apache.org/docs/format/CDataInterface.html */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
  const char* (*get_last_error)(struct ArrowArrayStream*);
  void (*release)(struct ArrowArrayStream*);
  void* private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/*
 * Receives the reply of a function, encoded as a message of spark_datasource.proto, and copies it.
 * `sink` is the argument passed with the sink to the function.
 */
typedef void (*SparkDsSink)(void* sink, const uint8_t* data, int64_t length);

/* The levels of the messages that a library logs with SparkDsHost.log. */
#define SPARK_DS_LOG_ERROR 1
#define SPARK_DS_LOG_WARN 2
#define SPARK_DS_LOG_INFO 3
#define SPARK_DS_LOG_DEBUG 4
#define SPARK_DS_LOG_TRACE 5

/* Spark, as spark_ds_open gets it. It stays valid until the JVM exits. */
typedef struct SparkDsHost {
  /*
   * The size of this struct in the header that Spark was built with. A later version of Spark may
   * append fields, so a library must only read the fields that fit in it.
   */
  int64_t size;
  /*
   * Logs a message, a NUL-terminated UTF-8 string, at one of the SPARK_DS_LOG_* levels: in the log
   * of the driver or of the executor, with the path of the library.
   */
  void (*log)(int32_t level, const char* message);
  /*
   * Returns whether the task with the given task_attempt_id of its TaskContext was killed, for
   * example because its query was canceled, or has ended. Spark cannot interrupt native code, so
   * a read or a write that runs for long, or blocks, should check it regularly, and fail once it is
   * true. When a task is killed, Spark only stops reading once the get_next function of the stream
   * returns: a get_next that blocks forever keeps the task running, unless the task reaper
   * (spark.task.reaper.enabled and spark.task.reaper.killTimeout) kills the executor.
   */
  bool (*is_interrupted)(int64_t task_attempt_id);
} SparkDsHost;

/* Returns SPARK_DS_ABI_VERSION, the version of this interface that the library implements. */
SPARK_DS_EXPORT int32_t spark_ds_abi_version(void);

/*
 * Returns the message of the last error on the calling thread, as a NUL-terminated UTF-8 string
 * that stays valid until the next call on that thread.
 */
SPARK_DS_EXPORT const char* spark_ds_last_error(void);

/*
 * Opens an object: a data source, a batch reader, a stream reader, a writer, a partition reader or
 * a data writer.
 *
 * host: Spark, the same struct for every call in a JVM
 * request, length: an OpenRequest
 * reply: called with an OpenReply, or not at all for an empty one
 * handle: set to the handle of the object
 */
SPARK_DS_EXPORT int spark_ds_open(const SparkDsHost* host, const uint8_t* request, int64_t length,
                                  SparkDsSink reply, void* sink, void** handle);

/*
 * Runs a control operation on an object.
 *
 * request, length: a CallRequest
 * reply: called with a CallReply, or not at all for an empty one
 */
SPARK_DS_EXPORT int spark_ds_call(void* handle, const uint8_t* request, int64_t length,
                                  SparkDsSink reply, void* sink);

/*
 * Exchanges the data of an object, as Arrow C streams.
 *
 * request, length: a StreamRequest
 * in: for a Write, the stream of the batches to write. It is NULL otherwise.
 * out: for a Read, the stream to initialize with the batches of the partition: each array of the
 *      stream is a struct array whose fields are the columns to read, those of the schema to read
 *      or the pruned columns. It is NULL otherwise.
 */
SPARK_DS_EXPORT int spark_ds_stream(void* handle, const uint8_t* request, int64_t length,
                                    struct ArrowArrayStream* in, struct ArrowArrayStream* out);

/* Releases the handle of an object. */
SPARK_DS_EXPORT void spark_ds_close(void* handle);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_DATASOURCE_H */
