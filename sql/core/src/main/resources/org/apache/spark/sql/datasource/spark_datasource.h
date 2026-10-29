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
 * A native data source is a shared library that exports a single C function, spark_ds_init,
 * which returns the table of the functions that implement the data source, a SparkDsLibrary.
 * Spark calls them with the Foreign Function and Memory API of Java, so native data sources
 * require Java 22 or later. Each library has its own table, so any number of libraries can be
 * loaded side by side. Spark adapts a native data source to Data Source V2, with batch and
 * micro-batch streaming reads, batch and streaming writes, and column and limit pushdown.
 *
 * This header is self-contained: it only depends on the C standard library, and defines the
 * structs of the Arrow C data interface unless they are already defined, as by Arrow C++. A library
 * can copy it into its source tree, or use the copy in the include directory of a Spark
 * distribution. A library built against it works with all the releases of Spark that support its
 * SPARK_DS_ABI_VERSION.
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
 * Spark loads a library once in each JVM, and calls spark_ds_init before any other function. It
 * creates a data source with create on the driver for each schema inference, scan and write, and
 * closes it right after. create also returns the capabilities of the data source: Spark reports
 * the operations that a data source does not declare as unsupported, without calling the library.
 * Spark also creates a data source while it analyzes a query, to reject a batch write that the
 * data source does not support before it computes the data to write. That data source has the
 * options of the table, those of CREATE TABLE for a table in a catalog, which the options of a
 * write can override, so Spark checks the capabilities of each operation again.
 *
 * - Batch reads. On the driver, Spark creates a reader with create_reader, pushes operations down
 *   by calling, each at most once and in this order, push_limit and prune_columns, plans the
 *   partitions with partitions, which also returns the state of the reader, and closes it. Then it
 *   calls read on the executors, in one task for each partition, with the schema to read and the
 *   state of the reader. If Spark never plans the partitions, for example to only explain the plan
 *   of the query, it closes the reader once the reader is no longer referenced.
 * - Streaming reads. On the driver, Spark creates a stream reader with create_stream_reader, with
 *   the columns to read and the location of the checkpoint of the source, and keeps it until the
 *   streaming query stops. When the checkpoint does not have the initial offset of the source yet,
 *   Spark asks for it with initial_offset, and keeps it in the checkpoint. For each micro-batch, or
 *   only before the first one with Trigger.AvailableNow, it asks for the latest_offset, with the
 *   offset that the micro-batch starts after, plans the partitions with stream_partitions, which
 *   also returns the state of the stream reader, then calls read on the executors. Offsets are
 *   defined by the library, as compact JSON on a single line: Spark stores each offset as a line of
 *   the offset log of the checkpoint, so it rejects an offset that is not valid JSON or contains a
 *   line break. When a streaming query restarts, from its checkpoint, Spark creates a new stream
 *   reader and may call stream_partitions again for the last range that it planned before, so the
 *   partitions of a range must read the same data each time.
 * - Writes. On the driver, right before it starts the tasks of the write (or of a micro-batch),
 *   Spark creates a writer with create_writer, which also returns the state of the writer. If Spark
 *   rejects what create_writer returned, such as a state set more than once, it aborts the writer,
 *   with the field rejected of SparkDsFinishWriteInfo set to 1, before it closes it. With adaptive
 *   query execution, Spark first runs the stages of the data to write that end with a shuffle, and
 *   creates no writer if they fail. The rest of the data, such as its last stage, is computed in
 *   the tasks of the write, so a writer may still be aborted because computing the data failed: do
 *   irreversible work, such as removing the existing data for an overwrite, in commit. On the
 *   executors, each task creates a data writer with create_data_writer, with the schema of the data
 *   and the state of the writer, calls write for each batch of at most
 *   spark.sql.execution.arrow.maxRecordsPerBatch rows, which also ends once it holds
 *   spark.sql.execution.arrow.maxBytesPerBatch bytes, and calls commit_data_writer, or
 *   abort_data_writer if the task or the commit fails, before it closes the data writer. Then the
 *   driver calls commit with the messages of the tasks if they all succeeded, or abort otherwise,
 *   including when commit fails, and closes the writer. Several attempts of a task may write the
 *   same partition, after a failure or with speculative execution, each with a data writer of its
 *   own. Spark lets the attempts of a partition commit one at a time, and passes to commit the
 *   message of one committed attempt for each partition, so the write consists of the data of these
 *   attempts only. It aborts the data writers of the other attempts, except on an executor that is
 *   lost, where a data writer is neither committed nor aborted, and may even have committed before
 *   its message reached the driver. A streaming write commits or aborts each micro-batch (epoch).
 *   Spark may commit the same epoch more than once, for example when a streaming query restarts
 *   after a failure, so committing an epoch that is already committed must have no effect, for
 *   exactly-once output. The ID of the query, which create_writer gets for a streaming write, stays
 *   the same when the query restarts from its checkpoint, so the ID of the query and the epoch ID
 *   identify the output of a micro-batch, also when other queries write to the same destination. A
 *   micro-batch streaming query creates a new stream writer for each micro-batch, and Spark closes
 *   the previous stream writer of the query when the query creates a new one. Spark is not told
 *   when a streaming query stops, so it closes the last stream writer of the query once the writer
 *   is no longer referenced, or when the query is restarted. Streaming writes are micro-batch only:
 *   a query with continuous processing fails before create_writer.
 *
 * Conventions
 * -----------
 * - Errors. The functions that return an int return 0 on success, or any other value on failure.
 *   They then set the message that last_error returns, and Spark reports it as a
 *   NATIVE_DATA_SOURCE_ERROR. The close functions cannot fail, and they may run after the query,
 *   from another thread: do the work that can fail a query, such as flushing data, in commit. A
 *   C++ exception or a Rust panic must not cross the interface: catch it and return an error
 *   instead.
 * - Handles. create, create_reader, create_stream_reader, create_writer and create_data_writer
 *   return an opaque, non-NULL handle in their last argument, typically a pointer: Spark reports a
 *   NULL handle as an error. Spark releases each handle exactly once, with the close function of
 *   its kind: close, close_reader, close_stream_reader, close_writer or close_data_writer, and no
 *   other function releases a handle. A handle is not always released right after its last use: a
 *   batch reader whose partitions are never planned and the last stream writer of a query are
 *   released once they are no longer referenced (see Lifecycle above). A reader or writer handle
 *   must not depend on the data source handle it was created from, because Spark closes the data
 *   source handle right after creating it.
 * - Memory. What Spark passes to a function, such as strings, byte arrays and the structs whose
 *   names end with Info, is only valid during the call, except for SparkDsHost and
 *   SparkDsTaskContext, which document how long they live. Strings are UTF-8 and NUL-terminated:
 *   Spark fails an operation whose options or column names contain the character U+0000. The
 *   library returns strings and byte arrays by passing them to a SparkDsSink, which copies them.
 * - State. Partitions, the state of readers and writers, and commit messages are byte arrays in a
 *   format defined by the library. Spark sends them to the executors and back.
 * - Arrow data. Schemas and data are exchanged with the Arrow C data interface
 *   (https://arrow.apache.org/docs/format/CDataInterface.html), as structs allocated by Spark.
 *   For an output struct, the library initializes it and Spark takes ownership. For an input
 *   struct, including the schemas in SparkDsReadInfo and SparkDsDataWriterInfo, the library may
 *   move it to take ownership; otherwise, Spark releases it after the call returns, so the library
 *   must not use it afterwards. The arrays that the library exports must have an offset of 0, at
 *   every level: Arrow Java ignores the offsets of the arrays it imports, so Spark rejects an
 *   array with another offset, such as a zero-copy slice. Copy a sliced array before exporting
 *   it. Spark also rejects an array with fewer values than its parent refers to, such as a column
 *   shorter than its batch, and a batch with null rows, since Arrow Java ignores the validity of
 *   the struct of a batch: mark the values of its columns as null instead. As the C data
 *   interface requires, an array without a validity buffer must have a null_count of 0, and the
 *   buffers of an array must hold all of its values: the interface does not carry the sizes of
 *   the buffers, so Spark cannot check them, and reads past the end of a buffer that is too short.
 * - Schemas. These are the types of version 1, with their Arrow types and the formats of the C
 *   data interface: boolean is bool (b), tinyint, smallint, int and bigint are int8 (c), int16
 *   (s), int32 (i) and int64 (l), float and double are float32 (f) and float64 (g), decimal(p, s)
 *   is decimal128(p, s) (d:p,s), string is utf8 (u), binary is binary (z), date is date32 (tdD),
 *   timestamp is timestamp[us] with the time zone of the session (tsu:<zone>), timestamp_ntz is
 *   timestamp[us] without a time zone (tsu:), and array, map and struct are list (+l), map (+m)
 *   and struct (+s). Spark fails an operation whose schema has another type, such as an
 *   interval, variant or time, before it passes the schema to the library: a later version may
 *   support other types, for the data sources that declare a capability for them. Data read from
 *   a library may also use large_utf8 (U) and utf8_view (vu) for strings, large_binary (Z) and
 *   binary_view (vz) for binary data, and any names for the fields of the entries of a map.
 *   Spark rejects the other Arrow types, such as unsigned integers, decimal256, large lists,
 *   fixed-size lists and list views, and dictionary encoding. The data must agree with the
 *   nullability of the schema to read: Spark fails a read whose data has a null where the schema
 *   declares a column, a field, an element or a map value not nullable.
 * - Options. Option keys are passed in lower case, because Spark matches them
 *   case-insensitively.
 * - Optional functions. A library only sets the functions of the operations it supports in its
 *   table, and leaves the others NULL. A data source must not declare a capability whose
 *   functions are NULL: Spark then fails the operation. The pushdown functions are optional:
 *   without them, Spark pushes nothing down. So are schema, without which the user must specify
 *   the schema, commit_offset and last_error. A reader or a writer without a state does not call
 *   the set_state sink of partitions, stream_partitions or create_writer.
 * - Threads. Spark calls the library from multiple threads, and calls on different handles may
 *   run concurrently. Calls on the same handle never run concurrently. A handle may be released
 *   from a thread other than the threads that used it, such as a JVM Cleaner thread when Spark
 *   releases a handle once it is no longer referenced, for example the last stream writer of a
 *   streaming query. A SparkDsSink must be called on the thread of the call that it was passed
 *   to, during that call. The functions of SparkDsHost and SparkDsTaskContext, and the callbacks
 *   of the stream that read initializes, can be called from any thread. The callbacks of the
 *   stream never run concurrently.
 *
 * Compatibility
 * -------------
 * The function table and the structs that Spark passes to the library start with their size, so
 * that they can grow. Within a SPARK_DS_ABI_VERSION:
 * - fields are only appended to the structs, including SparkDsLibrary, and the existing fields
 *   never change, nor do the semantics of the existing functions;
 * - Spark only reads the functions that fit in the size of the table of a library, so a library
 *   built against an earlier header keeps working, with the functions that it does not know as
 *   NULL. A library must only read the fields that fit in the size of a struct that Spark passes;
 * - Spark only uses a new capability, write mode or kind of read with a data source that declares
 *   it.
 * An incompatible change uses a new SPARK_DS_ABI_VERSION: spark_ds_init gets the newest version
 * that Spark supports, the library returns a table of a version that it implements, and Spark
 * rejects a table of a version that it does not support. Spark supports version 1 at least in all
 * the releases of Spark 4 from 4.4.0.
 */
#ifndef SPARK_DATASOURCE_H
#define SPARK_DATASOURCE_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The version of this interface. */
#define SPARK_DS_ABI_VERSION 1

/* Exports spark_ds_init from the library, also when the library hides its other symbols. */
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
 * Receives a string or a byte array that the library returns, and copies it. `sink` is the
 * argument passed with the sink to the function that returns it: a function that gets several
 * sinks gets a single such argument, for all of them.
 */
typedef void (*SparkDsSink)(void* sink, const uint8_t* data, int64_t length);

/* ---------------------------------------------------------------------------------------------
 * What Spark passes to the library
 * --------------------------------------------------------------------------------------------- */

/* The levels of the messages that a library logs with SparkDsHost.log. */
#define SPARK_DS_LOG_ERROR 1
#define SPARK_DS_LOG_WARN 2
#define SPARK_DS_LOG_INFO 3
#define SPARK_DS_LOG_DEBUG 4
#define SPARK_DS_LOG_TRACE 5

/* Spark, as spark_ds_init gets it. It stays valid until the JVM exits. */
typedef struct SparkDsHost {
  /*
   * The size of this struct in the header that Spark was built with. A later version of Spark may
   * append fields, so a library must only read the fields that fit in it.
   */
  int64_t size;
  /* The newest version of this interface that Spark supports. */
  int32_t abi_version;
  /* The version of Spark, such as "4.4.0". */
  const char* spark_version;
  /*
   * Logs a message, a NUL-terminated UTF-8 string, at one of the SPARK_DS_LOG_* levels: in the log
   * of the driver or of the executor, with the path of the library.
   */
  void (*log)(int32_t level, const char* message);
} SparkDsHost;

/* A data source, as create gets it. */
typedef struct SparkDsCreateInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * The name of the data source, as listed in the package manifest, or in lower case for an
   * installed library. A library can implement several data sources.
   */
  const char* name;
  /*
   * The num_options options given by the user, such as spark.read.option("key", "value"), with
   * the keys in lower case.
   */
  const char* const* option_keys;
  const char* const* option_values;
  int64_t num_options;
} SparkDsCreateInfo;

/* A batch scan, as create_reader gets it. */
typedef struct SparkDsReaderInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
} SparkDsReaderInfo;

/* A request for the partitions of a batch scan, as partitions gets it. */
typedef struct SparkDsPartitionsInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
} SparkDsPartitionsInfo;

/* A micro-batch streaming scan, as create_stream_reader gets it. */
typedef struct SparkDsStreamReaderInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * The location of a directory in the checkpoint of the streaming query that is reserved for this
   * source, such as file:/tmp/checkpoint/sources/0, where the library may keep what it needs to
   * recover from a failure. It stays the same when the query restarts from its checkpoint. It is a
   * fully qualified Hadoop path, whose characters are not escaped as in a URI: a space stays a
   * space, and % stays %. Its file system may be one that the library cannot access, such as s3a
   * or hdfs. Spark keeps what it needs there under the names that start with _spark, such as the
   * initial offset of the source, and the library must not use them.
   */
  const char* checkpoint_location;
} SparkDsStreamReaderInfo;

/* A request for the initial offset of a stream reader, as initial_offset gets it. */
typedef struct SparkDsInitialOffsetInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
} SparkDsInitialOffsetInfo;

/* A request for the latest offset of a stream reader, as latest_offset gets it. */
typedef struct SparkDsLatestOffsetInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * The offset, as JSON, that the next micro-batch starts after: the end of the last micro-batch,
   * or the initial offset before the first one, for every trigger. With Trigger.AvailableNow,
   * Spark asks for the latest offset only once, before the first micro-batch that it plans, and
   * reads up to it.
   */
  const char* start;
} SparkDsLatestOffsetInfo;

/* A request for the partitions of a micro-batch, as stream_partitions gets it. */
typedef struct SparkDsStreamPartitionsInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /* The offsets, as JSON, that the micro-batch reads the data after, up to and including end. */
  const char* start;
  const char* end;
} SparkDsStreamPartitionsInfo;

/* The end of the data that Spark processed, as commit_offset gets it. */
typedef struct SparkDsCommitOffsetInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /* The offset, as JSON, up to and including which Spark processed the data. */
  const char* end;
} SparkDsCommitOffsetInfo;

/*
 * The task that calls read or create_data_writer, on an executor. It stays valid until the stream
 * that read returns is released, or until the data writer is closed.
 */
typedef struct SparkDsTaskContext {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /* The ID of the stage of the task, and the attempt number of the stage. */
  int32_t stage_id;
  int32_t stage_attempt_number;
  /* The partition that the task reads or writes, and the attempt number of the task. */
  int32_t partition_id;
  int32_t attempt_number;
  /* The ID of the task attempt, unique in the Spark application. */
  int64_t task_attempt_id;
  /*
   * Returns whether the task was killed, for example because its query was canceled. Spark cannot
   * interrupt native code, so a read or a write that runs for long, or blocks, should check it
   * regularly, and fail once it is true.
   */
  bool (*is_interrupted)(const struct SparkDsTaskContext* task);
} SparkDsTaskContext;

/* The kinds of a read. */
#define SPARK_DS_READ_BATCH 0
#define SPARK_DS_READ_MICRO_BATCH 1

/* A partition to read, as read gets it, on an executor. */
typedef struct SparkDsReadInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * SPARK_DS_READ_BATCH for a partition of a batch scan, which partitions planned, or
   * SPARK_DS_READ_MICRO_BATCH for a partition of a micro-batch, which stream_partitions planned.
   */
  int32_t kind;
  /*
   * The schema to read: the schema that create_reader got, or only the columns that prune_columns
   * pruned it to, or the schema that create_stream_reader got. The stream that read initializes
   * has this schema.
   */
  struct ArrowSchema* schema;
  /* The state that partitions or stream_partitions returned with the partition, or empty. */
  const uint8_t* reader_state;
  int64_t reader_state_length;
  /* The partition, as partitions or stream_partitions returned it. */
  const uint8_t* partition;
  int64_t partition_length;
} SparkDsReadInfo;

/* The modes of a write. */
#define SPARK_DS_WRITE_APPEND 0
#define SPARK_DS_WRITE_TRUNCATE 1

/* A write, as create_writer gets it. */
typedef struct SparkDsWriteInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * SPARK_DS_WRITE_APPEND to add the data to the existing data, or SPARK_DS_WRITE_TRUNCATE to
   * replace the existing data: with the save mode "overwrite" of a batch write, and in each
   * micro-batch with the output mode "complete" of a streaming write. Spark only passes
   * SPARK_DS_WRITE_TRUNCATE to a data source with the capability SPARK_DS_BATCH_TRUNCATE for a
   * batch write, or SPARK_DS_STREAMING_TRUNCATE for a streaming write.
   */
  int32_t mode;
  /*
   * The ID of the streaming query of a streaming write, or NULL for a batch write. It stays the
   * same when the query restarts from its checkpoint, so with the epoch IDs, it identifies the
   * commits of the query: see Writes above.
   */
  const char* query_id;
} SparkDsWriteInfo;

/* The data writer of a task, as create_data_writer gets it, on an executor. */
typedef struct SparkDsDataWriterInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * The schema of the data to write, as create_writer got it. Each array that write gets has this
   * schema, so the library keeps it, or what it needs of it, to read the arrays.
   */
  struct ArrowSchema* schema;
  /* The state that create_writer returned, or empty. */
  const uint8_t* writer_state;
  int64_t writer_state_length;
  /* The ID of the micro-batch for a streaming write, or -1 for a batch write. */
  int64_t epoch_id;
} SparkDsDataWriterInfo;

/* The end of a write, as commit and abort get it. */
typedef struct SparkDsFinishWriteInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * The ID of the micro-batch for a streaming write, or -1 for a batch write, and for a writer
   * whose create_writer Spark rejected, before it knew the micro-batch.
   */
  int64_t epoch_id;
  /*
   * The num_messages messages returned by commit_data_writer of each task. For abort, a message
   * is NULL if its task did not commit.
   */
  const uint8_t* const* messages;
  const int64_t* message_lengths;
  int64_t num_messages;
  /*
   * For abort, 1 if Spark rejected what create_writer returned, such as a state set more than
   * once: no task ran, so there are no messages. 0 otherwise.
   */
  int64_t rejected;
} SparkDsFinishWriteInfo;

/* The end of the data written by a task, as commit_data_writer and abort_data_writer get it. */
typedef struct SparkDsFinishDataWriterInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
} SparkDsFinishDataWriterInfo;

/* ---------------------------------------------------------------------------------------------
 * What the library returns to Spark
 * --------------------------------------------------------------------------------------------- */

/* The capabilities of a data source, which create returns: the operations that it supports. */
#define SPARK_DS_BATCH_READ 1          /* batch reads, with create_reader */
#define SPARK_DS_MICRO_BATCH_READ 2    /* micro-batch streaming reads, with create_stream_reader */
#define SPARK_DS_BATCH_WRITE 4         /* batch writes, with create_writer */
#define SPARK_DS_STREAMING_WRITE 8     /* streaming writes, with create_writer */
#define SPARK_DS_BATCH_TRUNCATE 16     /* batch writes with the mode SPARK_DS_WRITE_TRUNCATE */
#define SPARK_DS_STREAMING_TRUNCATE 32 /* streaming writes with the mode SPARK_DS_WRITE_TRUNCATE */

/*
 * The functions of a library, which spark_ds_init returns. A library sets the functions of the
 * operations it supports, and leaves the others NULL: see "Optional functions" above.
 */
typedef struct SparkDsLibrary {
  /*
   * The size of this struct in the header that the library was built with. Spark only reads the
   * functions that fit in it.
   */
  int64_t size;
  /* The version of this interface that the table implements, SPARK_DS_ABI_VERSION. */
  int32_t abi_version;

  /*
   * Returns the message of the last error on the calling thread, as a NUL-terminated UTF-8 string
   * that stays valid until the next call on that thread. Optional: without it, Spark only reports
   * the value that the function returned.
   */
  const char* (*last_error)(void);

  /* -------------------------------------------------------------------------------------------
   * Data source (driver)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Creates a data source.
   *
   * info: the name of the data source, and the options given by the user
   * capabilities: set to the capabilities of the data source, SPARK_DS_* flags combined with |,
   *               which may depend on its name and its options. It is 0 when create is called.
   * source: set to the data source handle
   */
  int (*create)(const SparkDsCreateInfo* info, uint64_t* capabilities, void** source);

  /*
   * Exports the schema of the data. Spark calls it only when the user does not specify a schema.
   *
   * out: initialized with a struct type whose fields are the columns
   */
  int (*schema)(void* source, struct ArrowSchema* out);

  /*
   * Creates a reader for a batch scan.
   *
   * schema: the schema to read: the one the user specified, or the one returned by schema
   * info: nothing yet but its size, so that inputs can be added within this version
   * reader: set to the reader handle
   */
  int (*create_reader)(void* source, struct ArrowSchema* schema, const SparkDsReaderInfo* info,
                       void** reader);

  /*
   * Creates a reader for a micro-batch streaming scan.
   *
   * schema: the columns to read: those of the schema that the user specified, or of the one
   *         returned by schema, or only the top-level columns of it that the query needs. Each
   *         array that read returns has exactly these columns.
   * info: the location of the checkpoint of the source
   * reader: set to the stream reader handle
   */
  int (*create_stream_reader)(void* source, struct ArrowSchema* schema,
                              const SparkDsStreamReaderInfo* info, void** reader);

  /*
   * Creates a writer for a batch write, or for a micro-batch of a streaming write.
   *
   * schema: the schema of the data to write
   * info: the mode of the write, and the ID of the query of a streaming write
   * set_state: called once with the state of the writer that create_data_writer needs on the
   *            executors, or not at all if there is none
   * writer: set to the writer handle
   */
  int (*create_writer)(void* source, struct ArrowSchema* schema, const SparkDsWriteInfo* info,
                       SparkDsSink set_state, void* sink, void** writer);

  /* Releases a data source handle. */
  void (*close)(void* source);

  /* -------------------------------------------------------------------------------------------
   * Batch reader (driver)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Pushes a LIMIT down to the reader. Spark only pushes a limit when the query does not filter
   * the rows, since it filters them after reading.
   *
   * applied: set to whether the reader returns at most `limit` rows in total. Spark applies the
   *          limit again after reading either way.
   */
  int (*push_limit)(void* reader, int32_t limit, bool* applied);

  /*
   * Prunes the columns to read.
   *
   * columns: the names of the count top-level columns that the query needs, which include the
   *          columns that it filters on. There may be none, for example for SELECT count(*):
   *          then the batches only have a number of rows.
   * pruned: set to whether read returns exactly these columns, in this order. If not, it returns
   *         all the columns of the schema to read.
   */
  int (*prune_columns)(void* reader, const char* const* columns, int64_t count, bool* pruned);

  /*
   * Plans the partitions of the scan. Spark reads each partition in a separate task.
   *
   * info: nothing yet but its size, so that inputs can be added within this version
   * add_partition: called with each partition, serialized in a format defined by the library
   * set_state: called once with the state of the reader that read needs on the executors, such as
   *            the pruned columns, or not at all if there is none
   */
  int (*partitions)(void* reader, const SparkDsPartitionsInfo* info, SparkDsSink add_partition,
                    SparkDsSink set_state, void* sink);

  /* Releases a batch reader handle. */
  void (*close_reader)(void* reader);

  /* -------------------------------------------------------------------------------------------
   * Stream reader (driver)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Returns the offset to start from when the streaming query has no checkpoint. Spark keeps it
   * under checkpoint_location, and only asks for it when the checkpoint does not have it yet: a
   * query that restarts before its first micro-batch is committed starts from the same offset.
   *
   * info: nothing yet but its size, so that inputs can be added within this version
   * set_offset: called once with the offset, as compact JSON on a single line
   */
  int (*initial_offset)(void* reader, const SparkDsInitialOffsetInfo* info, SparkDsSink set_offset,
                        void* sink);

  /*
   * Returns the most recent offset available.
   *
   * info: the offset that the next micro-batch starts after
   * set_offset: called once with the offset, as compact JSON on a single line
   */
  int (*latest_offset)(void* reader, const SparkDsLatestOffsetInfo* info, SparkDsSink set_offset,
                       void* sink);

  /*
   * Plans the partitions of a micro-batch.
   *
   * info: the offsets that the micro-batch reads the data after, up to and including the end
   * add_partition: called with each partition, serialized in a format defined by the library
   * set_state: called once with the state of the stream reader that read needs on the executors
   *            to read these partitions, or not at all if there is none
   */
  int (*stream_partitions)(void* reader, const SparkDsStreamPartitionsInfo* info,
                           SparkDsSink add_partition, SparkDsSink set_state, void* sink);

  /*
   * Tells the library that Spark has processed all the data up to and including an offset, and
   * will not ask for it again.
   *
   * info: the offset
   */
  int (*commit_offset)(void* reader, const SparkDsCommitOffsetInfo* info);

  /* Releases a stream reader handle, when the streaming query stops. */
  void (*close_stream_reader)(void* reader);

  /* -------------------------------------------------------------------------------------------
   * Partition reader (executor)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Reads a partition of a batch or streaming scan.
   *
   * task: the task that reads the partition, valid until the stream is released
   * info: the partition, the kind of the scan, the schema to read and the state of the reader
   * out: the stream to initialize. Each array of the stream is a struct array whose fields are the
   *      columns of the schema to read, info->schema. The callbacks of the stream can be called
   *      from any thread, but never concurrently. get_next may block, for example on network
   *      I/O, but Spark cannot interrupt native code: when the task is killed, Spark only stops
   *      reading once get_next returns, and then releases the stream. So a get_next that blocks
   *      forever keeps the task running, unless the task reaper (spark.task.reaper.enabled and
   *      spark.task.reaper.killTimeout) kills the executor. Use timeouts for blocking
   *      operations, and check task->is_interrupted.
   */
  int (*read)(const SparkDsTaskContext* task, const SparkDsReadInfo* info,
              struct ArrowArrayStream* out);

  /* -------------------------------------------------------------------------------------------
   * Writer (driver)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Commits a write, when all the tasks succeeded.
   *
   * info: the ID of the micro-batch, and the messages of the tasks
   */
  int (*commit)(void* writer, const SparkDsFinishWriteInfo* info);

  /*
   * Aborts a write, when it failed, or when Spark rejected what create_writer returned, such as a
   * state set more than once.
   *
   * info: the ID of the micro-batch, the messages of the tasks, and whether Spark rejected what
   *       create_writer returned
   */
  int (*abort)(void* writer, const SparkDsFinishWriteInfo* info);

  /* Releases a writer handle. */
  void (*close_writer)(void* writer);

  /* -------------------------------------------------------------------------------------------
   * Data writer (executor)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Creates the writer for one task.
   *
   * task: the task, with the partition that it writes, valid until the data writer is closed
   * info: the schema of the data, the state of the writer, and the ID of the micro-batch
   * data_writer: set to the data writer handle
   */
  int (*create_data_writer)(const SparkDsTaskContext* task, const SparkDsDataWriterInfo* info,
                            void** data_writer);

  /*
   * Writes a batch of data.
   *
   * array: a struct array whose fields are the columns, with the schema that create_data_writer
   *        got. The library may move it to keep its data, until close_data_writer: its buffers
   *        belong to the data writer, so the library must release it before close_data_writer
   *        returns. When Spark writes the batches of a native scan without converting them to
   *        rows, it copies the values of their columns as the scan returned them, without the
   *        checks of a conversion, such as the precision of a decimal.
   */
  int (*write)(void* data_writer, struct ArrowArray* array);

  /*
   * Commits the data written by the task. If it fails, Spark calls abort_data_writer. If it
   * succeeds, Spark does not call abort_data_writer, even if it then fails the task, for example
   * because set_message was called more than once.
   *
   * info: nothing yet but its size, so that inputs can be added within this version
   * set_message: called once with the message to pass to commit on the driver, or not at all for
   *              an empty message
   */
  int (*commit_data_writer)(void* data_writer, const SparkDsFinishDataWriterInfo* info,
                            SparkDsSink set_message, void* sink);

  /*
   * Aborts the data written by the task.
   *
   * info: nothing yet but its size, so that inputs can be added within this version
   */
  int (*abort_data_writer)(void* data_writer, const SparkDsFinishDataWriterInfo* info);

  /* Releases a data writer handle, after its commit or abort. */
  void (*close_data_writer)(void* data_writer);
} SparkDsLibrary;

/*
 * The function that a library exports. Spark calls it once in each JVM, when it loads the library,
 * before any other function.
 *
 * host: Spark, valid until the JVM exits
 * library: set to the function table of the library, for the version of this interface that it
 *          implements, or the newest one up to host->abi_version if it implements several. Spark
 *          reads the table after the call, so it is typically the address of a static struct.
 *
 * Returns 0 on success, or any other value if the library cannot run, after it logs why with
 * host->log.
 */
SPARK_DS_EXPORT int spark_ds_init(const SparkDsHost* host, const SparkDsLibrary** library);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_DATASOURCE_H */
