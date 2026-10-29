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
 * micro-batch streaming reads, batch and streaming writes, and filter, limit and column pushdown.
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
 *   by calling, each at most once and in this order, push_domains, push_limit and prune_columns,
 *   plans the partitions with partitions, serializes the state of the reader with
 *   serialize_reader, and closes it. Then it calls read on the executors, in one task for each
 *   partition. If Spark never plans the partitions, for example to only explain the plan of the
 *   query, it closes the reader once the reader is no longer referenced.
 * - Streaming reads. On the driver, Spark creates a stream reader with create_stream_reader and
 *   keeps it until the streaming query stops. For each micro-batch, it asks for the latest_offset,
 *   plans the partitions with stream_partitions and serializes the state with
 *   serialize_stream_reader, then calls read on the executors. Offsets are defined by the library,
 *   as compact JSON on a single line: Spark stores each offset as a line of the offset log of the
 *   checkpoint, so it rejects an offset that is not valid JSON or contains a line break. When a
 *   streaming query restarts, from its checkpoint, Spark creates a new stream reader and may call
 *   stream_partitions again for the last range that it planned before, so the partitions of a
 *   range must read the same data each time.
 * - Writes. On the driver, right before it starts the tasks of the write (or of a micro-batch),
 *   Spark creates a writer with create_writer, and serializes its state with serialize_writer.
 *   With adaptive query execution, Spark first runs the stages of the data to write that end with
 *   a shuffle, and creates no writer if they fail. The rest of the data, such as its last stage,
 *   is computed in the tasks of the write, so a writer may still be aborted because computing the
 *   data failed: do irreversible work, such as removing the existing data for an overwrite, in
 *   commit. On the executors, each task creates a data writer with create_data_writer, calls write
 *   for each batch of at most spark.sql.execution.arrow.maxRecordsPerBatch rows, which also ends
 *   once it holds spark.sql.execution.arrow.maxBytesPerBatch bytes, and calls commit_data_writer,
 *   or abort_data_writer if the task or the commit fails, before it closes the data writer. Then
 *   the driver calls commit with the messages of the tasks if they all succeeded, or abort
 *   otherwise, including when commit fails, and closes the writer. Several attempts of a task may
 *   write the same partition, after a failure or with speculative execution, each with a data
 *   writer of its own. Spark lets the attempts of a partition commit one at a time, and passes to
 *   commit the message of one committed attempt for each partition, so the write consists of the
 *   data of these attempts only. It aborts the data writers of the other attempts, except on an
 *   executor that is lost, where a data writer is neither committed nor aborted, and may even have
 *   committed before its message reached the driver. A streaming write commits or aborts each
 *   micro-batch (epoch). Spark may commit the same epoch more than once, for example when a
 *   streaming query restarts after a failure, so committing an epoch that is already committed
 *   must have no effect, for exactly-once output. The ID of the query, which create_writer gets for
 *   a streaming write, stays the same when the query restarts from its checkpoint, so the ID of the
 *   query and the epoch ID identify the output of a micro-batch, also when other queries write to
 *   the same destination. A micro-batch streaming query creates a new stream writer for each
 *   micro-batch, and Spark closes the previous stream writer of the query when the query creates a
 *   new one. Spark is not told when a streaming query stops, so it closes the last stream writer of
 *   the query once the writer is no longer referenced, or when the query is restarted. Streaming
 *   writes are micro-batch only: a query with continuous processing fails before create_writer.
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
 * - Memory. What Spark passes to a function, such as strings, byte arrays, SparkDsCreateInfo,
 *   SparkDsWriteInfo and SparkDsDomain, is only valid during the call, except for SparkDsHost and
 *   SparkDsTaskContext, which document how long they live. Strings are UTF-8 and NUL-terminated:
 *   Spark fails an operation whose options or column names contain the character U+0000. The
 *   library returns strings and byte arrays by passing them to a SparkDsSink, which copies them.
 * - State. Partitions, the state of readers and writers, and commit messages are byte arrays in a
 *   format defined by the library. Spark sends them to the executors and back.
 * - Arrow data. Schemas and data are exchanged with the Arrow C data interface
 *   (https://arrow.apache.org/docs/format/CDataInterface.html), as structs allocated by Spark.
 *   For an output struct, the library initializes it and Spark takes ownership. For an input
 *   struct, the library may move it to take ownership; otherwise, Spark releases it after the
 *   call returns, so the library must not use it afterwards. The arrays that the library exports
 *   must have an offset of 0, at every level: Arrow Java ignores the offsets of the arrays it
 *   imports, so Spark rejects an array with another offset, such as a zero-copy slice. Copy a
 *   sliced array before exporting it. Spark also rejects an array with fewer values than its
 *   parent refers to, such as a column shorter than its batch, and a batch with null rows, since
 *   Arrow Java ignores the validity of the struct of a batch: mark the values of its columns as
 *   null instead. As the C data interface requires, an array without a validity buffer must have
 *   a null_count of 0, and the buffers of an array must hold all of its values: the interface
 *   does not carry the sizes of the buffers, so Spark cannot check them, and reads past the end
 *   of a buffer that is too short.
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
 *   functions are NULL: Spark then fails the operation. The pushdown functions and the serialize
 *   functions are optional: without them, Spark pushes nothing down, and the reader or writer has
 *   no state. So are schema, without which the user must specify the schema, commit_offset and
 *   last_error.
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
 * - Spark only uses a new capability or write mode with a data source that declares it.
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
 * argument passed with the sink to the function that returns it.
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
   * SPARK_DS_WRITE_TRUNCATE to a data source with the capability SPARK_DS_TRUNCATE.
   */
  int32_t mode;
  /*
   * The ID of the streaming query of a streaming write, or NULL for a batch write. It stays the
   * same when the query restarts from its checkpoint, so with the epoch IDs, it identifies the
   * commits of the query: see Writes above.
   */
  const char* query_id;
} SparkDsWriteInfo;

/*
 * The domain of a column, which push_domains gets: the values that the column can have in the
 * rows that match the filters of a query. A value is in the domain when it is:
 * - null, if null_allowed is true;
 * - NaN, if nan_allowed is true, for a float or double column;
 * - another value, if ranges is NULL, or if it is in one of the ranges.
 * A reader can skip the data whose statistics show that it has no value in the domain of a column,
 * such as a file or a row group whose values are all below the ranges. But it must never skip a
 * row whose values are all in their domains, which Spark cannot detect. Spark evaluates the
 * filters again after reading, so the rows that a reader returns do not need to match them.
 *
 * Values are compared with the order of Spark SQL:
 * - numbers, dates and timestamps by their values. The bounds of the ranges of a float or double
 *   column are never NaN, and -0.0 is equal to 0.0: compare them with the comparisons of IEEE 754,
 *   as the operators of C, C++ and Rust do, not with a total order, such as f64::total_cmp in Rust,
 *   that puts -0.0 before 0.0. Data that has NaN values, or may have, can only be skipped if
 *   nan_allowed is false;
 * - strings by their UTF-8 bytes, which order the characters outside the Basic Multilingual Plane
 *   after all the others, unlike comparisons of UTF-16 strings;
 * - binary values as unsigned bytes, in lexicographic order: X'80' is greater than X'7F', and a
 *   value is greater than its prefixes;
 * - booleans with false before true.
 */
typedef struct SparkDsDomain {
  /* The size of this struct in the header that Spark was built with, as in SparkDsHost. */
  int64_t size;
  /*
   * The column, by the path_length indices of its fields: the index of the column in the schema to
   * read, the one that create_reader got, and then the index of the field in each struct, for a
   * field of a struct. {2, 0} is the first field of the third column.
   */
  const int32_t* path;
  int64_t path_length;
  /* Whether null is in the domain. */
  bool null_allowed;
  /*
   * Whether NaN is in the domain. Always false for the columns of types other than float and
   * double.
   */
  bool nan_allowed;
  /*
   * The ranges of the other values in the domain, or NULL if they are all in it. Otherwise, a
   * struct array with one row for each range, sorted, and with ranges that do not overlap, whose
   * fields are:
   * - low and high: the bounds of the range, of the Arrow type of the column in the schema to
   *   read. A null bound is unbounded: the range has no lower or no upper bound;
   * - low_inclusive and high_inclusive, booleans that are not nullable: whether the bound is in the
   *   range. They are false for a null bound.
   * A single value is a range whose bounds are equal and inclusive. An array without rows means
   * that no other value than null or NaN is in the domain. The ranges are an input struct: see
   * "Arrow data" above.
   */
  struct ArrowArray* ranges;
  /* The schema of ranges, or NULL if ranges is NULL. */
  struct ArrowSchema* ranges_schema;
} SparkDsDomain;

/* ---------------------------------------------------------------------------------------------
 * What the library returns to Spark
 * --------------------------------------------------------------------------------------------- */

/* The capabilities of a data source, which create returns: the operations that it supports. */
#define SPARK_DS_BATCH_READ 1       /* batch reads, with create_reader */
#define SPARK_DS_MICRO_BATCH_READ 2 /* micro-batch streaming reads, with create_stream_reader */
#define SPARK_DS_BATCH_WRITE 4      /* batch writes, with create_writer */
#define SPARK_DS_STREAMING_WRITE 8  /* streaming writes, with create_writer */
#define SPARK_DS_TRUNCATE 16        /* writes with the mode SPARK_DS_WRITE_TRUNCATE */

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
   * reader: set to the reader handle
   */
  int (*create_reader)(void* source, struct ArrowSchema* schema, void** reader);

  /*
   * Creates a reader for a micro-batch streaming scan.
   *
   * schema: the schema to read: the one the user specified, or the one returned by schema
   * reader: set to the stream reader handle
   */
  int (*create_stream_reader)(void* source, struct ArrowSchema* schema, void** reader);

  /*
   * Creates a writer for a batch write, or for a micro-batch of a streaming write.
   *
   * schema: the schema of the data to write
   * info: the mode of the write, and the ID of the query of a streaming write
   * writer: set to the writer handle
   */
  int (*create_writer)(void* source, struct ArrowSchema* schema, const SparkDsWriteInfo* info,
                       void** writer);

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
   * add_partition: called with each partition, serialized in a format defined by the library
   */
  int (*partitions)(void* reader, SparkDsSink add_partition, void* sink);

  /*
   * Serializes the state of the reader that read needs on the executors, such as the pruned
   * columns. Spark calls it after partitions.
   *
   * set_state: called once with the state, or not at all if there is none
   */
  int (*serialize_reader)(void* reader, SparkDsSink set_state, void* sink);

  /* Releases a batch reader handle. */
  void (*close_reader)(void* reader);

  /* -------------------------------------------------------------------------------------------
   * Stream reader (driver)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Returns the offset to start from when the streaming query has no checkpoint.
   *
   * set_offset: called once with the offset, as compact JSON on a single line
   */
  int (*initial_offset)(void* reader, SparkDsSink set_offset, void* sink);

  /*
   * Returns the most recent offset available.
   *
   * set_offset: called once with the offset, as compact JSON on a single line
   */
  int (*latest_offset)(void* reader, SparkDsSink set_offset, void* sink);

  /*
   * Plans the partitions that read the data after `start` up to and including `end`, two offsets
   * as JSON.
   *
   * add_partition: called with each partition, serialized in a format defined by the library
   */
  int (*stream_partitions)(void* reader, const char* start, const char* end,
                           SparkDsSink add_partition, void* sink);

  /*
   * Serializes the state of the stream reader that read needs on the executors. Spark calls it
   * after each call to stream_partitions.
   *
   * set_state: called once with the state, or not at all if there is none
   */
  int (*serialize_stream_reader)(void* reader, SparkDsSink set_state, void* sink);

  /*
   * Tells the library that Spark has processed all the data up to and including `end`, an offset
   * as JSON, and will not ask for it again.
   */
  int (*commit_offset)(void* reader, const char* end);

  /* Releases a stream reader handle, when the streaming query stops. */
  void (*close_stream_reader)(void* reader);

  /* -------------------------------------------------------------------------------------------
   * Partition reader (executor)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Reads a partition of a batch or streaming scan.
   *
   * task: the task that reads the partition, valid until the stream is released
   * reader_state: the state returned by serialize_reader or serialize_stream_reader, empty if
   *               there is none
   * partition: a partition returned by partitions or stream_partitions
   * out: the stream to initialize. Each array of the stream is a struct array whose fields are the
   *      columns to read: the columns of the schema to read, or the pruned columns. The
   *      callbacks of the stream can be called from any thread, but never concurrently. get_next
   *      may block, for example on network I/O, but Spark cannot interrupt native code: when the
   *      task is killed, Spark only stops reading once get_next returns, and then releases the
   *      stream. So a get_next that blocks forever keeps the task running, unless the task reaper
   *      (spark.task.reaper.enabled and spark.task.reaper.killTimeout) kills the executor. Use
   *      timeouts for blocking operations, and check task->is_interrupted.
   */
  int (*read)(const SparkDsTaskContext* task, const uint8_t* reader_state,
              int64_t reader_state_length, const uint8_t* partition, int64_t partition_length,
              struct ArrowArrayStream* out);

  /* -------------------------------------------------------------------------------------------
   * Writer (driver)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Serializes the state of the writer that create_data_writer needs on the executors. Spark
   * calls it once, after creating the writer.
   *
   * set_state: called once with the state, or not at all if there is none
   */
  int (*serialize_writer)(void* writer, SparkDsSink set_state, void* sink);

  /*
   * Commits a write, when all the tasks succeeded.
   *
   * epoch_id: the ID of the micro-batch for a streaming write, or -1 for a batch write
   * messages, message_lengths: the num_messages messages returned by commit_data_writer of each
   *                            task
   */
  int (*commit)(void* writer, int64_t epoch_id, const uint8_t* const* messages,
                const int64_t* message_lengths, int64_t num_messages);

  /*
   * Aborts a write, when it failed.
   *
   * epoch_id: the ID of the micro-batch for a streaming write, or -1 for a batch write
   * messages, message_lengths: the num_messages messages returned by commit_data_writer of each
   *                            task. A message is NULL if its task did not commit.
   */
  int (*abort)(void* writer, int64_t epoch_id, const uint8_t* const* messages,
               const int64_t* message_lengths, int64_t num_messages);

  /* Releases a writer handle. */
  void (*close_writer)(void* writer);

  /* -------------------------------------------------------------------------------------------
   * Data writer (executor)
   * ------------------------------------------------------------------------------------------- */

  /*
   * Creates the writer for one task.
   *
   * task: the task, with the partition that it writes, valid until the data writer is closed
   * writer_state: the state returned by serialize_writer, empty if there is none
   * epoch_id: the ID of the micro-batch for a streaming write, or -1 for a batch write
   * data_writer: set to the data writer handle
   */
  int (*create_data_writer)(const SparkDsTaskContext* task, const uint8_t* writer_state,
                            int64_t writer_state_length, int64_t epoch_id, void** data_writer);

  /*
   * Writes a batch of data.
   *
   * array: a struct array whose fields are the columns
   * schema: the schema of the array
   */
  int (*write)(void* data_writer, struct ArrowArray* array, struct ArrowSchema* schema);

  /*
   * Commits the data written by the task. If it fails, Spark calls abort_data_writer. If it
   * succeeds, Spark does not call abort_data_writer, even if it then fails the task, for example
   * because set_message was called more than once.
   *
   * set_message: called once with the message to pass to commit on the driver, or not at all for
   *              an empty message
   */
  int (*commit_data_writer)(void* data_writer, SparkDsSink set_message, void* sink);

  /* Aborts the data written by the task. */
  int (*abort_data_writer)(void* data_writer);

  /* Releases a data writer handle, after its commit or abort. */
  void (*close_data_writer)(void* data_writer);

  /* -------------------------------------------------------------------------------------------
   * Batch reader (driver), continued. These functions were added after the others, so they are at
   * the end of the table: a library built against a header without them has a smaller table, and
   * Spark does not call them. See Compatibility above.
   * ------------------------------------------------------------------------------------------- */

  /*
   * Pushes the domains of the columns that the filters of the query constrain down to the reader,
   * so that it can skip data: see SparkDsDomain. Spark derives them from the filters that compare
   * a column with literals, with =, <>, <=> (equal, with null equal to null), <, <=, >, >=, IN,
   * IS NULL, IS NOT NULL and STARTS_WITH, combined with AND, OR and NOT. A domain has all the
   * values that its column has in the rows for which the filters are true, and may have others:
   * a column that only one side of an OR constrains gets no domain from it, and the other filters,
   * such as id + 1 > 5, a = b or ENDS_WITH(s, 'x'), constrain no column. Spark evaluates all the
   * filters after reading anyway.
   *
   * Spark passes domains for the columns and the fields of structs of the types boolean, tinyint,
   * smallint, int, bigint, float, double, decimal, string, binary, date, timestamp and
   * timestamp_ntz, but not for strings with a non-binary collation, such as UTF8_LCASE. A later
   * version of Spark may pass domains for other columns, so a reader must ignore a domain that it
   * cannot use. Spark calls it at most once for a reader, before push_limit and prune_columns, with
   * at least one domain, and at most one domain for each column.
   *
   * domains: the count domains
   * used: an array of count values, all false, to set to true for the domains that the reader
   *       uses. Spark shows the filters of the used domains as pushed in the plan of the query.
   */
  int (*push_domains)(void* reader, const SparkDsDomain* const* domains, int64_t count,
                      bool* used);
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
