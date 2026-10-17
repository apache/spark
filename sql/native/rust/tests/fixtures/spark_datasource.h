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
 * micro-batch streaming reads, batch and streaming writes, and predicate, column and limit
 * pushdown.
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
 *   by calling, each at most once and in this order, push_predicates, push_limit and
 *   prune_columns, plans the partitions with partitions, serializes the state of the reader with
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
 *   the query once the writer is no longer referenced, or when the query is restarted.
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
 * - Memory. What Spark passes to a function, such as strings, byte arrays, SparkDsCreateInfo and
 *   SparkDsWriteInfo, is only valid during the call, except for SparkDsHost and
 *   SparkDsTaskContext, which document how long they live. Strings are UTF-8 and NUL-terminated.
 *   The JSON of the predicates is ASCII: it escapes the other characters, which any JSON parser
 *   decodes. The library returns strings and byte arrays by passing them to a SparkDsSink, which
 *   copies them.
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
 *   parent refers to, such as a column shorter than its batch. The buffers of an array must hold
 *   all of its values, as the C data interface requires: it does not carry the sizes of the
 *   buffers, so Spark cannot check them, and reads past the end of a buffer that is too short.
 * - Schemas. Spark types map to Arrow types as in spark.sql.execution.arrow: for example, bigint
 *   is int64, string is utf8, timestamp is timestamp[us] with a time zone and timestamp_ntz is
 *   timestamp[us] without one. Data read from a library may also use large_utf8 and utf8_view for
 *   strings, large_binary and binary_view for binary data, and any names for the fields of the
 *   entries of a map. Spark rejects the other Arrow types, such as unsigned integers, decimal256,
 *   large lists, fixed-size lists and list views, and dictionary encoding.
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
 * - new values may appear where this documentation says so, such as the names of pushed
 *   predicates. Spark only uses a new capability or write mode with a data source that declares
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

/* ---------------------------------------------------------------------------------------------
 * What the library returns to Spark
 * --------------------------------------------------------------------------------------------- */

/* The capabilities of a data source, which create returns: the operations that it supports. */
#define SPARK_DS_BATCH_READ 1       /* batch reads, with create_reader */
#define SPARK_DS_MICRO_BATCH_READ 2 /* micro-batch streaming reads, with create_stream_reader */
#define SPARK_DS_BATCH_WRITE 4      /* batch writes, with create_writer */
#define SPARK_DS_STREAMING_WRITE 8  /* streaming writes, with create_writer */
#define SPARK_DS_TRUNCATE 16        /* writes with the mode SPARK_DS_WRITE_TRUNCATE */

/* How a reader uses a predicate pushed with push_predicates. */
#define SPARK_DS_PREDICATE_UNUSED 0 /* not at all */
/* only to skip data, so it may return rows for which the predicate is not true */
#define SPARK_DS_PREDICATE_PARTIAL 1
/* to only return the rows for which the predicate is true */
#define SPARK_DS_PREDICATE_COMPLETE 2

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
   * Pushes predicates down to the reader. The predicates are combined with AND.
   *
   * Each predicate is a JSON expression tree. An expression is one of:
   * - {"type": "column", "name": ["a", "b"]}: a column. A nested field has a name part for each
   *   level.
   * - {"type": "literal", "dataType": "bigint", "value": 1}: a literal. The data type is a Spark
   *   SQL type name such as int, string or decimal(10,2). The value is null, a JSON boolean or
   *   number for boolean and numeric types, a string for string, a base64 string for binary, a
   *   decimal string for decimal, the days since the epoch for date, and the microseconds since
   *   the epoch for timestamp and timestamp_ntz.
   * - {"type": "function", "name": ">", "children": [...]}: a predicate, with the name it has in
   *   org.apache.spark.sql.connector.expressions.filter.Predicate. The names are =, <>, <=>
   *   (equal, with null equal to null), <, <=, > and >= with two children, IN with the value to
   *   look for and then the values to look in, IS_NULL and IS_NOT_NULL with one child,
   *   STARTS_WITH, ENDS_WITH and CONTAINS with the string and the string to look for, AND and OR
   *   with two predicates, and NOT with one. The children of the other functions are columns,
   *   literals or predicates.
   * Spark does not pass predicates that it cannot express this way, such as predicates that use
   * arithmetic or other functions, nor predicates on columns that contain strings with a
   * non-binary collation, such as UTF8_LCASE: strings are compared by their UTF-8 bytes. A later
   * version of Spark may pass predicates with other names without changing SPARK_DS_ABI_VERSION,
   * so a reader must ignore a predicate that contains a name that it does not know, or a data
   * type that it does not support.
   *
   * Spark evaluates all the predicates again after reading, so a reader may use them only to skip
   * data, for example the files whose statistics show that no row matches. But it must never drop
   * or skip a row for which the predicates are true under the semantics of Spark SQL, which Spark
   * cannot detect. In particular:
   * - For floating-point columns, NaN is equal to NaN and greater than any other value, including
   *   positive infinity, and -0.0 is equal to 0.0. IEEE 754 comparisons treat NaN differently, and
   *   total orders, such as f64::total_cmp in Rust or the comparison kernels of arrow-rs, also
   *   order -0.0 before 0.0. For example, d > 1.0 is true for a NaN value of d, and d = 0.0 for
   *   -0.0.
   * - Comparisons with null are null, and a row only matches if the predicates are true. For
   *   example, NOT (i IN (1, NULL)) matches no row.
   * - Strings are compared by their UTF-8 bytes, which order the characters outside the Basic
   *   Multilingual Plane after all the others, unlike comparisons of UTF-16 strings.
   * - To use only a part of a predicate, a reader may take the parts that it does not support as
   *   true, but not under a NOT, where that would drop rows.
   * The file org/apache/spark/sql/datasource/predicate-cases.json in the jar of Spark SQL lists
   * predicates as Spark passes them to this function, with the rows of a table for which they are
   * true, to test a reader with.
   *
   * predicates: the count predicates, as JSON
   * uses: an array of count values, all SPARK_DS_PREDICATE_UNUSED, to set to how the reader uses
   *       each predicate, a SPARK_DS_PREDICATE_* value. Spark shows the predicates that the reader
   *       uses as pushed in the plan of the query, and evaluates all the predicates again after
   *       reading anyway. It takes other values as SPARK_DS_PREDICATE_PARTIAL.
   */
  int (*push_predicates)(void* reader, const char* const* predicates, int64_t count,
                         int32_t* uses);

  /*
   * Pushes a LIMIT down to the reader. Spark only pushes a limit when the query does not filter
   * the rows, since it evaluates the predicates after reading.
   *
   * applied: set to whether the reader returns at most `limit` rows in total. Spark applies the
   *          limit again after reading either way.
   */
  int (*push_limit)(void* reader, int32_t limit, bool* applied);

  /*
   * Prunes the columns to read.
   *
   * columns: the names of the count top-level columns that the query needs, which include the
   *          columns of the pushed predicates. There may be none, for example for
   *          SELECT count(*): then the batches only have a number of rows.
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
   * Serializes the state of the reader that read needs on the executors, such as the pushed
   * predicates. Spark calls it after partitions.
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
