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
 * A native data source is a shared library that exports the C functions declared here. Spark
 * calls them with the Foreign Function and Memory API of Java, so native data sources require
 * Java 22 or later. Spark looks the functions up in each library separately, so any number of
 * libraries that export the same functions can be loaded side by side. Spark adapts a native data
 * source to Data Source V2, with batch and micro-batch streaming reads, batch and streaming
 * writes, and predicate, column and limit pushdown.
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
 * Spark creates a data source with spark_ds_create on the driver for each schema inference, scan
 * and write, and closes it right after.
 *
 * - Batch reads. On the driver, Spark creates a reader with spark_ds_create_reader, pushes
 *   operations down by calling, each at most once and in this order, spark_ds_push_predicates,
 *   spark_ds_push_limit and spark_ds_prune_columns, plans the partitions with
 *   spark_ds_partitions, serializes the state of the reader with spark_ds_serialize_reader, and
 *   closes it. Then it calls spark_ds_read on the executors, in one task for each partition.
 * - Streaming reads. On the driver, Spark creates a stream reader with
 *   spark_ds_create_stream_reader and keeps it until the streaming query stops. For each
 *   micro-batch, it asks for the spark_ds_latest_offset, plans the partitions with
 *   spark_ds_stream_partitions and serializes the state with spark_ds_serialize_stream_reader,
 *   then calls spark_ds_read on the executors. Offsets are JSON strings defined by the library.
 *   When a streaming query restarts, from its checkpoint, Spark creates a new stream reader and
 *   may call spark_ds_stream_partitions again for the last range that it planned before, so the
 *   partitions of a range must read the same data each time.
 * - Writes. On the driver, once it computed the data to write, right before it starts the tasks
 *   of the write (or of a micro-batch), Spark creates a writer with spark_ds_create_writer or
 *   spark_ds_create_stream_writer, and serializes its state with spark_ds_serialize_writer. If
 *   computing the data fails, Spark creates no writer. On the executors, each task creates a data
 *   writer with spark_ds_create_data_writer, calls spark_ds_write for each batch of at most
 *   spark.sql.execution.arrow.maxRecordsPerBatch rows, which also ends once it holds
 *   spark.sql.execution.arrow.maxBytesPerBatch bytes, and calls spark_ds_commit_data_writer, or
 *   spark_ds_abort_data_writer if the task or the commit fails. Then the driver calls
 *   spark_ds_commit with the messages of the tasks if they all succeeded, or spark_ds_abort
 *   otherwise, including when spark_ds_commit fails, and closes the writer. Several attempts of
 *   a task may write the same partition, after a failure or with speculative execution, each
 *   with a data writer of its own. Spark lets the attempts of a partition commit one at a time,
 *   and passes to spark_ds_commit the message of one committed attempt for each partition, so
 *   the write consists of the data of these attempts only. It aborts the data writers of the
 *   other attempts, except on an executor that is lost, where a data writer is neither committed
 *   nor aborted, and may even have committed before its message reached the driver. A streaming
 *   write commits or aborts each micro-batch (epoch). Spark may commit the same epoch more than
 *   once, for example when a streaming query restarts after a failure, so committing an epoch
 *   that is already committed must have no effect, for exactly-once output. A micro-batch
 *   streaming query creates a new stream writer for each micro-batch, and Spark closes the
 *   previous stream writer of the query when the query creates a new one. Spark is not told when
 *   a streaming query stops, so it closes the last stream writer of the query once the writer is
 *   no longer referenced, or when the query is restarted.
 *
 * Conventions
 * -----------
 * - Errors. The functions that return an int return 0 on success, or any other value on failure.
 *   They then set the message that spark_ds_last_error returns, and Spark reports it as a
 *   NATIVE_DATA_SOURCE_ERROR. The close functions cannot fail. A C++ exception or a Rust panic
 *   must not cross the interface: catch it and return an error instead.
 * - Handles. The create functions return an opaque, non-NULL handle in their last argument,
 *   typically a pointer: Spark reports a NULL handle as an error. Spark releases each handle
 *   exactly once with the matching close function, or with spark_ds_commit_data_writer or
 *   spark_ds_abort_data_writer for data writers, though not always right after the last use of a
 *   stream writer (see Writes above). A reader or writer handle must not depend on the data
 *   source handle it was created from, because Spark closes the data source handle right after
 *   creating it.
 * - Memory. What Spark passes to a function, such as strings and byte arrays, is only valid
 *   during the call. Strings are UTF-8 and NUL-terminated. The library returns strings and byte
 *   arrays by passing them to a SparkDsSink, which copies them.
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
 *   timestamp[us] without one. Data read from a library may also use large_utf8, utf8_view and
 *   the other variants of the same type, and any names for the fields of the entries of a map,
 *   but not unsigned integers, decimal256, list views or dictionary encoding.
 * - Options. Option keys are passed in lower case, because Spark matches them
 *   case-insensitively.
 * - Optional functions. A library only needs to export the functions for the operations it
 *   supports. If a function is missing, Spark treats the operation as unsupported, except for the
 *   pushdown functions and the serialize functions, which then push nothing down and return no
 *   state, spark_ds_commit_offset, which then does nothing, and spark_ds_last_error.
 *   spark_ds_abi_version is required.
 * - Threads. Spark calls the library from multiple threads, and calls on different handles may
 *   run concurrently. Calls on the same handle never run concurrently. A handle may be released
 *   from a thread other than the threads that used it, such as a JVM Cleaner thread when Spark
 *   releases a handle once it is no longer referenced, for example the last stream writer of a
 *   streaming query. A SparkDsSink must be called on the thread of the call that it was passed
 *   to, during that call.
 *
 * Compatibility
 * -------------
 * Spark binds the functions of a library by their names, so the names of the functions and their
 * signatures are the interface. Within a SPARK_DS_ABI_VERSION:
 * - the signature and the semantics of an existing function never change;
 * - new functions may be added, only as optional functions, so that a library that does not
 *   export them keeps working as before;
 * - new values may appear where this documentation says so, such as the names of pushed
 *   predicates.
 * An incompatible change uses a new SPARK_DS_ABI_VERSION, with new names. A library reports the
 * version it implements with spark_ds_abi_version, and Spark rejects a library of a version it
 * does not support. Spark supports version 1 at least in all the releases of Spark 4 from 4.4.0.
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

/*
 * Returns the message of the last error on the calling thread, as a NUL-terminated UTF-8 string
 * that stays valid until the next call on that thread. Optional: without it, Spark only reports
 * the value that the function returned.
 */
const char* spark_ds_last_error(void);

/* Returns the version of this interface that the library implements, SPARK_DS_ABI_VERSION. */
int32_t spark_ds_abi_version(void);

/* ---------------------------------------------------------------------------------------------
 * Data source (driver)
 * --------------------------------------------------------------------------------------------- */

/*
 * Creates a data source with the options given by the user, such as
 * spark.read.option("key", "value").
 *
 * name: the name of the data source, as listed in the package manifest, or in lower case for an
 *       installed library. A library can implement several data sources.
 * option_keys, option_values: the num_options options, with the keys in lower case
 * source: set to the data source handle
 */
int spark_ds_create(const char* name, const char* const* option_keys,
                    const char* const* option_values, int64_t num_options, void** source);

/*
 * Exports the schema of the data. Spark calls it only when the user does not specify a schema.
 *
 * out: initialized with a struct type whose fields are the columns
 */
int spark_ds_schema(void* source, struct ArrowSchema* out);

/*
 * Creates a reader for a batch scan.
 *
 * schema: the schema to read: the one the user specified, or the one returned by spark_ds_schema
 * reader: set to the reader handle
 */
int spark_ds_create_reader(void* source, struct ArrowSchema* schema, void** reader);

/*
 * Creates a reader for a micro-batch streaming scan.
 *
 * schema: the schema to read: the one the user specified, or the one returned by spark_ds_schema
 * reader: set to the stream reader handle
 */
int spark_ds_create_stream_reader(void* source, struct ArrowSchema* schema, void** reader);

/*
 * Creates a writer for a batch write.
 *
 * schema: the schema of the data to write
 * overwrite: whether to replace the existing data (save mode "overwrite") instead of appending to
 *            it
 * writer: set to the writer handle
 */
int spark_ds_create_writer(void* source, struct ArrowSchema* schema, bool overwrite,
                           void** writer);

/*
 * Creates a writer for a streaming write.
 *
 * schema: the schema of the data to write
 * overwrite: whether to replace the existing data in each micro-batch (output mode "complete")
 *            instead of appending to it
 * writer: set to the writer handle
 */
int spark_ds_create_stream_writer(void* source, struct ArrowSchema* schema, bool overwrite,
                                  void** writer);

/* Releases a data source handle. */
void spark_ds_close(void* source);

/* ---------------------------------------------------------------------------------------------
 * Batch reader (driver)
 * --------------------------------------------------------------------------------------------- */

/*
 * Pushes predicates down to the reader. The predicates are combined with AND.
 *
 * Each predicate is a JSON expression tree. An expression is one of:
 * - {"type": "column", "name": ["a", "b"]}: a column. A nested field has a name part for each
 *   level.
 * - {"type": "literal", "dataType": "bigint", "value": 1}: a literal. The data type is a Spark SQL
 *   type name such as int, string or decimal(10,2). The value is null, a JSON boolean or number
 *   for boolean and numeric types, a string for string, a base64 string for binary, a decimal
 *   string for decimal, the days since the epoch for date, and the microseconds since the epoch
 *   for timestamp and timestamp_ntz.
 * - {"type": "function", "name": ">", "children": [...]}: a predicate, with the name it has in
 *   org.apache.spark.sql.connector.expressions.filter.Predicate. The names are =, <>, <=> (equal,
 *   with null equal to null), <, <=, > and >= with two children, IN with the value to look for
 *   and then the values to look in, IS_NULL and IS_NOT_NULL with one child, STARTS_WITH,
 *   ENDS_WITH and CONTAINS with the string and the string to look for, AND and OR with two
 *   predicates, and NOT with one. The children of the other functions are columns, literals or
 *   predicates.
 * Spark does not pass predicates that it cannot express this way, such as predicates that use
 * arithmetic or other functions, nor predicates on columns that contain strings with a
 * non-binary collation, such as UTF8_LCASE: strings are compared by their UTF-8 bytes. A later
 * version of Spark may pass predicates with other names without changing SPARK_DS_ABI_VERSION,
 * so a reader must not report a predicate as evaluated completely if it contains a name that the
 * reader does not know, or a data type that it does not support.
 *
 * A predicate that the reader evaluates completely must follow the semantics of Spark SQL. In
 * particular, for floating-point columns, NaN is equal to NaN and greater than any other value,
 * including positive infinity, and -0.0 is equal to 0.0, unlike IEEE 754 comparisons. For
 * example, d > 1.0 is true for a NaN value of d. Comparisons with null are null, so a row only
 * matches if the predicate is true.
 *
 * predicates: the count predicates, as JSON
 * evaluated: an array of count values, all false, to set to whether the reader evaluates each
 *            predicate completely, so that Spark does not have to evaluate it again. The reader
 *            can still use the other predicates to skip data.
 */
int spark_ds_push_predicates(void* reader, const char* const* predicates, int64_t count,
                             bool* evaluated);

/*
 * Pushes a LIMIT down to the reader.
 *
 * applied: set to whether the reader returns at most `limit` rows in total. Spark applies the
 *          limit again after reading either way.
 */
int spark_ds_push_limit(void* reader, int32_t limit, bool* applied);

/*
 * Prunes the columns to read.
 *
 * columns: the names of the count top-level columns that the query needs
 * pruned: set to whether spark_ds_read returns exactly these columns, in this order. If not, it
 *         returns all the columns of the schema to read.
 */
int spark_ds_prune_columns(void* reader, const char* const* columns, int64_t count,
                           bool* pruned);

/*
 * Plans the partitions of the scan. Spark reads each partition in a separate task.
 *
 * add_partition: called with each partition, serialized in a format defined by the library
 */
int spark_ds_partitions(void* reader, SparkDsSink add_partition, void* sink);

/*
 * Serializes the state of the reader that spark_ds_read needs on the executors, such as the
 * pushed predicates. Spark calls it after spark_ds_partitions.
 *
 * set_state: called once with the state, or not at all if there is none
 */
int spark_ds_serialize_reader(void* reader, SparkDsSink set_state, void* sink);

/* Releases a batch reader handle. */
void spark_ds_close_reader(void* reader);

/* ---------------------------------------------------------------------------------------------
 * Stream reader (driver)
 * --------------------------------------------------------------------------------------------- */

/*
 * Returns the offset to start from when the streaming query has no checkpoint.
 *
 * set_offset: called once with the offset, as JSON
 */
int spark_ds_initial_offset(void* reader, SparkDsSink set_offset, void* sink);

/*
 * Returns the most recent offset available.
 *
 * set_offset: called once with the offset, as JSON
 */
int spark_ds_latest_offset(void* reader, SparkDsSink set_offset, void* sink);

/*
 * Plans the partitions that read the data after `start` up to and including `end`, two offsets
 * as JSON.
 *
 * add_partition: called with each partition, serialized in a format defined by the library
 */
int spark_ds_stream_partitions(void* reader, const char* start, const char* end,
                               SparkDsSink add_partition, void* sink);

/*
 * Serializes the state of the stream reader that spark_ds_read needs on the executors. Spark
 * calls it after each call to spark_ds_stream_partitions.
 *
 * set_state: called once with the state, or not at all if there is none
 */
int spark_ds_serialize_stream_reader(void* reader, SparkDsSink set_state, void* sink);

/*
 * Tells the library that Spark has processed all the data up to and including `end`, an offset
 * as JSON, and will not ask for it again.
 */
int spark_ds_commit_offset(void* reader, const char* end);

/* Releases a stream reader handle, when the streaming query stops. */
void spark_ds_close_stream_reader(void* reader);

/* ---------------------------------------------------------------------------------------------
 * Partition reader (executor)
 * --------------------------------------------------------------------------------------------- */

/*
 * Reads a partition of a batch or streaming scan.
 *
 * reader_state: the state returned by spark_ds_serialize_reader or
 *               spark_ds_serialize_stream_reader, empty if there is none
 * partition: a partition returned by spark_ds_partitions or spark_ds_stream_partitions
 * out: the stream to initialize. Each array of the stream is a struct array whose fields are the
 *      columns to read: the columns of the schema to read, or the pruned columns. Spark calls the
 *      stream from the thread of the task that reads the partition. get_next may block, for
 *      example on network I/O, but Spark cannot interrupt native code: when the task is killed,
 *      Spark only stops reading once get_next returns, and then releases the stream. So a
 *      get_next that blocks forever keeps the task running, unless the task reaper
 *      (spark.task.reaper.enabled and spark.task.reaper.killTimeout) kills the executor. Use
 *      timeouts for blocking operations.
 */
int spark_ds_read(const uint8_t* reader_state, int64_t reader_state_length,
                  const uint8_t* partition, int64_t partition_length,
                  struct ArrowArrayStream* out);

/* ---------------------------------------------------------------------------------------------
 * Writer (driver)
 * --------------------------------------------------------------------------------------------- */

/*
 * Serializes the state of the writer that spark_ds_create_data_writer needs on the executors.
 * Spark calls it once, after creating the writer.
 *
 * set_state: called once with the state, or not at all if there is none
 */
int spark_ds_serialize_writer(void* writer, SparkDsSink set_state, void* sink);

/*
 * Commits a write, when all the tasks succeeded.
 *
 * epoch_id: the ID of the micro-batch for a streaming write, or -1 for a batch write
 * messages, message_lengths: the num_messages messages returned by spark_ds_commit_data_writer
 *                            of each task
 */
int spark_ds_commit(void* writer, int64_t epoch_id, const uint8_t* const* messages,
                    const int64_t* message_lengths, int64_t num_messages);

/*
 * Aborts a write, when it failed.
 *
 * epoch_id: the ID of the micro-batch for a streaming write, or -1 for a batch write
 * messages, message_lengths: the num_messages messages returned by spark_ds_commit_data_writer
 *                            of each task. A message is NULL if its task did not commit.
 */
int spark_ds_abort(void* writer, int64_t epoch_id, const uint8_t* const* messages,
                   const int64_t* message_lengths, int64_t num_messages);

/* Releases a writer handle. */
void spark_ds_close_writer(void* writer);

/* ---------------------------------------------------------------------------------------------
 * Data writer (executor)
 * --------------------------------------------------------------------------------------------- */

/*
 * Creates the writer for one task.
 *
 * writer_state: the state returned by spark_ds_serialize_writer, empty if there is none
 * partition_id: the partition that the task writes
 * task_id: the ID of the task attempt
 * epoch_id: the ID of the micro-batch for a streaming write, or -1 for a batch write
 * data_writer: set to the data writer handle
 */
int spark_ds_create_data_writer(const uint8_t* writer_state, int64_t writer_state_length,
                                int32_t partition_id, int64_t task_id, int64_t epoch_id,
                                void** data_writer);

/*
 * Writes a batch of data.
 *
 * array: a struct array whose fields are the columns
 * schema: the schema of the array
 */
int spark_ds_write(void* data_writer, struct ArrowArray* array, struct ArrowSchema* schema);

/*
 * Commits the data written by the task and releases the data writer handle. If it fails, the
 * handle is not released: Spark then calls spark_ds_abort_data_writer with it, to abort the data
 * written by the task and release the handle.
 *
 * set_message: called once with the message to pass to spark_ds_commit on the driver, or not at
 *              all for an empty message
 */
int spark_ds_commit_data_writer(void* data_writer, SparkDsSink set_message, void* sink);

/* Aborts the data written by the task and releases the data writer handle. */
int spark_ds_abort_data_writer(void* data_writer);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_DATASOURCE_H */
