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
 * spark_udf.h: the binary interface between Spark and user-defined functions implemented in
 * native code, such as Rust or C++. Since Spark 5.0.0.
 *
 * A native function library is a shared library that exports a single C function,
 * spark_udf_init, which returns the table of the functions that implement its user-defined
 * functions, a SparkUdfLibrary. Each library has its own table, so any number of libraries can be
 * loaded side by side. Spark evaluates a native function on Arrow batches, in the process that
 * loads the library, the host. The interface does not depend on the host: the JVM host of Spark
 * calls the library with the Foreign Function and Memory API of Java, so it requires Java 22 or
 * later, and evaluates the calls in the process of the executor, without a worker process.
 * Version 1 has scalar functions: a call gets a batch of rows and returns one value for each row.
 *
 * This header is self-contained: it only depends on the C standard library, and defines the
 * structs of the Arrow C data interface unless they are already defined, as by Arrow C++. It
 * needs C11 or C++11, for its anonymous unions, alignment specifiers and static assertions. A
 * library can copy it into its source tree, or use the copy in the include directory of a Spark
 * distribution. A library built against it works with all the releases of Spark that support its
 * SPARK_UDF_ABI_VERSION. A library can also implement native data sources: it then includes
 * spark_datasource.h too, and exports both spark_ds_init and spark_udf_init. The two headers
 * define the Arrow structs under the same guard, ARROW_C_DATA_INTERFACE, and no other name in
 * common, so a source file can include both, in either order.
 *
 * The functions of a library come in a native package, as native data sources do: a zip file with
 * the extension .sparkpkg that contains a manifest named spark-native-datasource.json and the
 * library built for one or more platforms. The manifest lists the functions of the library, with
 * the types of their arguments and of their result, and the version of this interface that the
 * library implements, as udfAbiVersion. A session registers a function of a package, for example
 * with CREATE TEMPORARY FUNCTION f AS 'my_function' USING PACKAGE 'my_package.sparkpkg', and then
 * calls it by name, as f. Spark analyzes and plans the calls with the types of the manifest: the
 * driver does not load the library, only the processes that evaluate the calls do.
 *
 * Lifecycle
 * ---------
 * Spark loads a library once in each process that evaluates one of its functions, and calls
 * spark_udf_init before any other function of its table. A library that also exports
 * spark_ds_init gets each of the two calls once, the first time Spark uses that interface, in
 * either order, but never at the same time.
 *
 * A task evaluates each call of a native function in a query with a state of its own, so that
 * SELECT f(a), f(b) has two states in each task that evaluates it. The first time the task
 * evaluates the call, it creates the state with create, which gets the name of the function, as
 * listed in the manifest, the schemas of its arguments and of its result, the time zone of the
 * session and the task. Then it calls eval once for each batch of rows, in the order of the rows,
 * and closes the state with close after the last batch, or earlier when it stops evaluating the
 * call, for example because the task fails or a LIMIT is reached. A task that evaluates no row of
 * a call creates no state for it. States are never shared: each task has its own, and so has each
 * attempt of a task, after a failure or with speculative execution.
 *
 * Spark may evaluate a function for rows that the query drops afterwards, such as the rows that a
 * later filter rejects, and evaluates rows again when a task runs again. It evaluates a call in a
 * branch of CASE WHEN or IF, in an argument of COALESCE or in an operand of AND or OR for every
 * row that reaches the call, also where the query does not use its value: a function that fails
 * on some arguments must not rely on a condition in the query to avoid them. A function that the
 * manifest declares deterministic must return the same result for the same arguments, and Spark
 * may then evaluate the identical calls of a query only once. Spark never evaluates a native
 * function on the driver, for example to fold a call whose arguments are constants.
 *
 * Conventions
 * -----------
 * - Errors. The functions that return an int return 0 on success, or any other value on failure.
 *   They then set the message that last_error returns, and Spark reports it as a NATIVE_UDF_ERROR,
 *   which names the function and its package, and fails the task. close cannot fail. A C++
 *   exception or a Rust panic must not cross the interface: catch it and return an error instead.
 *   A crash, such as a segmentation fault, ends the process that loaded the library.
 * - States. create returns an opaque, non-NULL state in its last argument, typically a pointer:
 *   Spark reports a NULL state as an error. Spark releases each state exactly once, with close, and
 *   no other function releases a state. It closes every state that create returned, also after an
 *   eval on it failed.
 * - Order of the calls. Spark calls eval and close only on a state that create returned and that it
 *   did not close yet. After eval fails on a state, Spark calls no other function on it than
 *   close. It does not call close after a create that failed, since there is no state.
 * - Memory. What Spark passes to a function, such as strings and the structs whose names end with
 *   Info, is only valid during the call, except SparkUdfHost and SparkUdfTaskContext, which
 *   document how long they live, and the Arrow structs that the library moves (see below).
 *   Strings are UTF-8 and NUL-terminated: Spark rejects a manifest in which the name of a
 *   function, or of a field of a struct type, contains the character U+0000.
 * - Arrow data. Schemas and data are exchanged with the Arrow C data interface
 *   (https://arrow.apache.org/docs/format/CDataInterface.html), as structs allocated by Spark.
 *   For an output struct, the library initializes it and Spark takes ownership. For an input
 *   struct, the library may move it to take ownership; otherwise, Spark releases it after the call
 *   returns, so the library must not use it afterwards.
 *   - The input of eval is a struct array of at least one row, with a null_count of 0, since no
 *     row is null, whose children are the arguments of the call, in order, with the types of the
 *     schema of the arguments that create got. The validity buffer of an argument marks its null
 *     values. The arrays may have an offset other than 0, at every level, and buffers that hold
 *     more values than the arrays refer to: read them from their offsets, as the C data interface
 *     specifies. A library that moves the input may keep its data until close returns, for
 *     example to return arrays that share its buffers: Spark does not write to these buffers until
 *     the library releases the input, which it must do before close returns. An array that eval
 *     returns must not share the buffers of an input that the library did not move. The library
 *     must not write to the buffers of an input array, whether or not it moved it: Spark may read
 *     them after the call.
 *   - eval initializes out with an array of exactly as many values as the input has rows, of the
 *     type of the result in the schema that create got: the same format, with the same number of
 *     children at every level. The JVM host imports it with Arrow Java, which ignores the offsets
 *     of the arrays that it imports, so the array must have an offset of 0, at every level: copy a
 *     sliced array before returning it.
 *     Spark rejects an array with another offset, an array with fewer values than its parent
 *     refers to, and an array without a validity buffer whose null_count is not 0. As the C data
 *     interface requires, the buffers of an array must hold all of its values: the interface does
 *     not carry the sizes of the buffers, so Spark cannot check them, and reads past the end of a
 *     buffer that is too short. Spark sets out to zeros before each call, releases it once it read
 *     it, and releases every array that eval returned before it closes the state. When eval fails,
 *     Spark only releases out if the library initialized it, that is, if its release is not NULL.
 * - Types. These are the types of version 1, as in spark_datasource.h, with their Arrow types and
 *   the formats of the C data interface: boolean is bool (b), tinyint, smallint, int and bigint are
 *   int8 (c), int16 (s), int32 (i) and int64 (l), float and double are float32 (f) and float64
 *   (g), decimal(p, s) is decimal128(p, s) (d:p,s), string is utf8 (u), binary is binary (z), date
 *   is date32 (tdD), timestamp is timestamp[us] with the time zone of the session (tsu:<zone>),
 *   timestamp_ntz is timestamp[us] without a time zone (tsu:), and array, map and struct are list
 *   (+l), map (+m) and struct (+s). The manifest gives the types of the arguments and of the
 *   result of each function in Spark SQL, such as array<string>, and Spark rejects a manifest with
 *   another type, such as an interval, variant or time, before a query uses the function: a later
 *   version may support other types. Spark casts the arguments of a call to the types of the
 *   manifest, as for other functions with declared types, so the schemas that create gets always
 *   have them. The names of the fields of the schemas are not specified, except for the fields of
 *   structs. An argument is only null where its schema is nullable. The result is nullable, so
 *   eval may return null values.
 * - Threads. Spark calls the library from multiple threads, and calls on different states may run
 *   concurrently, as in the tasks that an executor runs at the same time: data that states share,
 *   such as a table that the library builds once, must be safe to use from several threads. Calls
 *   on the same state never run concurrently, but may come from different threads: Spark may
 *   close a state, and release the arrays that eval returned, from a thread that runs when the
 *   task ends, though never while a call on the state runs. The functions of SparkUdfHost and
 *   SparkUdfTaskContext can be called from any thread.
 * - Interruption. Spark cannot interrupt native code: a killed task, for example of a canceled
 *   query, only stops once eval returns. Spark checks whether the task was killed between the
 *   batches, so an eval that runs for long, or blocks, should check task->is_interrupted
 *   regularly, for example every few thousand rows, and fail once it is true. An eval that never
 *   returns keeps the task running, unless the host ends its process: the JVM host does so with
 *   the task reaper (spark.task.reaper.enabled and spark.task.reaper.killTimeout), which kills the
 *   executor.
 * - Batches. A batch has at most spark.sql.execution.arrow.maxRecordsPerBatch rows, and also ends
 *   once its arguments and the input rows that Spark keeps until it has the results of the batch
 *   hold spark.sql.execution.arrow.maxBytesPerBatch bytes, or at the end of the rows of the task.
 * - Memory accounting. Spark does not manage the memory of a library: what the library allocates,
 *   and the arrays that eval returns, are not accounted by the host. On the JVM host, which reads
 *   the arrays without copying them, they come out of the memory overhead of the executor. Bound
 *   this memory, for example by the size of the batches.
 * - Optional functions. last_error is optional: without it, Spark only reports the value that a
 *   function returned. create, eval and close are required: Spark rejects a table without them,
 *   such as one whose size ends before close.
 *
 * Compatibility
 * -------------
 * As in spark_datasource.h, the function table and the structs that Spark passes to the library
 * start with their size, so that they can grow. They have the same layout on every target, that
 * of 64-bit targets: each pointer and function pointer is in an 8-byte slot, a union with an
 * int64_t aligned to 8, and this header checks the size of each struct and the offset of each
 * field. On a target with 4-byte pointers, a pointer fills the first 4 bytes of its slot, and the
 * other 4 bytes are zero: the host zeroes the structs that it passes, and a library zeroes the
 * structs that it returns before it sets their fields, as a static or zero-initialized table
 * already is. So on a little-endian target, a host can read a slot as an int64_t. The structs
 * have no padding at their end, and each field appended to them is 8 bytes wide: an int64_t, or a
 * pointer or a function pointer in a slot, with flags and booleans as int64_t. So each appended
 * field grows the size by 8 bytes, and the size tells whether a field is there: a narrower field
 * could take the padding after the field before it, without growing the size. The structs of the
 * Arrow C data interface keep the layout that each target gives them. Within a
 * SPARK_UDF_ABI_VERSION:
 * - fields are only appended to the structs, including SparkUdfLibrary, and the existing fields
 *   never change, nor do the semantics of the existing functions;
 * - Spark only reads the functions that fit in the size of the table of a library, so a library
 *   built against an earlier header keeps working, with the functions that it does not know as
 *   NULL. A library must only read the fields that fit in the size of a struct that Spark passes,
 *   those whose offset plus size is at most that size;
 * - Spark only creates a state of a new kind of function, such as a table or an aggregate function
 *   that a later version may add, for a function that the manifest declares of that kind.
 * An incompatible change uses a new SPARK_UDF_ABI_VERSION: spark_udf_init gets the newest version
 * that Spark supports, the library returns a table of a version that it implements, and Spark
 * rejects a table of a version that it does not support. The manifest gives the oldest version
 * that the library implements. SPARK_UDF_ABI_VERSION is independent of SPARK_DS_ABI_VERSION.
 * Spark supports version 1 at least in all the releases of Spark 5.
 */
#ifndef SPARK_UDF_H
#define SPARK_UDF_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The version of this interface. */
#define SPARK_UDF_ABI_VERSION 1

/* Exports spark_udf_init from the library, also when the library hides its other symbols. */
#if defined(_WIN32)
#define SPARK_UDF_EXPORT __declspec(dllexport)
#else
#define SPARK_UDF_EXPORT __attribute__((visibility("default")))
#endif

/* An int64_t inside a struct is aligned to 4 bytes on i386, so the 8-byte slots are aligned. */
#ifdef __cplusplus
#define SPARK_UDF_ALIGNED(n) alignas(n)
#else
#define SPARK_UDF_ALIGNED(n) _Alignas(n)
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

/* ---------------------------------------------------------------------------------------------
 * What Spark passes to the library
 * --------------------------------------------------------------------------------------------- */

/* The levels of the messages of SparkUdfHost.log and SparkUdfHost.is_log_enabled. */
#define SPARK_UDF_LOG_ERROR 1
#define SPARK_UDF_LOG_WARN 2
#define SPARK_UDF_LOG_INFO 3
#define SPARK_UDF_LOG_DEBUG 4
#define SPARK_UDF_LOG_TRACE 5

/* The host, as spark_udf_init gets it. It stays valid until the process exits. */
typedef struct SparkUdfHost {
  /*
   * The size of this struct in the header that Spark was built with. A later version of Spark may
   * append fields, so a library must only read the fields that fit in it.
   */
  int64_t size;
  /* The newest version of this interface that Spark supports. */
  int32_t abi_version;
  /* The version of Spark, such as "5.0.0". */
  union {
    const char* spark_version;
    SPARK_UDF_ALIGNED(8) int64_t spark_version_slot;
  };
  /*
   * Logs a message, a NUL-terminated UTF-8 string, at one of the SPARK_UDF_LOG_* levels, in the
   * log of the host, such as the log of the executor, with the path of the library. Another level
   * is treated as SPARK_UDF_LOG_INFO.
   */
  union {
    void (*log)(int32_t level, const char* message);
    SPARK_UDF_ALIGNED(8) int64_t log_slot;
  };
  /*
   * Returns whether the logger that log writes to is enabled for one of the SPARK_UDF_LOG_*
   * levels, another level being treated as SPARK_UDF_LOG_INFO, so that the library only formats
   * the messages that Spark may write. It follows the level of the logger, which can change while
   * Spark runs, but each call is a call into the host: a library may keep the answer for a while,
   * and ask again from time to time.
   */
  union {
    bool (*is_log_enabled)(int32_t level);
    SPARK_UDF_ALIGNED(8) int64_t is_log_enabled_slot;
  };
} SparkUdfHost;

/*
 * The task that evaluates a call, on an executor, as create gets it. It stays valid until close
 * returns for the state that create returned.
 */
typedef struct SparkUdfTaskContext {
  /* The size of this struct in the header that Spark was built with, as in SparkUdfHost. */
  int64_t size;
  /* The ID of the stage of the task, and the attempt number of the stage. */
  int32_t stage_id;
  int32_t stage_attempt_number;
  /* The partition of the task, and the attempt number of the task. */
  int32_t partition_id;
  int32_t attempt_number;
  /* The ID of the task attempt, unique in the Spark application. */
  int64_t task_attempt_id;
  /*
   * Returns whether the task was killed, for example because its query was canceled. Spark cannot
   * interrupt native code, so an eval that runs for long, or blocks, should check it regularly,
   * and fail once it is true.
   */
  union {
    bool (*is_interrupted)(const struct SparkUdfTaskContext* task);
    SPARK_UDF_ALIGNED(8) int64_t is_interrupted_slot;
  };
} SparkUdfTaskContext;

/* The kinds of functions. */
#define SPARK_UDF_SCALAR 1 /* one value for each row of a batch, with eval */

/* A call of a function in a task, as create gets it, on an executor. */
typedef struct SparkUdfCreateInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkUdfHost. */
  int64_t size;
  /* The kind of the function, as the manifest declares it: SPARK_UDF_SCALAR in version 1. */
  int32_t kind;
  /* The name of the function, as listed in the manifest. A library can implement several. */
  union {
    const char* name;
    SPARK_UDF_ALIGNED(8) int64_t name_slot;
  };
  /*
   * The arguments of the call: a struct type with one field for each argument, in order, of the
   * type that the manifest gives. A field is nullable if the argument can be null. A function
   * without arguments gets a struct type without fields. A library must check the types, and
   * fail if it does not implement the function for them: the manifest and the library can
   * disagree, for example if the manifest was edited, and Spark cannot tell.
   */
  union {
    struct ArrowSchema* arguments;
    SPARK_UDF_ALIGNED(8) int64_t arguments_slot;
  };
  /* The result: a nullable field of the type that the manifest gives, as eval returns it. */
  union {
    struct ArrowSchema* result;
    SPARK_UDF_ALIGNED(8) int64_t result_slot;
  };
  /*
   * The time zone of the session, such as "America/Los_Angeles", which the formats of the
   * timestamps of the schemas also have.
   */
  union {
    const char* session_time_zone;
    SPARK_UDF_ALIGNED(8) int64_t session_time_zone_slot;
  };
  /* The task, valid until close returns for the state. */
  union {
    const SparkUdfTaskContext* task;
    SPARK_UDF_ALIGNED(8) int64_t task_slot;
  };
} SparkUdfCreateInfo;

/* A batch to evaluate, as eval gets it. */
typedef struct SparkUdfEvalInfo {
  /* The size of this struct in the header that Spark was built with, as in SparkUdfHost. */
  int64_t size;
} SparkUdfEvalInfo;

/* ---------------------------------------------------------------------------------------------
 * What the library returns to Spark
 * --------------------------------------------------------------------------------------------- */

/* The functions of a library, which spark_udf_init returns. */
typedef struct SparkUdfLibrary {
  /*
   * The size of this struct in the header that the library was built with. Spark only reads the
   * functions that fit in it.
   */
  int64_t size;
  /* The version of this interface that the table implements, SPARK_UDF_ABI_VERSION. */
  int32_t abi_version;

  /*
   * Returns the message of the last error on the calling thread, as a NUL-terminated UTF-8 string
   * that stays valid until the next call on that thread. Optional: without it, Spark only reports
   * the value that the function returned.
   */
  union {
    const char* (*last_error)(void);
    SPARK_UDF_ALIGNED(8) int64_t last_error_slot;
  };

  /*
   * Creates the state of a call of a function, in a task, the first time the task evaluates the
   * call.
   *
   * info: the name and the kind of the function, the schemas of its arguments and of its result,
   *       the time zone of the session and the task. The schemas are input structs: see "Arrow
   *       data" above.
   * state: set to the state
   */
  union {
    int (*create)(const SparkUdfCreateInfo* info, void** state);
    SPARK_UDF_ALIGNED(8) int64_t create_slot;
  };

  /*
   * Evaluates a scalar function for a batch of rows.
   *
   * info: nothing yet but its size, so that inputs can be added within this version
   * input: a struct array whose children are the arguments: see "Arrow data" above
   * out: set to the array of the results, one for each row of the input
   */
  union {
    int (*eval)(void* state, const SparkUdfEvalInfo* info, struct ArrowArray* input,
                struct ArrowArray* out);
    SPARK_UDF_ALIGNED(8) int64_t eval_slot;
  };

  /* Releases a state. */
  union {
    void (*close)(void* state);
    SPARK_UDF_ALIGNED(8) int64_t close_slot;
  };
} SparkUdfLibrary;

/* The layout of these structs is the same on every target: see Compatibility. */
#ifdef __cplusplus
#define SPARK_UDF_STATIC_ASSERT(condition) static_assert(condition, #condition)
#else
#define SPARK_UDF_STATIC_ASSERT(condition) _Static_assert(condition, #condition)
#endif
SPARK_UDF_STATIC_ASSERT(sizeof(SparkUdfHost) == 40);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfHost, size) == 0);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfHost, abi_version) == 8);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfHost, spark_version) == 16);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfHost, log) == 24);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfHost, is_log_enabled) == 32);
SPARK_UDF_STATIC_ASSERT(sizeof(SparkUdfTaskContext) == 40);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, size) == 0);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, stage_id) == 8);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, stage_attempt_number) == 12);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, partition_id) == 16);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, attempt_number) == 20);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, task_attempt_id) == 24);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfTaskContext, is_interrupted) == 32);
SPARK_UDF_STATIC_ASSERT(sizeof(SparkUdfCreateInfo) == 56);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, size) == 0);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, kind) == 8);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, name) == 16);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, arguments) == 24);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, result) == 32);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, session_time_zone) == 40);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfCreateInfo, task) == 48);
SPARK_UDF_STATIC_ASSERT(sizeof(SparkUdfEvalInfo) == 8);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfEvalInfo, size) == 0);
SPARK_UDF_STATIC_ASSERT(sizeof(SparkUdfLibrary) == 48);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfLibrary, size) == 0);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfLibrary, abi_version) == 8);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfLibrary, last_error) == 16);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfLibrary, create) == 24);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfLibrary, eval) == 32);
SPARK_UDF_STATIC_ASSERT(offsetof(SparkUdfLibrary, close) == 40);
#undef SPARK_UDF_STATIC_ASSERT
#undef SPARK_UDF_ALIGNED

/*
 * The function that a library exports. Spark calls it once in each process, the first time it
 * uses a function of the library, before any other function of the table.
 *
 * host: the host, valid until the process exits
 * library: set to the function table of the library, for the version of this interface that it
 *          implements, or the newest one up to host->abi_version if it implements several. Spark
 *          reads the table after the call, so it is typically the address of a static struct.
 *          Fill in the table by field name, for example with designated initializers or by
 *          assignment: an initializer in the order of the fields needs braces around each slot to
 *          compile without warnings.
 *
 * Returns 0 on success, or any other value if the library cannot run, after it logs why with
 * host->log.
 */
SPARK_UDF_EXPORT int spark_udf_init(const SparkUdfHost* host, const SparkUdfLibrary** library);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_UDF_H */
