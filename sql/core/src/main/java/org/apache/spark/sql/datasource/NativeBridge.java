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

package org.apache.spark.sql.datasource;

import org.apache.spark.annotation.Evolving;

/**
 * The binary interface between Spark and data sources implemented in native code, such as Rust
 * or C++.
 * <p>
 * A native data source is a shared library that implements the {@code native} methods of this
 * class as JNI functions, for example
 * {@code Java_org_apache_spark_sql_datasource_NativeBridge_createDataSource}. Spark loads each
 * library into its own copy of this class, so any number of libraries that export the same
 * functions can be loaded side by side. Spark adapts a native data source to Data Source V2, with
 * batch and micro-batch streaming reads, batch and streaming writes, and predicate, column and
 * limit pushdown.
 * <p>
 * Spark finds the library of a data source automatically when it is installed like other native
 * libraries and named after the data source: the library of {@code my_source} is
 * {@code spark_datasource_my_source}, such as {@code libspark_datasource_my_source.so} in
 * {@code /usr/local/lib} on Linux. Spark looks for it in the directories of
 * {@code java.library.path}, which include {@code LD_LIBRARY_PATH} on Linux, and in the
 * {@code lib} directory of each installation prefix in {@code PATH}. A library can also be
 * distributed in a native data source package: a zip file with the extension {@code .sparkpkg}
 * that contains a manifest named {@code spark-native-datasource.json} and the library built for
 * one or more platforms, added to a session with {@code spark.addArtifact} or under the paths in
 * {@code spark.sql.dataSource.native.paths}. See the "Native Data Sources" page of the Spark SQL
 * guide for details and examples.
 *
 * <h2>Lifecycle</h2>
 * Spark creates a data source with {@link #createDataSource} on the driver for each schema
 * inference, scan and write, and closes it right after.
 * <ul>
 *   <li><b>Batch reads.</b> On the driver, Spark creates a reader with {@link #createReader},
 *   pushes operations down by calling, each at most once and in this order,
 *   {@link #pushPredicates}, {@link #pushLimit} and {@link #pruneColumns}, plans the partitions
 *   with {@link #partitions}, serializes the state of the reader with {@link #serializeReader},
 *   and closes it. Then it calls {@link #read} on the executors, in one task for each partition.
 *   If Spark never plans the partitions, for example to only explain the plan of the query, it
 *   closes the reader once the reader is no longer referenced.</li>
 *   <li><b>Streaming reads.</b> On the driver, Spark creates a stream reader with
 *   {@link #createStreamReader} and keeps it until the streaming query stops. For each
 *   micro-batch, it asks for the {@link #latestOffset}, plans the partitions with
 *   {@link #streamPartitions} and serializes the state with {@link #serializeStreamReader}, then
 *   calls {@link #read} on the executors. Offsets are defined by the library, as compact JSON on
 *   a single line: Spark stores each offset as a line of the offset log of the checkpoint, so it
 *   rejects an offset that is null, is not valid JSON or contains a line break. When a streaming
 *   query restarts, from its checkpoint, Spark creates a new stream reader and may call
 *   {@link #streamPartitions} again for the last range that it planned before, so the partitions
 *   of a range must read the same data each time.</li>
 *   <li><b>Writes.</b> On the driver, right before it starts the tasks of the write (or of a
 *   micro-batch), Spark creates a writer with {@link #createWriter} or {@link #createStreamWriter},
 *   and serializes its state with {@link #serializeWriter}. With adaptive query execution, Spark
 *   first runs the stages of the data to write that end with a shuffle, and creates no writer if
 *   they fail. The rest of the data, such as its last stage, is computed in the tasks of the
 *   write, so a writer may still be aborted because computing the data failed: do irreversible
 *   work, such as removing the existing data for an overwrite, in {@link #commit}. On the
 *   executors, each task creates a data writer with {@link #createDataWriter}, calls
 *   {@link #write} for each batch of at most
 *   {@code spark.sql.execution.arrow.maxRecordsPerBatch} rows, which also ends once it holds
 *   {@code spark.sql.execution.arrow.maxBytesPerBatch} bytes, and calls {@link #commitDataWriter},
 *   or {@link #abortDataWriter} if the task or the commit fails. Then the driver calls
 *   {@link #commit} with the messages of the tasks if they all succeeded, or {@link #abort}
 *   otherwise, including when {@link #commit} fails, and closes the writer. Several attempts of a
 *   task may write the same partition, after a failure or with speculative execution, each with a
 *   data writer of its own. Spark lets the attempts of a partition commit one at a time, and passes
 *   to {@link #commit} the message of one committed attempt for each partition, so the write
 *   consists of the data of these attempts only. It aborts the data writers of the other attempts,
 *   except on an executor that is lost, where a data writer is neither committed nor aborted, and
 *   may even have committed before its message reached the driver. A streaming write commits or
 *   aborts each micro-batch (epoch). Spark may commit the same epoch more than once, for example
 *   when a streaming query restarts after a failure, so committing an epoch that is already
 *   committed must have no effect, for exactly-once output. The ID of the query, which
 *   {@link #createStreamWriter} gets, stays the same when the query restarts from its checkpoint,
 *   so the ID of the query and the epoch ID identify the output of a micro-batch, also when other
 *   queries write to the same destination. A micro-batch streaming query creates a
 *   new stream writer for each micro-batch, and Spark closes the previous stream writer of the
 *   query when the query creates a new one. Spark is not told when a streaming query stops, so it
 *   closes the last stream writer of the query once the writer is no longer referenced, or when the
 *   query is restarted.</li>
 * </ul>
 *
 * <h2>Conventions</h2>
 * <ul>
 *   <li><b>Handles.</b> The {@code create} methods return an opaque, non-zero handle, typically a
 *   pointer: Spark reports a handle of 0 as an error. Spark releases each handle exactly once
 *   with the matching {@code close} method, or with {@link #commitDataWriter} or
 *   {@link #abortDataWriter} for data writers, though not always right after its last use: a batch
 *   reader whose partitions are never planned and the last stream writer of a query are released
 *   once they are no longer referenced (see Lifecycle above). A reader or writer handle must not
 *   depend on the data source handle it was created from, because Spark closes the data source
 *   handle right after creating it.</li>
 *   <li><b>State.</b> Partitions, the state of readers and writers, and commit messages are byte
 *   arrays in a format defined by the library. Spark sends them to the executors and back.</li>
 *   <li><b>Errors.</b> To report an error, throw a Java exception, for example with the JNI
 *   function {@code ThrowNew} on {@code java.lang.RuntimeException}, and return any value. Spark
 *   rethrows it as a {@code NATIVE_DATA_SOURCE_ERROR}, except for the errors of
 *   {@link #closeReader}, {@link #closeStreamReader} and {@link #closeWriter}, which Spark only
 *   logs: they may run after the query, from another thread. So do the work that can fail a
 *   query, such as flushing data, in {@link #commit} rather than when closing.</li>
 *   <li><b>Strings.</b> The strings of this interface, such as the name and the options of
 *   {@link #createDataSource}, the predicates, the column names of {@link #pruneColumns} and the
 *   offsets, are Java strings. The JNI functions {@code GetStringUTFChars} and
 *   {@code NewStringUTF} convert them from and to modified UTF-8, which is UTF-8 except for the
 *   character U+0000 and the characters outside the Basic Multilingual Plane, such as emoji, which
 *   modified UTF-8 encodes in 6 bytes rather than 4. Convert the strings where it matters, for
 *   example with the Java method {@code String.getBytes} and the charset UTF-8. The {@code jni}
 *   crate of Rust converts them to and from UTF-8. The JSON of the predicates is ASCII: it escapes
 *   the other characters, which any JSON parser decodes.</li>
 *   <li><b>Arrow data.</b> Schemas and data are exchanged with the
 *   <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C data
 *   interface</a>. A {@code long} argument named {@code ...Address} is the address of a C struct
 *   allocated by Spark. For an output struct, the library initializes it and Spark takes
 *   ownership. For an input struct, the library may move it to take ownership; otherwise, Spark
 *   releases it after the call returns, so the library must not use it afterwards. The arrays
 *   that the library exports must have an offset of 0, at every level: Arrow Java ignores the
 *   offsets of the arrays it imports, so Spark rejects an array with another offset, such as a
 *   zero-copy slice. Copy a sliced array before exporting it. Spark also rejects an array with
 *   fewer values than its parent refers to, such as a column shorter than its batch. The buffers
 *   of an array must hold all of its values, as the C data interface requires: it does not carry
 *   the sizes of the buffers, so Spark cannot check them, and reads past the end of a buffer that
 *   is too short.</li>
 *   <li><b>Schemas.</b> Spark types map to Arrow types as in {@code spark.sql.execution.arrow}:
 *   for example, {@code bigint} is {@code int64}, {@code string} is {@code utf8},
 *   {@code timestamp} is {@code timestamp[us]} with a time zone and {@code timestamp_ntz} is
 *   {@code timestamp[us]} without one. Data read from a library may also use {@code large_utf8}
 *   and {@code utf8_view} for strings, {@code large_binary} and {@code binary_view} for binary
 *   data, and any names for the fields of the entries of a map. Spark rejects the other Arrow
 *   types, such as unsigned integers, {@code decimal256}, large lists, fixed-size lists and list
 *   views, and dictionary encoding.</li>
 *   <li><b>Options.</b> Option keys are passed in lower case, because Spark matches them
 *   case-insensitively.</li>
 *   <li><b>Optional methods.</b> A library only needs to export the functions for the operations
 *   it supports. If a function is missing, Spark treats the operation as unsupported, except for
 *   the pushdown methods, the {@code serialize} methods and {@link #commitOffset}: then Spark
 *   pushes nothing down, sends no state and does not tell the library which offsets it
 *   processed.</li>
 *   <li><b>Threads.</b> Spark calls the library from multiple threads, and calls on different
 *   handles may run concurrently. Calls on the same handle never run concurrently. A handle may
 *   be released from a thread other than the threads that used it, such as a JVM
 *   {@code Cleaner} thread when Spark releases a handle once it is no longer referenced, for
 *   example the last stream writer of a streaming query.</li>
 * </ul>
 *
 * <h2>Compatibility</h2>
 * The JVM binds the functions of a library by their names only, so the name of this class, the
 * names of its methods and their signatures are the interface. Within an {@link #ABI_VERSION}:
 * <ul>
 *   <li>the signature and the semantics of an existing method never change;</li>
 *   <li>new methods may be added, only as optional methods, so that a library that does not
 *   export them keeps working as before;</li>
 *   <li>new values may appear where this documentation says so, such as the names of pushed
 *   predicates.</li>
 * </ul>
 * An incompatible change uses a new {@link #ABI_VERSION}, with new names or a new class. A
 * library reports the version it implements with {@link #abiVersion}, and Spark rejects a library
 * of a version it does not support. Spark supports version 1 at least in all the releases of
 * Spark 4 from 4.4.0.
 *
 * @since 4.4.0
 */
@Evolving
public final class NativeBridge {

  /**
   * The version of this interface. It changes when the interface changes incompatibly: see
   * Compatibility above.
   */
  public static final int ABI_VERSION = 1;

  private NativeBridge() {}

  /**
   * Loads the library that implements the native methods of this copy of the class. Spark calls
   * it on the copy of the class it creates for the library.
   */
  static void load(String path) {
    System.load(path);
  }

  /** Returns the version of this interface that the library implements, {@link #ABI_VERSION}. */
  public static native int abiVersion();

  // ---------------------------------------------------------------------------------------------
  // Data source (driver)
  // ---------------------------------------------------------------------------------------------

  /**
   * Creates a data source with the options given by the user, such as
   * {@code spark.read.option("key", "value")}.
   *
   * @param name the name of the data source, as listed in the package manifest, or in lower case
   *             for an installed library. A library can implement several data sources.
   * @param optionKeys the option keys, in lower case
   * @param optionValues the option values, in the same order as the keys
   * @return the data source handle
   */
  public static native long createDataSource(
      String name, String[] optionKeys, String[] optionValues);

  /**
   * Exports the schema of the data. Spark calls it only when the user does not specify a schema.
   *
   * @param schemaAddress the address of the {@code ArrowSchema} to initialize with a struct type
   *                      whose fields are the columns
   */
  public static native void schema(long dataSource, long schemaAddress);

  /**
   * Creates a reader for a batch scan.
   *
   * @param schemaAddress the address of the {@code ArrowSchema} of the schema to read: the one the
   *                      user specified, or the one returned by {@link #schema}
   * @return the reader handle
   */
  public static native long createReader(long dataSource, long schemaAddress);

  /**
   * Creates a reader for a micro-batch streaming scan.
   *
   * @param schemaAddress the address of the {@code ArrowSchema} of the schema to read: the one the
   *                      user specified, or the one returned by {@link #schema}
   * @return the stream reader handle
   */
  public static native long createStreamReader(long dataSource, long schemaAddress);

  /**
   * Creates a writer for a batch write.
   *
   * @param schemaAddress the address of the {@code ArrowSchema} of the data to write
   * @param overwrite whether to replace the existing data (save mode "overwrite") instead of
   *                  appending to it
   * @return the writer handle
   */
  public static native long createWriter(long dataSource, long schemaAddress, boolean overwrite);

  /**
   * Creates a writer for a streaming write.
   *
   * @param schemaAddress the address of the {@code ArrowSchema} of the data to write
   * @param overwrite whether to replace the existing data in each micro-batch (output mode
   *                  "complete") instead of appending to it
   * @param queryId the ID of the streaming query. It stays the same when the query restarts from
   *                its checkpoint, so with the epoch IDs, it identifies the commits of the query:
   *                see Writes above.
   * @return the writer handle
   */
  public static native long createStreamWriter(
      long dataSource, long schemaAddress, boolean overwrite, String queryId);

  /** Releases a data source handle. */
  public static native void closeDataSource(long dataSource);

  // ---------------------------------------------------------------------------------------------
  // Batch reader (driver)
  // ---------------------------------------------------------------------------------------------

  /**
   * Pushes predicates down to the reader. The predicates are combined with AND.
   * <p>
   * Each predicate is a JSON expression tree. An expression is one of:
   * <ul>
   *   <li>{@code {"type": "column", "name": ["a", "b"]}}: a column. A nested field has a name
   *   part for each level.</li>
   *   <li>{@code {"type": "literal", "dataType": "bigint", "value": 1}}: a literal. The data type
   *   is a Spark SQL type name such as {@code int}, {@code string} or {@code decimal(10,2)}. The
   *   value is {@code null}, a JSON boolean or number for boolean and numeric types, a string
   *   for {@code string}, a base64 string for {@code binary}, a decimal string for
   *   {@code decimal}, the days since the epoch for {@code date}, and the microseconds since the
   *   epoch for {@code timestamp} and {@code timestamp_ntz}.</li>
   *   <li>{@code {"type": "function", "name": ">", "children": [...]}}: a predicate, with the
   *   name it has in {@code org.apache.spark.sql.connector.expressions.filter.Predicate}. The
   *   names are {@code =}, {@code <>}, {@code <=>} (equal, with null equal to null), {@code <},
   *   {@code <=}, {@code >} and {@code >=} with two children, {@code IN} with the value to look
   *   for and then the values to look in, {@code IS_NULL} and {@code IS_NOT_NULL} with one
   *   child, {@code STARTS_WITH}, {@code ENDS_WITH} and {@code CONTAINS} with the string and the
   *   string to look for, {@code AND} and {@code OR} with two predicates, and {@code NOT} with
   *   one. The children of the other functions are columns, literals or predicates.</li>
   * </ul>
   * Spark does not pass predicates that it cannot express this way, such as predicates that use
   * arithmetic or other functions, nor predicates on columns that contain strings with a
   * non-binary collation, such as {@code UTF8_LCASE}: strings are compared by their UTF-8 bytes.
   * A later version of Spark may pass predicates with other names without changing
   * {@link #ABI_VERSION}, so a reader must ignore a predicate that contains a name that it does
   * not know, or a data type that it does not support.
   * <p>
   * Spark evaluates all the predicates again after reading, so a reader may use them only to skip
   * data, for example the files whose statistics show that no row matches. But it must never drop
   * or skip a row for which the predicates are true under the semantics of Spark SQL, which Spark
   * cannot detect. In particular:
   * <ul>
   *   <li>For floating-point columns, NaN is equal to NaN and greater than any other value,
   *   including positive infinity, and {@code -0.0} is equal to {@code 0.0}. IEEE 754 comparisons
   *   treat NaN differently, and total orders, such as {@code f64::total_cmp} in Rust or the
   *   comparison kernels of arrow-rs, also order {@code -0.0} before {@code 0.0}. For example,
   *   {@code d > 1.0} is true for a NaN value of {@code d}, and {@code d = 0.0} for
   *   {@code -0.0}.</li>
   *   <li>Comparisons with null are null, and a row only matches if the predicates are true. For
   *   example, {@code NOT (i IN (1, NULL))} matches no row.</li>
   *   <li>Strings are compared by their UTF-8 bytes, which order the characters outside the Basic
   *   Multilingual Plane after all the others, unlike comparisons of UTF-16 strings, such as
   *   {@code String.compareTo} in Java.</li>
   *   <li>To use only a part of a predicate, a reader may take the parts that it does not support
   *   as true, but not under a {@code NOT}, where that would drop rows.</li>
   * </ul>
   * The file {@code org/apache/spark/sql/datasource/predicate-cases.json} in the jar of Spark SQL
   * lists predicates as Spark passes them to this method, with the rows of a table for which they
   * are true, to test a reader with.
   *
   * @return for each predicate, whether the reader evaluates it completely. Spark shows these
   *         predicates as pushed in the plan of the query, and evaluates all the predicates again
   *         after reading anyway.
   */
  public static native boolean[] pushPredicates(long reader, String[] predicates);

  /**
   * Pushes a LIMIT down to the reader. Spark only pushes a limit when the query does not filter
   * the rows, since it evaluates the predicates after reading.
   *
   * @return whether the reader returns at most {@code limit} rows in total. Spark applies the
   *         limit again after reading either way.
   */
  public static native boolean pushLimit(long reader, int limit);

  /**
   * Prunes the columns to read.
   *
   * @param columnNames the names of the top-level columns that the query needs, which include
   *                    the columns of the pushed predicates. There may be none, for example for
   *                    {@code SELECT count(*)}: then the batches only have a number of rows.
   * @return whether {@link #read} returns exactly these columns, in this order. If not, it
   *         returns all the columns of the schema to read.
   */
  public static native boolean pruneColumns(long reader, String[] columnNames);

  /**
   * Plans the partitions of the scan. Spark reads each partition in a separate task.
   *
   * @return the partitions, each serialized in a format defined by the library
   */
  public static native byte[][] partitions(long reader);

  /**
   * Serializes the state of the reader that {@link #read} needs on the executors, such as the
   * pushed predicates. Spark calls it after {@link #partitions}.
   */
  public static native byte[] serializeReader(long reader);

  /** Releases a batch reader handle. */
  public static native void closeReader(long reader);

  // ---------------------------------------------------------------------------------------------
  // Stream reader (driver)
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns the offset to start from when the streaming query has no checkpoint, as compact JSON
   * on a single line.
   */
  public static native String initialOffset(long streamReader);

  /** Returns the most recent offset available, as compact JSON on a single line. */
  public static native String latestOffset(long streamReader);

  /**
   * Plans the partitions that read the data after {@code start} up to and including {@code end}.
   *
   * @return the partitions, each serialized in a format defined by the library
   */
  public static native byte[][] streamPartitions(long streamReader, String start, String end);

  /**
   * Serializes the state of the stream reader that {@link #read} needs on the executors. Spark
   * calls it after each call to {@link #streamPartitions}.
   */
  public static native byte[] serializeStreamReader(long streamReader);

  /**
   * Tells the library that Spark has processed all the data up to and including {@code end},
   * and will not ask for it again.
   */
  public static native void commitOffset(long streamReader, String end);

  /** Releases a stream reader handle, when the streaming query stops. */
  public static native void closeStreamReader(long streamReader);

  // ---------------------------------------------------------------------------------------------
  // Partition reader (executor)
  // ---------------------------------------------------------------------------------------------

  /**
   * Reads a partition of a batch or streaming scan.
   *
   * @param readerState the state returned by {@link #serializeReader} or
   *                    {@link #serializeStreamReader}, or an empty array if there is none
   * @param partition a partition returned by {@link #partitions} or {@link #streamPartitions}
   * @param streamAddress the address of the {@code ArrowArrayStream} to initialize. Each array
   *                      of the stream is a struct array whose fields are the columns to read:
   *                      the columns of the schema to read, or the pruned columns. Spark calls
   *                      the stream from the thread of the task that reads the partition.
   *                      {@code get_next} may block, for example on network I/O, but Spark
   *                      cannot interrupt native code: when the task is killed, Spark only
   *                      stops reading once {@code get_next} returns, and then releases the
   *                      stream. So a {@code get_next} that blocks forever keeps the task
   *                      running, unless the task reaper ({@code spark.task.reaper.enabled}
   *                      and {@code spark.task.reaper.killTimeout}) kills the executor. Use
   *                      timeouts for blocking operations.
   */
  public static native void read(byte[] readerState, byte[] partition, long streamAddress);

  // ---------------------------------------------------------------------------------------------
  // Writer (driver)
  // ---------------------------------------------------------------------------------------------

  /**
   * Serializes the state of the writer that {@link #createDataWriter} needs on the executors.
   * Spark calls it once, after creating the writer.
   */
  public static native byte[] serializeWriter(long writer);

  /**
   * Commits a write, when all the tasks succeeded.
   *
   * @param epochId the ID of the micro-batch for a streaming write, or -1 for a batch write
   * @param messages the messages returned by {@link #commitDataWriter} of each task
   */
  public static native void commit(long writer, long epochId, byte[][] messages);

  /**
   * Aborts a write, when it failed.
   *
   * @param epochId the ID of the micro-batch for a streaming write, or -1 for a batch write
   * @param messages the messages returned by {@link #commitDataWriter} of each task. An element
   *                 is null if its task did not commit.
   */
  public static native void abort(long writer, long epochId, byte[][] messages);

  /** Releases a writer handle. */
  public static native void closeWriter(long writer);

  // ---------------------------------------------------------------------------------------------
  // Data writer (executor)
  // ---------------------------------------------------------------------------------------------

  /**
   * Creates the writer for one task.
   *
   * @param writerState the state returned by {@link #serializeWriter}, or an empty array if there
   *                    is none
   * @param partitionId the partition that the task writes
   * @param taskId the ID of the task attempt
   * @param epochId the ID of the micro-batch for a streaming write, or -1 for a batch write
   * @return the data writer handle
   */
  public static native long createDataWriter(
      byte[] writerState, int partitionId, long taskId, long epochId);

  /**
   * Writes a batch of data.
   *
   * @param arrayAddress the address of the {@code ArrowArray}: a struct array whose fields are
   *                     the columns
   * @param schemaAddress the address of the {@code ArrowSchema} of the array
   */
  public static native void write(long dataWriter, long arrayAddress, long schemaAddress);

  /**
   * Commits the data written by the task and releases the data writer handle. If it fails, the
   * handle is not released: Spark then calls {@link #abortDataWriter} with it, to abort the data
   * written by the task and release the handle.
   *
   * @return the message to pass to {@link #commit} on the driver
   */
  public static native byte[] commitDataWriter(long dataWriter);

  /** Aborts the data written by the task and releases the data writer handle. */
  public static native void abortDataWriter(long dataWriter);
}
