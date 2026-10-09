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
 * The library is distributed in a native data source package: a zip file with the extension
 * {@code .sparkpkg} that contains a manifest named {@code spark-native-datasource.json} and the
 * library built for one or more platforms. Spark finds the packages added to a session with
 * {@code spark.addArtifact}, and the ones under the paths in
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
 *   and closes it. Then it calls {@link #read} on the executors, in one task for each
 *   partition.</li>
 *   <li><b>Streaming reads.</b> On the driver, Spark creates a stream reader with
 *   {@link #createStreamReader} and keeps it until the streaming query stops. For each
 *   micro-batch, it asks for the {@link #latestOffset}, plans the partitions with
 *   {@link #streamPartitions} and serializes the state with {@link #serializeStreamReader}, then
 *   calls {@link #read} on the executors. Offsets are JSON strings defined by the library.</li>
 *   <li><b>Writes.</b> On the driver, Spark creates a writer with {@link #createWriter} or
 *   {@link #createStreamWriter}, and serializes its state with {@link #serializeWriter}. On the
 *   executors, each task creates a data writer with {@link #createDataWriter}, calls
 *   {@link #write} for each batch of at most {@code spark.sql.execution.arrow.maxRecordsPerBatch}
 *   rows, and calls {@link #commitDataWriter}, or {@link #abortDataWriter} if it fails. Then the
 *   driver calls {@link #commit} with the messages of the tasks if they all succeeded, or
 *   {@link #abort} otherwise. A streaming write commits or aborts each micro-batch (epoch).</li>
 * </ul>
 *
 * <h2>Conventions</h2>
 * <ul>
 *   <li><b>Handles.</b> The {@code create} methods return an opaque, non-zero handle, typically a
 *   pointer. Spark releases each handle exactly once with the matching {@code close} method, or
 *   with {@link #commitDataWriter} or {@link #abortDataWriter} for data writers. A reader or
 *   writer handle must not depend on the data source handle it was created from, because Spark
 *   closes the data source handle right after creating it.</li>
 *   <li><b>State.</b> Partitions, the state of readers and writers, and commit messages are byte
 *   arrays in a format defined by the library. Spark sends them to the executors and back.</li>
 *   <li><b>Errors.</b> To report an error, throw a Java exception, for example with the JNI
 *   function {@code ThrowNew} on {@code java.lang.RuntimeException}, and return any value. Spark
 *   rethrows it as a {@code NATIVE_DATA_SOURCE_ERROR}.</li>
 *   <li><b>Arrow data.</b> Schemas and data are exchanged with the
 *   <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C data
 *   interface</a>. A {@code long} argument named {@code ...Address} is the address of a C struct
 *   allocated by Spark. For an output struct, the library initializes it and Spark takes
 *   ownership. For an input struct, the library may move it to take ownership; otherwise, Spark
 *   releases it after the call returns, so the library must not use it afterwards.</li>
 *   <li><b>Schemas.</b> Spark types map to Arrow types as in {@code spark.sql.execution.arrow}:
 *   for example, {@code bigint} is {@code int64}, {@code string} is {@code utf8},
 *   {@code timestamp} is {@code timestamp[us]} with a time zone and {@code timestamp_ntz} is
 *   {@code timestamp[us]} without one. Data read from a library may also use
 *   {@code large_utf8}, {@code utf8_view} and the other variants of the same type, but not
 *   unsigned integers or dictionary encoding.</li>
 *   <li><b>Options.</b> Option keys are passed in lower case, because Spark matches them
 *   case-insensitively.</li>
 *   <li><b>Optional methods.</b> A library only needs to export the functions for the operations
 *   it supports. If a function is missing, Spark treats the operation as unsupported, except for
 *   the pushdown methods and the {@code serialize} methods, which then push nothing down and
 *   return no state.</li>
 *   <li><b>Threads.</b> Spark calls the library from multiple threads, and calls on different
 *   handles may run concurrently. Calls on the same handle never run concurrently.</li>
 * </ul>
 *
 * @since 4.4.0
 */
@Evolving
public final class NativeBridge {

  /** The version of this interface. It changes when the interface changes incompatibly. */
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
   * @param name the name of the data source, as listed in the package manifest. A library can
   *             implement several data sources.
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
   * @return the writer handle
   */
  public static native long createStreamWriter(
      long dataSource, long schemaAddress, boolean overwrite);

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
   *   <li>{@code {"type": "function", "name": ">", "children": [...]}}: a predicate or function,
   *   with the names of {@code org.apache.spark.sql.connector.expressions.filter.Predicate} and
   *   {@code org.apache.spark.sql.connector.expressions.GeneralScalarExpression}, such as
   *   {@code =}, {@code <}, {@code IN}, {@code IS_NULL}, {@code STARTS_WITH}, {@code AND},
   *   {@code OR} and {@code NOT}.</li>
   * </ul>
   * Spark does not pass predicates that it cannot express this way.
   *
   * @return for each predicate, whether the reader evaluates it completely, so that Spark does
   *         not have to evaluate it again. The reader can still use the other predicates to skip
   *         data.
   */
  public static native boolean[] pushPredicates(long reader, String[] predicates);

  /**
   * Pushes a LIMIT down to the reader.
   *
   * @return whether the reader returns at most {@code limit} rows in total. Spark applies the
   *         limit again after reading either way.
   */
  public static native boolean pushLimit(long reader, int limit);

  /**
   * Prunes the columns to read.
   *
   * @param columnNames the names of the top-level columns that the query needs
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

  /** Returns the offset to start from when the streaming query has no checkpoint, as JSON. */
  public static native String initialOffset(long streamReader);

  /** Returns the most recent offset available, as JSON. */
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
   * Commits the data written by the task and releases the data writer handle.
   *
   * @return the message to pass to {@link #commit} on the driver
   */
  public static native byte[] commitDataWriter(long dataWriter);

  /** Aborts the data written by the task and releases the data writer handle. */
  public static native void abortDataWriter(long dataWriter);
}
