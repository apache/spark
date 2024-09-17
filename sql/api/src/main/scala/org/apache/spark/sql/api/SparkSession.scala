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
package org.apache.spark.sql.api

import scala.concurrent.duration.NANOSECONDS
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag

import _root_.java.io.Closeable
import _root_.java.lang
import _root_.java.net.URI
import _root_.java.util

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.{Encoder, Row, RuntimeConfig}
import org.apache.spark.sql.types.StructType

/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 *
 * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder to
 * get an existing session:
 *
 * {{{
 *   SparkSession.builder().getOrCreate()
 * }}}
 *
 * The builder can also be used to create a new session:
 *
 * {{{
 *   SparkSession.builder
 *     .master("local")
 *     .appName("Word Count")
 *     .config("spark.some.config.option", "some-value")
 *     .getOrCreate()
 * }}}
 */
abstract class SparkSession[DS[U] <: Dataset[U, DS]] extends Serializable with Closeable {

  /**
   * The version of Spark on which this application is running.
   *
   * @since 2.0.0
   */
  def version: String

  /**
   * Runtime configuration interface for Spark.
   *
   * This is the interface through which the user can get and set all Spark and Hadoop
   * configurations that are relevant to Spark SQL. When getting the value of a config, this
   * defaults to the value set in the underlying `SparkContext`, if any.
   *
   * @since 2.0.0
   */
  def conf: RuntimeConfig

  /**
   * A collection of methods for registering user-defined functions (UDF).
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sparkSession.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sparkSession.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType);
   * }}}
   *
   * @note
   *   The user-defined functions must be deterministic. Due to optimization, duplicate
   *   invocations may be eliminated or the function may even be invoked more times than it is
   *   present in the query.
   * @since 2.0.0
   */
  def udf: UDFRegistration

  /**
   * Start a new session with isolated SQL configurations, temporary tables, registered functions
   * are isolated, but sharing the underlying `SparkContext` and cached data.
   *
   * @note
   *   Other than the `SparkContext`, all shared state is initialized lazily. This method will
   *   force the initialization of the shared state to ensure that parent and child sessions are
   *   set up with the same shared state. If the underlying catalog implementation is Hive, this
   *   will initialize the metastore, which may take some time.
   * @since 2.0.0
   */
  def newSession(): SparkSession[DS]

  /* --------------------------------- *
   |  Methods for creating DataFrames  |
   * --------------------------------- */

  /**
   * Returns a `DataFrame` with no rows or columns.
   *
   * @since 2.0.0
   */
  @transient
  def emptyDataFrame: DS[Row]

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DS[Row]

  /**
   * :: DeveloperApi :: Creates a `DataFrame` from a `java.util.List` containing
   * [[org.apache.spark.sql.Row]]s using the given schema.It is important to make sure that the
   * structure of every [[org.apache.spark.sql.Row]] of the provided List matches the provided
   * schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  def createDataFrame(rows: util.List[Row], schema: StructType): DS[Row]

  /**
   * Applies a schema to a List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean, SELECT * queries
   * will return the columns in an undefined order.
   *
   * @since 1.6.0
   */
  def createDataFrame(data: util.List[_], beanClass: Class[_]): DS[Row]

  /* ------------------------------- *
   |  Methods for creating DataSets  |
   * ------------------------------- */

  /**
   * Creates a new [[Dataset]] of type T containing zero elements.
   *
   * @since 2.0.0
   */
  def emptyDataset[T: Encoder]: DS[T]

  /**
   * Creates a [[Dataset]] from a local Seq of data of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL
   * representation) that is generally created automatically through implicits from a
   * `SparkSession`, or can be created explicitly by calling static methods on `Encoders`.
   *
   * ==Example==
   *
   * {{{
   *
   *   import spark.implicits._
   *   case class Person(name: String, age: Long)
   *   val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+
   *   // |   name|age|
   *   // +-------+---+
   *   // |Michael| 29|
   *   // |   Andy| 30|
   *   // | Justin| 19|
   *   // +-------+---+
   * }}}
   *
   * @since 2.0.0
   */
  def createDataset[T: Encoder](data: Seq[T]): DS[T]

  /**
   * Creates a [[Dataset]] from a `java.util.List` of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL
   * representation) that is generally created automatically through implicits from a
   * `SparkSession`, or can be created explicitly by calling static methods on `Encoders`.
   *
   * ==Java Example==
   *
   * {{{
   *     List<String> data = Arrays.asList("hello", "world");
   *     Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
   * }}}
   *
   * @since 2.0.0
   */
  def createDataset[T: Encoder](data: util.List[T]): DS[T]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  def range(end: Long): DS[lang.Long]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long): DS[lang.Long]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long): DS[lang.Long]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value, with partition number specified.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DS[lang.Long]

  /* ------------------------- *
   |  Catalog-related methods  |
   * ------------------------- */

  /**
   * Interface through which the user may create, drop, alter or query underlying databases,
   * tables, functions etc.
   *
   * @since 2.0.0
   */
  def catalog: Catalog[DS]

  /**
   * Returns the specified table/view as a `DataFrame`. If it's a table, it must support batch
   * reading and the returned DataFrame is the batch scan query plan of this table. If it's a
   * view, the returned DataFrame is simply the query plan of the view, which can either be a
   * batch or streaming query plan.
   *
   * @param tableName
   *   is either a qualified or unqualified name that designates a table or view. If a database is
   *   specified, it identifies the table/view from the database. Otherwise, it first attempts to
   *   find a temporary view with the given name and then match the table/view from the current
   *   database. Note that, the global temporary view database is also valid here.
   * @since 2.0.0
   */
  def table(tableName: String): DS[Row]

  /* ----------------- *
   |  Everything else  |
   * ----------------- */

  /**
   * Executes a SQL query substituting positional parameters by the given arguments, returning the
   * result as a `DataFrame`. This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText
   *   A SQL statement with positional parameters to execute.
   * @param args
   *   An array of Java/Scala objects that can be converted to SQL literal expressions. See <a
   *   href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html"> Supported Data
   *   Types</a> for supported value types in Scala/Java. For example, 1, "Steven",
   *   LocalDate.of(2023, 4, 2). A value can be also a `Column` of a literal or collection
   *   constructor functions such as `map()`, `array()`, `struct()`, in that case it is taken as
   *   is.
   * @since 3.5.0
   */
  @Experimental
  def sql(sqlText: String, args: Array[_]): DS[Row]

  /**
   * Executes a SQL query substituting named parameters by the given arguments, returning the
   * result as a `DataFrame`. This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText
   *   A SQL statement with named parameters to execute.
   * @param args
   *   A map of parameter names to Java/Scala objects that can be converted to SQL literal
   *   expressions. See <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *   Supported Data Types</a> for supported value types in Scala/Java. For example, map keys:
   *   "rank", "name", "birthdate"; map values: 1, "Steven", LocalDate.of(2023, 4, 2). Map value
   *   can be also a `Column` of a literal or collection constructor functions such as `map()`,
   *   `array()`, `struct()`, in that case it is taken as is.
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: Map[String, Any]): DS[Row]

  /**
   * Executes a SQL query substituting named parameters by the given arguments, returning the
   * result as a `DataFrame`. This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText
   *   A SQL statement with named parameters to execute.
   * @param args
   *   A map of parameter names to Java/Scala objects that can be converted to SQL literal
   *   expressions. See <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *   Supported Data Types</a> for supported value types in Scala/Java. For example, map keys:
   *   "rank", "name", "birthdate"; map values: 1, "Steven", LocalDate.of(2023, 4, 2). Map value
   *   can be also a `Column` of a literal or collection constructor functions such as `map()`,
   *   `array()`, `struct()`, in that case it is taken as is.
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: util.Map[String, Any]): DS[Row] = {
    sql(sqlText, args.asScala.toMap)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 2.0.0
   */
  def sql(sqlText: String): DS[Row] = sql(sqlText, Map.empty[String, Any])

  /**
   * Add a single artifact to the current session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   *
   * @since 4.0.0
   */
  @Experimental
  def addArtifact(path: String): Unit

  /**
   * Add a single artifact to the current session.
   *
   * Currently it supports local files with extensions .jar and .class and Apache Ivy URIs.
   *
   * @since 4.0.0
   */
  @Experimental
  def addArtifact(uri: URI): Unit

  /**
   * Add a single in-memory artifact to the session while preserving the directory structure
   * specified by `target` under the session's working directory of that particular file
   * extension.
   *
   * Supported target file extensions are .jar and .class.
   *
   * ==Example==
   * {{{
   *  addArtifact(bytesBar, "foo/bar.class")
   *  addArtifact(bytesFlat, "flat.class")
   *  // Directory structure of the session's working directory for class files would look like:
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/flat.class
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/foo/bar.class
   * }}}
   *
   * @since 4.0.0
   */
  @Experimental
  def addArtifact(bytes: Array[Byte], target: String): Unit

  /**
   * Add a single artifact to the session while preserving the directory structure specified by
   * `target` under the session's working directory of that particular file extension.
   *
   * Supported target file extensions are .jar and .class.
   *
   * ==Example==
   * {{{
   *  addArtifact("/Users/dummyUser/files/foo/bar.class", "foo/bar.class")
   *  addArtifact("/Users/dummyUser/files/flat.class", "flat.class")
   *  // Directory structure of the session's working directory for class files would look like:
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/flat.class
   *  // ${WORKING_DIR_FOR_CLASS_FILES}/foo/bar.class
   * }}}
   *
   * @since 4.0.0
   */
  @Experimental
  def addArtifact(source: String, target: String): Unit

  /**
   * Add one or more artifacts to the session.
   *
   * Currently it supports local files with extensions .jar and .class and Apache Ivy URIs
   *
   * @since 4.0.0
   */
  @Experimental
  @scala.annotation.varargs
  def addArtifacts(uri: URI*): Unit

  /**
   * Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a
   * `DataFrame`.
   * {{{
   *   sparkSession.read.parquet("/path/to/file.parquet")
   *   sparkSession.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @since 2.0.0
   */
  def read: DataFrameReader[DS]

  /**
   * Executes some code block and prints to stdout the time taken to execute the block. This is
   * available in Scala only and is used primarily for interactive testing and debugging.
   *
   * @since 2.1.0
   */
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

  /**
   * Synonym for `close()`.
   *
   * @since 2.0.0
   */
  def stop(): Unit = close()
}
