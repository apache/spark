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
package org.apache.spark.sql

import java.{lang, util}
import java.io.Closeable
import java.net.URI
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.duration.NANOSECONDS
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.annotation.{ClassicOnly, DeveloperApi, Experimental, Stable, Unstable}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.SparkClassUtils

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
abstract class SparkSession extends Serializable with Closeable {

  /**
   * The Spark context associated with this Spark session.
   *
   * @note
   *   this is only supported in Classic.
   */
  @ClassicOnly
  def sparkContext: SparkContext

  /**
   * The version of Spark on which this application is running.
   *
   * @since 2.0.0
   */
  def version: String

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener, and a
   * catalog that interacts with external systems.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.2.0
   */
  @ClassicOnly
  @Unstable
  @transient
  def sharedState: SharedState

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a `org.apache.spark.sql.internal.SQLConf`. If
   * `parentSessionState` is not null, the `SessionState` will be a copy of the parent.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.2.0
   */
  @ClassicOnly
  @Unstable
  @transient
  def sessionState: SessionState

  /**
   * A wrapped version of this session in the form of a `SQLContext`, for backward compatibility.
   *
   * @since 2.0.0
   */
  @transient
  def sqlContext: SQLContext

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
   * An interface to register custom `org.apache.spark.sql.util.QueryExecutionListeners` that
   * listen for execution metrics.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def listenerManager: ExecutionListenerManager

  /**
   * :: Experimental :: A collection of methods that are considered experimental, but can be used
   * to hook into the query planner for advanced functionality.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  @Experimental
  @Unstable
  def experimental: ExperimentalMethods

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
   * Returns a `StreamingQueryManager` that allows managing all the `StreamingQuery`s active on
   * `this`.
   *
   * @since 2.0.0
   */
  @Unstable
  def streams: StreamingQueryManager

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
  def newSession(): SparkSession

  /* --------------------------------- *
   |  Methods for creating DataFrames  |
   * --------------------------------- */

  /**
   * Returns a `DataFrame` with no rows or columns.
   *
   * @since 2.0.0
   */
  @transient
  def emptyDataFrame: DataFrame

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DataFrame

  /**
   * :: DeveloperApi :: Creates a `DataFrame` from a `java.util.List` containing
   * [[org.apache.spark.sql.Row]]s using the given schema.It is important to make sure that the
   * structure of every [[org.apache.spark.sql.Row]] of the provided List matches the provided
   * schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  def createDataFrame(rows: util.List[Row], schema: StructType): DataFrame

  /**
   * Applies a schema to a List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean, SELECT * queries
   * will return the columns in an undefined order.
   *
   * @since 1.6.0
   */
  def createDataFrame(data: util.List[_], beanClass: Class[_]): DataFrame

  /**
   * Creates a `DataFrame` from an RDD of Product (e.g. case classes, tuples).
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def createDataFrame[A <: Product: TypeTag](rdd: RDD[A]): DataFrame

  /**
   * :: DeveloperApi :: Creates a `DataFrame` from an `RDD` containing
   * [[org.apache.spark.sql.Row]]s using the given schema. It is important to make sure that the
   * structure of every [[org.apache.spark.sql.Row]] of the provided RDD matches the provided
   * schema. Otherwise, there will be runtime exception. Example:
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sparkSession = new org.apache.spark.sql.SparkSession(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sparkSession.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.createOrReplaceTempView("people")
   *  sparkSession.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame

  /**
   * :: DeveloperApi :: Creates a `DataFrame` from a `JavaRDD` containing
   * [[org.apache.spark.sql.Row]]s using the given schema. It is important to make sure that the
   * structure of every [[org.apache.spark.sql.Row]] of the provided RDD matches the provided
   * schema. Otherwise, there will be runtime exception.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean, SELECT * queries
   * will return the columns in an undefined order.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean, SELECT * queries
   * will return the columns in an undefined order.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame

  /**
   * Convert a `BaseRelation` created for external data sources into a `DataFrame`.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame

  /* ------------------------------- *
   |  Methods for creating DataSets  |
   * ------------------------------- */

  /**
   * Creates a new [[Dataset]] of type T containing zero elements.
   *
   * @since 2.0.0
   */
  def emptyDataset[T: Encoder]: Dataset[T]

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
  def createDataset[T: Encoder](data: Seq[T]): Dataset[T]

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
  def createDataset[T: Encoder](data: util.List[T]): Dataset[T]

  /**
   * Creates a [[Dataset]] from an RDD of a given type. This method requires an encoder (to
   * convert a JVM object of type `T` to and from the internal Spark SQL representation) that is
   * generally created automatically through implicits from a `SparkSession`, or can be created
   * explicitly by calling static methods on `Encoders`.
   *
   * @note
   *   this method is not supported in Spark Connect.
   * @since 2.0.0
   */
  @ClassicOnly
  def createDataset[T: Encoder](data: RDD[T]): Dataset[T]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  def range(end: Long): Dataset[lang.Long]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long): Dataset[lang.Long]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long): Dataset[lang.Long]

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value, with partition number specified.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[lang.Long]

  /* ------------------------- *
   |  Catalog-related methods  |
   * ------------------------- */

  /**
   * Interface through which the user may create, drop, alter or query underlying databases,
   * tables, functions etc.
   *
   * @since 2.0.0
   */
  def catalog: Catalog

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
  def table(tableName: String): DataFrame

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
  def sql(sqlText: String, args: Array[_]): DataFrame

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
  def sql(sqlText: String, args: Map[String, Any]): DataFrame

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
  def sql(sqlText: String, args: util.Map[String, Any]): DataFrame = {
    sql(sqlText, args.asScala.toMap)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 2.0.0
   */
  def sql(sqlText: String): DataFrame = sql(sqlText, Map.empty[String, Any])

  /**
   * Execute an arbitrary string command inside an external execution engine rather than Spark.
   * This could be useful when user wants to execute some commands out of Spark. For example,
   * executing custom DDL/DML command for JDBC, creating index for ElasticSearch, creating cores
   * for Solr and so on.
   *
   * The command will be eagerly executed after this method is called and the returned DataFrame
   * will contain the output of the command(if any).
   *
   * @param runner
   *   The class name of the runner that implements `ExternalCommandRunner`.
   * @param command
   *   The target command to be executed
   * @param options
   *   The options for the runner.
   *
   * @since 3.0.0
   */
  @Unstable
  def executeCommand(runner: String, command: String, options: Map[String, String]): DataFrame

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
   * Add a tag to be assigned to all the operations started by this thread in this session.
   *
   * Often, a unit of execution in an application consists of multiple Spark executions.
   * Application programmers can use this method to group all those jobs together and give a group
   * tag. The application can use `org.apache.spark.sql.SparkSession.interruptTag` to cancel all
   * running executions with this tag. For example:
   * {{{
   * // In the main thread:
   * spark.addTag("myjobs")
   * spark.range(10).map(i => { Thread.sleep(10); i }).collect()
   *
   * // In a separate thread:
   * spark.interruptTag("myjobs")
   * }}}
   *
   * There may be multiple tags present at the same time, so different parts of application may
   * use different tags to perform cancellation at different levels of granularity.
   *
   * @param tag
   *   The tag to be added. Cannot contain ',' (comma) character or be an empty string.
   *
   * @since 4.0.0
   */
  def addTag(tag: String): Unit

  /**
   * Remove a tag previously added to be assigned to all the operations started by this thread in
   * this session. Noop if such a tag was not added earlier.
   *
   * @param tag
   *   The tag to be removed. Cannot contain ',' (comma) character or be an empty string.
   *
   * @since 4.0.0
   */
  def removeTag(tag: String): Unit

  /**
   * Get the operation tags that are currently set to be assigned to all the operations started by
   * this thread in this session.
   *
   * @since 4.0.0
   */
  def getTags(): Set[String]

  /**
   * Clear the current thread's operation tags.
   *
   * @since 4.0.0
   */
  def clearTags(): Unit

  /**
   * Request to interrupt all currently running operations of this session.
   *
   * @note
   *   This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return
   *   Sequence of operation IDs requested to be interrupted.
   *
   * @since 4.0.0
   */
  def interruptAll(): Seq[String]

  /**
   * Request to interrupt all currently running operations of this session with the given job tag.
   *
   * @note
   *   This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return
   *   Sequence of operation IDs requested to be interrupted.
   *
   * @since 4.0.0
   */
  def interruptTag(tag: String): Seq[String]

  /**
   * Request to interrupt an operation of this session, given its operation ID.
   *
   * @note
   *   This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return
   *   The operation ID requested to be interrupted, as a single-element sequence, or an empty
   *   sequence if the operation is not started by this session.
   *
   * @since 4.0.0
   */
  def interruptOperation(operationId: String): Seq[String]

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
  def read: DataFrameReader

  /**
   * Returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`.
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 2.0.0
   */
  def readStream: DataStreamReader

  /**
   * Returns a [[TableValuedFunction]] that can be used to call a table-valued function (TVF).
   *
   * @since 4.0.0
   */
  def tvf: TableValuedFunction

  /**
   * (Scala-specific) Implicit methods available in Scala for converting common Scala objects into
   * `DataFrame`s.
   *
   * {{{
   *   val sparkSession = SparkSession.builder.getOrCreate()
   *   import sparkSession.implicits._
   * }}}
   *
   * @since 2.0.0
   */
  val implicits: SQLImplicits

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

  /**
   * Check to see if the session is still usable.
   *
   * In Classic this means that the underlying `SparkContext` is still active. In Connect this
   * means the connection to the server is usable.
   */
  private[sql] def isUsable: Boolean

  /**
   * Execute a block of code with this session set as the active session, and restore the previous
   * session on completion.
   */
  @DeveloperApi
  def withActive[T](block: => T): T = {
    // Use the active session thread local directly to make sure we get the session that is actually
    // set and not the default session. This to prevent that we promote the default session to the
    // active session once we are done.
    val old = SparkSession.getActiveSession.orNull
    SparkSession.setActiveSession(this)
    try block
    finally {
      SparkSession.setActiveSession(old)
    }
  }
}

object SparkSession extends SparkSessionCompanion {
  type Session = SparkSession

  // Implementation specific companions
  private lazy val CLASSIC_COMPANION = lookupCompanion(
    "org.apache.spark.sql.classic.SparkSession")
  private lazy val CONNECT_COMPANION = lookupCompanion(
    "org.apache.spark.sql.connect.SparkSession")
  private def DEFAULT_COMPANION =
    Try(CLASSIC_COMPANION).orElse(Try(CONNECT_COMPANION)).getOrElse {
      throw new IllegalStateException(
        "Cannot find a SparkSession implementation on the Classpath.")
    }

  private[this] def lookupCompanion(name: String): SparkSessionCompanion = {
    val cls = SparkClassUtils.classForName(name)
    val mirror = scala.reflect.runtime.currentMirror
    val module = mirror.classSymbol(cls).companion.asModule
    mirror.reflectModule(module).instance.asInstanceOf[SparkSessionCompanion]
  }

  /** @inheritdoc */
  override def builder(): Builder = new Builder

  /** @inheritdoc */
  override def setActiveSession(session: SparkSession): Unit = super.setActiveSession(session)

  /** @inheritdoc */
  override def setDefaultSession(session: SparkSession): Unit = super.setDefaultSession(session)

  /** @inheritdoc */
  override def getActiveSession: Option[SparkSession] = super.getActiveSession

  /** @inheritdoc */
  override def getDefaultSession: Option[SparkSession] = super.getDefaultSession

  override protected def tryCastToImplementation(session: SparkSession): Option[SparkSession] =
    Some(session)

  class Builder extends SparkSessionBuilder {
    import SparkSessionBuilder._
    private val extensionModifications = mutable.Buffer.empty[SparkSessionExtensions => Unit]
    private var sc: Option[SparkContext] = None
    private var companion: SparkSessionCompanion = DEFAULT_COMPANION

    /** @inheritdoc */
    @ClassicOnly
    override def appName(name: String): this.type = super.appName(name)

    /** @inheritdoc */
    @ClassicOnly
    override def master(master: String): this.type = super.master(master)

    /** @inheritdoc */
    @ClassicOnly
    override def enableHiveSupport(): this.type = super.enableHiveSupport()

    /** @inheritdoc */
    override def config(key: String, value: String): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Long): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Double): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Boolean): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(map: Map[String, Any]): this.type = super.config(map)

    /** @inheritdoc */
    override def config(map: util.Map[String, Any]): this.type = super.config(map)

    /** @inheritdoc */
    override def config(conf: SparkConf): this.type = super.config(conf)

    /** @inheritdoc */
    override def remote(connectionString: String): this.type = super.remote(connectionString)

    /** @inheritdoc */
    @ClassicOnly
    override def withExtensions(f: SparkSessionExtensions => Unit): this.type = synchronized {
      extensionModifications += f
      this
    }

    /** @inheritdoc */
    @ClassicOnly
    override private[spark] def sparkContext(sparkContext: SparkContext): this.type =
      synchronized {
        sc = Option(sparkContext)
        this
      }

    /**
     * Make the builder create a Classic SparkSession.
     */
    def classic(): this.type = mode(CONNECT_COMPANION)

    /**
     * Make the builder create a Connect SparkSession.
     */
    def connect(): this.type = mode(CONNECT_COMPANION)

    private def mode(companion: SparkSessionCompanion): this.type = synchronized {
      this.companion = companion
      this
    }

    /** @inheritdoc */
    override def getOrCreate(): SparkSession = builder().getOrCreate()

    /** @inheritdoc */
    override def create(): SparkSession = builder().create()

    override protected def handleBuilderConfig(key: String, value: String): Boolean = key match {
      case API_MODE_KEY =>
        companion = value.toLowerCase(Locale.ROOT).trim match {
          case API_MODE_CLASSIC => CLASSIC_COMPANION
          case API_MODE_CONNECT => CONNECT_COMPANION
          case other =>
            throw new IllegalArgumentException(s"Unknown API mode: $other")
        }
        true
      case _ =>
        false
    }

    /**
     * Create an API mode implementation specific builder.
     */
    private def builder(): SparkSessionBuilder = synchronized {
      val builder = companion.builder()
      sc.foreach(builder.sparkContext)
      options.foreach(kv => builder.config(kv._1, kv._2))
      extensionModifications.foreach(builder.withExtensions)
      builder
    }
  }
}

/**
 * Interface for a [[SparkSession]] Companion. The companion is responsible for building the
 * session, and managing the active (thread local) and default (global) SparkSessions.
 */
private[sql] abstract class SparkSessionCompanion {
  private[sql] type Session <: SparkSession

  import SparkSessionCompanion._

  /**
   * Changes the SparkSession that will be returned in this thread and its children when
   * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SparkSession with an isolated session, instead of the global (first created) context.
   *
   * @since 2.0.0
   */
  def setActiveSession(session: Session): Unit = {
    activeThreadSession.set(session)
  }

  /**
   * Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
   *
   * @since 2.0.0
   */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /**
   * Sets the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def setDefaultSession(session: Session): Unit = {
    defaultSession.set(session)
  }

  /**
   * Clears the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  /**
   * Returns the active SparkSession for the current thread, returned by the builder.
   *
   * @note
   *   Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getActiveSession: Option[Session] = usableSession(activeThreadSession.get())

  /**
   * Returns the default SparkSession that is returned by the builder.
   *
   * @note
   *   Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getDefaultSession: Option[Session] = usableSession(defaultSession.get())

  /**
   * Returns the currently active SparkSession, otherwise the default one. If there is no default
   * SparkSession, throws an exception.
   *
   * @since 2.4.0
   */
  def active: Session = {
    getActiveSession.getOrElse(
      getDefaultSession.getOrElse(
        throw SparkException.internalError("No active or default Spark session found")))
  }

  private def usableSession(session: SparkSession): Option[Session] = {
    if ((session ne null) && session.isUsable) {
      tryCastToImplementation(session)
    } else {
      None
    }
  }

  protected def tryCastToImplementation(session: SparkSession): Option[Session]

  /**
   * Set the (global) default [[SparkSession]], and (thread-local) active [[SparkSession]] when
   * they are not set yet, or they are not usable.
   */
  protected def setDefaultAndActiveSession(session: Session): Unit = {
    val currentDefault = defaultSession.getAcquire
    if (currentDefault == null || !currentDefault.isUsable) {
      // Update `defaultSession` if it is null or the contained session is not usable. There is a
      // chance that the following `compareAndSet` fails if a new default session has just been set,
      // but that does not matter since that event has happened after this method was invoked.
      defaultSession.compareAndSet(currentDefault, session)
    }
    val active = getActiveSession
    if (active.isEmpty || !active.get.isUsable) {
      setActiveSession(session)
    }
  }

  /**
   * When the session is closed remove it from active and default.
   */
  private[sql] def onSessionClose(session: Session): Unit = {
    defaultSession.compareAndSet(session, null)
    if (getActiveSession.contains(session)) {
      clearActiveSession()
    }
  }

  /**
   * Creates a [[SparkSessionBuilder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): SparkSessionBuilder
}

/**
 * This object keeps track of the global (default) and the thread-local SparkSession.
 */
private[sql] object SparkSessionCompanion {

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[SparkSession]
}

/**
 * Builder for [[SparkSession]].
 */
@Stable
private[sql] abstract class SparkSessionBuilder {
  import SparkSessionBuilder._
  protected val options = new scala.collection.mutable.HashMap[String, String]

  /**
   * Sets a name for the application, which will be shown in the Spark web UI. If no application
   * name is set, a randomly generated name will be used.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def appName(name: String): this.type = config(APP_NAME_KEY, name)

  /**
   * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to run
   * locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def master(master: String): this.type = config(MASTER_KEY, master)

  /**
   * Enables Hive support, including connectivity to a persistent Hive metastore, support for Hive
   * serdes, and Hive user-defined functions.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  @ClassicOnly
  def enableHiveSupport(): this.type = config(CATALOG_IMPL_KEY, "hive")

  /**
   * Sets the Spark Connect remote URL.
   *
   * @note
   *   this is only supported in Connect.
   * @since 3.5.0
   */
  def remote(connectionString: String): this.type = config(CONNECT_REMOTE_KEY, connectionString)

  private def putConfig(key: String, value: String): this.type = {
    if (!handleBuilderConfig(key, value)) {
      options += key -> value
    }
    this
  }

  private def safePutConfig(key: String, value: String): this.type =
    synchronized(putConfig(key, value))

  /**
   * Handle a configuration change that is only relevant to the builder.
   *
   * @return
   *   `true` when the change if builder only, otherwise it will be added to the configurations.
   */
  protected def handleBuilderConfig(key: String, value: String): Boolean

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @note
   *   this is only supported in Connect mode.
   * @since 2.0.0
   */
  def config(key: String, value: String): this.type = safePutConfig(key, value)

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 2.0.0
   */
  def config(key: String, value: Long): this.type = safePutConfig(key, value.toString)

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 2.0.0
   */
  def config(key: String, value: Double): this.type = safePutConfig(key, value.toString)

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 2.0.0
   */
  def config(key: String, value: Boolean): this.type = safePutConfig(key, value.toString)

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 3.4.0
   */
  def config(map: Map[String, Any]): this.type = synchronized {
    map.foreach(kv => putConfig(kv._1, kv._2.toString))
    this
  }

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 3.4.0
   */
  def config(map: util.Map[String, Any]): this.type = synchronized {
    config(map.asScala.toMap)
  }

  /**
   * Sets a list of config options based on the given `SparkConf`.
   *
   * @since 2.0.0
   */
  def config(conf: SparkConf): this.type = synchronized {
    conf.getAll.foreach(kv => putConfig(kv._1, kv._2))
    this
  }

  /**
   * Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules,
   * Optimizer rules, Planning Strategies or a customized parser.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.2.0
   */
  @ClassicOnly
  def withExtensions(f: SparkSessionExtensions => Unit): this.type

  /**
   * Set the [[SparkContext]] to use for the [[SparkSession]].
   *
   * @note
   *   this is only supported in Classic.
   */
  @ClassicOnly
  private[spark] def sparkContext(sparkContext: SparkContext): this.type

  /**
   * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new one based on
   * the options set in this builder.
   *
   * This method first checks whether there is a valid thread-local SparkSession, and if yes,
   * return that one. It then checks whether there is a valid global default SparkSession, and if
   * yes, return that one. If no valid global default SparkSession exists, the method creates a
   * new SparkSession and assigns the newly created SparkSession as the global default.
   *
   * In case an existing SparkSession is returned, the non-static config options specified in this
   * builder will be applied to the existing SparkSession.
   *
   * @since 2.0.0
   */
  def getOrCreate(): SparkSession

  /**
   * Create a new [[SparkSession]].
   *
   * This will always return a newly created session.
   *
   * This method will update the default and/or active session if they are not set.
   *
   * @since 3.5.0
   */
  def create(): SparkSession
}

private[sql] object SparkSessionBuilder {
  val MASTER_KEY = "spark.master"
  val APP_NAME_KEY = "spark.app.name"
  val CATALOG_IMPL_KEY = "spark.sql.catalogImplementation"
  val CONNECT_REMOTE_KEY = "spark.connect.remote"
  // Config key/values used to set the SparkSession API mode.
  val API_MODE_KEY: String = "spark.api.mode"
  val API_MODE_CLASSIC: String = "classic"
  val API_MODE_CONNECT: String = "connect"
}
