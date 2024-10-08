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
import _root_.java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkException
import org.apache.spark.annotation.{DeveloperApi, Experimental, Stable, Unstable}
import org.apache.spark.sql.{Encoder, Row, RuntimeConfig}
import org.apache.spark.sql.types.StructType
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
  def emptyDataFrame: Dataset[Row]

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): Dataset[Row]

  /**
   * :: DeveloperApi :: Creates a `DataFrame` from a `java.util.List` containing
   * [[org.apache.spark.sql.Row]]s using the given schema.It is important to make sure that the
   * structure of every [[org.apache.spark.sql.Row]] of the provided List matches the provided
   * schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  def createDataFrame(rows: util.List[Row], schema: StructType): Dataset[Row]

  /**
   * Applies a schema to a List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean, SELECT * queries
   * will return the columns in an undefined order.
   *
   * @since 1.6.0
   */
  def createDataFrame(data: util.List[_], beanClass: Class[_]): Dataset[Row]

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
  def table(tableName: String): Dataset[Row]

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
  def sql(sqlText: String, args: Array[_]): Dataset[Row]

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
  def sql(sqlText: String, args: Map[String, Any]): Dataset[Row]

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
  def sql(sqlText: String, args: util.Map[String, Any]): Dataset[Row] = {
    sql(sqlText, args.asScala.toMap)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 2.0.0
   */
  def sql(sqlText: String): Dataset[Row] = sql(sqlText, Map.empty[String, Any])

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
}

object SparkSession extends SparkSessionCompanion {
  type Session = SparkSession

  private[this] val companion: SparkSessionCompanion = {
    val cls = SparkClassUtils.classForName("org.apache.spark.sql.SparkSession")
    val mirror = scala.reflect.runtime.currentMirror
    val module = mirror.classSymbol(cls).companion.asModule
    mirror.reflectModule(module).instance.asInstanceOf[SparkSessionCompanion]
  }

  /** @inheritdoc */
  override def builder(): SparkSessionBuilder = companion.builder()

  /** @inheritdoc */
  override def setActiveSession(session: SparkSession): Unit =
    companion.setActiveSession(session.asInstanceOf[companion.Session])

  /** @inheritdoc */
  override def clearActiveSession(): Unit = companion.clearActiveSession()

  /** @inheritdoc */
  override def setDefaultSession(session: SparkSession): Unit =
    companion.setDefaultSession(session.asInstanceOf[companion.Session])

  /** @inheritdoc */
  override def clearDefaultSession(): Unit = companion.clearDefaultSession()

  /** @inheritdoc */
  override def getActiveSession: Option[SparkSession] = companion.getActiveSession

  /** @inheritdoc */
  override def getDefaultSession: Option[SparkSession] = companion.getDefaultSession
}

/**
 * Interface for a [[SparkSession]] Companion. The companion is responsible for building the
 * session, and managing the active (thread local) and default (global) SparkSessions.
 */
private[sql] abstract class SparkSessionCompanion {
  private[sql] type Session <: SparkSession

  /**
   * Changes the SparkSession that will be returned in this thread and its children when
   * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SparkSession with an isolated session, instead of the global (first created) context.
   *
   * @since 2.0.0
   */
  def setActiveSession(session: Session): Unit

  /**
   * Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
   *
   * @since 2.0.0
   */
  def clearActiveSession(): Unit

  /**
   * Sets the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def setDefaultSession(session: Session): Unit

  /**
   * Clears the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def clearDefaultSession(): Unit

  /**
   * Returns the active SparkSession for the current thread, returned by the builder.
   *
   * @note
   *   Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getActiveSession: Option[Session]

  /**
   * Returns the default SparkSession that is returned by the builder.
   *
   * @note
   *   Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getDefaultSession: Option[Session]

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

  /**
   * Creates a [[SparkSessionBuilder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): SparkSessionBuilder
}

/**
 * Abstract class for [[SparkSession]] companions. This implements active and default session
 * management.
 */
private[sql] abstract class BaseSparkSessionCompanion extends SparkSessionCompanion {

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[Session]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[Session]

  /** @inheritdoc */
  def setActiveSession(session: Session): Unit = {
    activeThreadSession.set(session)
  }

  /** @inheritdoc */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /** @inheritdoc */
  def setDefaultSession(session: Session): Unit = {
    defaultSession.set(session)
  }

  /** @inheritdoc */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null.asInstanceOf[Session])
  }

  /** @inheritdoc */
  def getActiveSession: Option[Session] = usableSession(activeThreadSession.get())

  /** @inheritdoc */
  def getDefaultSession: Option[Session] = usableSession(defaultSession.get())

  private def usableSession(session: Session): Option[Session] = {
    if ((session ne null) && canUseSession(session)) {
      Some(session)
    } else {
      None
    }
  }

  protected def canUseSession(session: Session): Boolean = session.isUsable

  /**
   * Set the (global) default [[SparkSession]], and (thread-local) active [[SparkSession]] when
   * they are not set yet or they are not usable.
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
    defaultSession.compareAndSet(session, null.asInstanceOf[Session])
    if (getActiveSession.contains(session)) {
      clearActiveSession()
    }
  }
}

/**
 * Builder for [[SparkSession]].
 */
@Stable
abstract class SparkSessionBuilder {
  protected val options = new scala.collection.mutable.HashMap[String, String]

  /**
   * Sets a name for the application, which will be shown in the Spark web UI. If no application
   * name is set, a randomly generated name will be used.
   *
   * @since 2.0.0
   */
  def appName(name: String): this.type = config("spark.app.name", name)

  /**
   * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to run
   * locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  def master(master: String): this.type = config("spark.master", master)

  /**
   * Enables Hive support, including connectivity to a persistent Hive metastore, support for Hive
   * serdes, and Hive user-defined functions.
   *
   * @note
   *   this is only supported in Classic.
   * @since 2.0.0
   */
  def enableHiveSupport(): this.type = config("spark.sql.catalogImplementation", "hive")

  /**
   * Sets the Spark Connect remote URL.
   *
   * @note
   *   this is only supported in Connect.
   * @since 3.5.0
   */
  def remote(connectionString: String): this.type

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @note
   *   this is only supported in Connect mode.
   * @since 2.0.0
   */
  def config(key: String, value: String): this.type = synchronized {
    options += key -> value
    this
  }

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 2.0.0
   */
  def config(key: String, value: Long): this.type = synchronized {
    options += key -> value.toString
    this
  }

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 2.0.0
   */
  def config(key: String, value: Double): this.type = synchronized {
    options += key -> value.toString
    this
  }

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 2.0.0
   */
  def config(key: String, value: Boolean): this.type = synchronized {
    options += key -> value.toString
    this
  }

  /**
   * Sets a config option. Options set using this method are automatically propagated to both
   * `SparkConf` and SparkSession's own configuration.
   *
   * @since 3.4.0
   */
  def config(map: Map[String, Any]): this.type = synchronized {
    map.foreach { kv: (String, Any) =>
      {
        options += kv._1 -> kv._2.toString
      }
    }
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
