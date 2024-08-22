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

import java.io.Closeable
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag

import com.google.common.cache.{CacheBuilder, CacheLoader}
import io.grpc.ClientInterceptor
import org.apache.arrow.memory.RootAllocator

import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BoxedLongEncoder, UnboundRowEncoder}
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.{toExpr, toTypedExpr}
import org.apache.spark.sql.connect.client.{ClassFinder, CloseableIterator, SparkConnectClient, SparkResult}
import org.apache.spark.sql.connect.client.SparkConnectClient.Configuration
import org.apache.spark.sql.connect.client.arrow.ArrowSerializer
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.{CatalogImpl, SessionCleaner, SqlApiConf}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

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
 *     .remote("sc://localhost:15001/myapp")
 *     .getOrCreate()
 * }}}
 */
class SparkSession private[sql] (
    private[sql] val client: SparkConnectClient,
    private val planIdGenerator: AtomicLong)
    extends Serializable
    with Closeable
    with Logging {

  private[this] val allocator = new RootAllocator()
  private[sql] lazy val cleaner = new SessionCleaner(this)

  // a unique session ID for this session from client.
  private[sql] def sessionId: String = client.sessionId

  lazy val version: String = {
    client.analyze(proto.AnalyzePlanRequest.AnalyzeCase.SPARK_VERSION).getSparkVersion.getVersion
  }

  private[sql] val observationRegistry = new ConcurrentHashMap[Long, Observation]()

  private[sql] def hijackServerSideSessionIdForTesting(suffix: String) = {
    client.hijackServerSideSessionIdForTesting(suffix)
  }

  /**
   * Runtime configuration interface for Spark.
   *
   * This is the interface through which the user can get and set all Spark configurations that
   * are relevant to Spark SQL. When getting the value of a config, his defaults to the value set
   * in server, if any.
   *
   * @since 3.4.0
   */
  val conf: RuntimeConfig = new RuntimeConfig(client)

  /**
   * Executes some code block and prints to stdout the time taken to execute the block. This is
   * available in Scala only and is used primarily for interactive testing and debugging.
   *
   * @since 3.4.0
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
   * Returns a `DataFrame` with no rows or columns.
   *
   * @since 3.4.0
   */
  @transient
  val emptyDataFrame: DataFrame = emptyDataset(UnboundRowEncoder)

  /**
   * Creates a new [[Dataset]] of type T containing zero elements.
   *
   * @since 3.4.0
   */
  def emptyDataset[T: Encoder]: Dataset[T] = createDataset[T](Nil)

  private def createDataset[T](encoder: AgnosticEncoder[T], data: Iterator[T]): Dataset[T] = {
    newDataset(encoder) { builder =>
      if (data.nonEmpty) {
        val arrowData = ArrowSerializer.serialize(data, encoder, allocator, timeZoneId)
        if (arrowData.size() <= conf.get(SqlApiConf.LOCAL_RELATION_CACHE_THRESHOLD_KEY).toInt) {
          builder.getLocalRelationBuilder
            .setSchema(encoder.schema.json)
            .setData(arrowData)
        } else {
          val hash = client.cacheLocalRelation(arrowData, encoder.schema.json)
          builder.getCachedLocalRelationBuilder
            .setHash(hash)
        }
      } else {
        builder.getLocalRelationBuilder
          .setSchema(encoder.schema.json)
      }
    }
  }

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 3.4.0
   */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): DataFrame = {
    createDataset(ScalaReflection.encoderFor[A], data.iterator).toDF()
  }

  /**
   * :: DeveloperApi :: Creates a `DataFrame` from a `java.util.List` containing [[Row]]s using
   * the given schema. It is important to make sure that the structure of every [[Row]] of the
   * provided List matches the provided schema. Otherwise, there will be runtime exception.
   *
   * @since 3.4.0
   */
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = {
    createDataset(RowEncoder.encoderFor(schema), rows.iterator().asScala).toDF()
  }

  /**
   * Applies a schema to a List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean, SELECT * queries
   * will return the columns in an undefined order.
   * @since 3.4.0
   */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = {
    val encoder = JavaTypeInference.encoderFor(beanClass.asInstanceOf[Class[Any]])
    createDataset(encoder, data.iterator().asScala).toDF()
  }

  /**
   * Creates a [[Dataset]] from a local Seq of data of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL
   * representation) that is generally created automatically through implicits from a
   * `SparkSession`, or can be created explicitly by calling static methods on [[Encoders]].
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
   * @since 3.4.0
   */
  def createDataset[T: Encoder](data: Seq[T]): Dataset[T] = {
    createDataset(encoderFor[T], data.iterator)
  }

  /**
   * Creates a [[Dataset]] from a `java.util.List` of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL
   * representation) that is generally created automatically through implicits from a
   * `SparkSession`, or can be created explicitly by calling static methods on [[Encoders]].
   *
   * ==Java Example==
   *
   * {{{
   *     List<String> data = Arrays.asList("hello", "world");
   *     Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
   * }}}
   *
   * @since 3.4.0
   */
  def createDataset[T: Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala.toSeq)
  }

  /**
   * Executes a SQL query substituting positional parameters by the given arguments, returning the
   * result as a `DataFrame`. This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText
   *   A SQL statement with positional parameters to execute.
   * @param args
   *   An array of Java/Scala objects that can be converted to SQL literal expressions. See <a
   *   href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html"> Supported Data
   *   Types</a> for supported value types in Scala/Java. For example: 1, "Steven",
   *   LocalDate.of(2023, 4, 2). A value can be also a `Column` of a literal or collection
   *   constructor functions such as `map()`, `array()`, `struct()`, in that case it is taken as
   *   is.
   *
   * @since 3.5.0
   */
  @Experimental
  def sql(sqlText: String, args: Array[_]): DataFrame = newDataFrame { builder =>
    // Send the SQL once to the server and then check the output.
    val cmd = newCommand(b =>
      b.setSqlCommand(
        proto.SqlCommand
          .newBuilder()
          .setSql(sqlText)
          .addAllPosArguments(args.map(lit(_).expr).toImmutableArraySeq.asJava)))
    val plan = proto.Plan.newBuilder().setCommand(cmd)
    val responseIter = client.execute(plan.build())

    try {
      val response = responseIter
        .find(_.hasSqlCommandResult)
        .getOrElse(throw new RuntimeException("SQLCommandResult must be present"))
      // Update the builder with the values from the result.
      builder.mergeFrom(response.getSqlCommandResult.getRelation)
    } finally {
      // consume the rest of the iterator
      responseIter.foreach(_ => ())
    }
  }

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
   *
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: Map[String, Any]): DataFrame = {
    sql(sqlText, args.asJava)
  }

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
   *
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: java.util.Map[String, Any]): DataFrame = newDataFrame {
    builder =>
      // Send the SQL once to the server and then check the output.
      val cmd = newCommand(b =>
        b.setSqlCommand(
          proto.SqlCommand
            .newBuilder()
            .setSql(sqlText)
            .putAllNamedArguments(args.asScala.map { case (k, v) => (k, lit(v).expr) }.asJava)))
      val plan = proto.Plan.newBuilder().setCommand(cmd)
      val responseIter = client.execute(plan.build())

      try {
        val response = responseIter
          .find(_.hasSqlCommandResult)
          .getOrElse(throw new RuntimeException("SQLCommandResult must be present"))
        // Update the builder with the values from the result.
        builder.mergeFrom(response.getSqlCommandResult.getRelation)
      } finally {
        // consume the rest of the iterator
        responseIter.foreach(_ => ())
      }
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 3.4.0
   */
  def sql(query: String): DataFrame = {
    sql(query, Array.empty)
  }

  /**
   * Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a
   * `DataFrame`.
   * {{{
   *   sparkSession.read.parquet("/path/to/file.parquet")
   *   sparkSession.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @since 3.4.0
   */
  def read: DataFrameReader = new DataFrameReader(this)

  /**
   * Returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`.
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 3.5.0
   */
  def readStream: DataStreamReader = new DataStreamReader(this)

  lazy val streams: StreamingQueryManager = new StreamingQueryManager(this)

  /**
   * Interface through which the user may create, drop, alter or query underlying databases,
   * tables, functions etc.
   *
   * @since 3.5.0
   */
  lazy val catalog: Catalog = new CatalogImpl(this)

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
   * @since 3.4.0
   */
  def table(tableName: String): DataFrame = {
    read.table(tableName)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 3.4.0
   */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, None)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value, with partition number specified.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    range(start, end, step, Option(numPartitions))
  }

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
   *
   * @since 3.5.0
   */
  lazy val udf: UDFRegistration = new UDFRegistration(this)

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * (Scala-specific) Implicit methods available in Scala for converting common names and Symbols
   * into [[Column]]s, and for converting common Scala objects into DataFrame`s.
   *
   * {{{
   *   val sparkSession = SparkSession.builder.getOrCreate()
   *   import sparkSession.implicits._
   * }}}
   *
   * @since 3.4.0
   */
  object implicits extends SQLImplicits(this) with Serializable
  // scalastyle:on

  def newSession(): SparkSession = {
    SparkSession.builder().client(client.copy()).create()
  }

  private def range(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int]): Dataset[java.lang.Long] = {
    newDataset(BoxedLongEncoder) { builder =>
      val rangeBuilder = builder.getRangeBuilder
        .setStart(start)
        .setEnd(end)
        .setStep(step)
      numPartitions.foreach(rangeBuilder.setNumPartitions)
    }
  }

  @Since("4.0.0")
  @DeveloperApi
  def newDataFrame(f: proto.Relation.Builder => Unit): DataFrame = {
    newDataset(UnboundRowEncoder)(f)
  }

  @Since("4.0.0")
  @DeveloperApi
  def newDataset[T](encoder: AgnosticEncoder[T])(
      f: proto.Relation.Builder => Unit): Dataset[T] = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset[T](this, plan, encoder)
  }

  private[sql] def newCommand[T](f: proto.Command.Builder => Unit): proto.Command = {
    val builder = proto.Command.newBuilder()
    f(builder)
    builder.build()
  }

  private[sql] def analyze(
      plan: proto.Plan,
      method: proto.AnalyzePlanRequest.AnalyzeCase,
      explainMode: Option[proto.AnalyzePlanRequest.Explain.ExplainMode] = None)
      : proto.AnalyzePlanResponse = {
    client.analyze(method, Some(plan), explainMode)
  }

  private[sql] def analyze(
      f: proto.AnalyzePlanRequest.Builder => Unit): proto.AnalyzePlanResponse = {
    val builder = proto.AnalyzePlanRequest.newBuilder()
    f(builder)
    client.analyze(builder)
  }

  private[sql] def sameSemantics(plan: proto.Plan, otherPlan: proto.Plan): Boolean = {
    client.sameSemantics(plan, otherPlan).getSameSemantics.getResult
  }

  private[sql] def semanticHash(plan: proto.Plan): Int = {
    client.semanticHash(plan).getSemanticHash.getResult
  }

  private[sql] def timeZoneId: String = conf.get(SqlApiConf.SESSION_LOCAL_TIMEZONE_KEY)

  private[sql] def execute[T](plan: proto.Plan, encoder: AgnosticEncoder[T]): SparkResult[T] = {
    val value = client.execute(plan)
    new SparkResult(
      value,
      allocator,
      encoder,
      timeZoneId,
      Some(setMetricsAndUnregisterObservation))
  }

  private[sql] def execute(f: proto.Relation.Builder => Unit): Unit = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    // .foreach forces that the iterator is consumed and closed
    client.execute(plan).foreach(_ => ())
  }

  @Since("4.0.0")
  @DeveloperApi
  def execute(command: proto.Command): Seq[ExecutePlanResponse] = {
    val plan = proto.Plan.newBuilder().setCommand(command).build()
    // .toSeq forces that the iterator is consumed and closed. On top, ignore all
    // progress messages.
    client.execute(plan).filter(!_.hasExecutionProgress).toSeq
  }

  private[sql] def execute(plan: proto.Plan): CloseableIterator[ExecutePlanResponse] =
    client.execute(plan)

  private[sql] def registerUdf(udf: proto.CommonInlineUserDefinedFunction): Unit = {
    val command = proto.Command.newBuilder().setRegisterFunction(udf).build()
    execute(command)
  }

  /**
   * Add a single artifact to the client session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   *
   * @since 3.4.0
   */
  @Experimental
  def addArtifact(path: String): Unit = client.addArtifact(path)

  /**
   * Add a single artifact to the client session.
   *
   * Currently it supports local files with extensions .jar and .class and Apache Ivy URIs
   *
   * @since 3.4.0
   */
  @Experimental
  def addArtifact(uri: URI): Unit = client.addArtifact(uri)

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
  def addArtifact(bytes: Array[Byte], target: String): Unit = client.addArtifact(bytes, target)

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
  def addArtifact(source: String, target: String): Unit = client.addArtifact(source, target)

  /**
   * Add one or more artifacts to the session.
   *
   * Currently it supports local files with extensions .jar and .class and Apache Ivy URIs
   *
   * @since 3.4.0
   */
  @Experimental
  @scala.annotation.varargs
  def addArtifacts(uri: URI*): Unit = client.addArtifacts(uri)

  /**
   * Register a ClassFinder for dynamically generated classes.
   * @since 3.5.0
   */
  @Experimental
  def registerClassFinder(finder: ClassFinder): Unit = client.registerClassFinder(finder)

  /**
   * This resets the plan id generator so we can produce plans that are comparable.
   *
   * For testing only!
   */
  private[sql] def resetPlanIdGenerator(): Unit = {
    planIdGenerator.set(0)
  }

  /**
   * Interrupt all operations of this session currently running on the connected server.
   *
   * @return
   *   sequence of operationIds of interrupted operations. Note: there is still a possibility of
   *   operation finishing just as it is interrupted.
   *
   * @since 3.5.0
   */
  def interruptAll(): Seq[String] = {
    client.interruptAll().getInterruptedIdsList.asScala.toSeq
  }

  /**
   * Interrupt all operations of this session with the given operation tag.
   *
   * @return
   *   sequence of operationIds of interrupted operations. Note: there is still a possibility of
   *   operation finishing just as it is interrupted.
   *
   * @since 3.5.0
   */
  def interruptTag(tag: String): Seq[String] = {
    client.interruptTag(tag).getInterruptedIdsList.asScala.toSeq
  }

  /**
   * Interrupt an operation of this session with the given operationId.
   *
   * @return
   *   sequence of operationIds of interrupted operations. Note: there is still a possibility of
   *   operation finishing just as it is interrupted.
   *
   * @since 3.5.0
   */
  def interruptOperation(operationId: String): Seq[String] = {
    client.interruptOperation(operationId).getInterruptedIdsList.asScala.toSeq
  }

  /**
   * Synonym for `close()`.
   *
   * @since 3.4.0
   */
  def stop(): Unit = close()

  /**
   * Close the [[SparkSession]].
   *
   * Release the current session and close the GRPC connection to the server. The API will not
   * error if any of these operations fail. Closing a closed session is a no-op.
   *
   * Close the allocator. Fail if there are still open SparkResults.
   *
   * @since 3.4.0
   */
  override def close(): Unit = {
    if (releaseSessionOnClose) {
      try {
        client.releaseSession()
      } catch {
        case e: Exception => logWarning("session.stop: Failed to release session", e)
      }
    }
    try {
      client.shutdown()
    } catch {
      case e: Exception => logWarning("session.stop: Failed to shutdown the client", e)
    }
    allocator.close()
    SparkSession.onSessionClose(this)
  }

  /**
   * Add a tag to be assigned to all the operations started by this thread in this session.
   *
   * Often, a unit of execution in an application consists of multiple Spark executions.
   * Application programmers can use this method to group all those jobs together and give a group
   * tag. The application can use `org.apache.spark.sql.SparkSession.interruptTag` to cancel all
   * running running executions with this tag. For example:
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
   * @since 3.5.0
   */
  def addTag(tag: String): Unit = {
    client.addTag(tag)
  }

  /**
   * Remove a tag previously added to be assigned to all the operations started by this thread in
   * this session. Noop if such a tag was not added earlier.
   *
   * @param tag
   *   The tag to be removed. Cannot contain ',' (comma) character or be an empty string.
   *
   * @since 3.5.0
   */
  def removeTag(tag: String): Unit = {
    client.removeTag(tag)
  }

  /**
   * Get the tags that are currently set to be assigned to all the operations started by this
   * thread.
   *
   * @since 3.5.0
   */
  def getTags(): Set[String] = {
    client.getTags()
  }

  /**
   * Clear the current thread's operation tags.
   *
   * @since 3.5.0
   */
  def clearTags(): Unit = {
    client.clearTags()
  }

  /**
   * We cannot deserialize a connect [[SparkSession]] because of a class clash on the server side.
   * We null out the instance for now.
   */
  private def writeReplace(): Any = null

  /**
   * Set to false to prevent client.releaseSession on close() (testing only)
   */
  private[sql] var releaseSessionOnClose = true

  private[sql] def registerObservation(planId: Long, observation: Observation): Unit = {
    if (observationRegistry.putIfAbsent(planId, observation) != null) {
      throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
    }
  }

  private[sql] def setMetricsAndUnregisterObservation(
      planId: Long,
      metrics: Map[String, Any]): Unit = {
    val observationOrNull = observationRegistry.remove(planId)
    if (observationOrNull != null) {
      observationOrNull.setMetricsAndNotify(Some(metrics))
    }
  }

  implicit class RichColumn(c: Column) {
    def expr: proto.Expression = toExpr(c)
    def typedExpr[T](e: Encoder[T]): proto.Expression = toTypedExpr(c, e)
  }
}

// The minimal builder needed to create a spark session.
// TODO: implements all methods mentioned in the scaladoc of [[SparkSession]]
object SparkSession extends Logging {
  private val MAX_CACHED_SESSIONS = 100
  private val planIdGenerator = new AtomicLong

  private val sessions = CacheBuilder
    .newBuilder()
    .weakValues()
    .maximumSize(MAX_CACHED_SESSIONS)
    .build(new CacheLoader[Configuration, SparkSession] {
      override def load(c: Configuration): SparkSession = create(c)
    })

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[SparkSession]

  /**
   * Set the (global) default [[SparkSession]], and (thread-local) active [[SparkSession]] when
   * they are not set yet or the associated [[SparkConnectClient]] is unusable.
   */
  private def setDefaultAndActiveSession(session: SparkSession): Unit = {
    val currentDefault = defaultSession.getAcquire
    if (currentDefault == null || !currentDefault.client.isSessionValid) {
      // Update `defaultSession` if it is null or the contained session is not valid. There is a
      // chance that the following `compareAndSet` fails if a new default session has just been set,
      // but that does not matter since that event has happened after this method was invoked.
      defaultSession.compareAndSet(currentDefault, session)
    }
    if (getActiveSession.isEmpty) {
      setActiveSession(session)
    }
  }

  /**
   * Create a new [[SparkSession]] based on the connect client [[Configuration]].
   */
  private[sql] def create(configuration: Configuration): SparkSession = {
    new SparkSession(configuration.toSparkConnectClient, planIdGenerator)
  }

  /**
   * Hook called when a session is closed.
   */
  private[sql] def onSessionClose(session: SparkSession): Unit = {
    sessions.invalidate(session.client.configuration)
    defaultSession.compareAndSet(session, null)
    if (getActiveSession.contains(session)) {
      clearActiveSession()
    }
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 3.4.0
   */
  def builder(): Builder = new Builder()

  class Builder() extends Logging {
    // Initialize the connection string of the Spark Connect client builder from SPARK_REMOTE
    // by default, if it exists. The connection string can be overridden using
    // the remote() function, as it takes precedence over the SPARK_REMOTE environment variable.
    private val builder = SparkConnectClient.builder().loadFromEnvironment()
    private var client: SparkConnectClient = _
    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    def remote(connectionString: String): Builder = {
      builder.connectionString(connectionString)
      this
    }

    /**
     * Add an interceptor to be used during channel creation.
     *
     * Note that interceptors added last are executed first by gRPC.
     *
     * @since 3.5.0
     */
    def interceptor(interceptor: ClientInterceptor): Builder = {
      builder.interceptor(interceptor)
      this
    }

    private[sql] def client(client: SparkConnectClient): Builder = {
      this.client = client
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to the
     * Spark Connect session. Only runtime options are supported.
     *
     * @since 3.5.0
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to the
     * Spark Connect session. Only runtime options are supported.
     *
     * @since 3.5.0
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to the
     * Spark Connect session. Only runtime options are supported.
     *
     * @since 3.5.0
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to the
     * Spark Connect session. Only runtime options are supported.
     *
     * @since 3.5.0
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config a map of options. Options set using this method are automatically propagated
     * to the Spark Connect session. Only runtime options are supported.
     *
     * @since 3.5.0
     */
    def config(map: Map[String, Any]): Builder = synchronized {
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
     * @since 3.5.0
     */
    def config(map: java.util.Map[String, Any]): Builder = synchronized {
      config(map.asScala.toMap)
    }

    @deprecated("enableHiveSupport does not work in Spark Connect")
    def enableHiveSupport(): Builder = this

    @deprecated("master does not work in Spark Connect, please use remote instead")
    def master(master: String): Builder = this

    @deprecated("appName does not work in Spark Connect")
    def appName(name: String): Builder = this

    private def tryCreateSessionFromClient(): Option[SparkSession] = {
      if (client != null && client.isSessionValid) {
        Option(new SparkSession(client, planIdGenerator))
      } else {
        None
      }
    }

    private def applyOptions(session: SparkSession): Unit = {
      options.foreach { case (key, value) =>
        session.conf.set(key, value)
      }
    }

    /**
     * Build the [[SparkSession]].
     *
     * This will always return a newly created session.
     */
    @deprecated(message = "Please use create() instead.", since = "3.5.0")
    def build(): SparkSession = create()

    /**
     * Create a new [[SparkSession]].
     *
     * This will always return a newly created session.
     *
     * This method will update the default and/or active session if they are not set.
     *
     * @since 3.5.0
     */
    def create(): SparkSession = {
      val session = tryCreateSessionFromClient()
        .getOrElse(SparkSession.this.create(builder.configuration))
      setDefaultAndActiveSession(session)
      applyOptions(session)
      session
    }

    /**
     * Get or create a [[SparkSession]].
     *
     * If a session exist with the same configuration that is returned instead of creating a new
     * session.
     *
     * This method will update the default and/or active session if they are not set. This method
     * will always set the specified configuration options on the session, even when it is not
     * newly created.
     *
     * @since 3.5.0
     */
    def getOrCreate(): SparkSession = {
      val session = tryCreateSessionFromClient()
        .getOrElse({
          var existingSession = sessions.get(builder.configuration)
          if (!existingSession.client.isSessionValid) {
            // If the cached session has become invalid, e.g., due to a server restart, the cache
            // entry is invalidated.
            sessions.invalidate(builder.configuration)
            existingSession = sessions.get(builder.configuration)
          }
          existingSession
        })
      setDefaultAndActiveSession(session)
      applyOptions(session)
      session
    }
  }

  /**
   * Returns the default SparkSession. If the previously set default SparkSession becomes
   * unusable, returns None.
   *
   * @since 3.5.0
   */
  def getDefaultSession: Option[SparkSession] =
    Option(defaultSession.get()).filter(_.client.isSessionValid)

  /**
   * Sets the default SparkSession.
   *
   * @since 3.5.0
   */
  def setDefaultSession(session: SparkSession): Unit = {
    defaultSession.set(session)
  }

  /**
   * Clears the default SparkSession.
   *
   * @since 3.5.0
   */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  /**
   * Returns the active SparkSession for the current thread. If the previously set active
   * SparkSession becomes unusable, returns None.
   *
   * @since 3.5.0
   */
  def getActiveSession: Option[SparkSession] =
    Option(activeThreadSession.get()).filter(_.client.isSessionValid)

  /**
   * Changes the SparkSession that will be returned in this thread and its children when
   * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
   * an isolated SparkSession.
   *
   * @since 3.5.0
   */
  def setActiveSession(session: SparkSession): Unit = {
    activeThreadSession.set(session)
  }

  /**
   * Clears the active SparkSession for current thread.
   *
   * @since 3.5.0
   */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /**
   * Returns the currently active SparkSession, otherwise the default one. If there is no default
   * SparkSession, throws an exception.
   *
   * @since 3.5.0
   */
  def active: SparkSession = {
    getActiveSession
      .orElse(getDefaultSession)
      .getOrElse(throw new IllegalStateException("No active or default Spark session found"))
  }
}
