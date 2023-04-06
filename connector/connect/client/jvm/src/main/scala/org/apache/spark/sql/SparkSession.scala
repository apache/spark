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
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BoxedLongEncoder, UnboundRowEncoder}
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkResult}
import org.apache.spark.sql.connect.client.util.{Cleaner, ConvertToArrow}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.toLiteralProto
import org.apache.spark.sql.internal.CatalogImpl
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
class SparkSession private[sql] (
    private val client: SparkConnectClient,
    private val cleaner: Cleaner,
    private val planIdGenerator: AtomicLong)
    extends Serializable
    with Closeable
    with Logging {

  private[this] val allocator = new RootAllocator()

  lazy val version: String = {
    client.analyze(proto.AnalyzePlanRequest.AnalyzeCase.SPARK_VERSION).getSparkVersion.getVersion
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
      val localRelationBuilder = builder.getLocalRelationBuilder
        .setSchema(encoder.schema.json)
      if (data.nonEmpty) {
        val timeZoneId = conf.get("spark.sql.session.timeZone")
        val arrowData = ConvertToArrow(encoder, data, timeZoneId, allocator)
        localRelationBuilder.setData(arrowData)
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
   *   can be also a `Column` of literal expression, in that case it is taken as is.
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
   *   can be also a `Column` of literal expression, in that case it is taken as is.
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
            .putAllArgs(args.asScala.mapValues(toLiteralProto).toMap.asJava)))
      val plan = proto.Plan.newBuilder().setCommand(cmd)
      val responseIter = client.execute(plan.build())

      val response = responseIter.asScala
        .find(_.hasSqlCommandResult)
        .getOrElse(throw new RuntimeException("SQLCommandResult must be present"))

      // Update the builder with the values from the result.
      builder.mergeFrom(response.getSqlCommandResult.getRelation)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 3.4.0
   */
  def sql(query: String): DataFrame = {
    sql(query, Map.empty[String, String])
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

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * (Scala-specific) Implicit methods available in Scala for converting common names and
   * [[Symbol]]s into [[Column]]s, and for converting common Scala objects into `DataFrame`s.
   *
   * {{{
   *   val sparkSession = SparkSession.builder.getOrCreate()
   *   import sparkSession.implicits._
   * }}}
   *
   * @since 3.4.0
   */
  object implicits extends SQLImplicits(this)
  // scalastyle:on

  def newSession(): SparkSession = {
    SparkSession.builder().client(client.copy()).build()
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

  private[sql] def newDataFrame(f: proto.Relation.Builder => Unit): DataFrame = {
    newDataset(UnboundRowEncoder)(f)
  }

  private[sql] def newDataset[T](encoder: AgnosticEncoder[T])(
      f: proto.Relation.Builder => Unit): Dataset[T] = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset[T](this, plan, encoder)
  }

  @DeveloperApi
  def newDataFrame(extension: com.google.protobuf.Any): DataFrame = {
    newDataset(extension, UnboundRowEncoder)
  }

  @DeveloperApi
  def newDataset[T](
      extension: com.google.protobuf.Any,
      encoder: AgnosticEncoder[T]): Dataset[T] = {
    newDataset(encoder)(_.setExtension(extension))
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

  private[sql] def execute[T](plan: proto.Plan, encoder: AgnosticEncoder[T]): SparkResult[T] = {
    val value = client.execute(plan)
    val result = new SparkResult(value, allocator, encoder)
    cleaner.register(result)
    result
  }

  private[sql] def execute(f: proto.Relation.Builder => Unit): Unit = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    client.execute(plan).asScala.foreach(_ => ())
  }

  private[sql] def execute(command: proto.Command): Unit = {
    val plan = proto.Plan.newBuilder().setCommand(command).build()
    client.execute(plan).asScala.foreach(_ => ())
  }

  @DeveloperApi
  def execute(extension: com.google.protobuf.Any): Unit = {
    val command = proto.Command.newBuilder().setExtension(extension).build()
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
   * Currently only local files with extensions .jar and .class are supported.
   *
   * @since 3.4.0
   */
  @Experimental
  def addArtifact(uri: URI): Unit = client.addArtifact(uri)

  /**
   * Add one or more artifacts to the session.
   *
   * Currently only local files with extensions .jar and .class are supported.
   *
   * @since 3.4.0
   */
  @Experimental
  @scala.annotation.varargs
  def addArtifacts(uri: URI*): Unit = client.addArtifacts(uri)

  /**
   * This resets the plan id generator so we can produce plans that are comparable.
   *
   * For testing only!
   */
  private[sql] def resetPlanIdGenerator(): Unit = {
    planIdGenerator.set(0)
  }

  /**
   * Synonym for `close()`.
   *
   * @since 3.4.0
   */
  def stop(): Unit = close()

  /**
   * Close the [[SparkSession]]. This closes the connection, and the allocator. The latter will
   * throw an exception if there are still open [[SparkResult]]s.
   *
   * @since 3.4.0
   */
  override def close(): Unit = {
    client.shutdown()
    allocator.close()
  }
}

// The minimal builder needed to create a spark session.
// TODO: implements all methods mentioned in the scaladoc of [[SparkSession]]
object SparkSession extends Logging {
  private val planIdGenerator = new AtomicLong

  def builder(): Builder = new Builder()

  private[sql] lazy val cleaner = {
    val cleaner = new Cleaner
    cleaner.start()
    cleaner
  }

  class Builder() extends Logging {
    private var _client: SparkConnectClient = _

    def remote(connectionString: String): Builder = {
      client(SparkConnectClient.builder().connectionString(connectionString).build())
      this
    }

    private[sql] def client(client: SparkConnectClient): Builder = {
      _client = client
      this
    }

    def build(): SparkSession = {
      if (_client == null) {
        _client = SparkConnectClient.builder().build()
      }
      new SparkSession(_client, cleaner, planIdGenerator)
    }
  }

  def getActiveSession: Option[SparkSession] = {
    throw new UnsupportedOperationException("getActiveSession is not supported")
  }

  def getDefaultSession: Option[SparkSession] = {
    throw new UnsupportedOperationException("getDefaultSession is not supported")
  }

  def setActiveSession(session: SparkSession): Unit = {
    throw new UnsupportedOperationException("setActiveSession is not supported")
  }

  def clearActiveSession(): Unit = {
    throw new UnsupportedOperationException("clearActiveSession is not supported")
  }

  def active: SparkSession = {
    throw new UnsupportedOperationException("active is not supported")
  }
}
