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
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.SPARK_VERSION
import org.apache.spark.annotation.Experimental
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkResult}
import org.apache.spark.sql.connect.client.util.Cleaner

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
class SparkSession(
    private val client: SparkConnectClient,
    private val cleaner: Cleaner,
    private val planIdGenerator: AtomicLong)
    extends Serializable
    with Closeable
    with Logging {

  private[this] val allocator = new RootAllocator()

  def version: String = SPARK_VERSION

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
   * Executes a SQL query substituting named parameters by the given arguments, returning the
   * result as a `DataFrame`. This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText
   *   A SQL statement with named parameters to execute.
   * @param args
   *   A map of parameter names to literal values.
   *
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: Map[String, String]): DataFrame = {
    sql(sqlText, args.asJava)
  }

  /**
   * Executes a SQL query substituting named parameters by the given arguments, returning the
   * result as a `DataFrame`. This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText
   *   A SQL statement with named parameters to execute.
   * @param args
   *   A map of parameter names to literal values.
   *
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: java.util.Map[String, String]): DataFrame = newDataset {
    builder =>
      builder
        .setSql(proto.SQL.newBuilder().setQuery(sqlText).putAllArgs(args))
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
  def range(end: Long): Dataset[Row] = range(0, end)

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long): Dataset[Row] = {
    range(start, end, step = 1)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long): Dataset[Row] = {
    range(start, end, step, None)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements in a
   * range from `start` to `end` (exclusive) with a step value, with partition number specified.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[Row] = {
    range(start, end, step, Option(numPartitions))
  }

  private def range(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int]): Dataset[Row] = {
    newDataset { builder =>
      val rangeBuilder = builder.getRangeBuilder
        .setStart(start)
        .setEnd(end)
        .setStep(step)
      numPartitions.foreach(rangeBuilder.setNumPartitions)
    }
  }

  private[sql] def newDataset[T](f: proto.Relation.Builder => Unit): Dataset[T] = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    builder.getCommonBuilder.setPlanId(planIdGenerator.getAndIncrement())
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset[T](this, plan)
  }

  private[sql] def newCommand[T](f: proto.Command.Builder => Unit): proto.Command = {
    val builder = proto.Command.newBuilder()
    f(builder)
    builder.build()
  }

  private[sql] def analyze(
      plan: proto.Plan,
      mode: proto.Explain.ExplainMode): proto.AnalyzePlanResponse =
    client.analyze(plan, mode)

  private[sql] def execute(plan: proto.Plan): SparkResult = {
    val value = client.execute(plan)
    val result = new SparkResult(value, allocator)
    cleaner.register(result)
    result
  }

  private[sql] def execute(command: proto.Command): Unit = {
    val plan = proto.Plan.newBuilder().setCommand(command).build()
    client.execute(plan).asScala.foreach(_ => ())
  }

  /**
   * This resets the plan id generator so we can produce plans that are comparable.
   *
   * For testing only!
   */
  private[sql] def resetPlanIdGenerator(): Unit = {
    planIdGenerator.set(0)
  }

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
}
