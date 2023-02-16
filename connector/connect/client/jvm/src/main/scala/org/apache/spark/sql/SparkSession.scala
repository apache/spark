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

import org.apache.arrow.memory.RootAllocator

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
class SparkSession(private val client: SparkConnectClient, private val cleaner: Cleaner)
    extends Serializable
    with Closeable
    with Logging {

  private[this] val allocator = new RootAllocator()

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 3.4.0
   */
  def sql(query: String): DataFrame = newDataset { builder =>
    builder.setSql(proto.SQL.newBuilder().setQuery(query))
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
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset[T](this, plan)
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

  override def close(): Unit = {
    client.shutdown()
    allocator.close()
  }
}

// The minimal builder needed to create a spark session.
// TODO: implements all methods mentioned in the scaladoc of [[SparkSession]]
object SparkSession extends Logging {
  def builder(): Builder = new Builder()

  private lazy val cleaner = {
    val cleaner = new Cleaner
    cleaner.start()
    cleaner
  }

  class Builder() extends Logging {
    private var _client: SparkConnectClient = _

    def client(client: SparkConnectClient): Builder = {
      _client = client
      this
    }

    def build(): SparkSession = {
      if (_client == null) {
        _client = SparkConnectClient.builder().build()
      }
      new SparkSession(_client, cleaner)
    }
  }
}
