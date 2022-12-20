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

package org.apache.spark.sql.connect.client

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.SPARK_VERSION
import org.apache.spark.connect.proto


class SparkSession(private val userContext: proto.UserContext) extends AutoCloseable {

  private[this] val allocator = new RootAllocator()

  /**
   * The version of Spark on which this application is running.
   */
  def version: String = SPARK_VERSION

  /**
   * Returns a `DataFrame` with no rows or columns.
   *
   * @since 3.4.0
   */
  @transient
  lazy val emptyDataFrame: Dataset = newDataset { builder =>
    builder.getLocalRelationBuilder
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value, with partition number
   * specified.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset = {
    range(start, end, step, Option(numPartitions))
  }

  private def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): Dataset = {
    newDataset { builder =>
      val rangeBuilder = builder.getRangeBuilder
        .setStart(start)
        .setEnd(end)
        .setStep(step)
      numPartitions.foreach(rangeBuilder.setNumPartitions)
    }
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 3.4.0
   */
  def sql(query: String): Dataset = newDataset { builder =>
    builder.setSql(proto.SQL.newBuilder().setQuery(query))
  }

  private[client] def newDataset(f: proto.Relation.Builder => Unit): Dataset = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset(this, plan)
  }

  private[client] def analyze(plan: proto.Plan): proto.AnalyzePlanResponse =
    throw  new UnsupportedOperationException()

  override def close(): Unit = {
    allocator.close()
  }
}

object SparkSession {
  def builder(): Builder = new Builder()

  class Builder() {
    private val userContextBuilder = proto.UserContext.newBuilder()

    def userId(id: String): Builder = {
      userContextBuilder.setUserId(id)
      this
    }

    def build(): SparkSession = {
      new SparkSession(userContextBuilder.build())
    }
  }
}
