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

package org.apache.spark.sql.execution.joins

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs an inner hash join for multi child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 *
 */
@AlphaComponent
case class DimensionJoin(
    keys: Seq[JoinKey],
    filters: Seq[JoinFilter],
    children: Seq[SparkPlan])
  extends SparkPlan with MultiwayJoin {

  val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  // bind the join filter with the join intermediate output
  val joinFilters = filters.map { f =>
    f.copy(joinType = f.joinType, filter = BindReferences.bindReference(f.filter, output))
  }

  private def joinKey(tblIdx: Int): Seq[Expression] = {
    if (tblIdx == 0) keys(0).leftKeys else keys(tblIdx - 1).rightKeys
  }

  private def correlatedJoinKey(tblIdx: Int): Seq[Expression] = {
    if (tblIdx == 0) keys(0).rightKeys else keys(tblIdx - 1).leftKeys
  }

  private def streamPlan = children(0)

  override def outputPartitioning: Partitioning = {
    streamPlan.outputPartitioning
  }

  override def childrenOutputs: Seq[Seq[Attribute]] = children.map(_.output)

  override def requiredChildDistribution = children.map(_ => UnspecifiedDistribution)

  @transient protected lazy val keyGenerators: Array[Projection] =
    Array.tabulate(children.size)(i => newProjection(joinKey(i), children(i).output))

  @transient protected lazy val correlatedJeyGenerators: Array[Projection] =
    Array.tabulate(children.size)(i => newProjection(correlatedJoinKey(i), output))

  @transient
  private val broadcastFuture = future {
    val hashed = children.zipWithIndex.map { case (buildPlan, index) =>
      if (index == 0) {
        // we put the null for stream side as place holder,
        // so we will not break the index-based relation accessing
        null: HashedRelation
      } else {
        // Note that we use .execute().collect() because we
        // don't want to convert data to Scala types
        val input: Array[Row] = buildPlan.execute().map(_.copy()).collect()
        HashedRelation(input.iterator, keyGenerators(index), input.length)
      }
    }.toArray

    sparkContext.broadcast(hashed)
  }

  override def execute(): RDD[Row] = {
    val relationFuture = Await.result(broadcastFuture, timeout)

    streamPlan.execute().mapPartitions { streamedIter =>
      val relations = relationFuture.value

      var iteratorBuilder: IteratorBufferBuilder = null
      val builders = Array.tabulate[CompactBufferBuilder](children.size) { i =>
        if (i == 0) {
          iteratorBuilder = new IteratorBufferBuilder()
          iteratorBuilder
        } else {
          val keyProjection = correlatedJeyGenerators(i)
          new CorrelatedBufferBuilder(keyProjection, relations(i))
        }
      }

      streamedIter.flatMap { row =>
        iteratorBuilder.withIterator(row)
        product(builders)
      }
    }
  }
}
