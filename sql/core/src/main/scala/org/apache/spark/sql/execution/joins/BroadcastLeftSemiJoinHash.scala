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

import org.apache.spark.{InternalAccumulator, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Build the right table's join keys into a HashSet, and iteratively go through the left
 * table, to find the if join keys are in the Hash set.
 */
case class BroadcastLeftSemiJoinHash(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryNode with HashSemiJoin {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    val input = right.execute().map { row =>
      numRightRows += 1
      row.copy()
    }.collect()

    if (condition.isEmpty) {
      val hashSet = buildKeyHashSet(input.toIterator, SQLMetrics.nullLongMetric)
      val broadcastedRelation = sparkContext.broadcast(hashSet)

      left.execute().mapPartitionsInternal { streamIter =>
        hashSemiJoin(streamIter, numLeftRows, broadcastedRelation.value, numOutputRows)
      }
    } else {
      val hashRelation =
        HashedRelation(input.toIterator, SQLMetrics.nullLongMetric, rightKeyGenerator, input.size)
      val broadcastedRelation = sparkContext.broadcast(hashRelation)

      left.execute().mapPartitionsInternal { streamIter =>
        val hashedRelation = broadcastedRelation.value
        hashedRelation match {
          case unsafe: UnsafeHashedRelation =>
            TaskContext.get().internalMetricsToAccumulators(
              InternalAccumulator.PEAK_EXECUTION_MEMORY).add(unsafe.getUnsafeSize)
          case _ =>
        }
        hashSemiJoin(streamIter, numLeftRows, hashedRelation, numOutputRows)
      }
    }
  }
}
