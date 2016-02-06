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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a outer hash join for two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class BroadcastHashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with HashOuterJoin {

  private[this] def failOnWrongJoinType(jt: JoinType): Nothing = {
    throw new IllegalArgumentException(s"HashOuterJoin should not take $jt as the JoinType")
  }

  val streamSideName = joinType match {
    case RightOuter => "right"
    case LeftOuter => "left"
    case jt => failOnWrongJoinType(jt)
  }

  override private[sql] lazy val metrics = Map(
    "numStreamRows" -> SQLMetrics.createLongMetric(sparkContext, s"number of $streamSideName rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def requiredChildDistribution: Seq[Distribution] = joinType match {
    case RightOuter => longMetric("numLeftRows")
      BroadcastDistribution(buildRelation) :: UnspecifiedDistribution :: Nil
    case LeftOuter =>
      UnspecifiedDistribution :: BroadcastDistribution(buildRelation) :: Nil
    case x =>
      failOnWrongJoinType(x)
  }

  private val buildRelation: Iterable[InternalRow] => HashedRelation = { input =>
    HashedRelation(input.iterator, SQLMetrics.nullLongMetric, buildKeyGenerator, input.size)
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def doExecute(): RDD[InternalRow] = {
    val numStreamedRows = longMetric("numStreamRows")
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = buildPlan.executeBroadcast[UnsafeHashedRelation]()

    streamedPlan.execute().mapPartitions { streamedIter =>
      val joinedRow = new JoinedRow()
      val hashTable = broadcastRelation.value
      val keyGenerator = streamedKeyGenerator

      hashTable match {
        case unsafe: UnsafeHashedRelation =>
          TaskContext.get().taskMetrics().incPeakExecutionMemory(unsafe.getUnsafeSize)
        case _ =>
      }

      val resultProj = resultProjection
      joinType match {
        case LeftOuter =>
          streamedIter.flatMap(currentRow => {
            numStreamedRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashTable.get(rowKey), resultProj, numOutputRows)
          })

        case RightOuter =>
          streamedIter.flatMap(currentRow => {
            numStreamedRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashTable.get(rowKey), joinedRow, resultProj, numOutputRows)
          })

        case x =>
          failOnWrongJoinType(x)
      }
    }
  }
}
