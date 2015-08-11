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

import scala.collection.JavaConversions._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * :: DeveloperApi ::
 * Performs a hash based outer join for two child relations by shuffling the data using
 * the join keys. This operator requires loading the associated partition in both side into memory.
 */
@DeveloperApi
case class ShuffledHashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with HashOuterJoin {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x =>
      throw new IllegalArgumentException(s"HashOuterJoin should not take $x as the JoinType")
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    val joinedRow = new JoinedRow()
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // TODO this probably can be replaced by external sort (sort merged join?)
      joinType match {
        case LeftOuter =>
          val hashed = HashedRelation(rightIter, numRightRows, buildKeyGenerator)
          val keyGenerator = streamedKeyGenerator
          val resultProj = resultProjection
          leftIter.flatMap( currentRow => {
            numLeftRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashed.get(rowKey), resultProj, numOutputRows)
          })

        case RightOuter =>
          val hashed = HashedRelation(leftIter, numLeftRows, buildKeyGenerator)
          val keyGenerator = streamedKeyGenerator
          val resultProj = resultProjection
          rightIter.flatMap ( currentRow => {
            numRightRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashed.get(rowKey), joinedRow, resultProj, numOutputRows)
          })

        case FullOuter =>
          // TODO(davies): use UnsafeRow
          val leftHashTable =
            buildHashTable(leftIter, numLeftRows, newProjection(leftKeys, left.output))
          val rightHashTable =
            buildHashTable(rightIter, numRightRows, newProjection(rightKeys, right.output))
          (leftHashTable.keySet ++ rightHashTable.keySet).iterator.flatMap { key =>
            fullOuterIterator(key,
              leftHashTable.getOrElse(key, EMPTY_LIST),
              rightHashTable.getOrElse(key, EMPTY_LIST),
              joinedRow,
              numOutputRows)
          }

        case x =>
          throw new IllegalArgumentException(
            s"ShuffledHashOuterJoin should not take $x as the JoinType")
      }
    }
  }
}
