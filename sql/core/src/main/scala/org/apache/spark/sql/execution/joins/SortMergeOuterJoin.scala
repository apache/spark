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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{FullOuter, RightOuter, LeftOuter, JoinType}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs an sort merge outer join of two child relations.
 */
@DeveloperApi
case class SortMergeOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with OuterJoin {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil // when doing Full Outer join, NULL rows from both sides are not ordered.
    case _ => requiredOrders(leftKeys)
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val joinedRow = new JoinedRow()
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
      joinType match {
        case LeftOuter =>
          val resultProj = createResultProjection()
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator,
            buildKeyGenerator,
            keyOrdering,
            streamedIter = leftIter,
            buildIter = rightIter
          )
          // TODO(josh): this is a little terse and needs explanation:
          Iterator.continually(0).takeWhile(_ => smjScanner.findNextOuterJoinRows()).flatMap { _ =>
            leftOuterIterator(
              joinedRow.withLeft(smjScanner.getStreamedRow),
              smjScanner.getBuildMatches,
              resultProj)
          }

        case RightOuter =>
          val resultProj = createResultProjection()
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator,
            buildKeyGenerator,
            keyOrdering,
            streamedIter = rightIter,
            buildIter = leftIter
          )
          // TODO(josh): this is a little terse and needs explanation:
          Iterator.continually(0).takeWhile(_ => smjScanner.findNextOuterJoinRows()).flatMap { _ =>
            rightOuterIterator(
              smjScanner.getBuildMatches,
              joinedRow.withRight(smjScanner.getStreamedRow),
              resultProj)
          }

        case FullOuter =>
          // TODO(josh): handle this case efficiently in SMJ
          // TODO(davies): use UnsafeRow
          val leftHashTable = buildHashTable(leftIter, newProjection(leftKeys, left.output))
          val rightHashTable = buildHashTable(rightIter, newProjection(rightKeys, right.output))
          (leftHashTable.keySet.asScala ++ rightHashTable.keySet.asScala).iterator.flatMap { key =>
            val leftRows: CompactBuffer[InternalRow] = {
              val rows = leftHashTable.get(key)
              if (rows == null) EMPTY_LIST else rows
            }
            val rightRows: CompactBuffer[InternalRow] = {
              val rows = rightHashTable.get(key)
              if (rows == null) EMPTY_LIST else rows
            }
            fullOuterIterator(key, leftRows, rightRows, joinedRow)
          }

        case x =>
          throw new IllegalArgumentException(
            s"SortMergeOuterJoin should not take $x as the JoinType")
      }
    }
  }
}
