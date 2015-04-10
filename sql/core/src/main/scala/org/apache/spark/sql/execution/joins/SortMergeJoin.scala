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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
 */
@DeveloperApi
case class SortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning = HashSortedPartitioning(leftKeys, 0)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredOrderedDistribution(leftKeys) :: ClusteredOrderedDistribution(rightKeys) :: Nil

  private val orders: Seq[SortOrder] = leftKeys.zipWithIndex.map {
    case(expr, index) => SortOrder(BoundReference(index, expr.dataType, expr.nullable), Ascending)
  }
  private val ordering: RowOrdering = new RowOrdering(orders, left.output)

  @transient protected lazy val leftKeyGenerator = newProjection(leftKeys, left.output)
  @transient protected lazy val rightKeyGenerator = newProjection(rightKeys, right.output)

  override def execute(): RDD[Row] = {
    val leftResults = left.execute().map(_.copy())
    val rightResults = right.execute().map(_.copy())

    leftResults.zipPartitions(rightResults) { (leftIter, rightIter) =>
      new Iterator[Row] {
        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow5
        private[this] var leftElement: Row = _
        private[this] var rightElement: Row = _
        private[this] var leftKey: Row = _
        private[this] var rightKey: Row = _
        private[this] var leftMatches: CompactBuffer[Row] = _
        private[this] var rightMatches: CompactBuffer[Row] = _
        private[this] var leftPosition: Int = -1
        private[this] var rightPosition: Int = -1

        override final def hasNext: Boolean = leftPosition != -1 || nextMatchingPair

        override final def next(): Row = {
          if (hasNext) {
            val joinedRow = joinRow(leftMatches(leftPosition), rightMatches(rightPosition))
            rightPosition += 1
            if (rightPosition >= rightMatches.size) {
              leftPosition += 1
              rightPosition = 0
              if (leftPosition >= leftMatches.size) {
                leftPosition = -1
              }
            }
            joinedRow
          } else {
            // according to Scala doc, this is undefined
            null
          }
        }

        private def fetchLeft() = {
          if (leftIter.hasNext) {
            leftElement = leftIter.next()
            leftKey = leftKeyGenerator(leftElement)
          } else {
            leftElement = null
          }
        }

        private def fetchRight() = {
          if (rightIter.hasNext) {
            rightElement = rightIter.next()
            rightKey = rightKeyGenerator(rightElement)
          } else {
            rightElement = null
          }
        }

        private def initialize() = {
          fetchLeft()
          fetchRight()
        }
        // initialize iterator
        initialize()

        /**
         * Searches the left/right iterator for the next rows that matches.
         *
         * @return true if the search is successful, and false if the left/right iterator runs out
         *         of tuples.
         */
        private def nextMatchingPair(): Boolean = {
          if (leftPosition == -1) {
            leftMatches = null
            var stop: Boolean = false
            while (!stop && leftElement != null && rightElement != null) {
              stop = ordering.compare(leftKey, rightKey) == 0 && !leftKey.anyNull
              if (ordering.compare(leftKey, rightKey) > 0 || rightKey.anyNull) {
                fetchRight()
              } else if (ordering.compare(leftKey, rightKey) < 0 || leftKey.anyNull) {
                fetchLeft()
              }
            }
            rightMatches = new CompactBuffer[Row]()
            while (stop && rightElement != null) {
              rightMatches += rightElement
              fetchRight()
              // exit loop when run out of right matches
              stop = ordering.compare(leftKey, rightKey) == 0
            }
            if (rightMatches.size > 0) {
              leftMatches = new CompactBuffer[Row]()
              val leftMatch = leftKey.copy()
              while (ordering.compare(leftKey, leftMatch) == 0 && leftElement != null) {
                leftMatches += leftElement
                fetchLeft()
              }
            }

            if (leftMatches != null) {
              leftPosition = 0
              rightPosition = 0
            }
          }
          leftPosition > -1
        }
      }
    }
  }
}
