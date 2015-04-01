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
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning = HashSortedPartitioning(leftKeys, 0)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredOrderedDistribution(leftKeys) :: ClusteredOrderedDistribution(rightKeys) :: Nil

  private val orders: Seq[SortOrder] = leftKeys.map(s => SortOrder(s, Ascending))
  private val ordering: RowOrdering = new RowOrdering(orders, left.output)

  @transient protected lazy val leftKeyGenerator = newProjection(leftKeys, left.output)
  @transient protected lazy val rightKeyGenerator = newProjection(rightKeys, right.output)

  override def execute() = {
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
        private[this] var currentlMatches: CompactBuffer[Row] = _
        private[this] var currentrMatches: CompactBuffer[Row] = _
        private[this] var currentlPosition: Int = -1
        private[this] var currentrPosition: Int = -1

        override final def hasNext: Boolean = currentlPosition != -1 || nextMatchingPair

        override final def next(): Row = {
          if (!hasNext) {
            return null
          }
          val joinedRow =
            joinRow(currentlMatches(currentlPosition), currentrMatches(currentrPosition))
          currentrPosition += 1
          if (currentrPosition >= currentrMatches.size) {
            currentlPosition += 1
            currentrPosition = 0
            if (currentlPosition >= currentlMatches.size) {
              currentlPosition = -1
            }
          }
          joinedRow
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

        private def fetchFirst() = {
          fetchLeft()
          fetchRight()
          currentrPosition = 0
        }
        // initialize iterator
        fetchFirst()

        /**
         * Searches the left/right iterator for the next rows that matches.
         *
         * @return true if the search is successful, and false if the left/right iterator runs out
         *         of tuples.
         */
        private def nextMatchingPair(): Boolean = {
          if (currentlPosition > -1) {
            true
          } else {
            currentlPosition = -1
            currentlMatches = null
            var stop: Boolean = false
            while (!stop && leftElement != null && rightElement != null) {
              if (ordering.compare(leftKey, rightKey) == 0 && !leftKey.anyNull) {
                stop = true
              } else if (ordering.compare(leftKey, rightKey) > 0 || rightKey.anyNull) {
                fetchRight()
              } else { //if (ordering.compare(leftKey, rightKey) < 0 || leftKey.anyNull)
                fetchLeft()
              }
            }
            currentrMatches = new CompactBuffer[Row]()
            while (stop && rightElement != null) {
              currentrMatches += rightElement
              fetchRight()
              if (ordering.compare(leftKey, rightKey) != 0) {
                stop = false
              }
            }
            if (currentrMatches.size > 0) {
              stop = false
              currentlMatches = new CompactBuffer[Row]()
              val leftMatch = leftKey.copy()
              while (!stop && leftElement != null) {
                currentlMatches += leftElement
                fetchLeft()
                if (ordering.compare(leftKey, leftMatch) != 0) {
                  stop = true
                }
              }
            }

            if (currentlMatches == null) {
              false
            } else {
              currentlPosition = 0
              currentrPosition = 0
              true
            }
          }
        }
      }
    }
  }
}
