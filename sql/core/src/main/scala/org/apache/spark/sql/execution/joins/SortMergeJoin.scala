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
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredOrderedDistribution, Partitioning}
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

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[ClusteredOrderedDistribution] =
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
        private[this] var read: Boolean = false
        private[this] var currentlMatches: CompactBuffer[Row] = _
        private[this] var currentrMatches: CompactBuffer[Row] = _
        private[this] var currentlPosition: Int = -1
        private[this] var currentrPosition: Int = -1

        override final def hasNext: Boolean =
          (currentlPosition != -1 && currentlPosition < currentlMatches.size) ||
            (leftIter.hasNext && rightIter.hasNext && nextMatchingPair)

        override final def next(): Row = {
          val joinedRow =
            joinRow(currentlMatches(currentlPosition), currentrMatches(currentrPosition))
          currentrPosition += 1
          if (currentrPosition >= currentrMatches.size) {
            currentlPosition += 1
            currentrPosition = 0
          }
          joinedRow
        }

        /**
         * Searches the left/right iterator for the next rows that matches.
         *
         * @return true if the search is successful, and false if the left/right iterator runs out
         *         of tuples.
         */
        private def nextMatchingPair(): Boolean = {
          currentlPosition = -1
          currentlMatches = null
          if (rightElement == null) {
            rightElement = rightIter.next()
            rightKey = rightKeyGenerator(rightElement)
          }
          while (currentlMatches == null && leftIter.hasNext) {
            if (!read) {
              leftElement = leftIter.next()
              leftKey = leftKeyGenerator(leftElement)
            }
            while (ordering.compare(leftKey, rightKey) > 0 && rightIter.hasNext) {
              rightElement = rightIter.next()
              rightKey = rightKeyGenerator(rightElement)
            }
            currentrMatches = new CompactBuffer[Row]()
            while (ordering.compare(leftKey, rightKey) == 0 && rightIter.hasNext) {
              currentrMatches += rightElement
              rightElement = rightIter.next()
              rightKey = rightKeyGenerator(rightElement)
            }
            if (ordering.compare(leftKey, rightKey) == 0) {
              currentrMatches += rightElement
            }
            if (currentrMatches.size > 0) {
              // there exists rows match in right table, should search left table
              currentlMatches = new CompactBuffer[Row]()
              val leftMatch = leftKey.copy()
              while (ordering.compare(leftKey, leftMatch) == 0 && leftIter.hasNext) {
                currentlMatches += leftElement
                leftElement = leftIter.next()
                leftKey = leftKeyGenerator(leftElement)
              }
              if (ordering.compare(leftKey, leftMatch) == 0) {
                currentlMatches += leftElement
              } else {
                read = true
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
