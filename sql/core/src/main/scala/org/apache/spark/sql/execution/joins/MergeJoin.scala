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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredOrderedDistribution, Partitioning}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer
/* : Developer Api : 
 * Performs sort-merge join of two child relations by first shuffling the data using the join
 * keys. Also, when shuffling the data, sort the data by join keys. 
*/
@DeveloperApi
case class MergeJoin(
  leftKeys: Seq[Expression],
  rightKeys: Seq[Expression],
  left: SparkPlan,
  right: SparkPlan
) extends BinaryNode {
  // Implementation: the tricky part is handling duplicate join keys.
  // To handle duplicate keys, we use a buffer to store all matching elements 
  // in right iterator for a certain join key. The buffer is used for 
  // generating join tuples when the join key of the next left element is 
  // the same as the current join key. 
  // TODO: add outer join support
  override def outputPartitioning: Partitioning = left.outputPartitioning
  
  override def output = left.output ++ right.output

  private val orders = leftKeys.map(s => SortOrder(s, Ascending))

  override def requiredChildDistribution =
    ClusteredOrderedDistribution(leftKeys) :: ClusteredOrderedDistribution(rightKeys) :: Nil

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  @transient protected lazy val rightKeyGenerator: Projection =
    newProjection(rightKeys, right.output)

  private val ordering = new RowOrdering(orders, left.output)

  override def execute() = {
    
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) => 
      new Iterator[Row] {
        private[this] val joinRow = new JoinedRow2
        private[this] var leftElement:Row = _
        private[this] var rightElement:Row = _
        private[this] var leftKey:Row = _
        private[this] var rightKey:Row = _
        private[this] var buffer:CompactBuffer[Row] = _
        private[this] var index = -1
        private[this] var last = false
        
        // initialize iterator  
        private def initialize() = {
          if (leftIter.hasNext) {
            leftElement = leftIter.next()
            leftKey = leftKeyGenerator(leftElement)
          } else {
            last = true
          }
          if (rightIter.hasNext) {
            rightElement = rightIter.next()
            rightKey = rightKeyGenerator(rightElement)
          } else {
            last = true
          }
        }     

        initialize()

        override final def hasNext: Boolean = {
          // Two cases that hasNext returns true
          // 1. We are iterating the buffer
          // 2. We can find tuple pairs that have matching join key
          // 
          // hasNext is stateless as nextMatchingPair() is called when
          // index == -1 and will set index to 0 when nextMatchingPair() 
          // returns true. Muptiple calls to hasNext modifies iterator
          // state at most once. 
          if (index != -1) return true
          if (last) return false
          return nextMatchingPair()
        }
        
        override final def next(): Row = {
          if (index == -1) {
            // We need this becasue the client of the join iterator may 
            // call next() without calling hasNext 
            if (!hasNext) return null
          }
          val joinedRow = joinRow(leftElement, buffer(index))
          index += 1
          if (index == buffer.size) {
            // finished iterating the buffer, fetch
            // next element from left iterator
            if (leftIter.hasNext) {
              // fetch next element 
              val leftElem = leftElement
              val leftK = leftKeyGenerator(leftElem)
              leftElement = leftIter.next()
              leftKey = leftKeyGenerator(leftElement)
              if (ordering.compare(leftKey,leftK) == 0) {
                // need to go over the buffer again
                // as we have the same join key for 
                // next left element
                index = 0
              } else {
                // need to find a matching element from
                // right iterator
                index = -1
              }
            } else {
              // no next left element, we are done
              index = -1
              last = true
            }
          }
          joinedRow
        }

        // find the next pair of left/right tuples that have a
        // matching join key
        private def nextMatchingPair(): Boolean = {
          while (ordering.compare(leftKey, rightKey) != 0) {
            if (ordering.compare(leftKey, rightKey) < 0) {
              if (leftIter.hasNext) {
                leftElement = leftIter.next()
                leftKey = leftKeyGenerator(leftElement)
              } else {
                last = true
                return false
              }
            } else {
              if (rightIter.hasNext) {
                rightElement = rightIter.next()
                rightKey = rightKeyGenerator(rightElement)
              } else {
                last = true
                return false
              }
            }
          }
          // outer == inner
          index = 0
          buffer = null
          buffer = new CompactBuffer[Row]()
          buffer += rightElement
          val rightElem = rightElement
          val rightK = rightKeyGenerator(rightElem)
          while(rightIter.hasNext) {
            rightElement = rightIter.next()
            rightKey = rightKeyGenerator(rightElement)
            if (ordering.compare(rightKey,rightK) == 0) {
              buffer += rightElement
            } else {
              return true
            }
          }
          true
        }
      }
    }
  }
}
