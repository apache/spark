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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, OrderedDistribution, Partitioning}
import org.apache.spark.sql.catalyst.plans.{Inner, FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer
/* : Developer Api : 
  Sort-merge join
*/
@DeveloperApi
case class MergeJoin(
  leftKeys: Seq[Expression],
  rightKeys: Seq[Expression],
  joinType: JoinType,
  condition: Option[Expression],
  left: SparkPlan,
  right: SparkPlan
) extends BinaryNode {

  
  override def outputPartitioning: Partitioning = left.outputPartitioning
  
  override def output = left.output ++ right.output

  //SortOrder meaning? 
  private val leftOrders = leftKeys.map(s => SortOrder(s, Ascending))
  private val rightOrders = leftKeys.map(s => SortOrder(s, Ascending))

  //Ordered distribution, what order? 
  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  @transient protected lazy val leftKeyGenerator: Projection =
    newProjection(leftKeys, left.output)

  @transient protected lazy val rightKeyGenerator: Projection =
    newProjection(rightKeys, right.output)

  val ordering = new RowOrdering(leftOrders, left.output)

 //    According to Postgres' merge join
 //    Join {
 //      get initial outer and inner tuples       INITIALIZE
 //      do forever {
 //         while (outer != inner) {              SKIP_TEST
 //           if (outer < inner)
 //             advance outer                     SKIPOUTER_ADVANCE
 //           else
 //             advance inner                     SKIPINNER_ADVANCE
 //         }
 //         mark inner position                   SKIP_TEST
 //         do forever {
 //           while (outer == inner) {
 //             join tuples                       JOINTUPLES
 //             advance inner position            NEXTINNER
 //           }
 //           advance outer position              NEXTOUTER
 //           if (outer == mark)                  TESTOUTER
 //             restore inner position to mark    TESTOUTER
 //           else
 //             break // return to top of outer loop
 //         }
 //       }
 //     }

  // put maching tuples in rightIter into compact buffer
  // find a matching tuple between left and right
  // 
  override def execute() = {
    
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) => 
      new Iterator[Row] {
        private[this] val joinRow = new JoinedRow2
        var currentRow:Row = null
        var leftElement:Row = null
        var rightElement:Row = null
        var leftKey:Row = null
        var rightKey:Row = null
        val leftLength = leftIter.length
        val rightLength = rightIter.length
        var leftIndex = 0
        var rightIndex = 0
        var mark = 0
        val buffer = new CompactBuffer[Row]()

        private def initialize() = {
          if (leftIter.hasNext) {
            leftElement = leftIter.next()
          }
          if (rightIter.hasNext) {
            rightElement = rightIter.next()
          }
        }     

        initialize()

        override final def hasNext: Boolean = {
          leftIter.hasNext && rigthIter.hasNext


          while(leftIndex < leftLength) {
            if (leftElement == null) leftElement = leftIter.next()
            if (rightElement == null) rightElement = rightIter.next()
            leftKey = leftKeyGenerator(leftElement)
            rightKey = rightKeyGenerator(rightElement)
            if (ordering.compare(leftKey, rightKey) == 0) {
              buffer += rightElement.copy()
              currentRow = joinRow(leftElement, rightElement)
              return true
            } 

            if (ordering.compare(leftKey, rightKey) <= 0 && rightIndex < rightLength) {
              rightIndex += 1
              rightElement = rightIter.next()
              rightKey = rightKeyGenerator(rightElement)
              if (ordering.compare(leftKey, rightKey) < 0) {
                mark = rightIndex
              }
            } else {
              leftIndex += 1
              leftElement = leftIter.next()
              rightIndex = mark
            }
          }
          false
          // while(leftIter.hasNext && rightIter.hasNext) {
          //   if (ordering.compare(leftKey, rightKey) == 0) {
          //     currentRow = joinRow(leftElement, rightElement)
          //     leftElement = null
          //     rightElement = null
          //     return true
          //   } else if (ordering.compare(leftKey,rightKey) < 0) {
          //     if (leftIter.hasNext) leftElement = leftIter.next()
          //   } else {
          //     if (rightIter.hasNext) rightElement = rightIter.next()
          //   }
          // }
          // false
        }
        
        override final def next() = {
          currentRow.copy()
        }
      }
    }
  }
}