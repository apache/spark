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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * When the physical operators are created for JOIN, the ordering of join keys is based on order
 * in which the join keys appear in the user query. That might not match with the output
 * partitioning of the join node's children (thus leading to extra sort / shuffle being
 * introduced). This rule will change the ordering of the join keys to match with the
 * partitioning of the join nodes' children.
 */
class ReorderJoinPredicates extends Rule[SparkPlan] {
  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {

    def reorder(
        expectedOrderOfKeys: Seq[Expression],
        currentOrderOfKeys: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
      val leftKeysBuffer = ArrayBuffer[Expression]()
      val rightKeysBuffer = ArrayBuffer[Expression]()

      expectedOrderOfKeys.foreach(expression => {
        val index = currentOrderOfKeys.indexWhere(e => e.semanticEquals(expression))
        leftKeysBuffer.append(leftKeys(index))
        rightKeysBuffer.append(rightKeys(index))
      })
      (leftKeysBuffer, rightKeysBuffer)
    }

    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      leftPartitioning match {
        case HashPartitioning(leftExpressions, _)
          if leftExpressions.length == leftKeys.length &&
            leftKeys.forall(x => leftExpressions.exists(_.semanticEquals(x))) =>
          reorder(leftExpressions, leftKeys)

        case _ => rightPartitioning match {
          case HashPartitioning(rightExpressions, _)
            if rightExpressions.length == rightKeys.length &&
              rightKeys.forall(x => rightExpressions.exists(_.semanticEquals(x))) =>
            reorder(rightExpressions, rightKeys)

          case _ => (leftKeys, rightKeys)
        }
      }
    } else {
      (leftKeys, rightKeys)
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case BroadcastHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) =>
      val (reorderedLeftKeys, reorderedRightKeys) =
        reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
      BroadcastHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
        left, right)

    case ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) =>
      val (reorderedLeftKeys, reorderedRightKeys) =
        reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
      ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
        left, right)

    case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
      val (reorderedLeftKeys, reorderedRightKeys) =
        reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
      SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition, left, right)
  }
}
