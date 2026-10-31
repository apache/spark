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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AS_OF_JOIN
import org.apache.spark.sql.catalyst.util._

/**
 * Resolves SQL `MATCH_CONDITION` operands and DataFrame keys into [[MaterializedMatchCondition]].
 * Validates DataFrame tolerance and selects its execution strategy during analysis.
 * Expands `USING` column lists into equi-join predicates.
 */
object ResolveAsOfJoin extends Rule[LogicalPlan] with SQLConfHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(AS_OF_JOIN), ruleId) {
    case j: AsOfJoin
        if j.left.resolved && j.right.resolved && j.condition.forall(_.resolved) =>
      val left = j.left
      val right = j.right
      val condition = j.condition
      val (matchLeft, matchOp, matchRight) =
        (j.matchLeftOperand, j.matchOperator, j.matchRightOperand)
      val (joinBase, usingProjection) = j.usingColumns match {
        case Some(cols) if condition.isEmpty =>
          val (projectList, hiddenList, newCondition) =
            NaturalAndUsingJoinResolution.computeJoinOutputsAndNewCondition(
              left = left,
              leftOutput = left.output,
              right = right,
              rightOutput = right.output,
              joinType = j.joinType,
              joinNames = cols,
              condition = None,
              resolveName = conf.resolver
            )
          (j.copy(condition = newCondition, usingColumns = None), Some((projectList, hiddenList)))
        case _ => (j, None)
      }
      val leftSet = AttributeSet(left.output ++ left.metadataOutput.filter(_.qualifiedAccessOnly))
      val rightSet = AttributeSet(
        right.output ++ right.metadataOutput.filter(_.qualifiedAccessOnly))
      val resolvedJoin = (matchLeft, matchOp, matchRight) match {
        case (Some(leftExpr), Some(operator), Some(rightExpr)) =>
          AsOfJoinMatchConditionResolution.materialize(
            join = joinBase,
            leftSet = leftSet,
            rightSet = rightSet,
            leftOperand = leftExpr,
            operator = operator,
            rightOperand = rightExpr
          ) match {
            case Some(materialized) =>
              materialized.toJoin(joinBase)
            case None => joinBase
          }
        case (Some(leftExpr), None, Some(rightExpr)) =>
          AsOfJoinMatchConditionResolution.materializeDataFrame(
            join = joinBase,
            leftSet = leftSet,
            rightSet = rightSet,
            leftOperand = leftExpr,
            rightOperand = rightExpr
          ).map(_.toJoin(joinBase)).getOrElse(joinBase)
        case (None, None, None) => joinBase
        case _ => joinBase
      }
      usingProjection match {
        case Some((projectList, hiddenList)) =>
          NaturalAndUsingJoinResolution.buildUsingJoinProject(
            projectList = projectList,
            hiddenList = hiddenList,
            join = resolvedJoin,
            joinHiddenOutput = resolvedJoin.metadataOutput
          )
        case None => resolvedJoin
      }
  }
}
