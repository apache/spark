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
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  RowOrdering,
  SubqueryExpression,
  WindowExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.AsOfMatchCondition
import org.apache.spark.sql.catalyst.plans.logical.{AsOfJoin, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AS_OF_JOIN, GENERATOR}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.{ArrayType, DataType, DatetimeType, StringType, StructType}

/**
 * Resolves SQL [[AsOfJoin]] operators: materializes `MATCH_CONDITION` into `asOfCondition` and
 * `orderExpression`, and expands `USING` column lists into equi-join predicates.
 */
object ResolveAsOfJoin extends Rule[LogicalPlan] with SQLConfHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(AS_OF_JOIN), ruleId) {
    case j @ AsOfJoin(left, right, _, condition, _, _, _, usingColumns, matchCmp)
        if left.resolved && right.resolved =>
      val (joinBase, usingProjection) = usingColumns match {
        case Some(cols) if condition.isEmpty =>
          val (projectList, hiddenList, newCondition) =
            NaturalAndUsingJoinResolution.computeJoinOutputsAndNewCondition(
              left,
              left.output,
              right,
              right.output,
              j.joinType,
              cols,
              None,
              (l, r) => conf.resolver(l, r))
          (j.copy(condition = newCondition, usingColumns = None), Some((projectList, hiddenList)))
        case _ => (j, None)
      }
      val resolvedJoin = matchCmp match {
        case Some(AsOfMatchCondition(leftExpr, operator, rightExpr))
            if leftExpr.resolved && rightExpr.resolved =>
          AsOfJoinValidation.validateMatchConditionOperands(joinBase, leftExpr, rightExpr)
          val (asOfCondition, orderExpression) =
            AsOfJoin.resolveMatchComparison(left, right, leftExpr, operator, rightExpr)
          joinBase.copy(
            asOfCondition = asOfCondition,
            orderExpression = orderExpression,
            matchComparison = None)
        case _ => joinBase
      }
      usingProjection match {
        case Some((projectList, hiddenList)) =>
          val project = Project(projectList, resolvedJoin)
          project.setTagValue(
            Project.hiddenOutputTag,
            hiddenList.map(_.markAsQualifiedAccessOnly()))
          project
        case None => resolvedJoin
      }
  }
}

private[analysis] object AsOfJoinValidation extends QueryErrorsBase {

  def validateMatchConditionOperands(
      join: AsOfJoin,
      leftExpr: Expression,
      rightExpr: Expression): Unit = {
    Seq(leftExpr, rightExpr).foreach { expr =>
      findInvalidMatchConditionExpression(expr).foreach { invalidExpr =>
        join.failAnalysis(
          errorClass = "ASOF_JOIN_MATCH_CONDITION_INVALID_EXPRESSION",
          messageParameters = Map("expr" -> toSQLExpr(invalidExpr)))
      }
    }
    if (!RowOrdering.isOrderable(leftExpr.dataType) ||
        !RowOrdering.isOrderable(rightExpr.dataType) ||
        !areMatchConditionTypesCompatible(leftExpr.dataType, rightExpr.dataType)) {
      join.failAnalysis(
        errorClass = "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE",
        messageParameters = Map(
          "type1" -> toSQLType(leftExpr.dataType),
          "type2" -> toSQLType(rightExpr.dataType)))
    }
  }

  /**
   * Tuple/struct operands may use different field names on each side; compare field-wise by
   * position when [[TypeCoercion.findWiderTypeForTwo]] does not apply.
   */
  private def areMatchConditionTypesCompatible(t1: DataType, t2: DataType): Boolean = {
    if (isIncompatibleMatchConditionPair(t1, t2)) {
      false
    } else {
      TypeCoercion.findWiderTypeForTwo(t1, t2).isDefined ||
        areStructurallyComparableTypes(t1, t2)
    }
  }

  private def isIncompatibleMatchConditionPair(t1: DataType, t2: DataType): Boolean = {
    def isString(dt: DataType): Boolean = dt.isInstanceOf[StringType]
    def isTemporal(dt: DataType): Boolean = dt.isInstanceOf[DatetimeType]
    (isTemporal(t1) && isString(t2)) || (isString(t1) && isTemporal(t2))
  }

  private def areStructurallyComparableTypes(t1: DataType, t2: DataType): Boolean = {
    (t1, t2) match {
      case (s1: StructType, s2: StructType) if s1.length == s2.length =>
        s1.zip(s2).forall { case (f1, f2) =>
          RowOrdering.isOrderable(f1.dataType) &&
            RowOrdering.isOrderable(f2.dataType) &&
            areMatchConditionTypesCompatible(f1.dataType, f2.dataType)
        }
      case (ArrayType(e1, _), ArrayType(e2, _)) =>
        RowOrdering.isOrderable(e1) && RowOrdering.isOrderable(e2) &&
          areMatchConditionTypesCompatible(e1, e2)
      case _ => false
    }
  }

  private def findInvalidMatchConditionExpression(expr: Expression): Option[Expression] = {
    expr.collect {
      case e: SubqueryExpression => e
      case e: AggregateExpression => e
      case e: WindowExpression => e
      case e if e.containsPattern(GENERATOR) => e
      case e if !e.deterministic => e
    }.headOption
  }
}
