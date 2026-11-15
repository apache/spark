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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegralType, NullType}

/** The executable fields produced by resolving ASOF keys. */
private[sql] case class MaterializedMatchCondition(
    asOfCondition: Expression,
    direction: AsOfJoinDirection,
    leftSortExpressions: Seq[Expression],
    rightSortExpressions: Seq[Expression],
    orderExpression: Option[Expression] = None) {

  /** Replaces the pending operands with the resolved comparison and sort keys. */
  def toJoin(join: AsOfJoin): AsOfJoin = {
    join.copy(
      asOfCondition = asOfCondition,
      direction = direction,
      leftSortExprs = leftSortExpressions,
      rightSortExprs = rightSortExpressions,
      matchLeftOperand = None,
      matchOperator = None,
      matchRightOperand = None,
      requiresSortMergeAsOfJoin = orderExpression.isEmpty,
      orderExpression = orderExpression
    )
  }
}

/** Shared ASOF key normalization for the fixed-point and single-pass analyzers. */
private[sql] object AsOfJoinMatchConditionResolution {

  /** Materializes a SQL MATCH_CONDITION once both operands are resolved. */
  def materialize(
      join: AsOfJoin,
      leftSet: AttributeSet,
      rightSet: AttributeSet,
      leftOperand: Expression,
      operator: MatchComparisonOperator,
      rightOperand: Expression): Option[MaterializedMatchCondition] = {
    AsOfJoinValidation.validateMatchConditionTableReferences(
      join = join,
      leftSet = leftSet,
      rightSet = rightSet,
      leftExpr = leftOperand,
      rightExpr = rightOperand
    )

    if (leftOperand.resolved && rightOperand.resolved) {
      AsOfJoinValidation.validateMatchConditionOperands(
        join = join,
        leftExpr = leftOperand,
        rightExpr = rightOperand
      )
      val (normalizedLeft, normalizedRight, normalizedOperator) =
        AsOfJoin.normalizeMatchOperands(
          leftSet = leftSet,
          rightSet = rightSet,
          expr1 = leftOperand,
          operator = operator,
          expr2 = rightOperand
        )
      val (asOfCondition, leftSort, rightSort) = AsOfJoin.materializeMatchComparison(
        leftOperand = normalizedLeft,
        rightOperand = normalizedRight,
        normalizedOp = normalizedOperator
      )
      Some(MaterializedMatchCondition(
        asOfCondition = asOfCondition,
        direction = normalizedOperator.direction,
        leftSortExpressions = leftSort,
        rightSortExpressions = rightSort
      ))
    } else {
      None
    }
  }

  /** Resolves DataFrame keys and selects the execution strategy during analysis. */
  def materializeDataFrame(
      join: AsOfJoin,
      leftSet: AttributeSet,
      rightSet: AttributeSet,
      leftOperand: Expression,
      rightOperand: Expression): Option[MaterializedMatchCondition] = {
    if (!leftOperand.resolved || !rightOperand.resolved || !join.tolerance.forall(_.resolved)) {
      return None
    }

    AsOfJoinValidation.validateTolerance(join)

    val useSortMerge = SQLConf.get.sortMergeAsOfJoinEnabled
    if (useSortMerge) {
      AsOfJoinValidation.validateMatchConditionTableReferences(
        join = join,
        leftSet = leftSet,
        rightSet = rightSet,
        leftExpr = leftOperand,
        rightExpr = rightOperand,
        isDataFrame = true
      )
      AsOfJoinValidation.validateMatchConditionOperands(
        join = join,
        leftExpr = leftOperand,
        rightExpr = rightOperand,
        isDataFrame = true
      )
    }
    AsOfJoinValidation.validateNearestStringKeys(
      join = join,
      leftKey = leftOperand,
      rightKey = rightOperand
    )
    if (useSortMerge) {
      AsOfJoinValidation.validateSortMergeToleranceKeys(
        join = join,
        leftKey = leftOperand,
        rightKey = rightOperand
      )
    }
    val asOfCondition = AsOfJoin.makeAsOfCond(
      leftAsOf = leftOperand,
      rightAsOf = rightOperand,
      tolerance = join.tolerance,
      allowExactMatches = join.allowExactMatches,
      direction = join.direction,
      evalContext = join.evalContext
    )

    if (useSortMerge) {
      val operator = if (join.direction == Forward) LessThanOrEqualOp else GreaterThanOrEqualOp
      val (normalizedLeft, normalizedRight, normalizedOperator) = AsOfJoin.normalizeMatchOperands(
        leftSet = leftSet,
        rightSet = rightSet,
        expr1 = leftOperand,
        operator = operator,
        expr2 = rightOperand,
        isDataFrame = true
      )
      val (coercedLeft, coercedRight) =
        AsOfJoin.coerceMatchStringOperands(normalizedLeft, normalizedRight)
      val (leftSort, rightSort) = if (join.direction == Nearest) {
        val (leftKey, rightKey) = coerceNearestKeys(coercedLeft, coercedRight)
        AsOfJoinValidation.validateNearestKeys(
          join = join,
          leftKey = leftKey,
          rightKey = rightKey
        )
        (Seq(leftKey), Seq(rightKey))
      } else {
        AsOfJoin.matchSortExpressions(coercedLeft, coercedRight)
      }
      Some(MaterializedMatchCondition(
        asOfCondition = asOfCondition,
        direction = if (join.direction == Nearest) Nearest else normalizedOperator.direction,
        leftSortExpressions = leftSort,
        rightSortExpressions = rightSort
      ))
    } else {
      Some(MaterializedMatchCondition(
        asOfCondition = asOfCondition,
        direction = join.direction,
        leftSortExpressions = Nil,
        rightSortExpressions = Nil,
        orderExpression = Some(AsOfJoin.makeOrderingExpr(
          leftAsOf = leftOperand,
          rightAsOf = rightOperand,
          direction = join.direction,
          evalContext = join.evalContext
        ))
      ))
    }
  }

  private def coerceNearestKeys(
      leftKey: Expression,
      rightKey: Expression): (Expression, Expression) = {
    val leftType = leftKey.dataType
    val rightType = rightKey.dataType
    if (leftType.isInstanceOf[DecimalType] && rightType.isInstanceOf[IntegralType]) {
      (leftKey, Cast(rightKey, DecimalType.forType(rightType)))
    } else if (leftType.isInstanceOf[IntegralType] && rightType.isInstanceOf[DecimalType]) {
      (Cast(leftKey, DecimalType.forType(leftType)), rightKey)
    } else if (AsOfJoin.supportsNearestKeys(leftType, rightType)) {
      // Decimal keys retain their individual scales for exact gap comparisons.
      (leftKey, rightKey)
    } else {
      val commonType = if (leftType == NullType && rightType == NullType) {
        Some(DoubleType)
      } else {
        val coercion = if (SQLConf.get.ansiEnabled) AnsiTypeCoercion else TypeCoercion
        coercion.findWiderTypeForTwo(leftType, rightType)
      }
      commonType.filter(t => AsOfJoin.supportsNearestKeys(t, t)) match {
        case Some(dataType) => (Cast(leftKey, dataType), Cast(rightKey, dataType))
        case None => (leftKey, rightKey)
      }
    }
  }
}
