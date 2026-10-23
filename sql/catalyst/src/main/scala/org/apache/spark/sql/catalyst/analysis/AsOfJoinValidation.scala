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
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, SubqueryExpression, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.Nearest
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin.MatchConditionTypes
import org.apache.spark.sql.catalyst.trees.TreePattern.GENERATOR
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.types.StringType

/** Shared validation for SQL and DataFrame ASOF joins. */
private[sql] object AsOfJoinValidation extends QueryErrorsBase with SQLConfHelper {

  /** Rejects a MATCH_CONDITION operand that references both inputs. */
  def validateMatchConditionTableReferences(
      join: AsOfJoin,
      leftSet: AttributeSet,
      rightSet: AttributeSet,
      leftExpr: Expression,
      rightExpr: Expression): Unit = {
    def referencesBothJoinSides(refs: AttributeSet): Boolean = {
      refs.nonEmpty &&
        refs.intersect(leftSet).nonEmpty &&
        refs.intersect(rightSet).nonEmpty
    }

    val leftRefs = leftExpr.references
    val rightRefs = rightExpr.references
    if (referencesBothJoinSides(leftRefs) || referencesBothJoinSides(rightRefs)) {
      join.failAnalysis(
        errorClass = "ASOF_JOIN_MATCH_CONDITION_TABLE_REFERENCE",
        messageParameters = Map(
          "refs1" -> toSQLExpr(leftExpr),
          "refs2" -> toSQLExpr(rightExpr)))
    }
  }

  /** Validates ASOF key expressions, types and input references. */
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
    if (!MatchConditionTypes.areOperandsCompatible(leftExpr.dataType, rightExpr.dataType)) {
      join.failAnalysis(
        errorClass = "ASOF_JOIN_MATCH_CONDITION_INVALID_TYPE",
        messageParameters = Map(
          "type1" -> toSQLType(leftExpr.dataType),
          "type2" -> toSQLType(rightExpr.dataType)))
    }

    if (leftExpr.references.isEmpty || rightExpr.references.isEmpty) {
      throw QueryCompilationErrors.asOfJoinMatchConditionTableReferenceError(leftExpr, rightExpr)
    }
  }

  /** Validates the type, foldability and sign of a DataFrame ASOF tolerance. */
  def validateTolerance(join: AsOfJoin): Unit = {
    join.toleranceAssertion.foreach { assertion =>
      if (assertion.checkInputDataTypes().isFailure) {
        TypeCoercionValidation.failOnTypeCheckResult(assertion, Some(join))
      }
      if (!assertion.foldable) {
        join.failAnalysis(
          errorClass = "AS_OF_JOIN.TOLERANCE_IS_UNFOLDABLE",
          messageParameters = Map.empty
        )
      }
      if (!assertion.eval().asInstanceOf[Boolean]) {
        join.failAnalysis(
          errorClass = "AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE",
          messageParameters = Map.empty
        )
      }
    }
  }

  /** Rejects nearest STRING matching for either DataFrame execution strategy. */
  def validateNearestStringKeys(
      join: AsOfJoin,
      leftKey: Expression,
      rightKey: Expression): Unit = {
    if (join.direction == Nearest &&
        leftKey.dataType.isInstanceOf[StringType] &&
        rightKey.dataType.isInstanceOf[StringType]) {
      validateNearestKeys(
        join = join,
        leftKey = leftKey,
        rightKey = rightKey
      )
    }
  }

  /** Validates the runtime types used to compare nearest gaps. */
  def validateNearestKeys(
      join: AsOfJoin,
      leftKey: Expression,
      rightKey: Expression): Unit = {
    if (join.direction == Nearest &&
        !AsOfJoin.supportsNearestKeys(leftKey.dataType, rightKey.dataType)) {
      join.failAnalysis(
        errorClass = "AS_OF_JOIN.UNSUPPORTED_NEAREST_KEY_TYPE",
        messageParameters = Map("dataType" -> toSQLType(leftKey.dataType))
      )
    }
  }

  private def findInvalidMatchConditionExpression(expr: Expression): Option[Expression] = {
    expr.collectFirst {
      case e: SubqueryExpression => e
      case e: AggregateExpression => e
      case e: WindowExpression => e
      case e if e.containsPattern(GENERATOR) => e
      case e if !e.deterministic => e
    }
  }
}
