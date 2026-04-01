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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

/**
 * Evaluates the boolean [[condition]] and marks a point in the plan where a metric should be
 * incremented when the condition is true. Returns the condition's value unchanged.
 *
 * This is the unresolved form - resolved into IncrementMetricIf by a preparation rule.
 *
 * Marked as Nondeterministic to prevent the optimizer from pruning or reordering it.
 * Cannot mix in [[Unevaluable]] because both [[Unevaluable]] and [[Nondeterministic]] declare
 * [[foldable]] as final.
 *
 * @param condition the boolean expression to evaluate.
 * @param metricName the name of the metric to increment.
 */
case class UnresolvedIncrementMetricIf(
    condition: Expression,
    metricName: String)
  extends UnaryExpression with Nondeterministic {

  override def child: Expression = condition

  override def nullable: Boolean = condition.nullable

  override def dataType: DataType = condition.dataType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override def prettyName: String = "unresolved_increment_metric_if"

  override def toString: String = s"unresolved_increment_metric_if($condition, $metricName)"

  override protected def evalInternal(input: InternalRow): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override protected def withNewChildInternal(
      newChild: Expression): UnresolvedIncrementMetricIf =
    copy(condition = newChild)
}

/**
 * Evaluates the boolean [[condition]], marks a point in the plan where a metric should
 * be incremented when the condition is true, then evaluates and returns [[returnExpr]].
 *
 * This is the unresolved form - resolved into IncrementMetricIfThenReturn by a preparation rule.
 *
 * Marked as Nondeterministic to prevent the optimizer from pruning or reordering it.
 * Cannot mix in [[Unevaluable]] because both [[Unevaluable]] and [[Nondeterministic]] declare
 * [[foldable]] as final.
 *
 * @param condition the boolean expression to evaluate.
 * @param returnExpr the expression whose value is returned.
 * @param metricName the name of the metric to increment.
 */
case class UnresolvedIncrementMetricIfThenReturn(
    condition: Expression,
    returnExpr: Expression,
    metricName: String)
  extends Expression with BinaryLike[Expression] with Nondeterministic {

  override def left: Expression = condition

  override def right: Expression = returnExpr

  override def nullable: Boolean = returnExpr.nullable

  override def dataType: DataType = returnExpr.dataType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override def prettyName: String = "unresolved_increment_metric_if_then_return"

  override def toString: String =
    s"unresolved_increment_metric_if_then_return($condition, $returnExpr, $metricName)"

  override protected def evalInternal(input: InternalRow): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): UnresolvedIncrementMetricIfThenReturn =
    copy(condition = newLeft, returnExpr = newRight)
}
