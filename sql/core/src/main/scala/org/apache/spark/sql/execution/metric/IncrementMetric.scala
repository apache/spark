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

package org.apache.spark.sql.execution.metric

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Nondeterministic, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.DataType

/**
 * Evaluates the boolean [[condition]] and increments an SQLMetric when it is true.
 * Returns the condition's value unchanged.
 *
 * This is the resolved form of
 * [[org.apache.spark.sql.catalyst.expressions.UnresolvedIncrementMetricIf]].
 *
 * @param condition the boolean expression to evaluate.
 * @param metric the SQLMetric accumulator to conditionally increment.
 */
case class IncrementMetricIf(condition: Expression, metric: SQLMetric)
  extends UnaryExpression with Nondeterministic {

  override def child: Expression = condition

  override def nullable: Boolean = condition.nullable

  override def dataType: DataType = condition.dataType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override def prettyName: String = "increment_metric_if"

  override def toString: String =
    s"increment_metric_if($condition, ${metric.name.getOrElse("metric")})"

  override protected def evalInternal(input: InternalRow): Any = {
    val result = condition.eval(input)
    if (result != null && result.asInstanceOf[Boolean]) {
      metric.add(1L)
    }
    result
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = condition.genCode(ctx)
    val metricRef = ctx.addReferenceObj(metric.name.getOrElse("metric"), metric)
    condEval.copy(code = condEval.code + code"""
      if (!${condEval.isNull} && ${condEval.value}) {
        $metricRef.add(1L);
      }
    """)
  }

  override protected def withNewChildInternal(newChild: Expression): IncrementMetricIf =
    copy(condition = newChild)
}

/**
 * Evaluates the boolean [[condition]], increments an SQLMetric when it is true,
 * then evaluates and returns [[returnExpr]].
 *
 * This is the resolved form of
 * [[org.apache.spark.sql.catalyst.expressions.UnresolvedIncrementMetricIfThenReturn]].
 *
 * @param condition the boolean expression to evaluate.
 * @param returnExpr the expression whose value is returned.
 * @param metric the SQLMetric accumulator to conditionally increment.
 */
case class IncrementMetricIfThenReturn(
    condition: Expression,
    returnExpr: Expression,
    metric: SQLMetric)
  extends Expression with BinaryLike[Expression] with Nondeterministic {

  override def left: Expression = condition

  override def right: Expression = returnExpr

  override def nullable: Boolean = returnExpr.nullable

  override def dataType: DataType = returnExpr.dataType

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override def prettyName: String = "increment_metric_if_then_return"

  override def toString: String =
    s"increment_metric_if_then_return($condition, $returnExpr, ${metric.name.getOrElse("metric")})"

  override protected def evalInternal(input: InternalRow): Any = {
    val condResult = condition.eval(input)
    if (condResult != null && condResult.asInstanceOf[Boolean]) {
      metric.add(1L)
    }
    returnExpr.eval(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = condition.genCode(ctx)
    val returnEval = returnExpr.genCode(ctx)
    val metricRef = ctx.addReferenceObj(metric.name.getOrElse("metric"), metric)
    returnEval.copy(code = condEval.code + code"""
      if (!${condEval.isNull} && ${condEval.value}) {
        $metricRef.add(1L);
      }
    """ + returnEval.code)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): IncrementMetricIfThenReturn =
    copy(condition = newLeft, returnExpr = newRight)
}
