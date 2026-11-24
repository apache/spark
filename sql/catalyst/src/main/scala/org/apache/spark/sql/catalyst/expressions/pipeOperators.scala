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

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{PIPE_EXPRESSION, PIPE_OPERATOR, TreePattern}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.DataType

/**
 * Represents an expression when used with a SQL pipe operator.
 * We use this to check invariants about whether aggregate functions may exist in these expressions.
 * @param child The child expression.
 * @param isAggregate Whether the pipe operator is |> AGGREGATE.
 *                    If true, the child expression must contain at least one aggregate function.
 *                    If false, the child expression must not contain any aggregate functions.
 * @param clause The clause of the pipe operator. This is used to generate error messages.
 */
case class PipeExpression(child: Expression, isAggregate: Boolean, clause: String)
  extends UnaryExpression with Unevaluable {
  final override val nodePatterns = Seq(PIPE_EXPRESSION)
  final override lazy val resolved = false
  override def withNewChildInternal(newChild: Expression): Expression =
    PipeExpression(newChild, isAggregate, clause)
  override def dataType: DataType = child.dataType
}

/**
 * Represents the location within a logical plan that a SQL pipe operator appeared.
 * This acts as a logical boundary that works to prevent the analyzer from modifying the logical
 * operators above and below the boundary.
 */
case class PipeOperator(child: LogicalPlan) extends UnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(PIPE_OPERATOR)
  override def output: Seq[Attribute] = child.output
  override def withNewChildInternal(newChild: LogicalPlan): PipeOperator = copy(child = newChild)
}

/** This rule removes all PipeOperator nodes from a logical plan at the end of analysis. */
object EliminatePipeOperators extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(PIPE_OPERATOR), ruleId) {
    case PipeOperator(child) => child
  }
}

/**
 * Validates and strips PipeExpression nodes from a logical plan once their children are ready:
 * either resolved, or a generator whose arguments are resolved.
 *
 * A generator, optionally wrapped in GeneratorOuter (for the *_outer functions), is never resolved
 * while it is wrapped, and may only become resolved after this rule runs, when type coercion fixes
 * its arguments (for example, stack(2, id, null)). So the PipeExpression is stripped as soon as
 * the generator's arguments are resolved, keeping GeneratorOuter in place for ExtractGenerator to
 * plan; otherwise ExtractGenerator sees the PipeExpression above the generator and rejects it as
 * nested inside another expression. A parenthesized alias list stays above the PipeExpression
 * (see AstBuilder), so ExtractGenerator never trims its names.
 */
case object ValidateAndStripPipeExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(PIPE_EXPRESSION), ruleId) {
    case node: LogicalPlan =>
      node.resolveExpressions {
        case p: PipeExpression if p.child.resolved || isReadyGenerator(p.child) =>
          validateAndStripPipeExpression(p, p.child)
      }
  }

  private def isReadyGenerator(e: Expression): Boolean = e match {
    case GeneratorItem(g, None, _) => g.childrenResolved
    case _ => false
  }

  /**
   * Validates aggregate function constraints for a [[PipeExpression]] and returns its child
   * expression (stripping the [[PipeExpression]] wrapper).
   *
   * @param pipeExpression The [[PipeExpression]] containing metadata about the pipe clause.
   * @param child The child expression to validate and return. It is either resolved, or a
   *              generator whose arguments are resolved but which is not resolved itself,
   *              optionally wrapped in [[GeneratorOuter]]. The aggregate checks run before such
   *              a generator's own type check.
   * @return The child expression after validation.
   */
  def validateAndStripPipeExpression(
      pipeExpression: PipeExpression,
      child: Expression): Expression = {
    val firstAggregateFunction: Option[AggregateFunction] = findFirstAggregate(child)
    if (pipeExpression.isAggregate && firstAggregateFunction.isEmpty) {
      throw QueryCompilationErrors
          .pipeOperatorAggregateExpressionContainsNoAggregateFunction(child)
    }
    if (!pipeExpression.isAggregate) {
      // For non-aggregate clauses, only allow aggregate functions in SELECT.
      // All other clauses (EXTEND, SET, etc.) disallow aggregates.
      val aggregateAllowed = pipeExpression.clause == PipeOperators.selectClause
      if (!aggregateAllowed) {
        firstAggregateFunction.foreach { a =>
          throw QueryCompilationErrors.pipeOperatorContainsAggregateFunction(
            a,
            pipeExpression.clause
          )
        }
      }
    }
    child
  }

  /** Returns the first aggregate function in the given expression, or None if not found. */
  private def findFirstAggregate(e: Expression): Option[AggregateFunction] = e match {
    case a: AggregateFunction =>
      Some(a)
    case _: WindowExpression =>
      // Window functions are allowed in these pipe operators, so do not traverse into children.
      None
    case _ =>
      e.children.flatMap(findFirstAggregate).headOption
  }
}

object PipeOperators {
  // These are definitions of query result clauses that can be used with the pipe operator.
  val aggregateClause = "AGGREGATE"
  val clusterByClause = "CLUSTER BY"
  val distributeByClause = "DISTRIBUTE BY"
  val evalClause = "EVAL"
  val extendClause = "EXTEND"
  val limitClause = "LIMIT"
  val offsetClause = "OFFSET"
  val orderByClause = "ORDER BY"
  val selectClause = "SELECT"
  val setClause = "SET"
  val sortByClause = "SORT BY"
  val sortByDistributeByClause = "SORT BY ... DISTRIBUTE BY ..."
  val windowClause = "WINDOW"
}
