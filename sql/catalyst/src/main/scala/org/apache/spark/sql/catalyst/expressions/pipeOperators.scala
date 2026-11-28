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

import org.apache.spark.sql.catalyst.analysis.UnresolvedExecuteImmediate
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.{
  Command,
  CTEInChildren,
  ExecutableDuringAnalysis,
  LogicalPlan,
  Project,
  UnaryNode
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{
  PIPE_EXPRESSION,
  PIPE_OPERATOR,
  PLAN_EXPRESSION,
  PROJECT,
  TreePattern
}
import org.apache.spark.sql.catalyst.util._
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

/**
 * Preserves the qualified input row of a SQL pipe SET assignment as metadata output.
 *
 * The SET projection replaces assigned columns in its visible output. Keeping the original row as
 * qualified-access-only metadata lets a later `t.a` or `t.*` continue to refer to the input row
 * without changing the SET output schema or unqualified star expansion.
 */
case class PipeSetInput(child: LogicalPlan) extends UnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(PIPE_OPERATOR)

  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  // A node and its children are immutable, so the retained row is derived once per instance
  // rather than on every lookup, which would re-derive every earlier assignment's row.
  override lazy val metadataOutput: Seq[Attribute] = {
    val childMetadataOutput = child.metadataOutput
    val retainedQualifiedOutput = AttributeSeq
      .mergeHiddenAndVisibleOutput(
        childMetadataOutput.filter(_.qualifiedAccessOnly), child.output)
      .filter(_.qualifier.nonEmpty)
      .map(_.markAsQualifiedAccessOnly().markAsPipeSetRetained())
    val retainedQualifiedOutputIds = retainedQualifiedOutput.iterator.map(_.exprId).toSet
    retainedQualifiedOutput ++ childMetadataOutput.filterNot { attribute =>
      attribute.qualifiedAccessOnly || retainedQualifiedOutputIds.contains(attribute.exprId)
    }
  }

  override def withNewChildInternal(newChild: LogicalPlan): PipeSetInput = copy(child = newChild)
}

/**
 * Hides the columns that a SQL pipe SET retained (see [[PipeSetInput]]) from the operators above
 * a parsed SQL query that is analyzed together with them, as Spark Connect does when a client
 * composes a DataFrame operation on a SQL relation. References inside the query resolve below
 * this node and still see the retained row; operators above it see only the query's output and
 * the metadata output that the query has once analysis removed the retained rows, as they would
 * after classic `spark.sql` analyzed the query eagerly. A hidden USING join key, for example,
 * stays reachable even though a pipe SET above the join retained it as part of its input row.
 */
case class PipeSetBoundary(child: LogicalPlan) extends UnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(PIPE_OPERATOR)

  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  // A node and its children are immutable, so this is derived once per instance.
  override lazy val metadataOutput: Seq[Attribute] =
    EliminateResolvedPipeSetInputs.withoutRetainedRows(child).metadataOutput

  override def withNewChildInternal(newChild: LogicalPlan): PipeSetBoundary =
    copy(child = newChild)
}

object PipeSetBoundary {

  /**
   * Wraps a parsed SQL query that uses pipe SET in a [[PipeSetBoundary]]. Plans that contain a
   * command or a statement are left unchanged, because they are executed rather than composed.
   */
  def wrapQuery(plan: LogicalPlan): LogicalPlan = {
    val usesPipeSet = plan.containsPattern(PIPE_OPERATOR) && plan.exists {
      case _: PipeSetInput => true
      case _ => false
    }
    val isQuery = !plan.exists {
      case _: Command | _: CTEInChildren | _: ExecutableDuringAnalysis |
          _: UnresolvedExecuteImmediate => true
      case _ => false
    }
    if (usesPipeSet && isQuery) PipeSetBoundary(plan) else plan
  }
}

/**
 * Removes resolved [[PipeSetInput]] and [[PipeSetBoundary]] nodes before analyzed plans cross a
 * Dataset boundary.
 *
 * Qualified source attributes referenced later in the same pipe query have already been
 * materialized by this point. Removing the marker and any retained attributes copied to hidden
 * output prevents them from being resolved by a subsequent Dataset operation.
 *
 * The rule skips already-analyzed sub-plans, which their own analysis cleaned, and enters
 * expressions only where a subquery occurs. It also cleans a [[Project]] whose marker an
 * extension rule already removed. A cleaned [[Project]] is rebuilt rather than updated in place,
 * so that name lookups cached on the old instance are not reused.
 */
object EliminateResolvedPipeSetInputs extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = cleanup(plan, onlyResolved = true)

  /**
   * Returns `plan` without the retained rows of its pipe SET operators, resolved or not. This is
   * the operator tree whose metadata output the cleaned plan exposes after analysis.
   */
  private[sql] def withoutRetainedRows(plan: LogicalPlan): LogicalPlan =
    cleanup(plan, onlyResolved = false)

  /**
   * Cleans `plan` bottom-up. A cleaned [[Project]] is structurally equal to the original one, so
   * the traversal keeps replacements by reference rather than by equality, as `transform` would.
   */
  private def cleanup(plan: LogicalPlan, onlyResolved: Boolean): LogicalPlan = {
    if (plan.analyzed || !plan.containsAnyPattern(PIPE_OPERATOR, PLAN_EXPRESSION, PROJECT)) {
      return plan
    }
    val withCleanedChildren = plan.mapChildrenWithReferenceEquality(cleanup(_, onlyResolved))
    val withCleanedSubqueries = if (withCleanedChildren.containsPattern(PLAN_EXPRESSION)) {
      withCleanedChildren.mapExpressionsWithReferenceEquality(
        cleanupSubqueries(_, onlyResolved))
    } else {
      withCleanedChildren
    }
    withCleanedSubqueries match {
      case marker @ (_: PipeSetInput | _: PipeSetBoundary) if !onlyResolved || marker.resolved =>
        val child = marker.children.head
        if (onlyResolved) {
          child.copyTagsFrom(marker)
        }
        child
      case project: Project => withoutRetainedHiddenOutput(project)
      case other => other
    }
  }

  private def cleanupSubqueries(expression: Expression, onlyResolved: Boolean): Expression = {
    if (!expression.containsPattern(PLAN_EXPRESSION)) {
      expression
    } else {
      expression.transformDownWithReferenceEquality {
        case subquery: SubqueryExpression =>
          val newPlan = cleanup(subquery.plan, onlyResolved)
          if (newPlan eq subquery.plan) subquery else subquery.withNewPlan(newPlan)
      }
    }
  }

  /**
   * Removes retained attributes from a [[Project]]'s hidden output. The [[Project]] is rebuilt
   * rather than updated in place, so that name lookups cached on the old instance are not reused.
   */
  private def withoutRetainedHiddenOutput(project: Project): Project =
    project.getTagValue(Project.hiddenOutputTag) match {
      case Some(hiddenOutput) if hiddenOutput.exists(_.pipeSetRetained) =>
        val cleanedProject = project.copy()
        cleanedProject.copyTagsFrom(project)
        cleanedProject.setTagValue(
          Project.hiddenOutputTag, hiddenOutput.filterNot(_.pipeSetRetained))
        cleanedProject
      case _ => project
    }
}

/** This rule removes transparent pipe-operator nodes from a logical plan after analysis. */
object EliminatePipeOperators extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(PIPE_OPERATOR), ruleId) {
    case PipeOperator(child) => child
    case PipeSetInput(child) => child
    case PipeSetBoundary(child) => child
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
