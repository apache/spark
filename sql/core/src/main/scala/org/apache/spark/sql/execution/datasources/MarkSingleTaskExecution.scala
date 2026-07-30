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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.internal.SQLConf

/**
 * This optimizer rule marks eligible query plans for single-task execution. The optimization
 * targets a conservative, specific query shape to ensure predictable and efficient behavior.
 *
 * The rule matches simple query plans with a single small file scan or a single small in-memory
 * relation, optionally with a shuffle-inducing operator (sort, aggregation, window, expand, or
 * limit/offset) on top. When it detects such a shape, it marks the underlying scan:
 *
 *  - a [[LogicalRelation]] or [[LocalRelation]] is marked with the
 *    [[MarkSingleTaskExecution.markTag]] tag, as is any [[Expand]] in the plan so that the
 *    physical Expand can forward the child's `SinglePartition` output partitioning.
 *
 * The physical scan then reports a `SinglePartition` output partitioning, which allows
 * [[org.apache.spark.sql.execution.exchange.EnsureRequirements]] to elide the shuffle that would
 * otherwise be inserted before the operator on top. This shuffle is not required for correctness
 * of the query, so removing it reduces scheduling overhead for small, low-latency queries.
 *
 * The matching is deliberately strict and conservative to minimize the risk of unintended
 * performance regressions. It can be broadened in the future as needed.
 *
 * This rule is controlled by [[SQLConf.SINGLE_TASK_EXECUTION_ENABLED]] and the per-operator
 * sub-flags in [[SQLConf]].
 */
object MarkSingleTaskExecution extends Rule[LogicalPlan] {

  /**
   * Tag placed on a [[LogicalRelation]] or [[LocalRelation]] that has been marked eligible for
   * single-task execution, and on any [[Expand]] in such a plan. The planning strategies read
   * this tag to propagate the decision to the physical
   * [[org.apache.spark.sql.execution.FileSourceScanExec]] /
   * [[org.apache.spark.sql.execution.LocalTableScanExec]] /
   * [[org.apache.spark.sql.execution.ExpandExec]].
   */
  val markTag: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("__single_task_execution")

  /**
   * Plan patterns that make a query ineligible for the optimization. These operators either
   * require shuffles that we cannot safely elide, or run user code whose behavior we should not
   * change. User-defined aggregations are excluded defensively: an optimization that collapses
   * the partial and final aggregates when no exchange separates them would skip the user's merge
   * step, so single-task plans must never be assumed safe for them.
   */
  val unsupportedPatterns: Seq[TreePattern] = Seq(
    EVAL_PYTHON_UDF,
    EVAL_PYTHON_UDTF,
    EXISTS_SUBQUERY,
    FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION,
    LATERAL_SUBQUERY,
    LIST_SUBQUERY,
    PYTHON_UDF,
    SCALAR_SUBQUERY,
    USER_DEFINED_AGGREGATION)

  /**
   * The per-operator sub-flags, resolved once per invocation. Each field indicates whether the
   * corresponding shuffle-inducing operator is allowed on top of a single small scan.
   */
  private case class EnabledOperators(
      aggregation: Boolean,
      expand: Boolean,
      limitOffset: Boolean,
      sort: Boolean,
      window: Boolean)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // An explicit leaf-node parallelism override expresses the user's intent about how many
    // partitions leaf scans should produce, so do not force scans into a single partition.
    if (!conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_ENABLED) ||
        conf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM).isDefined) {
      return plan
    }
    val enabled = EnabledOperators(
      aggregation = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_AGGREGATION),
      expand = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_EXPAND),
      limitOffset = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_LIMIT_OFFSET),
      sort = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_SORT),
      window = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_WINDOW))

    if (plan.containsAnyPattern(unsupportedPatterns: _*)) {
      plan
    } else if (isSupportedShape(plan, enabled)) {
      // Mark a private clone of the plan rather than the plan itself, so that this rule never
      // mutates a node it was handed: a tag set on a shared node would propagate through
      // `TreeNode.clone`/`copyTagsFrom` and could leak the marking into unrelated plans. Note
      // that marking a per-node copy would not work either: a copy differing only in tags is
      // structurally equal to the original, so tree-rebuilding APIs such as `withNewChildren`
      // would discard it and keep the original node.
      val cloned = plan.clone()
      markSingleTaskExecution(cloned)
      cloned
    } else {
      plan
    }
  }

  /**
   * Returns true if every operator in the plan is one that we support keeping on top of a single
   * small scan. Only operators that either do not require a shuffle, or whose shuffle-inducing
   * sub-flag is enabled, are allowed. Any other operator makes the plan ineligible.
   */
  private def isSupportedShape(plan: LogicalPlan, enabled: EnabledOperators): Boolean = plan match {
    case _: LogicalRelation | _: LocalRelation => true
    // Operators that never introduce a shuffle by themselves. Note that `Distinct` and
    // `SubqueryAlias` need no cases here: they are rewritten away by non-excludable rules
    // (`ReplaceDistinctWithAggregate` and `EliminateSubqueryAliases`) long before this rule runs.
    case _: Project | _: Filter |
         _: DeserializeToObject | _: SerializeFromObject =>
      plan.children.forall(isSupportedShape(_, enabled))
    // Shuffle-inducing operators, allowed only when the matching sub-flag is enabled.
    case _: Aggregate if enabled.aggregation =>
      plan.children.forall(isSupportedShape(_, enabled))
    case _: Expand if enabled.expand =>
      plan.children.forall(isSupportedShape(_, enabled))
    case (_: GlobalLimit | _: LocalLimit | _: Offset) if enabled.limitOffset =>
      plan.children.forall(isSupportedShape(_, enabled))
    case _: Sort if enabled.sort =>
      plan.children.forall(isSupportedShape(_, enabled))
    case _: Window if enabled.window =>
      plan.children.forall(isSupportedShape(_, enabled))
    case _ => false
  }

  /**
   * Sets the mark on each scan in the given (already validated) plan. Marking mutates the nodes
   * in place, which is only safe because the caller passes this rule's private clone of the plan.
   */
  private def markSingleTaskExecution(plan: LogicalPlan): Unit = plan match {
    case lr: LogicalRelation =>
      lr.setTagValue(markTag, true)
    case r: LocalRelation =>
      if (isLocalRelationEligible(r)) {
        r.setTagValue(markTag, true)
      }
    case e: Expand =>
      // Also mark the Expand itself: the physical `ExpandExec` reads this tag to forward the
      // child's `SinglePartition` output partitioning, which it must only do within a plan
      // marked for single-task execution.
      e.setTagValue(markTag, true)
      e.children.foreach(markSingleTaskExecution)
    case other =>
      other.children.foreach(markSingleTaskExecution)
  }

  /**
   * A local in-memory relation is eligible when its row count falls within the configured bounds.
   */
  private def isLocalRelationEligible(r: LocalRelation): Boolean = {
    val minRows = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_LOCAL_TABLE_SCAN_MIN_ROWS)
    val threshold = conf.getConf(SQLConf.SINGLE_TASK_EXECUTION_LOCAL_TABLE_SCAN_THRESHOLD)
    r.data.length >= minRows && r.data.length <= threshold
  }
}
