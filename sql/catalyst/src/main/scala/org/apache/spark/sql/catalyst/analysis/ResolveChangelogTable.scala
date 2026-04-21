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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.IntegerType

/**
 * Post-processes a resolved [[DataSourceV2Relation]] backed by a [[ChangelogTable]] to inject
 * carry-over removal and/or update detection plans.
 *
 * Fires after [[ResolveRelations]] has wrapped the connector's [[Changelog]] in a
 * [[DataSourceV2Relation]]. Skipped entirely when [[Changelog.rowId]] is empty (no row identity
 * available; raw delete/insert rows are returned as-is).
 *
 * Carry-over removal and update detection are fused into a single pass over a
 * (rowId, _commit_version)-partitioned Window: the Filter drops CoW carry-over pairs
 * (same rowVersion on both sides) and the subsequent Project relabels real delete+insert
 * pairs as update_preimage / update_postimage. Net change computation runs afterwards
 * as a separate rule step.
 */
object ResolveChangelogTable extends Rule[LogicalPlan] {

  private val CHANGELOG_TRANSFORMED_TAG =
    TreeNodeTag[Boolean]("changelog_transformed")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (isAlreadyTransformed(plan)) return plan
    var updatedPlan = plan
    updatedPlan = plan.resolveOperatorsUp {
      // Guard: without row identity, carry-over removal and update detection cannot
      // correctly group rows. A match-miss leaves the relation unchanged -- exactly what
      // we want for connectors that surface an empty rowId().
      case rel @ DataSourceV2Relation(table: ChangelogTable, _, _, _, _, _)
          if table.changelog.rowId().nonEmpty =>
        val changelog = table.changelog
        val options = table.changelogInfo

        val requiresCarryOverRemoval =
          options.deduplicationMode() != ChangelogInfo.DeduplicationMode.NONE &&
            changelog.containsCarryoverRows()
        val requiresUpdateDetection =
          options.computeUpdates() && changelog.representsUpdateAsDeleteAndInsert()
        val requiresNetChanges =
          options.deduplicationMode() == ChangelogInfo.DeduplicationMode.NET_CHANGES &&
            changelog.containsIntermediateChanges()

        var updatedRel: LogicalPlan = rel
        if (requiresCarryOverRemoval || requiresUpdateDetection) {
          updatedRel = addRowLevelPostProcessing(
            rel, changelog, requiresCarryOverRemoval, requiresUpdateDetection)
        }
        if (requiresNetChanges) {
          updatedRel = injectNetChangeComputation(updatedRel, changelog)
        }
        updatedRel
    }
    if (updatedPlan ne plan) {
      updatedPlan.setTagValue(CHANGELOG_TRANSFORMED_TAG, true)
    }
    updatedPlan
  }

  // ---------------------------------------------------------------------------
  // Row Level Post Processing (Update Detection & Carry-over Removal)
  // ---------------------------------------------------------------------------

  /**
   * Adds row-level post-processing (carry-over removal and/or update detection) on top of
   * the given plan:
   *   - both active    -> Window(counts + rv bounds) -> Filter -> Project(relabel) -> Drop helpers
   *   - carry-over only -> Window(counts + rv bounds) -> Filter -> Drop helpers
   *   - update only    -> Window(counts only) -> Project(relabel) -> Drop helpers
   *   - neither        -> plan is returned unchanged (caller guards this case)
   */
  private def addRowLevelPostProcessing(
      plan: LogicalPlan,
      cl: Changelog,
      requiresCarryOverRemoval: Boolean,
      requiresUpdateDetection: Boolean): LogicalPlan = {
    if (requiresCarryOverRemoval && requiresUpdateDetection) {
      val planWithWindow = addWindowWithInsertAndDeleteCountsAndRowVersionBounds(plan, cl)
      val planWithFilter = addCarryOverPairFilter(planWithWindow)
      val planWithProjection = addUpdateRelabelProjection(planWithFilter)
      removeHelperColumns(planWithProjection)
    } else if (requiresCarryOverRemoval) {
      val planWithWindow = addWindowWithInsertAndDeleteCountsAndRowVersionBounds(plan, cl)
      val planWithFilter = addCarryOverPairFilter(planWithWindow)
      removeHelperColumns(planWithFilter)
    } else if (requiresUpdateDetection) {
      val planWithWindow = addWindowWithInsertAndDeleteCountsOnly(plan, cl)
      val planWithProjection = addUpdateRelabelProjection(planWithWindow)
      removeHelperColumns(planWithProjection)
    } else {
      plan
    }
  }

  /**
   * Adds a Window node partitioned by (rowId, _commit_version) that computes
   * `_del_cnt` (number of `delete` rows in the partition) and `_ins_cnt` (number of
   * `insert` rows in the partition). These counts drive update detection: a partition
   * with `_del_cnt >= 1 AND _ins_cnt >= 1` is relabeled as update_preimage /
   * update_postimage.
   */
  private def addWindowWithInsertAndDeleteCountsOnly(
      plan: LogicalPlan, cl: Changelog): LogicalPlan = {
    val changeTypeAttr = getAttribute(plan, "_change_type")
    val rowIdExprs = V2ExpressionUtils.resolveRefs[NamedExpression](cl.rowId().toSeq, plan)
    val commitVersionAttr = getAttribute(plan, "_commit_version")
    val partitionByCols = rowIdExprs ++ Seq(commitVersionAttr)
    val windowSpec = WindowSpecDefinition(partitionByCols, Nil, UnspecifiedFrame)

    val insertIf = If(EqualTo(changeTypeAttr, Literal("insert")),
      Literal(1), Literal(null, IntegerType))
    val deleteIf = If(EqualTo(changeTypeAttr, Literal("delete")),
      Literal(1), Literal(null, IntegerType))

    val insCntAlias = Alias(WindowExpression(
      Count(Seq(insertIf)).toAggregateExpression(), windowSpec), "_ins_cnt")()
    val delCntAlias = Alias(WindowExpression(
      Count(Seq(deleteIf)).toAggregateExpression(), windowSpec), "_del_cnt")()

    Window(Seq(delCntAlias, insCntAlias), partitionByCols, Nil, plan)
  }

  /**
   * Adds a Window node partitioned by (rowId, _commit_version) that computes
   * `_del_cnt` (number of `delete` rows in the partition), `_ins_cnt` (number of `insert`
   * rows in the partition), `_min_rv` (minimum of the connector's `rowVersion()`
   * expression in the partition) and `_max_rv` (maximum of `rowVersion()` in the
   * partition). Used whenever carry-over detection is needed: within a delete+insert
   * pair, `_min_rv == _max_rv` signals that both sides share the same rowVersion and
   * the pair is a CoW carry-over.
   */
  private def addWindowWithInsertAndDeleteCountsAndRowVersionBounds(
      plan: LogicalPlan, cl: Changelog): LogicalPlan = {
    val changeTypeAttr = getAttribute(plan, "_change_type")
    val rowIdExprs = V2ExpressionUtils.resolveRefs[NamedExpression](cl.rowId().toSeq, plan)
    val commitVersionAttr = getAttribute(plan, "_commit_version")
    val rowVersionExpr = V2ExpressionUtils.resolveRef[NamedExpression](cl.rowVersion(), plan)
    val partitionByCols = rowIdExprs ++ Seq(commitVersionAttr)
    val windowSpec = WindowSpecDefinition(partitionByCols, Nil, UnspecifiedFrame)

    val insertIf = If(EqualTo(changeTypeAttr, Literal("insert")),
      Literal(1), Literal(null, IntegerType))
    val deleteIf = If(EqualTo(changeTypeAttr, Literal("delete")),
      Literal(1), Literal(null, IntegerType))

    val insCntAlias = Alias(WindowExpression(
      Count(Seq(insertIf)).toAggregateExpression(), windowSpec), "_ins_cnt")()
    val delCntAlias = Alias(WindowExpression(
      Count(Seq(deleteIf)).toAggregateExpression(), windowSpec), "_del_cnt")()
    val minRvAlias = Alias(WindowExpression(
      Min(rowVersionExpr).toAggregateExpression(), windowSpec), "_min_rv")()
    val maxRvAlias = Alias(WindowExpression(
      Max(rowVersionExpr).toAggregateExpression(), windowSpec), "_max_rv")()

    Window(Seq(delCntAlias, insCntAlias, minRvAlias, maxRvAlias),
      partitionByCols, Nil, plan)
  }

  /**
   * Adds a Filter node that drops rows belonging to a CoW carry-over pair.
   * Expects the input to expose `_del_cnt`, `_ins_cnt`, `_min_rv`, `_max_rv`.
   * A pair is a carry-over iff `_del_cnt = 1 AND _ins_cnt = 1 AND _min_rv = _max_rv`.
   */
  private def addCarryOverPairFilter(input: LogicalPlan): LogicalPlan = {
    val delCnt = getAttribute(input, "_del_cnt")
    val insCnt = getAttribute(input, "_ins_cnt")
    val minRv = getAttribute(input, "_min_rv")
    val maxRv = getAttribute(input, "_max_rv")

    val isCarryoverPair = And(
      And(EqualTo(delCnt, Literal(1L)), EqualTo(insCnt, Literal(1L))),
      EqualTo(minRv, maxRv))
    Filter(Not(isCarryoverPair), input)
  }

  /**
   * Adds a Project node that rewrites `_change_type` to `update_preimage` /
   * `update_postimage` whenever a delete+insert pair is present in the partition.
   * Expects the input to expose `_del_cnt` and `_ins_cnt`.
   */
  private def addUpdateRelabelProjection(input: LogicalPlan): LogicalPlan = {
    val changeTypeAttr = getAttribute(input, "_change_type")
    val delCnt = getAttribute(input, "_del_cnt")
    val insCnt = getAttribute(input, "_ins_cnt")

    val isUpdate = And(
      GreaterThanOrEqual(delCnt, Literal(1L)),
      GreaterThanOrEqual(insCnt, Literal(1L)))
    val updateType = If(EqualTo(changeTypeAttr, Literal("insert")),
      Literal("update_postimage"), Literal("update_preimage"))
    val caseExpr = CaseWhen(Seq((isUpdate, updateType)), changeTypeAttr)

    val projectList = input.output.map { attr =>
      if (attr.name == "_change_type") Alias(caseExpr, "_change_type")()
      else attr
    }
    Project(projectList, input)
  }

  // ---------------------------------------------------------------------------
  // Net Change Computation (future)
  // ---------------------------------------------------------------------------

  /**
   * Collapses multiple changes per row identity into the net effect. Not in
   * scope for this implementation.
   */
  private def injectNetChangeComputation(
      plan: LogicalPlan,
      cl: Changelog): LogicalPlan = {
    throw new UnsupportedOperationException(
      "Net change computation is not yet supported")
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns true if this plan has already been transformed by this rule.
   * Uses a plan-level tag to prevent re-processing on subsequent rule executor iterations.
   */
  private def isAlreadyTransformed(plan: LogicalPlan): Boolean = {
    plan.getTagValue(CHANGELOG_TRANSFORMED_TAG).getOrElse(false)
  }

  /**
   * Removes any helper columns (`_del_cnt`, `_ins_cnt`, `_min_rv`, `_max_rv`) that
   * earlier steps added to the plan. Helper columns not present in the input are
   * silently ignored, so the same method works for all three branches in
   * [[addRowLevelPostProcessing]].
   */
  private def removeHelperColumns(input: LogicalPlan): LogicalPlan = {
    val helperNames = Set("_del_cnt", "_ins_cnt", "_min_rv", "_max_rv")
    Project(input.output.filterNot(a => helperNames.contains(a.name)), input)
  }

  /**
   * Looks up an attribute by name in a plan's output. Throws a clear error if missing --
   * used for required columns like `_change_type` / `_commit_version` / helper columns
   * added by earlier steps; a missing column is always a programming error.
   */
  private def getAttribute(plan: LogicalPlan, name: String): Attribute =
    plan.output.find(_.name == name).getOrElse(
      throw new IllegalStateException(
        s"Required column '$name' not found in plan output: " +
          plan.output.map(_.name).mkString(", ")))
}
