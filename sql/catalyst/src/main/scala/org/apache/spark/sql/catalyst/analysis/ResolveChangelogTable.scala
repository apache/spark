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
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.{IntegerType, StringType}

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

  private object HelperColumn {
    final val DelCnt = "_del_cnt"
    final val InsCnt = "_ins_cnt"
    final val MinRv = "_min_rv"
    final val MaxRv = "_max_rv"

    val all: Set[String] = Set(DelCnt, InsCnt, MinRv, MaxRv)
  }

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

        // Net change computation is not yet implemented.
        if (options.deduplicationMode() == ChangelogInfo.DeduplicationMode.NET_CHANGES) {
          throw QueryCompilationErrors.cdcNetChangesNotYetSupported(changelog.name())
        }

        val requiresCarryOverRemoval =
          options.deduplicationMode() != ChangelogInfo.DeduplicationMode.NONE &&
            changelog.containsCarryoverRows()
        val requiresUpdateDetection =
          options.computeUpdates() && changelog.representsUpdateAsDeleteAndInsert()
        val requiresNetChanges =
          options.deduplicationMode() == ChangelogInfo.DeduplicationMode.NET_CHANGES &&
            changelog.containsIntermediateChanges()

        // If carry-overs are surfaced and update detection is enabled, carry-overs will
        // falsely be classified as updates, leading to false results. Hence we throw.
        if (requiresUpdateDetection &&
            changelog.containsCarryoverRows() &&
            options.deduplicationMode() == ChangelogInfo.DeduplicationMode.NONE) {
          throw QueryCompilationErrors.cdcUpdateDetectionRequiresCarryOverRemoval(
            changelog.name())
        }

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
   *   - neither        -> not invoked (caller guards this case)
   */
  private def addRowLevelPostProcessing(
      plan: LogicalPlan,
      cl: Changelog,
      requiresCarryOverRemoval: Boolean,
      requiresUpdateDetection: Boolean): LogicalPlan = {
    // Row-version bounds in the Window are needed iff we filter carry-over pairs.
    var modifiedPlan = addPostProcessingWindow(plan, cl,
      includeRowVersionBounds = requiresCarryOverRemoval)
    if (requiresCarryOverRemoval) modifiedPlan = addCarryOverPairFilter(modifiedPlan)
    if (requiresUpdateDetection) modifiedPlan = addUpdateRelabelProjection(modifiedPlan)
    removeHelperColumns(modifiedPlan)
  }

  /**
   * Adds a Window node partitioned by (rowId, _commit_version) that computes
   * `_del_cnt` and `_ins_cnt` per partition, and, when `includeRowVersionBounds`
   * is true, additionally `_min_rv` / `_max_rv` (min/max of `Changelog.rowVersion()`).
   *
   * `_del_cnt` / `_ins_cnt` drive update detection (1 each -> relabel as
   * update_preimage / update_postimage). `_min_rv` / `_max_rv` drive carry-over
   * detection (within a delete+insert pair, equal bounds signal a CoW carry-over).
   */
  private def addPostProcessingWindow(
      plan: LogicalPlan,
      cl: Changelog,
      includeRowVersionBounds: Boolean): LogicalPlan = {
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
      Count(Seq(insertIf)).toAggregateExpression(), windowSpec), HelperColumn.InsCnt)()
    val delCntAlias = Alias(WindowExpression(
      Count(Seq(deleteIf)).toAggregateExpression(), windowSpec), HelperColumn.DelCnt)()
    val baseAliases = Seq(delCntAlias, insCntAlias)
    val rowVersionAliases = if (includeRowVersionBounds) {
      val rowVersionExpr =
        V2ExpressionUtils.resolveRef[NamedExpression](cl.rowVersion(), plan)
      Seq(
        Alias(WindowExpression(
          Min(rowVersionExpr).toAggregateExpression(), windowSpec), HelperColumn.MinRv)(),
        Alias(WindowExpression(
          Max(rowVersionExpr).toAggregateExpression(), windowSpec), HelperColumn.MaxRv)())
    } else {
      Seq.empty
    }
    Window(baseAliases ++ rowVersionAliases, partitionByCols, Nil, plan)
  }

  /**
   * Adds a Filter node that drops rows belonging to a CoW carry-over pair.
   * Expects the input to expose `_del_cnt`, `_ins_cnt`, `_min_rv`, `_max_rv`.
   * A pair is a carry-over iff `_del_cnt = 1 AND _ins_cnt = 1 AND _min_rv = _max_rv`.
   */
  private def addCarryOverPairFilter(input: LogicalPlan): LogicalPlan = {
    val delCnt = getAttribute(input, HelperColumn.DelCnt)
    val insCnt = getAttribute(input, HelperColumn.InsCnt)
    val minRv = getAttribute(input, HelperColumn.MinRv)
    val maxRv = getAttribute(input, HelperColumn.MaxRv)

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
    val delCnt = getAttribute(input, HelperColumn.DelCnt)
    val insCnt = getAttribute(input, HelperColumn.InsCnt)

    val isUpdate = And(
      EqualTo(delCnt, Literal(1L)),
      EqualTo(insCnt, Literal(1L)))
    val isInvalid = Or(GreaterThan(delCnt, Literal(1L)), GreaterThan(insCnt, Literal(1L)))
    val updateType = If(EqualTo(changeTypeAttr, Literal("insert")),
      Literal("update_postimage"), Literal("update_preimage"))

    val raiseInvalid = RaiseError(
      Literal("INVALID_CDC_OPTION.UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION"),
      CreateMap(Nil),
      StringType)
    val caseExpr = CaseWhen(Seq(isInvalid -> raiseInvalid, isUpdate -> updateType), changeTypeAttr)

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
        plan
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
   * Removes any helper columns (see [[HelperColumn]]) that earlier steps added to the
   * plan. Helper columns not present in the input are silently ignored, so this method
   * can be applied unconditionally regardless of which post-processing steps ran.
   */
  private def removeHelperColumns(input: LogicalPlan): LogicalPlan = {
    Project(input.output.filterNot(a => HelperColumn.all.contains(a.name)), input)
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
