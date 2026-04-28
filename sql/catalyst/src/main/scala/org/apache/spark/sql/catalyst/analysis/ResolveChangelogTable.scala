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
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Post-processes a resolved [[ChangelogTable]] read to apply CDC option semantics
 * (carry-over removal, update detection) and to enforce supported option combinations.
 *
 * Fires after [[ResolveRelations]] has wrapped the connector's [[Changelog]] in a
 * [[ChangelogTable]]. Both batch ([[DataSourceV2Relation]]) and streaming
 * ([[StreamingRelationV2]]) reads are handled:
 *   - Batch: the requested post-processing passes are injected as logical operators on top
 *     of the relation. Carry-over removal and update detection are fused into a single
 *     pass over a (rowId, _commit_version)-partitioned Window: the Filter drops CoW
 *     carry-over pairs (same rowVersion on both sides) and the subsequent Project relabels
 *     real delete+insert pairs as update_preimage / update_postimage.
 *   - Streaming: post-processing is not yet supported. If the requested options would
 *     require any post-processing, the rule throws an explicit [[AnalysisException]] to
 *     prevent silent wrong results. Streams that don't require post-processing pass
 *     through unchanged.
 *
 * Net change computation (`deduplicationMode = netChanges`) is not yet implemented and
 * is rejected up-front for both batch and streaming.
 */
object ResolveChangelogTable extends Rule[LogicalPlan] {

  /**
   * Reserved (`__spark_cdc_*`) column names used internally by post-processing;
   * connectors must not emit columns with these names.
   */
  object HelperColumn {
    final val DelCnt = "__spark_cdc_del_cnt"
    final val InsCnt = "__spark_cdc_ins_cnt"
    final val MinRv = "__spark_cdc_min_rv"
    final val MaxRv = "__spark_cdc_max_rv"
    final val RvCnt = "__spark_cdc_rv_cnt"

    val all: Set[String] = Set(DelCnt, InsCnt, MinRv, MaxRv, RvCnt)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case rel @ DataSourceV2Relation(table: ChangelogTable, _, _, _, _, _) if !table.resolved =>
      val changelog = table.changelog
      val req = evaluateRequirements(changelog, table.changelogInfo)

      val resolvedRel = rel.copy(table = table.copy(resolved = true))
      var updatedRel: LogicalPlan = resolvedRel
      if (req.requiresCarryOverRemoval || req.requiresUpdateDetection) {
        updatedRel = addRowLevelPostProcessing(
          resolvedRel, changelog, req.requiresCarryOverRemoval, req.requiresUpdateDetection)
      }
      if (req.requiresNetChanges) {
        updatedRel = injectNetChangeComputation(updatedRel, changelog)
      }
      updatedRel

    case rel @ StreamingRelationV2(_, _, table: ChangelogTable, _, _, _, _, _, _)
        if !table.resolved =>
      // Streaming CDC reads do not yet apply post-processing. Run the same option /
      // capability validation as the batch path so silent wrong results are impossible:
      // either no post-processing would be required (fall through, return raw stream),
      // or we throw an explicit AnalysisException.
      val changelog = table.changelog
      val req = evaluateRequirements(changelog, table.changelogInfo)
      if (req.needsAny) {
        throw QueryCompilationErrors.cdcStreamingPostProcessingNotSupported(changelog.name())
      }
      rel.copy(table = table.copy(resolved = true))
  }

  // ---------------------------------------------------------------------------
  // Option validation & Requirement Computation
  // ---------------------------------------------------------------------------

  /**
   * Captures which post-processing passes a CDC query requires, derived from the
   * user-provided [[ChangelogInfo]] options and the connector-declared [[Changelog]]
   * capability flags.
   */
  private case class PostProcessingRequirements(
      requiresCarryOverRemoval: Boolean,
      requiresUpdateDetection: Boolean,
      requiresNetChanges: Boolean) {
    def needsAny: Boolean =
      requiresCarryOverRemoval || requiresUpdateDetection || requiresNetChanges
  }

  /**
   * Validates CDC option/capability combinations and computes which post-processing
   * passes are required. Throws an [[org.apache.spark.sql.AnalysisException]] for
   * unsupported or contradictory combinations (currently: `netChanges` deduplication,
   * and `computeUpdates` with surfaced carry-overs but no carry-over removal).
   */
  private def evaluateRequirements(
      changelog: Changelog,
      options: ChangelogInfo): PostProcessingRequirements = {
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

    // If carry-overs are surfaced and update detection is enabled without carry-over
    // removal, carry-overs would be falsely classified as updates, leading to wrong
    // results. Hence we throw.
    if (requiresUpdateDetection &&
        changelog.containsCarryoverRows() &&
        options.deduplicationMode() == ChangelogInfo.DeduplicationMode.NONE) {
      throw QueryCompilationErrors.cdcUpdateDetectionRequiresCarryOverRemoval(
        changelog.name())
    }

    PostProcessingRequirements(
      requiresCarryOverRemoval, requiresUpdateDetection, requiresNetChanges)
  }

  // ---------------------------------------------------------------------------
  // Row Level Post Processing (Update Detection & Carry-over Removal)
  // ---------------------------------------------------------------------------

  /**
   * Adds row-level post-processing (carry-over removal and/or update detection) on top of
   * the given plan. `counts` = per-partition delete and insert change_type row counts over
   * `(rowId, _commit_version)`. `rv bounds` = per-partition min/max of `rowVersion`.
   * Equal bounds signal a copy-on-write carry-over.
   *   - both active     -> Window(counts + rv bounds) -> Filter -> Project(relabel) -> Drop helpers
   *   - carry-over only -> Window(counts + rv bounds) -> Filter -> Drop helpers
   *   - update only     -> Window(counts only) -> Project(relabel) -> Drop helpers
   *   - neither         -> not invoked (caller guards this case)
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
   * is true, additionally `_min_rv` / `_max_rv` / `_rv_cnt` (min, max and non-null
   * count of `Changelog.rowVersion()`).
   *
   * `_del_cnt` / `_ins_cnt` drive update detection (1 each -> relabel as
   * update_preimage / update_postimage). `_min_rv` / `_max_rv` / `_rv_cnt` drive
   * carry-over detection (within a delete+insert pair, `_rv_cnt = 2` AND equal
   * bounds signal a CoW carry-over).
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

    val insertIf = If(EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_INSERT)),
      Literal(1), Literal(null, IntegerType))
    val deleteIf = If(EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_DELETE)),
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
          Max(rowVersionExpr).toAggregateExpression(), windowSpec), HelperColumn.MaxRv)(),
        Alias(WindowExpression(
          Count(Seq(rowVersionExpr)).toAggregateExpression(), windowSpec), HelperColumn.RvCnt)())
    } else {
      Seq.empty
    }
    Window(baseAliases ++ rowVersionAliases, partitionByCols, Nil, plan)
  }

  /**
   * Adds a Filter node that drops rows belonging to a CoW carry-over pair.
   * A pair is a carry-over iff
   * `_del_cnt = 1 AND _ins_cnt = 1 AND _rv_cnt = 2 AND _min_rv = _max_rv`.
   * The `_rv_cnt = 2` clause guards against a NULL rowVersion silently matching
   * `_min_rv = _max_rv` (Spark's min/max skip NULLs).
   */
  private def addCarryOverPairFilter(input: LogicalPlan): LogicalPlan = {
    val delCnt = getAttribute(input, HelperColumn.DelCnt)
    val insCnt = getAttribute(input, HelperColumn.InsCnt)
    val minRv = getAttribute(input, HelperColumn.MinRv)
    val maxRv = getAttribute(input, HelperColumn.MaxRv)
    val rvCnt = getAttribute(input, HelperColumn.RvCnt)

    val isCarryoverPair = And(
      And(EqualTo(delCnt, Literal(1L)), EqualTo(insCnt, Literal(1L))),
      And(EqualTo(rvCnt, Literal(2L)), EqualTo(minRv, maxRv)))
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
    val updateType = If(EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_INSERT)),
      Literal(Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE),
      Literal(Changelog.CHANGE_TYPE_UPDATE_PREIMAGE))

    val raiseInvalid = RaiseError(
      Literal("CHANGELOG_CONTRACT_VIOLATION.UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION"),
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
  // Net Change Computation
  // ---------------------------------------------------------------------------

  /**
   * Collapses multiple changes per row identity into the net effect.
   * Not yet implemented.
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
