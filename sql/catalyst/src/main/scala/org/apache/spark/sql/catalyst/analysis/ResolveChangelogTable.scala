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

import java.util.UUID

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  CollectList,
  Count,
  First,
  Last,
  Max,
  Min
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Post-processes a resolved [[ChangelogTable]] read to apply CDC option semantics
 * (carry-over removal, update detection, net change computation) and to enforce
 * supported option combinations.
 *
 * Fires after [[ResolveRelations]] has wrapped the connector's [[Changelog]] in a
 * [[ChangelogTable]]. Both batch ([[DataSourceV2Relation]]) and streaming
 * ([[StreamingRelationV2]]) reads are handled:
 *   - Batch: the requested post-processing passes are injected as logical operators on top
 *     of the relation. Carry-over removal and update detection are fused into a single
 *     pass over a (rowId, _commit_version)-partitioned Window: the Filter drops CoW
 *     carry-over pairs (same rowVersion on both sides) and the subsequent Project relabels
 *     real delete+insert pairs as update_preimage / update_postimage. Net change
 *     computation runs on top of that, collapsing intermediate states per `rowId`.
 *   - Streaming: row-level passes (carry-over removal and update detection) are supported
 *     by rewriting the same logic in streaming-allowed primitives -- an
 *     [[EventTimeWatermark]] on `_commit_timestamp`, a stateful [[Aggregate]] keyed by
 *     `(rowId, _commit_version, _commit_timestamp)` that buffers events into an array, an
 *     optional [[Filter]] for carry-over removal, a [[Generate]] using `Inline` to
 *     re-emit the buffered events as rows, and an optional relabel [[Project]] for
 *     update detection. Net change computation is supported by delegating per-row-identity
 *     state management to a [[CdcNetChangesStatefulProcessor]] driven by
 *     [[TransformWithState]] -- the processor keeps the first and last event observed for
 *     each row identity and emits the SPIP collapse output when the global watermark
 *     advances past the last `_commit_timestamp` seen for that key. Row identities only
 *     touched in the latest observed commit are held back until a later commit advances
 *     the watermark or the source terminates. Streams that don't require any
 *     post-processing pass through unchanged.
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
    // Streaming-only: array of struct buffering all input rows for one (rowId,
    // _commit_version) group, fed into Generate(Inline(...)) to re-emit per-row.
    final val Events = "__spark_cdc_events"

    val all: Set[String] = Set(DelCnt, InsCnt, MinRv, MaxRv, RvCnt, Events)
  }

  /**
   * Reserved (`__spark_cdc_*`) column names used internally by net-change
   * computation; connectors must not emit columns with these names.
   */
  object NetChangesHelperColumns {
    final val RowNumber = "__spark_cdc_row_number"
    final val RowCount = "__spark_cdc_row_count"
    final val FirstRowChangeTypeValue =
      "__spark_cdc_first_row_change_type_value"
    final val LastRowChangeTypeValue = "__spark_cdc_last_row_change_type_value"
    // Streaming-only: rowId expressions are aliased to top-level helper columns named
    // `__spark_cdc_rowid_<idx>` so they can be referenced as plain Attributes in the
    // grouping list of `transformWithState`.
    def rowIdColumn(idx: Int): String = s"__spark_cdc_rowid_$idx"

    val all: Set[String] =
      Set(RowNumber, RowCount, FirstRowChangeTypeValue, LastRowChangeTypeValue)
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
        // Resolve rowId against the bare DataSourceV2Relation. V2ExpressionUtils.resolveRefs
        // requires a V2-shaped plan; addRowLevelPostProcessing may have wrapped the relation
        // in Project/Window, which would break resolution against `updatedRel`. Catalyst
        // preserves these resolved attributes by ExprId through any wrapping operators, so
        // they remain valid references for the netChanges Window built on top.
        val rowIdExprs =
          V2ExpressionUtils.resolveRefs[NamedExpression](changelog.rowId().toSeq, resolvedRel)
        updatedRel = injectNetChangeComputation(
          updatedRel, rowIdExprs, table.changelogInfo.computeUpdates())
      }
      updatedRel

    case rel @ StreamingRelationV2(_, _, table: ChangelogTable, _, _, _, _, _, _)
        if !table.resolved =>
      val changelog = table.changelog
      val req = evaluateRequirements(changelog, table.changelogInfo)
      val resolvedRel = rel.copy(table = table.copy(resolved = true))
      var updatedRel: LogicalPlan = resolvedRel
      if (req.requiresCarryOverRemoval || req.requiresUpdateDetection) {
        updatedRel = addStreamingRowLevelPostProcessing(
          resolvedRel, changelog, req.requiresCarryOverRemoval, req.requiresUpdateDetection)
      }
      if (req.requiresNetChanges) {
        // Resolve the rowId references against `updatedRel` (the post-row-level plan)
        // rather than the bare `resolvedRel`. The streaming row-level rewrite uses
        // Aggregate + Generate(Inline), neither of which preserves the original
        // attribute ExprIds for the inlined columns; resolving against `resolvedRel`
        // yields stale ExprIds that fail post-analysis attribute resolution. The
        // row-level rewrite preserves the connector's schema (column names) on its
        // output, so name-based resolution against `updatedRel` recovers the right
        // attributes regardless of any preceding wrapping.
        updatedRel = addStreamingNetChangeComputation(
          updatedRel, changelog, table.changelogInfo.computeUpdates())
      }
      updatedRel
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
   * unsupported or contradictory combinations (currently: `computeUpdates` with
   * surfaced carry-overs but no carry-over removal).
   */
  private def evaluateRequirements(
      changelog: Changelog,
      options: ChangelogInfo): PostProcessingRequirements = {
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
   * Streaming counterpart of [[addRowLevelPostProcessing]].
   *
   * ==Why a different shape from the batch path?==
   *
   * The batch rewrite is Window-based:
   * {{{
   *   DataSourceV2Relation
   *     -> Window partitioned by (rowId..., _commit_version)
   *     -> [Filter (carry-over)]
   *     -> [Project (update relabel)]
   *     -> Project (drop helper columns)
   * }}}
   * [[org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker]] rejects
   * `Window` on streaming queries (`NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING`).
   * Replacing it with a plain [[Aggregate]] is not enough on its own: an aggregate
   * collapses each group to a single row, losing the per-input rows we still need to
   * relabel/filter; and an append-mode streaming aggregate without an event-time
   * watermark on a grouping key is itself rejected by the checker.
   *
   * ==The rewritten plan==
   *
   * Two adjustments over the naive substitution: (a) inject an [[EventTimeWatermark]]
   * on `_commit_timestamp` (zero delay) so the aggregate is legal in append mode, and
   * (b) buffer every input row of a group as `Inline`-able structs and re-explode after
   * the aggregate so no rows are lost.
   * {{{
   *   DataSourceV2Relation
   *     -> Filter (RaiseError on NULL _commit_timestamp)
   *     -> EventTimeWatermark(_commit_timestamp, 0s)
   *     -> Aggregate
   *          group by (rowId..., _commit_version, _commit_timestamp)
   *          aggs    : _del_cnt, _ins_cnt
   *                    [, _min_rv, _max_rv, _rv_cnt  (carry-over removal only)]
   *                    , __spark_cdc_events = collect_list(struct(*))
   *     -> [Filter (carry-over: _del_cnt=1 AND _ins_cnt=1
   *                             AND _rv_cnt=2 AND _min_rv=_max_rv)]
   *     -> Generate(Inline(__spark_cdc_events))   // re-emit one row per buffered input
   *     -> [Project (update relabel)]
   *     -> Project (drop helper columns)
   *     -> Project (strip internal EventTimeWatermark metadata)
   * }}}
   *
   * ==Runtime walkthrough==
   *
   * Append-mode streaming aggregates emit a group when its event-time grouping key
   * falls at or below the global watermark (eviction predicate `eventTime <= watermark`,
   * applied at the start of the next micro-batch). Suppose three commits with
   * `_commit_timestamp` 10, 20, 30 each arrive in their own micro-batch:
   * {{{
   *   batch  max _ts seen  watermark after batch  groups emitted by this batch
   *   -----  ------------  ---------------------  ----------------------------
   *     1         10                10            <none>
   *     2         20                20            groups with _commit_timestamp == 10
   *     3         30                30            groups with _commit_timestamp == 20
   *   end-of-stream final flush                   groups with _commit_timestamp == 30
   * }}}
   * Because every row of a single commit shares the same `_commit_timestamp` (CDC
   * contract), advancing past commit T releases every group whose grouping
   * `_commit_timestamp` equals T -- one commit's worth of post-processed output per
   * micro-batch, with the final commit flushed on stream termination.
   *
   * ==Per-operator detail==
   *
   *  0. [[Filter]] guarding against NULL `_commit_timestamp` -- raises
   *     `CHANGELOG_CONTRACT_VIOLATION.NULL_COMMIT_TIMESTAMP` for any row that
   *     violates the contract. A NULL would never satisfy the downstream Aggregate's
   *     `eventTime <= watermark` eviction predicate (NULL is silent in MAX, never
   *     compares less-than-or-equal), so its group would be held in state forever.
   *     Failing fast surfaces the connector bug instead of producing no output.
   *  1. [[EventTimeWatermark]] on `_commit_timestamp` (zero delay) -- required so the
   *     downstream stateful aggregate can emit groups in append output mode. By CDC
   *     contract every row in a single commit shares `_commit_timestamp`, so taking it
   *     as event time is safe.
   *  2. [[Aggregate]] keyed by `(rowId..., _commit_version, _commit_timestamp)`. Computes
   *     the same `_del_cnt` / `_ins_cnt` / (`_min_rv` / `_max_rv` / `_rv_cnt`) helpers as
   *     the batch path, plus an `__spark_cdc_events` array-of-struct buffering every
   *     input row of the group. `_commit_timestamp` is included in the grouping keys
   *     (besides being a no-op given the contract) to satisfy
   *     [[org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker]]'s
   *     requirement that the watermark attribute appear among grouping expressions for
   *     append-mode streaming aggregations.
   *  3. [[Filter]] (only when carry-over removal is requested) on the same predicate as
   *     the batch path -- groups with `_del_cnt = 1 AND _ins_cnt = 1 AND _rv_cnt = 2 AND
   *     _min_rv = _max_rv` are dropped wholesale.
   *  4. [[Generate]] using `Inline(events)` to re-emit one output row per buffered input
   *     row. `unrequiredChildIndex` drops the duplicate grouping columns and the events
   *     buffer; the helper count columns flow through.
   *  5. [[Project]] (only when update detection is requested) applying the same
   *     `CHANGELOG_CONTRACT_VIOLATION.UNEXPECTED_MULTIPLE_CHANGES_PER_ROW_VERSION`
   *     guard and `_change_type` relabel as the batch path.
   *  6. [[Project]] (via [[removeHelperColumns]]) drops `__spark_cdc_*` helpers so
   *     the output schema matches the connector's declared schema.
   *  7. Final [[Project]] (via [[stripCommitTimestampWatermarkMetadata]]) clears the
   *     `EventTimeWatermark.delayKey` from the user-visible `_commit_timestamp`
   *     attribute so a downstream user-supplied `withWatermark` on a different column
   *     does not interact with our internal watermark via the global multi-watermark
   *     policy.
   */
  private def addStreamingRowLevelPostProcessing(
      plan: LogicalPlan,
      cl: Changelog,
      requiresCarryOverRemoval: Boolean,
      requiresUpdateDetection: Boolean): LogicalPlan = {
    // Fail fast on a NULL `_commit_timestamp`. The downstream Aggregate uses it as
    // both an event-time watermark column and a grouping key; a NULL group-key value
    // would never satisfy the `eventTime <= watermark` eviction predicate, so the
    // group would silently stall (held in state until end of stream). Mirrors the
    // runtime check in [[CdcNetChangesStatefulProcessor]] -- fail fast at the
    // contract violation rather than producing no output.
    val plan1 = addNullCommitTimestampGuard(plan)
    val rawCommitTsAttr = getAttribute(plan1, "_commit_timestamp")
    val watermarked = EventTimeWatermark(
      UUID.randomUUID(), rawCommitTsAttr, new CalendarInterval(0, 0, 0L), plan1)

    val rowIdExprs = V2ExpressionUtils.resolveRefs[NamedExpression](
      cl.rowId().toSeq, watermarked)
    val commitVersionAttr = getAttribute(watermarked, "_commit_version")
    // Pick up the post-watermark `_commit_timestamp` attribute -- it carries the
    // EventTimeWatermark.delayKey metadata that UnsupportedOperationChecker scans for.
    val commitTimestampAttr = getAttribute(watermarked, "_commit_timestamp")
    val changeTypeAttr = getAttribute(watermarked, "_change_type")

    val groupingExprs: Seq[Expression] =
      rowIdExprs ++ Seq(commitVersionAttr, commitTimestampAttr)
    val groupingNamedExprs: Seq[NamedExpression] =
      groupingExprs.map(_.asInstanceOf[NamedExpression])

    val insertIf = If(EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_INSERT)),
      Literal(1), Literal(null, IntegerType))
    val deleteIf = If(EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_DELETE)),
      Literal(1), Literal(null, IntegerType))
    val delCntAlias = Alias(
      Count(Seq(deleteIf)).toAggregateExpression(), HelperColumn.DelCnt)()
    val insCntAlias = Alias(
      Count(Seq(insertIf)).toAggregateExpression(), HelperColumn.InsCnt)()

    val rvAliases = if (requiresCarryOverRemoval) {
      val rowVersionExpr = V2ExpressionUtils.resolveRef[NamedExpression](
        cl.rowVersion(), watermarked)
      Seq(
        Alias(Min(rowVersionExpr).toAggregateExpression(), HelperColumn.MinRv)(),
        Alias(Max(rowVersionExpr).toAggregateExpression(), HelperColumn.MaxRv)(),
        Alias(Count(Seq(rowVersionExpr)).toAggregateExpression(), HelperColumn.RvCnt)())
    } else Seq.empty

    // Buffer every input row as a struct so Inline can re-emit them after the aggregate.
    // The grouping-key columns (rowId..., `_commit_version`, `_commit_timestamp`) appear
    // both inside the struct and as top-level grouping outputs; the top-level duplicates
    // are dropped via `unrequiredChildIndex` below.
    val structOfAllCols = CreateStruct(watermarked.output)
    val eventsAlias = Alias(
      new CollectList(structOfAllCols).toAggregateExpression(), HelperColumn.Events)()

    val aggregateExprs: Seq[NamedExpression] =
      groupingNamedExprs ++ Seq(delCntAlias, insCntAlias) ++ rvAliases :+ eventsAlias
    val aggregated = Aggregate(groupingExprs, aggregateExprs, watermarked)

    val filtered: LogicalPlan = if (requiresCarryOverRemoval) {
      val delCnt = getAttribute(aggregated, HelperColumn.DelCnt)
      val insCnt = getAttribute(aggregated, HelperColumn.InsCnt)
      val minRv = getAttribute(aggregated, HelperColumn.MinRv)
      val maxRv = getAttribute(aggregated, HelperColumn.MaxRv)
      val rvCnt = getAttribute(aggregated, HelperColumn.RvCnt)
      val isCarryoverPair = And(
        And(EqualTo(delCnt, Literal(1L)), EqualTo(insCnt, Literal(1L))),
        And(EqualTo(rvCnt, Literal(2L)), EqualTo(minRv, maxRv)))
      Filter(Not(isCarryoverPair), aggregated)
    } else aggregated

    // Inline the struct array back into rows. Drop the events column (consumed by Inline)
    // and the grouping-key columns (re-emitted from inside the struct) so the final shape
    // matches the connector's schema plus the surviving helper count columns.
    val eventsAttr = getAttribute(filtered, HelperColumn.Events)
    val groupingAttrSet = AttributeSet(groupingNamedExprs.map(_.toAttribute))
    val unrequiredChildIndex: Seq[Int] = filtered.output.zipWithIndex.collect {
      case (a, i) if a.exprId == eventsAttr.exprId => i
      case (a, i) if groupingAttrSet.contains(a) => i
    }
    val generatorOutput: Seq[Attribute] = watermarked.output.map { col =>
      AttributeReference(col.name, col.dataType, col.nullable, col.metadata)()
    }
    val generated = Generate(
      Inline(eventsAttr),
      unrequiredChildIndex = unrequiredChildIndex,
      outer = false,
      qualifier = None,
      generatorOutput = generatorOutput,
      child = filtered)

    val withRelabel: LogicalPlan = if (requiresUpdateDetection) {
      addUpdateRelabelProjection(generated)
    } else generated

    // Strip the auto-injected EventTimeWatermark metadata from the user-visible
    // `_commit_timestamp` so it does not interact with downstream user-supplied
    // watermarks via the global multi-watermark policy. The metadata flows through
    // Generate(Inline) (which copies attribute metadata) and the relabel Project, so
    // it must be cleared here at the boundary of the rewrite.
    val cleaned = stripCommitTimestampWatermarkMetadata(withRelabel)
    removeHelperColumns(cleaned)
  }

  /**
   * Adds a `Filter` that raises
   * `CHANGELOG_CONTRACT_VIOLATION.NULL_COMMIT_TIMESTAMP` for any input row whose
   * `_commit_timestamp` is `NULL`. Used as the first step of the streaming row-level
   * rewrite so a contract-violating connector fails fast instead of silently stalling
   * the downstream stateful aggregate's group.
   */
  private def addNullCommitTimestampGuard(input: LogicalPlan): LogicalPlan = {
    val commitTsAttr = getAttribute(input, "_commit_timestamp")
    // Use a dedicated, side-effecting catalyst expression rather than a
    // `CaseWhen(IsNull(c) -> RaiseError, true)` predicate. Spark's
    // `NullPropagation` rule rewrites `IsNull(c)` to `false` whenever `c.nullable`
    // is `false` and similarly eliminates `AssertNotNull(c)` for non-nullable `c`
    // (`expressions.scala:920-926`). A connector can reasonably declare
    // `_commit_timestamp` as non-nullable in its schema while still emitting NULL
    // at runtime in violation of the contract -- under those rules the guard
    // would be optimized away and the runtime NULL would silently stall the
    // group. `CdcAssertCommitTimestampNotNull` is unrecognised by
    // `NullPropagation` and stays in the plan regardless of the column's
    // declared nullability, surfacing the violation immediately.
    Filter(CdcAssertCommitTimestampNotNull(commitTsAttr), input)
  }

  /**
   * Final boundary for the streaming row-level rewrite: rebuilds the user-visible
   * `_commit_timestamp` attribute with empty watermark-related metadata. Other
   * attributes flow through unchanged.
   */
  private def stripCommitTimestampWatermarkMetadata(input: LogicalPlan): LogicalPlan = {
    val projectList: Seq[NamedExpression] = input.output.map { attr =>
      if (attr.name == "_commit_timestamp" &&
          attr.metadata.contains(EventTimeWatermark.delayKey)) {
        val cleanedMetadata = new MetadataBuilder()
          .withMetadata(attr.metadata)
          .remove(EventTimeWatermark.delayKey)
          .build()
        Alias(attr.withMetadata(cleanedMetadata), attr.name)(
          exprId = attr.exprId,
          qualifier = attr.qualifier)
      } else {
        attr
      }
    }
    Project(projectList, input)
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
   * Collapses multiple changes per row identity across versions into the net effect:
   *
   * | existedBefore | existsAfter | output                              |
   * |---------------|-------------|-------------------------------------|
   * | false         | false       | (cancel)                            |
   * | false         | true        | insert                              |
   * | true          | false       | delete                              |
   * | true          | true        | update_preimage + update_postimage  |
   *
   * If `computeUpdates = false`, the `update_preimage` + `update_postimage` pair is
   * emitted as `delete` + `insert` instead.
   *
   * `existedBefore` is true iff the partition's first event is `delete` or
   * `update_preimage`. `existsAfter` is true iff the partition's last event is
   * `insert` or `update_postimage`.
   *
   * Pipeline: Window (per-rowId aggregates, sort by version) -> Filter (keep first/last per
   * partition) -> Project (relabel `_change_type` and drop helper columns).
   */
  private def injectNetChangeComputation(
      plan: LogicalPlan,
      rowIdExprs: Seq[NamedExpression],
      computeUpdates: Boolean): LogicalPlan = {
    val windowedPlan = addNetChangesWindow(plan, rowIdExprs)
    val filteredAndRelabeledPlan =
      removeIntermediateChangelogEntriesAndRelabelChangeTypes(windowedPlan, computeUpdates)
    filteredAndRelabeledPlan
  }

  /**
   * Streaming counterpart of [[injectNetChangeComputation]]. The batch version uses a
   * Catalyst `Window` partitioned by `rowId`, which is rejected on streaming queries.
   * This version delegates the per-`rowId` first/last extraction and the SPIP collapse
   * matrix to a [[CdcNetChangesStatefulProcessor]] driven by `transformWithState`:
   *
   *  1. [[EventTimeWatermark]] on `_commit_timestamp` (zero delay) so the global query
   *     watermark advances with each batch. When this rewrite runs on top of the row-level
   *     post-processing rewrite (combined `containsCarryoverRows` /
   *     `representsUpdateAsDeleteAndInsert` + `containsIntermediateChanges` path), the
   *     row-level rewrite has already injected an identical `EventTimeWatermark` and we
   *     reuse it instead of stacking a second one. Stacking watermarks on the same column
   *     fails the multi-watermark check unless `STATEFUL_OPERATOR_ALLOW_MULTIPLE` is set,
   *     and even then it would just produce two redundant nodes.
   *  2. [[Project]] that aliases each rowId expression to a top-level helper column. This
   *     lets us address the rowId as an `Attribute` for the `transformWithState` grouping,
   *     which in turn makes nested rowId paths (e.g. `payload.id`) work without special
   *     casing.
   *  3. [[TransformWithState]] keyed by the rowId helper attributes, in
   *     [[org.apache.spark.sql.catalyst.plans.logical.EventTime]] mode. The processor
   *     buffers the first and last event per row identity; an event-time timer set to the
   *     latest observed `_commit_timestamp` fires once the global watermark advances past
   *     it, at which point the processor evaluates the SPIP `(existedBefore, existsAfter)`
   *     matrix and emits 0, 1, or 2 output rows.
   *  4. [[SerializeFromObject]] (added by the `transformWithState` factory) brings the
   *     processor's `Row` outputs back into a regular tabular shape.
   *  5. [[Project]] (via [[stripCommitTimestampWatermarkMetadata]]) clears the
   *     auto-injected `EventTimeWatermark.delayKey` metadata from the user-visible
   *     `_commit_timestamp`. The metadata is preserved through the `transformWithState`
   *     encoder roundtrip and would otherwise interact with downstream user-supplied
   *     watermarks via the global multi-watermark policy.
   *  6. Final [[Project]] drops the rowId helper columns so the user-visible schema
   *     matches the connector's declared changelog schema.
   *
   * Streaming netChanges is incremental, not range-scoped: per-row-identity state is
   * cleared on emission, so a later commit on the same identity starts a fresh window
   * and produces additional output rows. Batch netChanges over the same version range
   * would have collapsed those changes; streaming cannot retract already-emitted rows
   * to match that. End-of-stream flushes all pending timers, so a bounded stream's
   * output matches batch only when no row identity is touched again after its first
   * emission.
   */
  private def addStreamingNetChangeComputation(
      plan: LogicalPlan,
      cl: Changelog,
      computeUpdates: Boolean): LogicalPlan = {
    // 1. Inject (or reuse, if already injected by the row-level rewrite) a watermark on
    //    `_commit_timestamp`. The row-level rewrite already adds one with zero delay, so
    //    we only add it when no watermark is present in the lineage to avoid stacking
    //    EventTimeWatermark nodes (which is rejected by the multi-watermark check
    //    unless STATEFUL_OPERATOR_ALLOW_MULTIPLE is set).
    val needsWatermark = !plan.exists {
      case _: EventTimeWatermark => true
      case _ => false
    }
    val watermarked: LogicalPlan = if (needsWatermark) {
      val rawCommitTsAttr = getAttribute(plan, "_commit_timestamp")
      EventTimeWatermark(
        UUID.randomUUID(), rawCommitTsAttr, new CalendarInterval(0, 0, 0L), plan)
    } else plan

    // 2. Resolve rowId expressions against the watermarked plan. Resolving here (after
    //    any preceding row-level rewrite) ensures the attribute ExprIds match the
    //    columns in `plan.output` -- name-based resolution recovers them by their
    //    connector-declared names. Then project them to top-level helper columns so
    //    they can be referenced as plain Attributes by `transformWithState`'s grouping.
    val rowIdExprs =
      V2ExpressionUtils.resolveRefs[NamedExpression](cl.rowId().toSeq, watermarked)
    val rowIdHelpers: Seq[Alias] = rowIdExprs.zipWithIndex.map { case (expr, idx) =>
      Alias(expr, NetChangesHelperColumns.rowIdColumn(idx))()
    }
    val originalCols: Seq[Attribute] = watermarked.output
    val withHelpers = Project(originalCols ++ rowIdHelpers, watermarked)

    // 3. Build the input/output Row encoder for the processor. The schema is the
    //    watermarked plan's schema plus the rowId helper columns.
    val processorInputSchema = StructType(
      withHelpers.output.map { a =>
        StructField(a.name, a.dataType, a.nullable, a.metadata)
      })
    val rowEncoder = ExpressionEncoder(processorInputSchema)
    val groupingAttrs: Seq[Attribute] = rowIdHelpers.map(_.toAttribute)
    val keyEncoder = ExpressionEncoder(StructType(rowIdHelpers.map { a =>
      StructField(a.name, a.dataType, a.nullable, a.metadata)
    }))

    val processor = new CdcNetChangesStatefulProcessor(processorInputSchema, computeUpdates)

    val tws = new TransformWithState(
      keyDeserializer = UnresolvedDeserializer(keyEncoder.deserializer, groupingAttrs),
      valueDeserializer = UnresolvedDeserializer(rowEncoder.deserializer, withHelpers.output),
      groupingAttributes = groupingAttrs,
      dataAttributes = withHelpers.output,
      statefulProcessor = processor.asInstanceOf[StatefulProcessor[Any, Any, Any]],
      timeMode = EventTime,
      outputMode = OutputMode.Append(),
      keyEncoder = keyEncoder.asInstanceOf[ExpressionEncoder[Any]],
      outputObjAttr = CatalystSerde.generateObjAttr(rowEncoder),
      child = withHelpers,
      hasInitialState = false,
      initialStateGroupingAttrs = groupingAttrs,
      initialStateDataAttrs = withHelpers.output,
      initialStateDeserializer = UnresolvedDeserializer(keyEncoder.deserializer, groupingAttrs),
      initialState = LocalRelation(keyEncoder.schema))

    // 4. Wrap with SerializeFromObject so the obj column becomes regular tabular output.
    val serialized = CatalystSerde.serialize(tws)(rowEncoder)

    // 5. Strip the auto-injected EventTimeWatermark metadata from the user-visible
    //    `_commit_timestamp`. The metadata is preserved through the `transformWithState`
    //    encoder roundtrip (the encoder schema carries StructField metadata), so we
    //    must clear it here at the boundary of the rewrite -- otherwise downstream
    //    user-supplied watermarks would interact with our internal watermark via the
    //    global multi-watermark policy. Mirrors the row-level path's call at the end
    //    of `addStreamingRowLevelPostProcessing`.
    val cleaned = stripCommitTimestampWatermarkMetadata(serialized)

    // 6. Drop the rowId helper columns so the final output matches the connector's schema.
    val helperNames = rowIdHelpers.map(_.name).toSet
    Project(cleaned.output.filterNot(a => helperNames.contains(a.name)), cleaned)
  }

  /**
   * Adds a Window node partitioned by `rowId` and ordered by
   * `(_commit_version, change_type_rank)` where pre-events (`update_preimage`,
   * `delete`) sort before post-events (`update_postimage`, `insert`) within the same
   * commit. Computes per-partition helper columns:
   *   - `__spark_cdc_row_number` (1..n) answers: "is this the first or last row?".
   *   - `__spark_cdc_row_count` is the partition size which combined with row_number is
   *     used to detect the last row.
   *   - `__spark_cdc_first_row_change_type_value` and
   *     `__spark_cdc_last_row_change_type_value` drive the first/last classification at
   *     filter and relabel time.
   */
  private def addNetChangesWindow(
      plan: LogicalPlan,
      rowIdExprs: Seq[NamedExpression]): LogicalPlan = {
    val changeTypeAttr = getAttribute(plan, "_change_type")
    val commitVersionAttr = getAttribute(plan, "_commit_version")
    val raiseUnexpectedChangeType = RaiseError(
      Literal("CHANGELOG_CONTRACT_VIOLATION.UNEXPECTED_CHANGE_TYPE"),
      CreateMap(Nil),
      IntegerType)
    val changeTypeRank = CaseWhen(Seq(
      EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_UPDATE_PREIMAGE)) -> Literal(0),
      EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_DELETE)) -> Literal(0),
      EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_INSERT)) -> Literal(1),
      EqualTo(changeTypeAttr, Literal(Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE)) -> Literal(1)),
      raiseUnexpectedChangeType)
    val partitionByCols = rowIdExprs
    val orderSpec = Seq(
      SortOrder(commitVersionAttr, Ascending),
      SortOrder(changeTypeRank, Ascending))
    val rowNumberWindowSpec = WindowSpecDefinition(
      partitionByCols, orderSpec,
      UnspecifiedFrame)
    val aggregateWindowSpec = WindowSpecDefinition(
      partitionByCols, orderSpec,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))

    val rowNumberAlias = Alias(
      WindowExpression(RowNumber(), rowNumberWindowSpec),
      NetChangesHelperColumns.RowNumber)()
    val rowCountAlias = Alias(
      WindowExpression(Count(Seq(Literal(1))).toAggregateExpression(), aggregateWindowSpec),
      NetChangesHelperColumns.RowCount)()
    val firstRowChangeTypeValueAlias = Alias(
      WindowExpression(
        First(changeTypeAttr, ignoreNulls = false).toAggregateExpression(),
        aggregateWindowSpec),
      NetChangesHelperColumns.FirstRowChangeTypeValue)()
    val lastRowChangeTypeValueAlias = Alias(
      WindowExpression(
        Last(changeTypeAttr, ignoreNulls = false).toAggregateExpression(),
        aggregateWindowSpec),
      NetChangesHelperColumns.LastRowChangeTypeValue)()

    Window(
      Seq(rowNumberAlias, rowCountAlias, firstRowChangeTypeValueAlias,
        lastRowChangeTypeValueAlias),
      partitionByCols, orderSpec, plan)
  }

  /**
   * Filters and relabels the windowed plan: keeps only the first and/or last row per
   * `rowId` partition, then rewrites the surviving rows' `_change_type` and drops the
   * helper columns.
   *
   * | existedBefore | existsAfter | output                              |
   * |---------------|-------------|-------------------------------------|
   * | false         | false       | (cancel)                            |
   * | false         | true        | insert                              |
   * | true          | false       | delete                              |
   * | true          | true        | update_preimage + update_postimage  |
   *
   * If `computeUpdates = false`, the `update_preimage` + `update_postimage` pair is
   * emitted as `delete` + `insert` instead.
   *
   * `existedBefore` is true iff the partition's first event is `delete` or
   * `update_preimage`. `existsAfter` is true iff the partition's last event is
   * `insert` or `update_postimage`.
   *
   * Helper columns (`__spark_cdc_*`) are dropped in the same Project that does the
   * relabel, saving a follow-up cleanup pass.
   */
  private def removeIntermediateChangelogEntriesAndRelabelChangeTypes(
       windowedPlan: LogicalPlan,
       computeUpdates: Boolean
     ): LogicalPlan = {
    val rowNumberAttr = getAttribute(windowedPlan, NetChangesHelperColumns.RowNumber)
    val rowCountAttr = getAttribute(windowedPlan, NetChangesHelperColumns.RowCount)
    val firstRowChangeTypeAttr =
      getAttribute(windowedPlan, NetChangesHelperColumns.FirstRowChangeTypeValue)
    val lastRowChangeTypeAttr =
      getAttribute(windowedPlan, NetChangesHelperColumns.LastRowChangeTypeValue)

    val existedBeforeVersionRange = In(firstRowChangeTypeAttr, Seq(
      Literal(Changelog.CHANGE_TYPE_DELETE),
      Literal(Changelog.CHANGE_TYPE_UPDATE_PREIMAGE)))
    val existsAfterVersionRange = In(lastRowChangeTypeAttr, Seq(
      Literal(Changelog.CHANGE_TYPE_INSERT),
      Literal(Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE)))

    val isFirst = EqualTo(rowNumberAttr, Literal(1))
    val isLast = EqualTo(rowNumberAttr, rowCountAttr)

    // only keep first and last entry per set of changes for a rowId, order of cases is important!
    val keep = CaseWhen(Seq(
      // filter out if inserted and deleted within range
      And(Not(existedBeforeVersionRange), Not(existsAfterVersionRange)) -> Literal(false),
      // for persisting new row keep only last state
      And(Not(existedBeforeVersionRange), existsAfterVersionRange) -> isLast,
      // for previously existing row keep first state
      And(existedBeforeVersionRange, Not(existsAfterVersionRange)) -> isFirst),
      // for persisting row keep first and last state
      // existedBeforeVersionRange = true, existsAfterVersionRange = true
      Or(isFirst, isLast))

    val filteredPlan = Filter(keep, windowedPlan)

    val computedPreUpdateLabel =
      if (computeUpdates) Literal(Changelog.CHANGE_TYPE_UPDATE_PREIMAGE)
      else Literal(Changelog.CHANGE_TYPE_DELETE)
    val computedPostUpdateLabel =
      if (computeUpdates) Literal(Changelog.CHANGE_TYPE_UPDATE_POSTIMAGE)
      else Literal(Changelog.CHANGE_TYPE_INSERT)

    val changeTypeAttr = getAttribute(filteredPlan, "_change_type")

    // Each case relabels the kept row(s) to match the required output label. The tuple
    // is (first event, last event) of the partition; cases below assume computeUpdates=true.
    //   Case 1 (insert, update_postimage): keep update_postimage; relabel it to insert.
    //   Case 2 (update_preimage, delete): keep update_preimage; relabel it to delete.
    //   Case 3 (delete, update_postimage): keep delete and update_postimage; relabel delete to
    //           update_preimage.
    //   Case 4 (update_preimage, insert): keep update_preimage and insert; relabel insert to
    //           update_postimage.
    // No-op cases (e.g. (insert, insert)) are not listed. If computeUpdates=false insert/deletes
    // will be used instead of update_pre/postimage.
    val relabel = CaseWhen(Seq(
      And(Not(existedBeforeVersionRange), isLast) -> Literal(Changelog.CHANGE_TYPE_INSERT),
      And(Not(existsAfterVersionRange), isFirst) -> Literal(Changelog.CHANGE_TYPE_DELETE),
      And(And(existedBeforeVersionRange, existsAfterVersionRange), isFirst)
        -> computedPreUpdateLabel,
      And(And(existedBeforeVersionRange, existsAfterVersionRange), isLast)
        -> computedPostUpdateLabel),
      changeTypeAttr)

    val projectList = filteredPlan.output.flatMap { attr =>
      if (NetChangesHelperColumns.all.contains(attr.name)) None
      else if (attr.name == "_change_type") Some(Alias(relabel, "_change_type")())
      else Some(attr)
    }

    val projectedPlan = Project(projectList, filteredPlan)
    projectedPlan
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

/**
 * Side-effecting Boolean expression: returns `true` if the child is non-NULL and throws
 * `CHANGELOG_CONTRACT_VIOLATION.NULL_COMMIT_TIMESTAMP` if the child is NULL. Used as the
 * predicate of the streaming row-level rewrite's NULL guard `Filter`.
 *
 * The point of this dedicated expression is to remain in the plan no matter what the
 * connector declares for `_commit_timestamp.nullable`: Spark's `NullPropagation` rules
 * (`Optimizer.scala`'s `expressions.scala:920-926`) rewrite `IsNull(c) -> false` and
 * eliminate `AssertNotNull(c)` whenever `c.nullable` is `false`. A connector that
 * declares `_commit_timestamp` non-nullable but emits NULL at runtime would slip past
 * those simpler shapes; this class is unrecognised by `NullPropagation` so the runtime
 * check stays put.
 */
case class CdcAssertCommitTimestampNotNull(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with NonSQLExpression {

  override def dataType: DataType = BooleanType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    if (child.eval(input) == null) {
      throw new SparkRuntimeException(
        errorClass = "CHANGELOG_CONTRACT_VIOLATION.NULL_COMMIT_TIMESTAMP",
        messageParameters = Map.empty)
    }
    true
  }

  override protected def withNewChildInternal(
      newChild: Expression): CdcAssertCommitTimestampNotNull =
    copy(child = newChild)
}
