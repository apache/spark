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

package org.apache.spark.sql.connector

import java.util.Collections

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Inline
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate, EventTimeWatermark, Filter, Generate, LogicalPlan, Project, TransformWithState}
import org.apache.spark.sql.connector.catalog.{
  ChangelogProperties, Column, Identifier, InMemoryChangelogCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Plan-shape tests for the streaming arm of
 * [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]]. These mirror the batch
 * checks in [[ResolveChangelogTablePostProcessingSuite]] but assert on the rewritten
 * logical plan rather than running the streaming query end-to-end (the end-to-end
 * coverage lives in [[ChangelogEndToEndSuite]]).
 *
 * The streaming row-level rewrite is:
 *   EventTimeWatermark(_commit_timestamp, 0s)
 *     -> Aggregate keyed by (rowId..., _commit_version, _commit_timestamp)
 *     -> [Filter (carry-over)]
 *     -> Generate(Inline(events))
 *     -> [Project (update relabel)]
 *     -> Project (drop helper columns)
 */
class ResolveChangelogTableStreamingPostProcessingSuite extends SharedSparkSession {

  private val catalogName = "cdc_streaming_pp"
  private val testTableName = "events"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[InMemoryChangelogCatalog].getName)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val cat = catalog
    val ident = identifier
    if (cat.tableExists(ident)) cat.dropTable(ident)
    cat.clearChangeRows(ident)
    cat.setChangelogProperties(ident, ChangelogProperties())
    cat.createTable(
      ident,
      Array(
        Column.create("id", LongType, false),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
  }

  private def catalog: InMemoryChangelogCatalog =
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[InMemoryChangelogCatalog]

  private def identifier: Identifier = Identifier.of(Array.empty, testTableName)

  private def streamingDf(opts: (String, String)*): DataFrame = {
    val reader = spark.readStream.option("startingVersion", "1")
    opts.foldLeft(reader) { case (r, (k, v)) => r.option(k, v) }
      .changes(s"$catalogName.$testTableName")
  }

  private def assertWatermarkOnCommitTimestamp(plan: LogicalPlan): Unit = {
    val wm = plan.collect { case w: EventTimeWatermark => w }
    assert(wm.size == 1,
      s"Expected exactly one EventTimeWatermark; found ${wm.size}. Plan:\n$plan")
    assert(wm.head.eventTime.name == "_commit_timestamp",
      s"Watermark must be on `_commit_timestamp` but was on `${wm.head.eventTime.name}`. " +
        s"Plan:\n$plan")
    assert(wm.head.delay.months == 0 && wm.head.delay.days == 0 &&
      wm.head.delay.microseconds == 0L,
      s"Watermark delay must be zero. Plan:\n$plan")
  }

  private def assertNoStreamingPostProcessing(plan: LogicalPlan): Unit = {
    assert(plan.collect { case w: EventTimeWatermark => w }.isEmpty,
      s"No EventTimeWatermark expected for raw streaming pass-through. Plan:\n$plan")
    val planStr = plan.treeString
    assert(!planStr.contains("__spark_cdc_"),
      s"Helper columns must not appear in pass-through plan. Plan:\n$planStr")
  }

  private def assertHelperColumnsRemoved(plan: LogicalPlan): Unit = {
    val outputNames = plan.output.map(_.name).toSet
    assert(!outputNames.exists(_.startsWith("__spark_cdc_")),
      s"Helper columns must be dropped before the user-visible output. Output: " +
        outputNames.mkString(", "))
  }

  private def assertNoWatermarkMetadataOnOutput(plan: LogicalPlan): Unit = {
    import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
    val leaks = plan.output.filter(_.metadata.contains(EventTimeWatermark.delayKey))
    assert(leaks.isEmpty,
      s"User-visible output must not carry EventTimeWatermark.delayKey metadata; " +
        s"found on: ${leaks.map(_.name).mkString(",")}. Plan:\n$plan")
  }

  private def assertInlineGenerate(plan: LogicalPlan): Unit = {
    val gens = plan.collect { case g: Generate => g }
    assert(gens.size == 1,
      s"Expected exactly one Generate; found ${gens.size}. Plan:\n$plan")
    assert(gens.head.generator.isInstanceOf[Inline],
      s"Generate must use Inline. Plan:\n$plan")
  }

  private def assertContainsNullCommitTimestampGuard(plan: LogicalPlan): Unit = {
    import org.apache.spark.sql.catalyst.analysis.CdcAssertCommitTimestampNotNull
    val nullGuards = plan.collect {
      case f: Filter
        if f.condition.isInstanceOf[CdcAssertCommitTimestampNotNull] => f
    }
    assert(nullGuards.size == 1,
      s"Expected exactly one CdcAssertCommitTimestampNotNull guard Filter. Plan:\n$plan")
  }

  // ===========================================================================
  // Carry-over removal only
  // ===========================================================================

  test("carry-over removal injects watermark + Aggregate + Filter + Generate") {
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf().queryExecution.analyzed
    assertWatermarkOnCommitTimestamp(analyzed)

    val aggs = analyzed.collect { case a: Aggregate => a }
    assert(aggs.size == 1, s"Expected one Aggregate. Plan:\n$analyzed")
    val groupingNames = aggs.head.groupingExpressions.collect {
      case ne: org.apache.spark.sql.catalyst.expressions.NamedExpression => ne.name
    }
    assert(groupingNames.toSet == Set("id", "_commit_version", "_commit_timestamp"),
      s"Expected grouping by (id, _commit_version, _commit_timestamp); got $groupingNames")

    // Two Filters: the NULL `_commit_timestamp` guard + the carry-over predicate.
    val filters = analyzed.collect { case f: Filter => f }
    assert(filters.size == 2,
      s"Expected NULL guard + carry-over Filter. Plan:\n$analyzed")
    assertContainsNullCommitTimestampGuard(analyzed)

    assertInlineGenerate(analyzed)
    assertHelperColumnsRemoved(analyzed)
    assertNoWatermarkMetadataOnOutput(analyzed)
  }

  // ===========================================================================
  // Update detection only
  // ===========================================================================

  test("update detection alone injects watermark + Aggregate + Generate + relabel Project") {
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf(
      "computeUpdates" -> "true",
      "deduplicationMode" -> "none").queryExecution.analyzed
    assertWatermarkOnCommitTimestamp(analyzed)

    // No carry-over Filter when only update detection runs -- but the NULL
    // `_commit_timestamp` guard Filter is always present.
    val filters = analyzed.collect { case f: Filter => f }
    assert(filters.size == 1,
      s"Only the NULL guard Filter is expected for update-detection-only path. " +
        s"Plan:\n$analyzed")
    assertContainsNullCommitTimestampGuard(analyzed)

    assertInlineGenerate(analyzed)

    // The relabel Project must reference _change_type (CaseWhen rewrites it).
    val projects = analyzed.collect { case p: Project => p }
    assert(projects.exists { p =>
      p.projectList.exists(
        _.toString.toLowerCase(java.util.Locale.ROOT).contains("update_preimage"))
    }, s"Expected a Project that emits `update_preimage`. Plan:\n$analyzed")

    assertHelperColumnsRemoved(analyzed)
    assertNoWatermarkMetadataOnOutput(analyzed)
  }

  // ===========================================================================
  // Both passes
  // ===========================================================================

  test("carry-over + update detection share a single Aggregate") {
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsCarryoverRows = true,
      representsUpdateAsDeleteAndInsert = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf("computeUpdates" -> "true").queryExecution.analyzed
    assertWatermarkOnCommitTimestamp(analyzed)

    val aggs = analyzed.collect { case a: Aggregate => a }
    assert(aggs.size == 1, s"Should fuse both passes into a single Aggregate. Plan:\n$analyzed")

    // Two Filters: NULL guard + carry-over removal.
    val filters = analyzed.collect { case f: Filter => f }
    assert(filters.size == 2,
      s"Expected NULL guard + carry-over Filter for combined path. Plan:\n$analyzed")
    assertContainsNullCommitTimestampGuard(analyzed)

    assertInlineGenerate(analyzed)
    assertHelperColumnsRemoved(analyzed)
    assertNoWatermarkMetadataOnOutput(analyzed)
  }

  // ===========================================================================
  // Net changes
  // ===========================================================================

  test("netChanges alone injects watermark + TransformWithState") {
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf(
      "deduplicationMode" -> "netChanges").queryExecution.analyzed
    assertWatermarkOnCommitTimestamp(analyzed)
    val tws = analyzed.collect { case t: TransformWithState => t }
    assert(tws.size == 1,
      s"Expected exactly one TransformWithState; found ${tws.size}. Plan:\n$analyzed")
    // Guard against a regression that grouped by the wrong attributes (e.g. omitting a
    // rowId column or grouping by `_commit_version`) -- the size check alone would still
    // pass.
    val groupingNames = tws.head.groupingAttributes.map(_.name)
    assert(groupingNames == Seq("__spark_cdc_rowid_0"),
      s"Expected TransformWithState grouping by [__spark_cdc_rowid_0]; got $groupingNames. " +
        s"Plan:\n$analyzed")
    assertHelperColumnsRemoved(analyzed)
    // The auto-injected `EventTimeWatermark` metadata flows through the
    // `transformWithState` encoder roundtrip on the netChanges-only path. The
    // rewrite must strip it from the user-visible `_commit_timestamp` so a
    // downstream user-supplied watermark cannot accidentally interact with our
    // internal watermark via the global multi-watermark policy.
    assertNoWatermarkMetadataOnOutput(analyzed)
  }

  test("netChanges with composite rowId groups by all helper columns") {
    // Recreate with a two-column rowId so we exercise the rowIdColumn(idx) helper
    // for idx > 0. The single-rowId test asserts the size-1 case; this guards
    // against a regression that hard-codes a single helper attribute.
    val cat = catalog
    val ident = identifier
    cat.dropTable(ident)
    cat.createTable(
      ident,
      Array(
        Column.create("ns", LongType, false),
        Column.create("id", LongType, false),
        Column.create("row_commit_version", LongType, false)),
      Array.empty[Transform],
      Collections.emptyMap[String, String]())
    cat.setChangelogProperties(ident, ChangelogProperties(
      containsIntermediateChanges = true,
      rowIdNames = Seq("ns", "id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf(
      "deduplicationMode" -> "netChanges").queryExecution.analyzed
    assertWatermarkOnCommitTimestamp(analyzed)
    val tws = analyzed.collect { case t: TransformWithState => t }
    assert(tws.size == 1, s"Expected one TransformWithState. Plan:\n$analyzed")
    val groupingNames = tws.head.groupingAttributes.map(_.name)
    assert(groupingNames == Seq("__spark_cdc_rowid_0", "__spark_cdc_rowid_1"),
      s"Expected grouping by [__spark_cdc_rowid_0, __spark_cdc_rowid_1]; got $groupingNames. " +
        s"Plan:\n$analyzed")
    assertHelperColumnsRemoved(analyzed)
  }

  test("netChanges + carry-over removal share a single watermark") {
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsCarryoverRows = true,
      containsIntermediateChanges = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf(
      "deduplicationMode" -> "netChanges").queryExecution.analyzed
    val watermarks = analyzed.collect { case w: EventTimeWatermark => w }
    assert(watermarks.size == 1,
      s"Combined row-level + netChanges path should share one EventTimeWatermark. " +
        s"Plan:\n$analyzed")
    val aggs = analyzed.collect { case a: Aggregate => a }
    assert(aggs.size == 1,
      s"Row-level Aggregate should still be present. Plan:\n$analyzed")
    val tws = analyzed.collect { case t: TransformWithState => t }
    assert(tws.size == 1,
      s"netChanges TransformWithState should be on top of the row-level rewrite. " +
        s"Plan:\n$analyzed")
    assertHelperColumnsRemoved(analyzed)
  }

  // ===========================================================================
  // No post-processing -> no rewrite
  // ===========================================================================

  test("no post-processing required: raw streaming relation passes through") {
    // No capability flags set -> no Aggregate, no watermark.
    val analyzed = streamingDf().queryExecution.analyzed
    assertNoStreamingPostProcessing(analyzed)
  }

  test("computeUpdates without representsUpdateAsDeleteAndInsert: no rewrite") {
    // Connector says updates are already materialized -> nothing to do.
    val analyzed = streamingDf(
      "computeUpdates" -> "true").queryExecution.analyzed
    assertNoStreamingPostProcessing(analyzed)
  }

  // ===========================================================================
  // Watermark metadata is internal-only and stripped from user-visible output
  // ===========================================================================

  test("watermark metadata is stripped from user-visible _commit_timestamp") {
    import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf().queryExecution.analyzed
    // Internally, the EventTimeWatermark must still be present.
    assertWatermarkOnCommitTimestamp(analyzed)
    // But none of the user-visible output attributes should leak the watermark
    // metadata; downstream user-supplied watermarks must not interact with our
    // auto-injected internal watermark via the global multi-watermark policy.
    val ts = analyzed.output.find(_.name == "_commit_timestamp")
    assert(ts.isDefined, s"Expected `_commit_timestamp` in output. Plan:\n$analyzed")
    assert(!ts.get.metadata.contains(EventTimeWatermark.delayKey),
      s"Watermark metadata leaked to user-visible `_commit_timestamp`. Plan:\n$analyzed")
  }

  // ===========================================================================
  // NULL _commit_timestamp guard
  // ===========================================================================

  test("NULL _commit_timestamp guard Filter is the first operator after the source") {
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version")))

    val analyzed = streamingDf().queryExecution.analyzed
    import org.apache.spark.sql.catalyst.analysis.CdcAssertCommitTimestampNotNull
    // The guard must sit BELOW the EventTimeWatermark (we don't want a NULL row to
    // be considered for watermark advancement at all). Verify by walking the plan
    // top-down and finding the guard before any Aggregate.
    val guards = analyzed.collect {
      case f: Filter if f.condition.isInstanceOf[CdcAssertCommitTimestampNotNull] => f
    }
    assert(guards.size == 1, s"Expected exactly one guard. Plan:\n$analyzed")
    val guard = guards.head
    val guardChild = guard.child
    // The guard's child should be the bare relation (or a SubqueryAlias wrapping it),
    // not the EventTimeWatermark.
    val isSourceBelowGuard = guardChild match {
      case _: org.apache.spark.sql.catalyst.streaming.StreamingRelationV2 => true
      case org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias(_,
            _: org.apache.spark.sql.catalyst.streaming.StreamingRelationV2) => true
      case _ => false
    }
    assert(isSourceBelowGuard,
      s"NULL guard Filter should sit directly above the streaming relation. Plan:\n$analyzed")
  }

  test("NULL guard predicate is not foldable when _commit_timestamp is non-nullable") {
    import org.apache.spark.sql.catalyst.analysis.CdcAssertCommitTimestampNotNull
    import org.apache.spark.sql.catalyst.expressions.{IsNull, Literal}
    import org.apache.spark.sql.catalyst.optimizer.NullPropagation
    // Streaming plans can't be sent through the batch optimizer (UnsupportedOperationChecker
    // rejects streaming sources in that path), so we directly exercise the rule that the
    // reviewer flagged: NullPropagation must not eliminate our predicate even when the
    // child column is non-nullable. Spark's NullPropagation simplifies `IsNull(c)` and
    // `AssertNotNull(c)` to constants for non-nullable `c`, but it has no rule for
    // `CdcAssertCommitTimestampNotNull`, so the predicate stays in place.
    catalog.setChangelogProperties(identifier, ChangelogProperties(
      containsCarryoverRows = true,
      rowIdNames = Seq("id"),
      rowVersionName = Some("row_commit_version"),
      commitTimestampNullable = false))

    val analyzed = streamingDf().queryExecution.analyzed
    val tsAttrAnalyzed = analyzed.collect {
      case rel: org.apache.spark.sql.catalyst.streaming.StreamingRelationV2 =>
        rel.output.find(_.name == "_commit_timestamp").get
    }.head
    assert(!tsAttrAnalyzed.nullable,
      s"Test setup expected non-nullable `_commit_timestamp` on the source. Plan:\n$analyzed")

    val guardsBefore = analyzed.collect {
      case f: Filter if f.condition.isInstanceOf[CdcAssertCommitTimestampNotNull] => f
    }
    assert(guardsBefore.size == 1,
      s"NULL guard must be present in the analyzed plan. Plan:\n$analyzed")

    // Run NullPropagation on the predicate. It should be a no-op because the rule does
    // not recognize `CdcAssertCommitTimestampNotNull`. (As a sanity check: the same rule
    // would simplify a plain `IsNull(non-nullable)` to a Boolean literal.)
    val tsAttr = guardsBefore.head.condition.asInstanceOf[CdcAssertCommitTimestampNotNull].child
    val analyzedPredicate = guardsBefore.head.condition
    val tsIsNullPlan = Filter(IsNull(tsAttr), analyzed)
    val optimizedTsIsNull = NullPropagation(tsIsNullPlan).asInstanceOf[Filter].condition
    assert(optimizedTsIsNull.isInstanceOf[Literal],
      s"Sanity check: NullPropagation should fold IsNull(non-nullable) to a literal. " +
        s"Got: $optimizedTsIsNull")

    val cdcGuardPlan = Filter(analyzedPredicate, analyzed)
    val optimizedGuard = NullPropagation(cdcGuardPlan).asInstanceOf[Filter].condition
    assert(optimizedGuard.isInstanceOf[CdcAssertCommitTimestampNotNull],
      s"NullPropagation must NOT simplify CdcAssertCommitTimestampNotNull. " +
        s"Got: $optimizedGuard")
  }
}
