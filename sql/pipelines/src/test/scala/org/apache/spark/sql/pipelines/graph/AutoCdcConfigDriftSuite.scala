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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.autocdc.{ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests covering AutoCDC configuration-drift validation for the sequencing result type
 * (SCD1 and SCD2) and the SCD2 track-history column set, validated at flow execution-init time
 * against the auxiliary table's recorded configuration (mirroring
 * [[AutoCdcScd1KeyDriftSuite]] for keys).
 *
 * Guiding principle: guard the invariants that keep already-persisted state coherent, not the
 * expressions themselves. The sequencing expression and delete condition may change across runs;
 * the sequencing result *type* and the SCD2 track-history column *set* may not.
 */
class AutoCdcConfigDriftSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  private def targetName: String =
    fullyQualifiedIdentifier("target", Some(catalog), Some(namespace)).unquotedString

  /** SCD2 target DDL: user columns + the SCD2 framework columns (sequencing type long). */
  private def createScd2Target(userCols: String): Unit = {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target ($userCols, $scd2MetadataDdl)"
    )
  }

  // ===========================================================================================
  // Sequencing type drift
  // ===========================================================================================

  test("AutoCDC source validation uses pipeline case sensitivity, not session default") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val stream = MemoryStream[(Int, Long, Long)]
      stream.addData((1, 1L, 1L))

      val ctx = new TestGraphRegistrationContext(
        spark,
        Map(SQLConf.CASE_SENSITIVE.key -> "true")) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        registerFlow(autoCdcFlow(
          name = "flow",
          target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "version", "__start_at")),
          keys = Seq("id"),
          sequencing = $"version",
          scdType = ScdType.Type2))
      }

      ctx.resolveToDataflowGraph()
    }
  }

  test("an SCD1 flow whose sequencing type differs from the recorded type triggers " +
    "SEQUENCING_TYPE_DRIFT") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, seq_long BIGINT, seq_int INT, $scd1MetadataDdl)"
    )

    // Pipeline #1 sequences by a BIGINT column; aux records sequencingType = long.
    val stream1 = MemoryStream[(Int, Long, Int)]
    stream1.addData((1, 1L, 1))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "seq_long", "seq_int"),
      keys = Seq("id"),
      sequencing = $"seq_long"))

    // Pipeline #2 sequences by an INT column - type drift (int vs long), even though the
    // expression (a different column) is otherwise a legal change.
    val stream2 = MemoryStream[(Int, Long, Int)]
    stream2.addData((1, 2L, 2))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "seq_long", "seq_int"),
      keys = Seq("id"),
      sequencing = $"seq_int")

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedSequencingType" -> "INT",
        "recordedSequencingType" -> "BIGINT"
      )
    )
  }

  test("an SCD1 flow that changes the sequencing expression but keeps the same type does NOT " +
    "trigger drift") {
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, seq BIGINT, $scd1MetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 10L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "seq"),
      keys = Seq("id"),
      sequencing = $"seq"))

    // A different expression over the same column, still yielding BIGINT: legal, no drift.
    val stream2 = MemoryStream[(Int, Long)]
    stream2.addData((1, 20L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "seq"),
      keys = Seq("id"),
      sequencing = $"seq" + 1L))
  }

  test("an SCD2 flow whose sequencing type differs from the recorded type triggers " +
    "SEQUENCING_TYPE_DRIFT") {
    createScd2Target("id INT NOT NULL, seq_long BIGINT, seq_int INT")

    val stream1 = MemoryStream[(Int, Long, Int)]
    stream1.addData((1, 1L, 1))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "seq_long", "seq_int"),
      keys = Seq("id"),
      sequencing = $"seq_long",
      scdType = ScdType.Type2))

    val stream2 = MemoryStream[(Int, Long, Int)]
    stream2.addData((1, 2L, 2))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "seq_long", "seq_int"),
      keys = Seq("id"),
      sequencing = $"seq_int",
      scdType = ScdType.Type2)

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedSequencingType" -> "INT",
        "recordedSequencingType" -> "BIGINT"
      )
    )
  }

  // ===========================================================================================
  // Track-history drift (SCD2 only)
  // ===========================================================================================

  test("an SCD2 flow that changes its explicit TRACK HISTORY column set triggers " +
    "TRACK_HISTORY_DRIFT") {
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    // Pipeline #1 tracks history on `name` only.
    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name"))))))

    // Pipeline #2 tracks history on `amount` - a different set.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("amount")))))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedTrackHistoryColumns" -> "amount",
        "recordedTrackHistoryColumns" -> "name"
      )
    )
  }

  test("an SCD2 flow that reorders the same TRACK HISTORY columns does NOT trigger drift") {
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(ColumnSelection.IncludeColumns(
        Seq(UnqualifiedColumnName("name"), UnqualifiedColumnName("amount"))))))

    // Same set, reversed order: run semantics are order-insensitive, so no drift.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(ColumnSelection.IncludeColumns(
        Seq(UnqualifiedColumnName("amount"), UnqualifiedColumnName("name"))))))
  }

  test("an SCD2 flow with no TRACK HISTORY (default = all eligible columns) followed by an " +
    "explicit selection of that same set does NOT trigger drift") {
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    // Pipeline #1 omits trackHistorySelection: the recorded set is the default, every eligible
    // (non-key, non-framework) column, i.e. name, amount, seq.
    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2))

    // Pipeline #2 explicitly lists that same default set: the drift check compares resolved sets,
    // not user syntax, so an explicit restatement of the default must NOT drift.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(ColumnSelection.IncludeColumns(Seq(
        UnqualifiedColumnName("name"),
        UnqualifiedColumnName("amount"),
        UnqualifiedColumnName("seq"))))))
  }

  test("an SCD2 flow with no TRACK HISTORY (default = all eligible columns) followed by an " +
    "explicit subset triggers TRACK_HISTORY_DRIFT") {
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    // Pipeline #1 records the default set (name, amount, seq).
    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2))

    // Pipeline #2 narrows to an explicit subset (name only): a real change to which transitions
    // open a new record, so it must drift against the recorded default set.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name")))))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedTrackHistoryColumns" -> "name",
        "recordedTrackHistoryColumns" -> "name, amount, seq"
      )
    )
  }

  test("an SCD2 flow's EXCEPT-based TRACK HISTORY followed by an equivalent explicit include " +
    "set does NOT trigger drift") {
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    // Pipeline #1 uses TRACK HISTORY ON * EXCEPT (amount): the resolved set is the eligible
    // columns minus `amount`, i.e. name, seq.
    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("amount"))))))

    // Pipeline #2 states the same set as an explicit include list: EXCLUDE and INCLUDE that
    // resolve to the same set must not drift, since only the resolved set is recorded.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(ColumnSelection.IncludeColumns(
        Seq(UnqualifiedColumnName("name"), UnqualifiedColumnName("seq"))))))
  }

  test("SCD2 track-history drift validation is resolver-aware: a case-only difference does NOT " +
    "trigger drift under the default (case-insensitive) resolver") {
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    // Pipeline #1 records track-history on `name`.
    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name"))))))

    // Pipeline #2 selects `NAME` (different case). The source DF column is still lowercase `name`
    // so it resolves against the schema; only the tracking-column casing differs. Under the
    // default case-insensitive resolver the two sets are equal, so there must be no drift.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection = Some(
        ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("NAME"))))))
  }

  test("an existing SCD2 aux table missing the trackHistoryColumnNames property requires a " +
    "full refresh (AUXILIARY_TABLE_PROPERTY_MISSING)") {
    // Back-compat guard: an SCD2 auxiliary table created before this change carries no
    // trackHistoryColumnNames property. Track-history drift validation surfaces this as a
    // structured AUXILIARY_TABLE_PROPERTY_MISSING (remedy: full refresh) rather than silently
    // skipping the check. Simulate the pre-existing table by unsetting the property after the
    // first run, then run again.
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    val stream = MemoryStream[(Int, String, Int, Long)]
    def buildCtx(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "name", "amount", "seq"),
        keys = Seq("id"),
        sequencing = $"seq",
        scdType = ScdType.Type2)

    stream.addData((1, "a", 10, 1L))
    runPipeline(buildCtx())

    // Drop the property to mimic an aux table materialized before this change.
    spark.sql(
      s"ALTER TABLE ${auxTableNameFor("target")} " +
      s"UNSET TBLPROPERTIES ('${AutoCdcAuxiliaryTable.trackHistoryColumnNamesProperty}')"
    )

    stream.addData((1, "a", 20, 2L))
    val ex = intercept[RuntimeException] { runPipeline(buildCtx()) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "propertyName" -> AutoCdcAuxiliaryTable.trackHistoryColumnNamesProperty
      )
    )
  }

  // ===========================================================================================
  // Sequencing type: SCD2 expression-change symmetry with the SCD1 case above
  // ===========================================================================================

  test("an SCD2 flow that changes the sequencing expression but keeps the same type does NOT " +
    "trigger drift") {
    createScd2Target("id INT NOT NULL, seq BIGINT")

    val stream1 = MemoryStream[(Int, Long)]
    stream1.addData((1, 10L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2))

    // A different expression over the same column, still yielding BIGINT: legal, no drift.
    val stream2 = MemoryStream[(Int, Long)]
    stream2.addData((1, 20L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "seq"),
      keys = Seq("id"),
      sequencing = $"seq" + 1L,
      scdType = ScdType.Type2))
  }

  // ===========================================================================================
  // Intended divergence from SCD1: additive source-schema evolution under default / EXCEPT
  // tracking changes the effective tracked set, and so requires a full refresh.
  // ===========================================================================================

  test("adding a source column under default (all-column) tracking triggers TRACK_HISTORY_DRIFT") {
    // The effective tracked set is derived from the flow's selected source schema, so under default
    // tracking every selected non-key column is tracked. Adding a source column therefore changes
    // the tracked set, which reinterprets which transitions open a new SCD2 record and cannot be
    // applied to already-reconciled history. Unlike SCD1 (where a new nullable column is absorbed
    // by schema evolution), SCD2 requires a full refresh. Pins that intended divergence.
    createScd2Target("id INT NOT NULL, name STRING, seq BIGINT")

    // Run #1: source (id, name, seq); recorded tracked set = {name, seq}.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "a", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2))

    // Run #2: source gains a nullable `city`; default tracking now resolves to {name, city, seq}.
    val stream2 = MemoryStream[(Int, String, String, Long)]
    stream2.addData((1, "a", "nyc", 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "city", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2)

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedTrackHistoryColumns" -> "name, city, seq",
        "recordedTrackHistoryColumns" -> "name, seq"))

    // The drift check runs before the target's schema is evolved, so the rejected run must leave
    // the target untouched: `city` must NOT have been added. (Were it added, the "correct the
    // flow" remedy would then wedge the pipeline on a column-count mismatch during reconciliation.)
    assert(
      !spark.table(s"$catalog.$namespace.target").schema.fieldNames.contains("city"),
      "rejected run must not have evolved the target schema to add `city`")
  }

  test("dropping a source column under default (all-column) tracking triggers " +
    "TRACK_HISTORY_DRIFT") {
    // The mirror of the additive case: removing a selected column shrinks the default tracked set,
    // which is likewise a tracked-set change requiring a full refresh.
    createScd2Target("id INT NOT NULL, name STRING, city STRING, seq BIGINT")

    // Run #1: source (id, name, city, seq); recorded tracked set = {name, city, seq}.
    val stream1 = MemoryStream[(Int, String, String, Long)]
    stream1.addData((1, "a", "nyc", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "city", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2))

    // Run #2: `city` dropped from the source; default tracking now resolves to {name, seq}.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((1, "a", 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2)

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedTrackHistoryColumns" -> "name, seq",
        "recordedTrackHistoryColumns" -> "name, city, seq"))
  }

  test("dropping the target (but not the auxiliary table) between runs still detects drift") {
    // A user drops and recreates the target to reset it, but does not know to also drop the
    // internal auxiliary table. On the next run the target is absent (so it is re-created), but the
    // stale auxiliary table survives with the old recorded configuration. Drift validation reads
    // the auxiliary table, so it must still fire regardless of the target's existence -- otherwise
    // the aux table's additive evolve would silently overwrite the recorded track-history property
    // with the new run's value. Mirrors AutoCdcScd1AuxiliaryTableDurabilitySuite's
    // "auxiliary table is dropped between runs" case, with the two tables swapped.
    createScd2Target("id INT NOT NULL, name STRING, amount INT, seq BIGINT")

    // Run #1: track history on `name`; records trackHistoryColumnNames = [name] on the aux table.
    val stream1 = MemoryStream[(Int, String, Int, Long)]
    stream1.addData((1, "a", 10, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection =
        Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name"))))))

    // Drop ONLY the target; the auxiliary table survives with its recorded config.
    spark.sql(s"DROP TABLE $catalog.$namespace.target")
    assert(spark.catalog.tableExists(auxTableNameFor("target")),
      "auxiliary table should survive dropping the target")

    // Run #2: track history on `amount` instead -- a changed tracked set.
    // This will recreate the target.
    val stream2 = MemoryStream[(Int, String, Int, Long)]
    stream2.addData((1, "a", 20, 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "name", "amount", "seq"),
      keys = Seq("id"),
      sequencing = $"seq",
      scdType = ScdType.Type2,
      trackHistorySelection =
        Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("amount")))))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" -> targetName,
        "expectedTrackHistoryColumns" -> "amount",
        "recordedTrackHistoryColumns" -> "name"))
  }
}
