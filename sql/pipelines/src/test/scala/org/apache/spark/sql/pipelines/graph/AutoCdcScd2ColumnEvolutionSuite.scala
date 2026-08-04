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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.pipelines.autocdc.{ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for SCD Type 2 AutoCDC column-schema evolution across runs: a microbatch that
 * is narrower than the already-evolved target (a source column dropped, a nested struct/array field
 * dropped, or the `COLUMNS` selection narrowed) must reconcile correctly instead of failing the
 * internal union.
 *
 * These exercise the fix for SPARK-58418. Before it, `Scd2ForeachBatchHandler.reconcileMicrobatch`
 * unioned the microbatch with the affected target/aux rows without `allowMissingColumns`, so a
 * narrower microbatch failed with NUM_COLUMNS_MISMATCH (top-level) or INCOMPATIBLE_COLUMN_TYPE
 * (nested). The contract asserted here is additive-tolerant: records already written keep their
 * values for the no-longer-emitted column, and only records opened by the narrower microbatch carry
 * null for it.
 *
 * This matches SCD1's behavior for a dropped top-level column
 * ([[AutoCdcScd1SchemaEvolutionSuite]]). SCD2 applies the same behavior to a dropped *nested*
 * struct/array field, where SCD1 instead fails with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA:
 * SCD1's MERGE source is missing the nested field and the v2 writer's resolver rejects it, whereas
 * SCD2's `allowMissingColumns` pads the field before the union/MERGE. So SCD2 handles nested
 * subtractive evolution consistently with the top-level case (and with SCD1's top-level case),
 * rather than reproducing SCD1's nested-drop limitation.
 *
 * Changing the effective *tracked-history* column set is a distinct, separately-scoped concern
 * (SPARK-58452 / SPARK-58391) and is deliberately not exercised here: every scenario keeps the
 * effective tracked set unchanged across runs, so the only thing evolving is the set of user
 * columns the flow emits. Each scenario tracks history explicitly on `name` and drops the
 * non-tracked `email`, so the tracked set ({name}) is unchanged and these stay valid once the
 * track-history drift guard (SPARK-58391) lands.
 */
class AutoCdcScd2ColumnEvolutionSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  /** The SCD2 target's `_cdc_metadata` struct value for a given recordStartAt. */
  private def scd2Meta(recordStartAt: Long): Row = Row(recordStartAt)

  /** An explicit SCD2 `TRACK HISTORY ON (name)` selection, shared across the scenarios below. */
  private val trackName: Option[ColumnSelection] =
    Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name"))))

  test("a source column dropped between runs is preserved on existing records and null on new " +
    "ones") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, email STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Shared stream; run #2 projects `email` away so the microbatch is narrower than the target.
    // Track history on `name` only, so the dropped `email` is a non-tracked column and dropping it
    // is column-schema narrowing rather than a tracked-set change (robust once SPARK-58391 lands).
    val stream = MemoryStream[(Int, String, String, Long)]
    def buildCtx(includeEmail: Boolean): TestGraphRegistrationContext = {
      val df = stream.toDF().toDF("id", "name", "email", "version")
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = if (includeEmail) df else df.drop("email"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2,
        trackHistorySelection = trackName)
    }

    // Run #1 (wide): key=1 opens a record carrying email=a@x.
    stream.addData((1, "alice", "a@x", 1L))
    runPipeline(buildCtx(includeEmail = true))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", "a@x", 1L, 1L, null, scd2Meta(1L)))
    )

    // Run #2 (narrow): update key=1 (closes its record) + insert key=2. The dropped `email` is
    // preserved on key=1's now-closed record and is null on the newly-opened records.
    stream.addData((1, "alice2", "ignored", 2L), (2, "bob", "ignored", 1L))
    runPipeline(buildCtx(includeEmail = false))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", "a@x", 1L, 1L, 2L, scd2Meta(1L)),
        Row(1, "alice2", null, 2L, 2L, null, scd2Meta(2L)),
        Row(2, "bob", null, 1L, 1L, null, scd2Meta(1L))
      )
    )
  }

  test("narrowing the COLUMNS selection to drop a non-tracked column preserves it on existing " +
    "records and leaves it null on new ones") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, email STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // The source DF is fixed at (id, name, email, version) across both runs; only the flow's
    // `columnSelection` narrows. Tracking history on `name` only makes `email` a
    // selected-but-not-tracked column, so dropping it from the selection is pure column-schema
    // narrowing -- the effective tracked set ({name}) is unchanged, so this stays column evolution
    // rather than a tracked-set change even once the track-history drift guard (SPARK-58391) lands.
    val stream = MemoryStream[(Int, String, String, Long)]
    def buildCtx(selection: Option[ColumnSelection]): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "name", "email", "version"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        columnSelection = selection,
        scdType = ScdType.Type2,
        trackHistorySelection = trackName)

    // Run #1: no selection (all columns); key=1 carries email=a@x.
    stream.addData((1, "alice", "a@x", 1L))
    runPipeline(buildCtx(selection = None))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", "a@x", 1L, 1L, null, scd2Meta(1L)))
    )

    // Run #2: narrow the selection to (id, name, version), dropping the non-tracked `email`.
    // Because `name` (the sole tracked column) changes, this opens a new record; key=1's closed
    // record keeps a@x, and the new records carry null.
    stream.addData((1, "alice2", "ignored", 2L), (2, "bob", "ignored", 1L))
    runPipeline(buildCtx(selection = Some(ColumnSelection.IncludeColumns(
      Seq("id", "name", "version").map(UnqualifiedColumnName(_))
    ))))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", "a@x", 1L, 1L, 2L, scd2Meta(1L)),
        Row(1, "alice2", null, 2L, 2L, null, scd2Meta(2L)),
        Row(2, "bob", null, 1L, 1L, null, scd2Meta(1L))
      )
    )
  }

  test("a late narrower event weaves into history without rewriting existing records") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, email STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Track history on `name` only, so dropping `email` is column narrowing with an unchanged
    // tracked set (robust once SPARK-58391 lands).
    val stream = MemoryStream[(Int, String, String, Long)]
    def buildCtx(includeEmail: Boolean): TestGraphRegistrationContext = {
      val df = stream.toDF().toDF("id", "name", "email", "version")
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = if (includeEmail) df else df.drop("email"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2,
        trackHistorySelection = trackName)
    }

    // Run #1 (wide): two distinct-name records for key=1 at seq 10 and 30.
    stream.addData((1, "alice", "a@x", 10L), (1, "alicia", "b@x", 30L))
    runPipeline(buildCtx(includeEmail = true))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", "a@x", 10L, 10L, 30L, scd2Meta(10L)),
        Row(1, "alicia", "b@x", 30L, 30L, null, scd2Meta(30L))
      )
    )

    // Run #2 (narrow): a late event at seq=20 with a new name bisects the seq=10 record. The
    // pre-existing records keep their email values; the newly-inserted seq=20 record has null.
    stream.addData((1, "annie", "ignored", 20L))
    runPipeline(buildCtx(includeEmail = false))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", "a@x", 10L, 10L, 20L, scd2Meta(10L)),
        Row(1, "annie", null, 20L, 20L, 30L, scd2Meta(20L)),
        Row(1, "alicia", "b@x", 30L, 30L, null, scd2Meta(30L))
      )
    )
  }

  test("a nested struct field dropped between runs is preserved on existing records and null on " +
    "new ones (SCD2 is more permissive than SCD1 here)") {
    val session = spark
    import session.implicits._

    // Contrast with SCD1: AutoCdcScd1SchemaEvolutionSuite rejects this exact shape with
    // INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA, because its MERGE source is missing `value.b.c`
    // and the v2 writer's resolver cannot find data for the target's nested field. SCD2's
    // `allowMissingColumns` pads the missing field before the union/MERGE, so the nested drop is
    // handled the same additive-tolerant way as a top-level drop -- preserved on existing records,
    // null on new ones. This is a deliberate, consistency-improving divergence from SCD1, not
    // parity: SCD1's nested-drop failure is a writer limitation, not an intended policy.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, " +
      s"value STRUCT<a:INT,b:STRUCT<c:INT,d:INT>>, $scd2MetadataDdl)"
    )

    // Default tracking is fine here: the tracked set is a set of top-level column *names*, and only
    // the nested shape of `value` changes across runs -- the top-level name `value` is retained --
    // so the effective tracked set ({value}) is unchanged and this stays column evolution, not a
    // tracked-set change, even once SPARK-58391 lands.
    val stream = MemoryStream[(Int, Long, Int, Int, Int)]
    def buildCtx(includeC: Boolean): TestGraphRegistrationContext = {
      val src = stream.toDF().toDF("id", "version", "a", "b_c", "b_d")
      val inner = if (includeC) {
        functions.struct(functions.col("b_c").as("c"), functions.col("b_d").as("d"))
      } else {
        functions.struct(functions.col("b_d").as("d"))
      }
      val projected = src.select(
        functions.col("id"),
        functions.col("version"),
        functions.struct(functions.col("a"), inner.as("b")).as("value")
      )
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = projected,
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)
    }

    // Run #1 (wide): value.b carries both c and d for key=1.
    stream.addData((1, 1L, 1, 10, 100))
    runPipeline(buildCtx(includeC = true))

    // Run #2 (narrow): value.b drops `c`. The reconciliation union no longer fails; key=1's closed
    // record keeps c=10, and the newly-opened record has c=null (d flows through).
    stream.addData((1, 2L, 2, 99, 200))
    runPipeline(buildCtx(includeC = false))

    checkAnswer(
      spark.table(s"$catalog.$namespace.target").selectExpr(
        "id", "version", "value.a", "value.b.c", "value.b.d", "__START_AT", "__END_AT"),
      Seq(
        Row(1, 1L, 1, 10, 100, 1L, 2L),
        Row(1, 2L, 2, null, 200, 2L, null)
      )
    )
  }

  test("a field dropped inside an array<struct> element between runs is preserved on existing " +
    "records and null on new ones (SCD2 is more permissive than SCD1 here)") {
    val session = spark
    import session.implicits._

    // The array<struct> analog of the nested-struct-drop test above, and the counterpart to
    // AutoCdcScd1SchemaEvolutionSuite's array<struct> case, which fails with
    // INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA on `vals.element.b.d`. SCD2's
    // allowMissingColumns recurses into arrays as well as structs, so it pads the dropped element
    // field before the union/MERGE and reconciles additively. Default tracking is fine: the tracked
    // set is the top-level name `vals`, unchanged when only a nested element field is dropped.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, " +
      s"vals ARRAY<STRUCT<a:INT,b:STRUCT<c:INT,d:INT>>>, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(Int, Long, Int, Int, Int)]
    def buildCtx(includeD: Boolean): TestGraphRegistrationContext = {
      val src = stream.toDF().toDF("id", "version", "a", "b_c", "b_d")
      val inner = if (includeD) {
        functions.struct(functions.col("b_c").as("c"), functions.col("b_d").as("d"))
      } else {
        functions.struct(functions.col("b_c").as("c"))
      }
      val projected = src.select(
        functions.col("id"),
        functions.col("version"),
        functions.array(
          functions.struct(functions.col("a"), inner.as("b"))
        ).as("vals")
      )
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = projected,
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)
    }

    // Run #1 (wide): vals[0].b carries both c and d for key=1.
    stream.addData((1, 1L, 1, 10, 100))
    runPipeline(buildCtx(includeD = true))

    // Run #2 (narrow): drop `d` from the element struct. The union pads the missing nested element
    // field; key=1's closed record keeps d=100, and the newly-opened record has d=null.
    stream.addData((1, 2L, 2, 200, 99))
    runPipeline(buildCtx(includeD = false))

    checkAnswer(
      spark.table(s"$catalog.$namespace.target")
        .selectExpr("id", "version", "inline(vals) as (a, b)", "__START_AT", "__END_AT")
        .selectExpr("id", "version", "a", "b.c", "b.d", "__START_AT", "__END_AT"),
      Seq(
        Row(1, 1L, 1, 10, 100, 1L, 2L),
        Row(1, 2L, 2, 200, null, 2L, null)
      )
    )
  }
}
