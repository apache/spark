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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.autocdc.{ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering SCD Type 2 AutoCDC's interaction with non-key schema evolution across pipeline
 * runs. The SCD2 analog of [[AutoCdcScd1SchemaEvolutionSuite]]; documents the supported additive
 * cases (new top-level columns, a new field inside an array<struct> element, broadening column
 * selection) and the cases that fail loudly (incompatible type changes, case-only renames).
 *
 * Unlike SCD1, an SCD2 upsert to an existing key does not overwrite the row: it closes the prior
 * record and opens a new one, so evolution assertions carry the full interval history.
 *
 * Scope notes -- cases intentionally covered elsewhere rather than duplicated here:
 *   - The *narrowing* / dropped-column cases (a microbatch narrower than the already-evolved
 *     target, incl. dropped nested struct/array fields) live in [[AutoCdcScd2ColumnEvolutionSuite]]
 *     under SPARK-58418, which makes them reconcile correctly.
 *   - Changing `trackHistorySelection` between runs -- the SCD2-only evolution axis, which decides
 *     whether an upsert opens a new record -- is also exercised end-to-end in
 *     [[AutoCdcScd2ColumnEvolutionSuite]], so it is not repeated here.
 *
 * Two SCD1 evolution cases have no SCD2 analog and so are absent here by design: "extra columns on
 * the target that the AutoCDC flow does not emit are preserved" relies on SCD1's in-place overwrite
 * (an SCD2 upsert instead reads unemitted target columns as NULL onto the newly-opened record), and
 * there is no SCD2-specific preservation invariant to assert.
 */
class AutoCdcScd2SchemaEvolutionSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  /** The SCD2 target's `_cdc_metadata` struct value for a given recordStartAt. */
  private def scd2Meta(recordStartAt: Long): Row = Row(recordStartAt)

  test("a nullable non-key column merges correctly with mixed NULL and non-NULL values") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, email STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, Option[String], Long)]
    def buildCtx(): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "name", "email", "version"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)

    // Run #1: insert with NULL email opens a current record.
    stream.addData((1, "alice", None, 1L))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", null, 1L, 1L, null, scd2Meta(1L)))
    )

    // Run #2: upsert with non-NULL email at higher seq closes the prior record and opens a new one.
    stream.addData((1, "alice2", Some("a@x.com"), 2L))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", null, 1L, 1L, 2L, scd2Meta(1L)),
        Row(1, "alice2", "a@x.com", 2L, 2L, null, scd2Meta(2L))
      )
    )
  }

  test("widening a non-key column's type between runs fails with " +
    "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, age INT, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Int, Long)]
    stream1.addData((1, 30, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "age", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2))

    // Run #2: widen `age` from Int to Long.
    val stream2 = MemoryStream[(Int, Long, Long)]
    stream2.addData((1, 31L, 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "age", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2)
    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      sqlState = Some("42825"),
      parameters = Map(
        "left" -> "\"INT\"",
        "right" -> "\"BIGINT\""
      )
    )
  }

  test("narrowing a non-key column's type between runs fails with " +
    "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, payload BIGINT, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long, Long)]
    stream1.addData((1, 100L, 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream1.toDF().toDF("id", "payload", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2))

    // Run #2: narrow `payload` from Long (BIGINT) to Int (INT).
    val stream2 = MemoryStream[(Int, Int, Long)]
    stream2.addData((1, 5, 2L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream2.toDF().toDF("id", "payload", "version"),
      keys = Seq("id"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2)

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      sqlState = Some("42825"),
      parameters = Map(
        "left" -> "\"BIGINT\"",
        "right" -> "\"INT\""
      )
    )
  }

  test("a new top-level nullable column appearing in the source DF between runs is " +
    "added to the target") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, Option[String], Long)]
    def buildCtx(includeEmail: Boolean): TestGraphRegistrationContext = {
      val sourceDf = stream.toDF().toDF("id", "name", "email", "version")
      val projectedDf = if (includeEmail) sourceDf else sourceDf.drop("email")
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = projectedDf,
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)
    }

    // Run #1: source projects (id, name, version). Target schema is unchanged.
    stream.addData((1, "alice", None, 1L))
    runPipeline(buildCtx(includeEmail = false))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, 1L, null, scd2Meta(1L)))
    )

    // Run #2: source projects (id, name, email, version) for a new key id=2. mergeSchemas appends
    // `email` after the framework columns; the existing id=1 row gets NULL for the new column.
    stream.addData((2, "bob", Some("b@x.com"), 2L))
    runPipeline(buildCtx(includeEmail = true))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, 1L, null, scd2Meta(1L), null),
        Row(2, "bob", 2L, 2L, null, scd2Meta(2L), "b@x.com")
      )
    )
  }

  test("additive target-column evolution extends the SCD2 auxiliary table schema") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Shared (id, name, version) stream; run #1 projects away `name`, run #2 keeps it so the
    // target (and, unlike SCD1, the aux table -- which mirrors the full target row) gain `name`.
    val stream = MemoryStream[(Int, String, Long)]
    def buildCtx(includeName: Boolean): TestGraphRegistrationContext = {
      val sourceDf = stream.toDF().toDF("id", "name", "version")
      val projectedDf = if (includeName) sourceDf else sourceDf.drop("name")
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = projectedDf,
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)
    }

    // Run #1: target is (id, version, framework); aux mirrors it plus the marker.
    stream.addData((1, "ignored", 1L))
    runPipeline(buildCtx(includeName = false))
    val auxAfterRun1 = spark.table(auxTableNameFor("target")).schema.fieldNames.toSeq
    assert(!auxAfterRun1.contains("name"),
      s"aux schema after run #1 should not yet contain `name`; got $auxAfterRun1")

    // Run #2: `name` is added to the target for a new key id=2. The SCD2 aux table mirrors the
    // full target row schema, so it gains `name` too (unlike SCD1, whose aux holds only keys +
    // metadata and is unaffected by non-key evolution).
    stream.addData((2, "bob", 2L))
    runPipeline(buildCtx(includeName = true))

    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, 1L, 1L, null, scd2Meta(1L), null),
        Row(2, 2L, 2L, null, scd2Meta(2L), "bob")
      )
    )
    assert(spark.table(auxTableNameFor("target")).schema.fieldNames.contains("name"))
  }

  test("a new field added inside an array<struct> element between runs is added to the " +
    "target") {
    val session = spark
    import session.implicits._

    // SCD2 analog of AutoCdcScd1SchemaEvolutionSuite's array<struct> additive case: unlike the
    // top-level scalar additions above, this exercises unionByName / mergeSchemas recursing into
    // an array element struct. Unlike SCD1's overwrite-in-place, the SCD2 upsert closes the prior
    // record (which never saw `vals.element.b.d`, so it reads NULL there) and opens a new one
    // carrying the widened value.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(key INT NOT NULL, version BIGINT NOT NULL, " +
      s"vals ARRAY<STRUCT<a:INT,b:STRUCT<c:INT>>>, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(Int, Long, Int, Int, Int)]
    def buildCtx(includeD: Boolean): TestGraphRegistrationContext = {
      val src = stream.toDF().toDF("key", "version", "a", "b_c", "b_d")
      val inner = if (includeD) {
        functions.struct(functions.col("b_c").as("c"), functions.col("b_d").as("d"))
      } else {
        functions.struct(functions.col("b_c").as("c"))
      }
      val projected = src.select(
        functions.col("key"),
        functions.col("version"),
        functions.array(
          functions.struct(functions.col("a"), inner.as("b"))
        ).as("vals")
      )
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = projected,
        keys = Seq("key"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)
    }

    // Run #1: element struct is (a, b.c); no b.d yet. Opens key=1's current record at version=1.
    stream.addData((1, 1L, 1, 1, 99))
    runPipeline(buildCtx(includeD = false))

    // Run #2 widens the element struct with b.d. The version=2 upsert to key=1 closes its
    // version=1 record (which predates b.d, so reads NULL) and opens a new one with b.d=2; the
    // new key=3 lands as an open record with the full widened struct.
    stream.addData((1, 2L, 1, 1, 2), (3, 1L, 3, 3, 3))
    runPipeline(buildCtx(includeD = true))

    // Inline-explode flattens the array<struct>; carry the interval bounds to prove the closed
    // prior record reads NULL for the newly-added nested field.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target")
        .selectExpr("key", "__START_AT", "__END_AT", "inline(vals) as (a, b)")
        .select("key", "__START_AT", "__END_AT", "a", "b.c", "b.d"),
      Seq(
        Row(1, 1L, 2L, 1, 1, null),
        Row(1, 2L, null, 1, 1, 2),
        Row(3, 1L, null, 3, 3, 3)
      )
    )
  }

  test("broadening the column selection between runs adds the newly-included column to " +
    "the target") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, String, Long)]
    def buildCtx(selection: Option[ColumnSelection]): TestGraphRegistrationContext =
      singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = stream.toDF().toDF("id", "name", "email", "version"),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        columnSelection = selection,
        scdType = ScdType.Type2)

    // Run #1: only (id, name, version) selected; `email` is dropped before the MERGE.
    stream.addData((1, "alice", "ignored", 1L))
    runPipeline(buildCtx(selection = Some(ColumnSelection.IncludeColumns(
      Seq("id", "name", "version").map(UnqualifiedColumnName(_))
    ))))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, 1L, null, scd2Meta(1L)))
    )

    // Run #2: broaden to no selection for a new key id=2. mergeSchemas adds `email`; the existing
    // id=1 row gets NULL, the new row gets the actual value.
    stream.addData((2, "bob", "b@x.com", 2L))
    runPipeline(buildCtx(selection = None))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, 1L, null, scd2Meta(1L), null),
        Row(2, "bob", 2L, 2L, null, scd2Meta(2L), "b@x.com")
      )
    )
  }

  test("a source DF column whose name differs from the target only by case fails with " +
    "COLUMN_ALREADY_EXISTS under case-insensitive resolution") {
    val session = spark
    import session.implicits._

    // This pins current (buggy) behavior, tracked by SPARK-58517: DatasetManager's schema
    // evolution ignores spark.sql.caseSensitive and merges case-sensitively, so a target `value`
    // and a source `Value` are wrongly treated as distinct and the target is evolved to carry both
    // columns -- a schema self-inconsistent under the (default) case-insensitive resolver. The
    // failure then surfaces during microbatch reconciliation, not at table/aux creation:
    // Scd2ForeachBatchHandler.reconcileMicrobatch reads that now-two-column target back and folds
    // it into the affected-rows `unionByName`; ResolveUnion runs a case-insensitive
    // duplicate-column check over the union's schema and reports COLUMN_ALREADY_EXISTS. SCD1 has no
    // such reconcile union and instead surfaces AMBIGUOUS_REFERENCE deeper in the MERGE plan -- so
    // the same user mistake maps to two different conditions across the SCD types. Once SPARK-58517
    // is fixed the merge should be a no-op (source `Value` maps onto target `value`) and this test
    // should be updated to assert success.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      spark.sql(
        s"CREATE TABLE $catalog.$namespace.target " +
        s"(key INT NOT NULL, version BIGINT NOT NULL, value STRING, $scd2MetadataDdl)"
      )

      val stream = MemoryStream[(Int, Long, String)]
      stream.addData((1, 1L, "alice"))
      val df = stream.toDF().toDF("key", "version", "Value")
      val ctx = singleAutoCdcFlowPipeline(
        flowName = "auto_cdc_flow",
        target = "target",
        sourceDf = df,
        keys = Seq("key"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2)

      val ex = intercept[RuntimeException] { runPipeline(ctx) }
      checkErrorInPipelineFailure(
        failure = ex,
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`value`")
      )
    }
  }

  test("changing a non-key column type from TIMESTAMP to STRING between runs fails with " +
    "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(key INT NOT NULL, version BIGINT NOT NULL, value TIMESTAMP, $scd2MetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long, Timestamp)]
    stream1.addData((1, 1L, Timestamp.valueOf("2024-01-01 10:00:00")))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream1.toDF().toDF("key", "version", "value"),
      keys = Seq("key"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2))

    // Run #2 emits `value` as STRING. mergeSchemas rejects the type change.
    val stream2 = MemoryStream[(Int, Long, String)]
    stream2.addData((1, 2L, "2024-01-02 11:00:00"))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = stream2.toDF().toDF("key", "version", "value"),
      keys = Seq("key"),
      sequencing = functions.col("version"),
      scdType = ScdType.Type2)

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      sqlState = Some("42825"),
      parameters = Map(
        "left" -> "\"TIMESTAMP\"",
        "right" -> "\"STRING\""
      )
    )
  }
}
