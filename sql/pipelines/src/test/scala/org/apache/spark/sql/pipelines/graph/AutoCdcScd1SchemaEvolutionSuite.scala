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
import org.apache.spark.sql.pipelines.autocdc.{
  ColumnSelection,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering AutoCDC's interaction with schema evolution across pipeline runs. The
 * suite documents the supported additive cases (new top-level columns, new nested fields
 * in array-of-struct, broadening / narrowing column selection) and the cases that fail
 * loudly today (subtractive nested evolution, type-incompatible changes, case-only
 * renames).
 *
 * These behaviors are largely inherited from the lower layers (`SchemaMergingUtils` for
 * schema merge, the v2 writer's column-resolution layer for nested-field handling) rather
 * than implemented in AutoCDC itself; the tests here serve as the contract for AutoCDC's
 * observable behavior on top of those layers.
 */
class AutoCdcScd1SchemaEvolutionSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("a nullable non-key column merges correctly with mixed NULL and non-NULL values") {
    val session = spark
    import session.implicits._

    // Single MemoryStream with `email` as nullable from the start. Run #1 emits a row with
    // a NULL email; run #2 emits an upsert with a non-NULL email.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, email STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, Option[String], Long)]
    def buildCtx(): TestGraphRegistrationContext = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "email", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }

    // Run #1: insert with NULL email.
    stream.addData((1, "alice", None, 1L))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", null, 1L, cdcMeta(None, Some(1L))))
    )

    // Run #2: upsert with non-NULL email at higher seq replaces the row.
    stream.addData((1, "alice2", Some("a@x.com"), 2L))
    runPipeline(buildCtx())
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice2", "a@x.com", 2L, cdcMeta(None, Some(2L))))
    )
  }

  test("widening a non-key column's type between runs fails with " +
    "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") {
    val session = spark
    import session.implicits._

    // Changing a non-key column's type between pipeline runs is rejected by
    // `SchemaMergingUtils` with CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE even when the new type
    // is strictly wider. Users must full-refresh the target to change column types.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, age INT, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Int, Long)]
    stream1.addData((1, 30, 1L))
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream1.toDF().toDF("id", "age", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx1)

    // Run #2: widen `age` from Int to Long.
    val stream2 = MemoryStream[(Int, Long, Long)]
    stream2.addData((1, 31L, 2L))
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream2.toDF().toDF("id", "age", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }
    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      sqlState = Some("42825"),
      // `left` is the persisted (run #1) INT type; `right` is run #2's widened BIGINT.
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

    // Mirror image of the widening test above: changing a non-key column's type between
    // pipeline runs is rejected by SchemaMergingUtils with CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE
    // even when the new type is strictly narrower.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, payload BIGINT, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long, Long)]
    stream1.addData((1, 100L, 1L))
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream1.toDF().toDF("id", "payload", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx1)

    // Run #2: narrow `payload` from Long (BIGINT) to Int (INT).
    val stream2 = MemoryStream[(Int, Int, Long)]
    stream2.addData((1, 5, 2L))
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream2.toDF().toDF("id", "payload", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      sqlState = Some("42825"),
      // `left` is the persisted (run #1) BIGINT type; `right` is run #2's narrowed INT.
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
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Single MemoryStream of (id, name, email, version) shared across runs so the streaming
    // checkpoint can resume cleanly. Run #1's flow drops `email` so the source's resolved DF
    // schema is 3 columns; run #2 keeps all 4. The MemoryStream's underlying tuple schema is
    // unchanged (only the downstream projection differs), so the source identity that the
    // OffsetSeqLog records is stable across runs.
    val stream = MemoryStream[(Int, String, Option[String], Long)]
    def buildCtx(includeEmail: Boolean): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        val sourceDf = stream.toDF().toDF("id", "name", "email", "version")
        val projectedDf = if (includeEmail) sourceDf else sourceDf.drop("email")
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(projectedDf),
          keys = Seq("id"),
          sequencing = functions.col("version")
        ))
      }

    // Run #1: source projects (id, name, version). Target schema is unchanged.
    stream.addData((1, "alice", None, 1L))
    runPipeline(buildCtx(includeEmail = false))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, cdcMeta(None, Some(1L))))
    )

    // Run #2: source projects (id, name, email, version). mergeSchemas appends `email` to
    // the target (StructType.merge keeps the left schema's order and appends right-only
    // fields); existing rows get NULL for the new column.
    stream.addData((2, "bob", Some("b@x.com"), 2L))
    runPipeline(buildCtx(includeEmail = true))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(None, Some(1L)), null),
        Row(2, "bob", 2L, cdcMeta(None, Some(2L)), "b@x.com")
      )
    )
  }

  test("broadening the column selection between runs adds the newly-included column to " +
    "the target") {
    val session = spark
    import session.implicits._

    // Source DF schema is fixed at (id, name, email, version) across both runs. Only the
    // `columnSelection` knob differs: run #1 includes (id, name, version); run #2 selects
    // None (= all source columns). mergeSchemas adds `email` to the target via the same
    // generic SDP path as the new-source-column case, but driven by the
    // [[ColumnSelection]] knob rather than the source DF's own schema.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, String, Long)]
    def buildCtx(selection: Option[ColumnSelection]): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "name", "email", "version")),
          keys = Seq("id"),
          sequencing = functions.col("version"),
          columnSelection = selection
        ))
      }

    // Run #1: only (id, name, version) selected; `email` is dropped before the MERGE.
    stream.addData((1, "alice", "ignored", 1L))
    runPipeline(buildCtx(selection = Some(ColumnSelection.IncludeColumns(
      Seq("id", "name", "version").map(UnqualifiedColumnName(_))
    ))))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, cdcMeta(None, Some(1L))))
    )

    // Run #2: broaden to no selection. mergeSchemas adds `email`; existing rows get NULL,
    // new rows get the actual value.
    stream.addData((2, "bob", "b@x.com", 2L))
    runPipeline(buildCtx(selection = None))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(None, Some(1L)), null),
        Row(2, "bob", 2L, cdcMeta(None, Some(2L)), "b@x.com")
      )
    )
  }

  test("narrowing the column selection between runs preserves the dropped column on " +
    "existing rows and leaves it NULL on new rows") {
    val session = spark
    import session.implicits._

    // Validates the additive-only column-selection contract on the narrowing side:
    // tightening `columnSelection` between runs leaves the dropped column in place at the
    // schema level (SDP's `SchemaMergingUtils.mergeSchemas` is a union, never a subtraction).
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, email STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, String, Long)]
    def buildCtx(selection: Option[ColumnSelection]): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(stream.toDF().toDF("id", "name", "email", "version")),
          keys = Seq("id"),
          sequencing = functions.col("version"),
          columnSelection = selection
        ))
      }

    // Run #1: include all columns; populate `email` for key=1.
    stream.addData((1, "alice", "a@x.com", 1L))
    runPipeline(buildCtx(selection = None))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", "a@x.com", 1L, cdcMeta(None, Some(1L))))
    )

    // Run #2: narrow the selection to drop `email`. The merge omits `email` from both
    // INSERT and UPDATE assignment maps; key=1's `email` is preserved at "a@x.com" while
    // key=2 is inserted with `email = NULL`.
    stream.addData((2, "bob", "ignored", 2L))
    runPipeline(buildCtx(selection = Some(ColumnSelection.IncludeColumns(
      Seq("id", "name", "version").map(UnqualifiedColumnName(_))
    ))))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", "a@x.com", 1L, cdcMeta(None, Some(1L))),
        Row(2, "bob", null, 2L, cdcMeta(None, Some(2L)))
      )
    )
  }

  test("a top-level column dropped from the source DF between runs is preserved on " +
    "existing rows and left NULL on new rows") {
    val session = spark
    import session.implicits._

    // Symmetric to the new-source-column case (which exercises the source DF *gaining* a
    // column). Validates that the additive-only column-selection contract holds when the
    // narrowing is driven by the source DF's own schema shrinking, rather than by a
    // tightening [[ChangeArgs.columnSelection]].
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Same `MemoryStream[(Int, String, Option[String], Long)]` shape across runs; runs
    // differ in whether `email` is kept in the projected source DF.
    val stream = MemoryStream[(Int, String, Option[String], Long)]
    def buildCtx(includeEmail: Boolean): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        val sourceDf = stream.toDF().toDF("id", "name", "email", "version")
        val projectedDf = if (includeEmail) sourceDf else sourceDf.drop("email")
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(projectedDf),
          keys = Seq("id"),
          sequencing = functions.col("version")
        ))
      }

    // Run #1: wide source DF (id, name, email, version). mergeSchemas appends `email` to
    // the target.
    stream.addData((1, "alice", Some("a@x.com"), 1L))
    runPipeline(buildCtx(includeEmail = true))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, cdcMeta(None, Some(1L)), "a@x.com"))
    )

    // Run #2: source DF drops `email` upstream of the flow. Target still has `email`
    // (`StructType.merge` is additive-only); the merge omits `email` from both INSERT and
    // UPDATE assignment maps. Key=1's `email` is preserved at "a@x.com"; key=2 is inserted
    // with `email = NULL`.
    stream.addData((2, "bob", None, 2L))
    runPipeline(buildCtx(includeEmail = false))
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(None, Some(1L)), "a@x.com"),
        Row(2, "bob", 2L, cdcMeta(None, Some(2L)), null)
      )
    )
  }

  test("dropping a nested struct field between runs fails with INCOMPATIBLE_DATA_FOR_TABLE") {
    val session = spark
    import session.implicits._

    // The v2 writer's column-resolution layer requires every nested target field to be
    // present in the microbatch DF. When run #2's source projection drops `b.c`, the merge
    // fails with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA. Users who want to drop a
    // nested field between runs must full-refresh the target.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(key INT NOT NULL, version BIGINT NOT NULL, " +
      s"value STRUCT<a:INT,b:STRUCT<c:INT,d:INT>>, $cdcMetadataDdl)"
    )

    // Stream is (key, version, a, b_c, b_d). Each run reshapes into different `value`
    // shapes; the underlying tuple shape is unchanged so the streaming source's identity
    // is stable across runs.
    val stream = MemoryStream[(Int, Long, Int, Int, Int)]
    def buildCtx(includeC: Boolean): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        val src = stream.toDF().toDF("key", "version", "a", "b_c", "b_d")
        val inner = if (includeC) {
          functions.struct(functions.col("b_c").as("c"), functions.col("b_d").as("d"))
        } else {
          functions.struct(functions.col("b_d").as("d"))
        }
        val projected = src.select(
          functions.col("key"),
          functions.col("version"),
          functions.struct(functions.col("a"), inner.as("b")).as("value")
        )
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(projected),
          keys = Seq("key"),
          sequencing = functions.col("version")
        ))
      }

    stream.addData((1, 1L, 1, 1, 1), (2, 1L, 2, 2, 2))
    runPipeline(buildCtx(includeC = true))

    // Run #2 drops b.c. The v2 writer rejects the merge because it cannot find data for
    // the target's `value.b.c` column.
    stream.addData((1, 2L, 10, 99, 10), (3, 1L, 3, 99, 3))
    val ex = intercept[RuntimeException] { runPipeline(buildCtx(includeC = false)) }
    val all = Iterator(ex) ++ ex.getSuppressed.iterator
    assert(
      all.exists(t => Option(t.getMessage).exists(m =>
        m.contains("INCOMPATIBLE_DATA_FOR_TABLE") && m.contains("value") && m.contains("b") &&
          m.contains("c"))),
      s"Expected INCOMPATIBLE_DATA_FOR_TABLE failure for value.b.c, got: ${ex.getMessage}"
    )
  }

  test("a new field added inside an array<struct> element between runs is added to the " +
    "target") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(key INT NOT NULL, version BIGINT NOT NULL, " +
      s"vals ARRAY<STRUCT<a:INT,b:STRUCT<c:INT>>>, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, Long, Int, Int, Int)]
    def buildCtx(includeD: Boolean): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
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
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(projected),
          keys = Seq("key"),
          sequencing = functions.col("version")
        ))
      }

    stream.addData((1, 1L, 1, 1, 99))
    runPipeline(buildCtx(includeD = false))

    // Run #2 widens to include b.d. Existing key=1 row's vals[0].b.d is NULL until the
    // upsert at version=2 writes the new value.
    stream.addData((1, 2L, 1, 1, 2), (3, 1L, 3, 3, 3))
    runPipeline(buildCtx(includeD = true))

    // Inline-explode flattens the array<struct> for assertion.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target")
        .selectExpr("key", "inline(vals) as (a, b)")
        .select("key", "a", "b.c", "b.d"),
      Seq(
        Row(1, 1, 1, 2),
        Row(3, 3, 3, 3)
      )
    )
  }

  test("dropping a field inside an array<struct> element between runs fails with " +
    "INCOMPATIBLE_DATA_FOR_TABLE") {
    val session = spark
    import session.implicits._

    // Symmetric to the nested-struct case, but for `array<struct>`. The v2 writer rejects
    // the merge because it cannot find data for the target's `vals.element.b.d` column
    // when run #2's projection drops `d` from the element struct. Users must full-refresh
    // the target to drop a nested array-element field.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(key INT NOT NULL, version BIGINT NOT NULL, " +
      s"vals ARRAY<STRUCT<a:INT,b:STRUCT<c:INT,d:INT>>>, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, Long, Int, Int, Int)]
    def buildCtx(includeD: Boolean): TestGraphRegistrationContext =
      new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
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
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(projected),
          keys = Seq("key"),
          sequencing = functions.col("version")
        ))
      }

    stream.addData((1, 1L, 1, 1, 1), (2, 1L, 2, 2, 2))
    runPipeline(buildCtx(includeD = true))

    stream.addData((1, 2L, 10, 10, 99), (3, 1L, 3, 3, 99))
    val ex = intercept[RuntimeException] { runPipeline(buildCtx(includeD = false)) }
    val all = Iterator(ex) ++ ex.getSuppressed.iterator
    assert(
      all.exists(t => Option(t.getMessage).exists(m =>
        m.contains("INCOMPATIBLE_DATA_FOR_TABLE") && m.contains("vals"))),
      s"Expected INCOMPATIBLE_DATA_FOR_TABLE failure for vals element, got: ${ex.getMessage}"
    )
  }

  test("a source DF column whose name differs from the target only by case fails with " +
    "AMBIGUOUS_REFERENCE under case-insensitive resolution") {
    val session = spark
    import session.implicits._

    // `DatasetManager`'s schema-merge compares the existing target schema and the flow's
    // output schema *case-sensitively*: `SchemaMergingUtils.mergeSchemas` calls
    // `StructType.merge` without forwarding the session-level case-sensitivity. When the
    // target has `value` and the source DF emits `Value`, the merged schema ends up with
    // both as separate columns. Reference resolution downstream is case-insensitive
    // (Spark's default), so the MERGE plan trips on the duplicate and reports
    // AMBIGUOUS_REFERENCE.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      spark.sql(
        s"CREATE TABLE $catalog.$namespace.target " +
        s"(key INT NOT NULL, version BIGINT NOT NULL, value STRING, $cdcMetadataDdl)"
      )

      val stream = MemoryStream[(Int, Long, String)]
      stream.addData((1, 1L, "alice"))
      val ctx = new TestGraphRegistrationContext(spark) {
        registerTable("target", catalog = Some(catalog), database = Some(namespace))
        // Source DF emits `Value` (capital), differing only in case from the target's
        // `value` column.
        val df = stream.toDF().toDF("key", "version", "Value")
        registerFlow(autoCdcFlow(
          name = "auto_cdc_flow",
          target = "target",
          query = dfFlowFunc(df),
          keys = Seq("key"),
          sequencing = functions.col("version")
        ))
      }

      val ex = intercept[RuntimeException] { runPipeline(ctx) }
      val all = Iterator(ex) ++ ex.getSuppressed.iterator
      assert(
        all.exists(t => Option(t.getMessage).exists(_.contains("AMBIGUOUS_REFERENCE"))),
        s"Expected AMBIGUOUS_REFERENCE failure, got: ${ex.getMessage}"
      )
    }
  }

  test("extra columns on the target that the AutoCDC flow does not emit are preserved " +
    "across the merge") {
    val session = spark
    import session.implicits._

    // The target is wider than the AutoCDC flow's source DF: column `extra` is present on
    // the target but never produced by the flow. AutoCDC must tolerate the extra target
    // column -- pre-existing rows keep their `extra` value, and newly-inserted rows
    // resolve `extra` to NULL.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, extra INT, $cdcMetadataDdl)"
    )
    insertPreloadedRow(
      s"$catalog.$namespace.target",
      colValues = "1, 'preloaded', 0, 42",
      sequence = 0L
    )

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L), (2, "bob", 1L))
    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx)

    checkAnswer(
      spark.table(s"$catalog.$namespace.target").select("id", "name", "version", "extra"),
      Seq(
        Row(1, "alice", 1L, 42), // extra preserved on the upsert
        Row(2, "bob",   1L, null) // extra is NULL for inserts
      )
    )
  }

  test("changing a non-key column type from TIMESTAMP to STRING between runs fails with " +
    "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") {
    val session = spark
    import session.implicits._

    // `mergeSchemas` rejects an incompatible type change between TIMESTAMP and STRING.
    // Captured alongside the type-widening / type-narrowing tests; users must full-refresh
    // the target to change a column's type.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(key INT NOT NULL, version BIGINT NOT NULL, value TIMESTAMP, $cdcMetadataDdl)"
    )

    val stream1 = MemoryStream[(Int, Long, Timestamp)]
    stream1.addData((1, 1L, Timestamp.valueOf("2024-01-01 10:00:00")))
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream1.toDF().toDF("key", "version", "value")),
        keys = Seq("key"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx1)

    // Run #2 emits `value` as STRING. mergeSchemas rejects the type change.
    val stream2 = MemoryStream[(Int, Long, String)]
    stream2.addData((1, 2L, "2024-01-02 11:00:00"))
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream2.toDF().toDF("key", "version", "value")),
        keys = Seq("key"),
        sequencing = functions.col("version")
      ))
    }

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    val all = Iterator(ex) ++ ex.getSuppressed.iterator
    assert(
      all.exists(t => Option(t.getMessage).exists(
        _.contains("CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE"))
      ),
      s"Expected CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE failure, got: ${ex.getMessage}"
    )
  }
}
