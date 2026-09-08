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
import org.apache.spark.sql.pipelines.autocdc.{ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests that exercise interactions between separate SCD Type 2 AutoCDC pipelines (i.e.
 * distinct [[DataflowGraph]] / [[TestPipelineUpdateContext]] invocations) sharing the same v2
 * catalog. The SCD2 analog of [[AutoCdcScd1MultiPipelineSuite]]: independent target/auxiliary
 * tables per target, downstream reads that ignore the framework columns, a shared target written
 * by two pipelines, schema evolution across pipelines, and key-drift rejection.
 */
class AutoCdcScd2MultiPipelineSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  /** The SCD2 target's `_cdc_metadata` struct value for a given recordStartAt. */
  private def scd2Meta(recordStartAt: Long): Row = Row(recordStartAt)

  test("two AutoCDC pipelines targeting separate tables maintain independent target and " +
    "auxiliary tables") {
    // Two distinct target tables created up-front.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.t_a " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.t_b " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Pipeline #1 only knows about `t_a`. Its auxiliary table must not affect pipeline #2's `t_b`.
    val streamA = MemoryStream[(Int, String, Long)]
    streamA.addData((1, "alice", 100L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_a",
      target = "t_a",
      sourceDf = streamA.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2))

    // Pipeline #2 only knows about `t_b`. Uses a deliberately *lower* sequence to verify the
    // watermark from pipeline #1's auxiliary table (seq=100) does not leak into pipeline #2.
    val streamB = MemoryStream[(Int, String, Long)]
    streamB.addData((9, "bob", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_b",
      target = "t_b",
      sourceDf = streamB.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2))

    checkAnswer(
      spark.table(s"$catalog.$namespace.t_a"),
      Seq(Row(1, "alice", 100L, 100L, null, scd2Meta(100L)))
    )
    checkAnswer(
      spark.table(s"$catalog.$namespace.t_b"),
      Seq(Row(9, "bob", 1L, 1L, null, scd2Meta(1L)))
    )

    // Each target has its own auxiliary table; no cross-contamination.
    assert(spark.catalog.tableExists(auxTableNameFor("t_a")))
    assert(spark.catalog.tableExists(auxTableNameFor("t_b")))
  }

  test("a downstream pipeline can read an AutoCDC target written by a different pipeline " +
    "without observing the framework columns") {
    // Pipeline #1 writes into target `src` via AutoCDC.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.src " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )
    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L), (2, "bob", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "writer",
      target = "src",
      sourceDf = stream.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2))

    // Pipeline #2 is a regular materialized view that selects the user-data columns from `src`
    // (a different graph entirely). It must observe the merged AutoCDC rows and be able to ignore
    // the framework columns without them polluting downstream consumers.
    val ctxReader = new TestGraphRegistrationContext(spark) {
      registerMaterializedView(
        "downstream_mv",
        query = dfFlowFunc(
          spark.read.table(s"$catalog.$namespace.src").select("id", "name", "version")
        )
      )
    }
    runPipeline(ctxReader)

    checkAnswer(
      spark.table(fullyQualifiedIdentifier("downstream_mv").toString),
      Seq(Row(1, "alice", 1L), Row(2, "bob", 1L))
    )
  }

  test("two AutoCDC pipelines targeting the same table with identical key and data " +
    "schemas merge into a shared target table") {
    // Target table is created once up-front; both pipelines target it with the same AutoCDC
    // `keys` and the same source-DF data schema. The two pipelines have distinct flow names so
    // they own independent streaming checkpoints, but share the target and its auxiliary table.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.shared_target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Pipeline #1: inserts rows with id=1 and id=2 at version=1.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 1L), (2, "bob", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "shared_target",
      sourceDf = stream1.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2))

    // Sanity-check pipeline #1's effect before pipeline #2 runs.
    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, 1L, null, scd2Meta(1L)),
        Row(2, "bob", 1L, 1L, null, scd2Meta(1L))
      )
    )

    // Pipeline #2: updates id=2 (existing key) to a higher sequence and inserts id=3 (new key).
    // id=1 is untouched and must survive into the final target unchanged.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((2, "bob-v2", 2L), (3, "carol", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "shared_target",
      sourceDf = stream2.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2))

    // Final target: id=1 untouched; id=2's original record closed at seq=2 with a new open record;
    // id=3 freshly inserted by pipeline #2.
    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, 1L, null, scd2Meta(1L)),
        Row(2, "bob", 1L, 1L, 2L, scd2Meta(1L)),
        Row(2, "bob-v2", 2L, 2L, null, scd2Meta(2L)),
        Row(3, "carol", 1L, 1L, null, scd2Meta(1L))
      )
    )

    // The auxiliary table for the shared target is itself shared across both pipelines.
    assert(spark.catalog.tableExists(auxTableNameFor("shared_target")))
  }

  test("two AutoCDC pipelines targeting the same table with the same key but different " +
    "data columns evolve the shared target schema") {
    // Target is created up-front with pipeline #1's schema only; pipeline #2 brings a new
    // top-level nullable `age` column that the dataset materialization layer is expected to
    // schema-merge into the target.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.shared_target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Pipeline #1: source DF schema is (id, name, version); inserts id=1 and id=2.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 1L), (2, "bob", 1L))
    val ctx1 = singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "shared_target",
      sourceDf = stream1.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2,
      // Both pipelines pin the tracked set to `name`, the one non-key column they share. Left to
      // the default (selection-derived) tracking, pipeline #2's extra `age` would also widen the
      // tracked set, which is rejected as TRACK_HISTORY_DRIFT (SPARK-58391); the axis under test
      // here is the shared target's data-column evolution.
      trackHistorySelection =
        Option(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name")))))
    runPipeline(ctx1)

    // Sanity-check pipeline #1's state before schema evolution kicks in.
    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, 1L, null, scd2Meta(1L)),
        Row(2, "bob", 1L, 1L, null, scd2Meta(1L))
      )
    )

    // Pipeline #2: source DF schema is (id, name, age, version). The new nullable `age` column
    // should be added to the target by dataset materialization; pipeline #1's untouched id=1 row
    // is backfilled to NULL. The `age` column lands after the framework columns in the target.
    val stream2 = MemoryStream[(Int, String, Option[Int], Long)]
    stream2.addData((2, "bob-v2", Some(25), 2L), (3, "carol", Some(30), 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "shared_target",
      sourceDf = stream2.toDF().toDF("id", "name", "age", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2,
      trackHistorySelection =
        Option(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name"))))))

    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, 1L, null, scd2Meta(1L), null),
        Row(2, "bob", 1L, 1L, 2L, scd2Meta(1L), null),
        Row(2, "bob-v2", 2L, 2L, null, scd2Meta(2L), 25),
        Row(3, "carol", 1L, 1L, null, scd2Meta(1L), 30)
      )
    )

    // NOTE: the SCD1 analog of this test additionally re-runs the narrower pipeline #1 against the
    // now-wider evolved target. For SCD2 that microbatch-narrower-than-target path is covered
    // separately by AutoCdcScd2ColumnEvolutionSuite (SPARK-58418), so it is not duplicated here.
  }

  test("a second pipeline targeting an existing AutoCDC table with different keys " +
    "fails with KEY_SCHEMA_DRIFT") {
    // Target table with both candidate keys present so the second pipeline would otherwise be
    // schema-compatible with the first; only the AutoCDC `keys` differ between flows.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.shared_target " +
      s"(id INT NOT NULL, name STRING NOT NULL, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )

    // Pipeline #1: AutoCDC flow keyed on `id`.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "flow_v1",
      target = "shared_target",
      sourceDf = stream1.toDF().toDF("id", "name", "version"),
      keys = Seq("id"),
      sequencing = $"version",
      scdType = ScdType.Type2))

    // Pipeline #2: completely separate graph, but targets the same physical `shared_target`
    // table with `keys = Seq("name")`.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((2, "alice", 1L))
    val ctx2 = singleAutoCdcFlowPipeline(
      flowName = "flow_v2",
      target = "shared_target",
      sourceDf = stream2.toDF().toDF("id", "name", "version"),
      keys = Seq("name"),
      sequencing = $"version",
      scdType = ScdType.Type2)

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "tableName" ->
          fullyQualifiedIdentifier("shared_target", Some(catalog), Some(namespace)).unquotedString,
        // Pipeline #2's AutoCDC key resolves from the source DF, where `MemoryStream[(Int, String,
        // Long)]` produces a nullable StringType for `name`.
        "expectedKeySchema" -> "name STRING",
        // Pipeline #1 persisted the aux table from a source DF whose `id` was a non-null Scala
        // primitive (`Int`), so the recorded key carries `NOT NULL`.
        "recordedKeySchema" -> "id INT NOT NULL"
      )
    )
  }
}
