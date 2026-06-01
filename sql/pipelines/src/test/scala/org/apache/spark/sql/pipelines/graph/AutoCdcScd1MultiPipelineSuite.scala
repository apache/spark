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
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests that exercise interactions between separate AutoCDC pipelines (i.e.
 * distinct [[DataflowGraph]] / [[TestPipelineUpdateContext]] invocations) sharing the same
 * v2 catalog. These complement the single-pipeline AutoCDC suites by validating the
 * boundary semantics between independently-deployed pipelines.
 *
 * Each test constructs two graphs and runs them sequentially. In real deployments these
 * could be two different pipeline definitions writing into the same metastore; the tests
 * here verify that AutoCDC's per-target catalog state (target table, auxiliary table,
 * schema invariants) behaves correctly across these pipeline boundaries.
 */
class AutoCdcScd1MultiPipelineSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("two AutoCDC pipelines targeting separate tables maintain independent target and " +
    "auxiliary tables") {
    val session = spark
    import session.implicits._

    // Two distinct target tables created up-front.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.t_a " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.t_b " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1 only knows about `t_a`. Its auxiliary table
    // cat.ns1.__spark_autocdc_aux_state_t_a must not affect pipeline #2's `t_b`.
    val streamA = MemoryStream[(Int, String, Long)]
    streamA.addData((1, "alice", 100L))
    runPipeline(singleAutoCdcFlowPipeline(
      "flow_a", "t_a", streamA.toDF().toDF("id", "name", "version"), Seq("id")))

    // Pipeline #2 only knows about `t_b`. Uses a deliberately *lower* sequence to verify
    // the watermark from pipeline #1's auxiliary table (seq=100) does not leak into
    // pipeline #2.
    val streamB = MemoryStream[(Int, String, Long)]
    streamB.addData((9, "bob", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      "flow_b", "t_b", streamB.toDF().toDF("id", "name", "version"), Seq("id")))

    checkAnswer(
      spark.table(s"$catalog.$namespace.t_a"),
      Seq(Row(1, "alice", 100L, cdcMeta(None, Some(100L))))
    )
    checkAnswer(
      spark.table(s"$catalog.$namespace.t_b"),
      Seq(Row(9, "bob", 1L, cdcMeta(None, Some(1L))))
    )

    // Each target has its own auxiliary table; no cross-contamination.
    assert(spark.catalog.tableExists(auxTableNameFor("t_a")))
    assert(spark.catalog.tableExists(auxTableNameFor("t_b")))
  }

  test("a downstream pipeline can read an AutoCDC target written by a different pipeline " +
    "without observing the CDC metadata column") {
    val session = spark
    import session.implicits._

    // Pipeline #1 writes into target `src` via AutoCDC.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.src " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L), (2, "bob", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      "writer", "src", stream.toDF().toDF("id", "name", "version"), Seq("id")))

    // Pipeline #2 is a regular materialized view that selects the user-data columns from
    // `src` (a different graph entirely). It must observe the merged AutoCDC rows and be
    // able to ignore the metadata column without it polluting downstream consumers.
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
    val session = spark
    import session.implicits._

    // Target table is created once up-front; both pipelines target it with the same
    // AutoCDC `keys` and the same source-DF data schema. The two pipelines have distinct
    // flow names ("flow_v1" / "flow_v2") so they own independent streaming checkpoints,
    // but share the target table and its auxiliary table.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.shared_target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1: inserts rows with id=1 and id=2 at version=1.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 1L), (2, "bob", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      "flow_v1", "shared_target", stream1.toDF().toDF("id", "name", "version"), Seq("id")))

    // Sanity-check pipeline #1's effect before pipeline #2 runs.
    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L))),
        Row(2, "bob", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)))
      )
    )

    // Pipeline #2: updates id=2 (existing key) to a higher sequence and inserts id=3
    // (new key). id=1 is untouched and must survive into the final target unchanged.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((2, "bob-v2", 2L), (3, "carol", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      "flow_v2", "shared_target", stream2.toDF().toDF("id", "name", "version"), Seq("id")))

    // Final target: id=1 untouched (pipeline #1's state), id=2 updated by pipeline #2,
    // id=3 freshly inserted by pipeline #2.
    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L))),
        Row(2, "bob-v2", 2L, cdcMeta(deleteSeq = None, upsertSeq = Some(2L))),
        Row(3, "carol", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)))
      )
    )

    // The auxiliary table for the shared target is itself shared across both pipelines.
    assert(spark.catalog.tableExists(auxTableNameFor("shared_target")))
  }

  test("two AutoCDC pipelines targeting the same table with the same key but different " +
    "data columns evolve the shared target schema") {
    val session = spark
    import session.implicits._

    // Target is created up-front with pipeline #1's schema only; pipeline #2 brings a new
    // top-level nullable `age` column that the dataset materialization layer is expected
    // to schema-merge into the target.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.shared_target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1: source DF schema is (id, name, version); inserts id=1 and id=2.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 1L), (2, "bob", 1L))
    val ctx1 = singleAutoCdcFlowPipeline(
      "flow_v1", "shared_target", stream1.toDF().toDF("id", "name", "version"), Seq("id"))
    runPipeline(ctx1)

    // Sanity-check pipeline #1's state before schema evolution kicks in.
    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L))),
        Row(2, "bob", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)))
      )
    )

    // Pipeline #2: source DF schema is (id, name, age, version). The new nullable `age` column
    // should be added to the target by dataset materialization; pipeline #1's untouched id=1 row
    // is backfilled to NULL.
    val stream2 = MemoryStream[(Int, String, Option[Int], Long)]
    stream2.addData((2, "bob-v2", Some(25), 2L), (3, "carol", Some(30), 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      "flow_v2", "shared_target", stream2.toDF().toDF("id", "name", "age", "version"), Seq("id")))

    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)), null),
        Row(2, "bob-v2", 2L, cdcMeta(deleteSeq = None, upsertSeq = Some(2L)), 25),
        Row(3, "carol", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)), 30)
      )
    )

    // Pipeline #1 runs again with its original (id, name, version) schema. The evolved
    // target schema with `age` must persist: id=1's update leaves age untouched, id=4 is
    // inserted with age=NULL, and pipeline #2's id=2/id=3 rows are unchanged.
    stream1.addData((1, "alice-v2", 2L), (4, "dave", 1L))
    runPipeline(ctx1)

    checkAnswer(
      spark.table(s"$catalog.$namespace.shared_target"),
      Seq(
        Row(1, "alice-v2", 2L, cdcMeta(deleteSeq = None, upsertSeq = Some(2L)), null),
        Row(2, "bob-v2", 2L, cdcMeta(deleteSeq = None, upsertSeq = Some(2L)), 25),
        Row(3, "carol", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)), 30),
        Row(4, "dave", 1L, cdcMeta(deleteSeq = None, upsertSeq = Some(1L)), null)
      )
    )
  }

  test("a second pipeline targeting an existing AutoCDC table with different keys " +
    "fails with KEY_SCHEMA_DRIFT") {
    val session = spark
    import session.implicits._

    // Target table with both candidate keys present so the second pipeline would otherwise
    // be schema-compatible with the first; only the AutoCDC `keys` differ between flows.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.shared_target " +
      s"(id INT NOT NULL, name STRING NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // Pipeline #1: AutoCDC flow keyed on `id`. Materializes the auxiliary table with schema
    // (id, _cdc_metadata).
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 1L))
    runPipeline(singleAutoCdcFlowPipeline(
      "flow_v1", "shared_target", stream1.toDF().toDF("id", "name", "version"), Seq("id")))

    // Pipeline #2: completely separate graph, but targets the same physical `shared_target`
    // table with `keys = Seq("name")`.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((2, "alice", 1L))
    val ctx2 = singleAutoCdcFlowPipeline(
      "flow_v2", "shared_target", stream2.toDF().toDF("id", "name", "version"), Seq("name"))

    val ex = intercept[RuntimeException] { runPipeline(ctx2) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = Some("42000"),
      parameters = Map(
        "flowName" ->
          fullyQualifiedIdentifier("flow_v2", Some(catalog), Some(namespace)).unquotedString,
        "auxTableName" -> auxTableNameFor("shared_target"),
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
