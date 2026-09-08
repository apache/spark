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
import org.apache.spark.sql.pipelines.autocdc.{
  ColumnSelection,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering AutoCDC's full-refresh semantics for SCD Type 2 targets: full refresh must wipe
 * both the target rows and the (richer) SCD2 auxiliary table for the refreshed targets, and must
 * leave non-refreshed targets untouched in selective-refresh mode. The SCD2 analog of
 * [[AutoCdcScd1FullRefreshSuite]].
 */
class AutoCdcScd2FullRefreshSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  /** The SCD2 target's `_cdc_metadata` struct value for a given recordStartAt. */
  private def scd2Meta(recordStartAt: Long): Row = Row(recordStartAt)

  /** Create an SCD2 target with user columns `(id, name, version)` plus the framework columns. */
  private def createScd2Target(table: String): Unit = {
    spark.sql(
      s"CREATE TABLE $table (" +
      s"id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )
  }

  test("full refresh wipes target rows and the auxiliary table for the refreshed flow") {
    createScd2Target(s"$catalog.$namespace.target")

    // Run #1: populate target + auxiliary table.
    val stream1 = MemoryStream[(Int, String, Long)]
    stream1.addData((1, "alice", 5L))
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream1.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
    }
    runPipeline(ctx1)
    assert(
      spark.catalog.tableExists(auxTableNameFor("target")),
      "Auxiliary table should exist after first run"
    )

    // Run #2 (full refresh): auxiliary table should be dropped by DatasetManager, target
    // truncated. The new run brings only id=2 at seq=1.
    val stream2 = MemoryStream[(Int, String, Long)]
    stream2.addData((2, "bob", 1L))
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream2.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
    }
    val updateCtx = TestPipelineUpdateContext(
      spark,
      ctx2.toDataflowGraph,
      storageRoot,
      fullRefreshTables = AllTables
    )
    updateCtx.pipelineExecution.runPipeline()
    updateCtx.pipelineExecution.awaitCompletion()

    // Only id=2 remains, as a single open current record; id=1 from run #1 is wiped.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(2, "bob", 1L, 1L, null, scd2Meta(1L)))
    )
  }

  test("after a full refresh, an event with a sequence below the previous run's " +
    "watermark now lands") {
    createScd2Target(s"$catalog.$namespace.target")

    // Run #1: delete at seq=10 sets a high watermark in the auxiliary table.
    val stream1 = MemoryStream[(Int, String, Long, Boolean)]
    stream1.addData((1, "alice", 10L, true))
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream1.toDF().toDF("id", "name", "version", "is_delete")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        deleteCondition = Some(functions.col("is_delete") === true),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("is_delete"))
        )),
        scdType = ScdType.Type2
      ))
    }
    runPipeline(ctx1)

    // Run #2 (full refresh): auxiliary table is dropped, watermark reset. seq=5 should
    // now land as an open current record.
    val stream2 = MemoryStream[(Int, String, Long, Boolean)]
    stream2.addData((1, "fresh", 5L, false))
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream2.toDF().toDF("id", "name", "version", "is_delete")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        deleteCondition = Some(functions.col("is_delete") === true),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("is_delete"))
        )),
        scdType = ScdType.Type2
      ))
    }
    val updateCtx = TestPipelineUpdateContext(
      spark,
      ctx2.toDataflowGraph,
      storageRoot,
      fullRefreshTables = AllTables
    )
    updateCtx.pipelineExecution.runPipeline()
    updateCtx.pipelineExecution.awaitCompletion()

    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "fresh", 5L, 5L, null, scd2Meta(5L)))
    )
  }

  test("selective full refresh wipes only the requested target's auxiliary state") {
    createScd2Target(s"$catalog.$namespace.t_a")
    createScd2Target(s"$catalog.$namespace.t_b")

    // t_b's run #1 is delete-only so that the state proving the aux was spared lives ONLY in the
    // aux: the target stays empty and the aux holds a seq=10 tombstone. In run #2, t_b's seq=5
    // upsert landing closed at 10 is possible only because the selective refresh left t_b's aux
    // intact -- target state alone could not supply the seq=10 closure. (An open upsert in run #1
    // would instead route to the target and leave the aux empty, so the assertion would hold even
    // if the aux were wiped, attributing the outcome to the wrong state.)
    //
    // streamA is replaced across runs because t_a is full-refreshed in run #2 (its streaming
    // checkpoint is reset by full-refresh, so a fresh source is fine and matches the user-visible
    // semantics). streamB is reused across runs because t_b is NOT full-refreshed -- its
    // streaming checkpoint must resume against the same MemoryStream instance, otherwise the
    // seq=5 assertion below could pass for the wrong reason (the source never produced seq=5
    // in run #2 instead of the aux tombstone shaping it).
    val streamA1 = MemoryStream[(Int, String, Long)]
    val streamB = MemoryStream[(Int, String, Long, Boolean)]
    streamA1.addData((1, "a", 10L))
    streamB.addData((1, "b", 10L, true)) // delete at seq=10: target empty, aux tombstone at 10
    // dfFlowFunc is a TestGraphRegistrationContext method, so it can only be called inside the
    // context blocks below; flowB takes the already-built query and adds t_b's delete knobs.
    def flowB(query: FlowFunction): AutoCdcFlow = autoCdcFlow(
      name = "flow_b",
      target = "t_b",
      query = query,
      keys = Seq("id"),
      sequencing = functions.col("version"),
      deleteCondition = Some(functions.col("is_delete") === true),
      columnSelection = Some(ColumnSelection.ExcludeColumns(
        Seq(UnqualifiedColumnName("is_delete"))
      )),
      scdType = ScdType.Type2
    )
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("t_a", catalog = Some(catalog), database = Some(namespace))
      registerTable("t_b", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "flow_a",
        target = "t_a",
        query = dfFlowFunc(streamA1.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
      registerFlow(flowB(dfFlowFunc(streamB.toDF().toDF("id", "name", "version", "is_delete"))))
    }
    runPipeline(ctx1)
    // Precondition: t_b's run #1 left the target empty with the seq=10 tombstone only in the aux.
    checkAnswer(spark.table(s"$catalog.$namespace.t_b"), Seq.empty)

    // Run #2: full refresh ONLY on t_a; t_b's auxiliary state must persist.
    val streamA2 = MemoryStream[(Int, String, Long)]
    // t_a's aux is wiped, so seq=5 is the only record it has ever seen: a fresh open record.
    streamA2.addData((1, "a2", 5L))
    // t_b keeps its aux (seq=10 tombstone). The late seq=5 upsert is woven into history as a
    // closed prior record ending at seq=10 -- a closure only the retained aux can supply.
    streamB.addData((1, "b2", 5L, false))
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("t_a", catalog = Some(catalog), database = Some(namespace))
      registerTable("t_b", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "flow_a",
        target = "t_a",
        query = dfFlowFunc(streamA2.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
      registerFlow(flowB(dfFlowFunc(streamB.toDF().toDF("id", "name", "version", "is_delete"))))
    }
    val updateCtx = TestPipelineUpdateContext(
      spark,
      ctx2.toDataflowGraph,
      storageRoot,
      fullRefreshTables = SomeTables(Set(
        fullyQualifiedIdentifier("t_a", Some(catalog), Some(namespace))
      ))
    )
    updateCtx.pipelineExecution.runPipeline()
    updateCtx.pipelineExecution.awaitCompletion()

    // t_a: refreshed, so the seq=5 event lands as a fresh open current record.
    checkAnswer(
      spark.table(s"$catalog.$namespace.t_a"),
      Seq(Row(1, "a2", 5L, 5L, null, scd2Meta(5L)))
    )
    // t_b: aux retained, so the late seq=5 event is woven in as a closed prior record ending at
    // the tombstoned seq=10. With no open successor (the seq=10 event was a delete), the closed
    // [5, 10) record is the only visible row.
    checkAnswer(
      spark.table(s"$catalog.$namespace.t_b"),
      Seq(Row(1, "b2", 5L, 5L, 10L, scd2Meta(5L)))
    )
  }
}
