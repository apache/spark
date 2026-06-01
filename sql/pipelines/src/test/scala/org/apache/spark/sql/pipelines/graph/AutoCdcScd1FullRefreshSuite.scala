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
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering AutoCDC's full-refresh semantics: full refresh must wipe both the
 * target rows and the AutoCDC auxiliary table for the refreshed targets, and must leave
 * non-refreshed targets untouched in selective-refresh mode.
 */
class AutoCdcScd1FullRefreshSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("full refresh wipes target rows and the auxiliary table for the refreshed flow") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

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
        sequencing = functions.col("version")
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
        sequencing = functions.col("version")
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
      Seq(Row(2, "bob", 1L, cdcMeta(None, Some(1L))))
    )
  }

  test("after a full refresh, an event with a sequence below the previous run's " +
    "watermark now lands") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

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
        ))
      ))
    }
    runPipeline(ctx1)

    // Run #2 (full refresh): auxiliary table is dropped, watermark reset. seq=5 should
    // now land.
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
        ))
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
      Seq(Row(1, "fresh", 5L, cdcMeta(None, Some(5L))))
    )
  }

  test("selective full refresh wipes only the requested target's auxiliary state") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.t_a " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.t_b " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    // streamA is replaced across runs because t_a is full-refreshed in run #2 (its streaming
    // checkpoint is reset by full-refresh, so a fresh source is fine and matches the user-visible
    // semantics). streamB is reused across runs because t_b is NOT full-refreshed -- its
    // streaming checkpoint must resume against the same MemoryStream instance, otherwise the
    // seq=5 assertion below could pass for the wrong reason (the source never produced seq=5
    // in run #2 instead of the aux watermark suppressing it).
    val streamA1 = MemoryStream[(Int, Long)]
    val streamB = MemoryStream[(Int, Long)]
    streamA1.addData((1, 10L))
    streamB.addData((1, 10L))
    val ctx1 = new TestGraphRegistrationContext(spark) {
      registerTable("t_a", catalog = Some(catalog), database = Some(namespace))
      registerTable("t_b", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "flow_a",
        target = "t_a",
        query = dfFlowFunc(streamA1.toDF().toDF("id", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
      registerFlow(autoCdcFlow(
        name = "flow_b",
        target = "t_b",
        query = dfFlowFunc(streamB.toDF().toDF("id", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx1)

    // Run #2: full refresh ONLY on t_a; t_b's auxiliary state must persist.
    val streamA2 = MemoryStream[(Int, Long)]
    streamA2.addData((1, 5L))   // would have been suppressed pre-refresh; now wins
    streamB.addData((1, 5L))    // must be suppressed (auxiliary table retains seq=10)
    val ctx2 = new TestGraphRegistrationContext(spark) {
      registerTable("t_a", catalog = Some(catalog), database = Some(namespace))
      registerTable("t_b", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "flow_a",
        target = "t_a",
        query = dfFlowFunc(streamA2.toDF().toDF("id", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
      registerFlow(autoCdcFlow(
        name = "flow_b",
        target = "t_b",
        query = dfFlowFunc(streamB.toDF().toDF("id", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version")
      ))
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

    checkAnswer(
      spark.table(s"$catalog.$namespace.t_a"),
      Seq(Row(1, 5L, cdcMeta(None, Some(5L))))
    )
    // t_b: pre-existing seq=10 row still wins; the seq=5 event is dropped.
    checkAnswer(
      spark.table(s"$catalog.$namespace.t_b"),
      Seq(Row(1, 10L, cdcMeta(None, Some(10L))))
    )
  }
}
