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
  AutoCdcReservedNames,
  ColumnSelection,
  Scd2BatchProcessor,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end smoke tests for AutoCDC SCD Type 2 flows running within a single pipeline: one
 * [[DataflowGraph]] / [[TestPipelineUpdateContext]] executes an SCD2 AutoCDC flow through the
 * [[Scd2MergeStreamingWrite]] streaming write, and both the target table and the auxiliary
 * table contents are asserted at the end.
 *
 * This exercises the full wiring landed for SCD2: the flow planner routing an SCD2
 * [[AutoCdcMergeFlow]] to [[Scd2MergeStreamingWrite]], the auxiliary-table materialization, and
 * the [[org.apache.spark.sql.pipelines.autocdc.Scd2ForeachBatchHandler]] reconciliation.
 */
class AutoCdcScd2SinglePipelineSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  import testImplicits._

  /** The SCD2 target's `_cdc_metadata` struct value for a given recordStartAt. */
  private def scd2Meta(recordStartAt: Long): Row = Row(recordStartAt)

  /**
   * DDL for an SCD2 target table with user columns `(id, name, version)` plus the framework
   * columns `__START_AT` / `__END_AT` (sequencing type BIGINT) and the SCD2 `_cdc_metadata`
   * struct. `version` is the sequencing column and, unless excluded via a column selection, is
   * retained as an ordinary user column in the target.
   */
  private def createScd2Target(table: String): Unit = {
    spark.sql(
      s"CREATE TABLE $table (" +
      s"id INT NOT NULL, name STRING, version BIGINT NOT NULL, $scd2MetadataDdl)"
    )
  }

  test("SCD2: an upsert lands an open current record in an empty target table") {
    createScd2Target(s"$catalog.$namespace.target")

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 10L))

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
    }

    runPipeline(ctx)

    // A single event opens a current record: START_AT = the event sequence, END_AT = null.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 10L, 10L, null, scd2Meta(10L)))
    )
  }

  test("SCD2: an update to a key closes the prior record and opens a new one") {
    createScd2Target(s"$catalog.$namespace.target")

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 10L), (1, "alicia", 20L))

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
    }

    runPipeline(ctx)

    // The first value is closed at the second event's sequence; the second value is open.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 10L, 10L, 20L, scd2Meta(10L)),
        Row(1, "alicia", 20L, 20L, null, scd2Meta(20L))
      )
    )
  }

  test("SCD2: a delete closes the current record with no open record remaining") {
    // Target omits `is_delete`: the source carries it as a control column driving the delete
    // condition, and it is excluded from the target projection.
    createScd2Target(s"$catalog.$namespace.target")

    val stream = MemoryStream[(Int, String, Long, Boolean)]
    stream.addData((1, "alice", 10L, false), (1, null, 20L, true))

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version", "is_delete")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        columnSelection = Some(
          ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("is_delete")))
        ),
        deleteCondition = Some(functions.col("is_delete")),
        scdType = ScdType.Type2
      ))
    }

    runPipeline(ctx)

    // The delete closes the open record at the delete's sequence; nothing remains open.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 10L, 10L, 20L, scd2Meta(10L)))
    )
  }

  test("SCD2: the auxiliary table is materialized for the target") {
    createScd2Target(s"$catalog.$namespace.target")

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 10L))

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        scdType = ScdType.Type2
      ))
    }

    runPipeline(ctx)

    // The SCD2 auxiliary table exists and carries the aux-only deleted-by-batch-id marker column
    // in addition to the full target row schema (user columns + the framework columns). Assert the
    // exact field list, via the reserved-name constants, so a rename of any framework column (in
    // particular the non-prefixed __START_AT / __END_AT, which a prefix check would not catch) is
    // caught here rather than silently passing a substring match.
    val auxColumns = spark.table(auxTableNameFor("target")).schema.fieldNames.toSeq
    assert(
      auxColumns == Seq(
        "id",
        "name",
        "version",
        Scd2BatchProcessor.startAtColName,
        Scd2BatchProcessor.endAtColName,
        AutoCdcReservedNames.cdcMetadataColName,
        Scd2BatchProcessor.deletedByBatchIdColName
      )
    )
  }
}
