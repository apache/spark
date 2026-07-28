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
import org.apache.spark.sql.pipelines.autocdc.{ColumnSelection, ScdType, UnqualifiedColumnName}
import org.apache.spark.sql.pipelines.utils.ExecutionTest
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
}
