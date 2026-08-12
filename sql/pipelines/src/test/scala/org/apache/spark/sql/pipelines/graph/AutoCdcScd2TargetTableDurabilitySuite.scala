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
import org.apache.spark.sql.pipelines.autocdc.{AutoCdcReservedNames, Scd2BatchProcessor, ScdType}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering SCD Type 2 AutoCDC's behavior when the target table is pre-populated by something
 * other than a prior AutoCDC run: hand-loaded open ("current") records and a target created
 * without the framework columns. These verify AutoCDC interoperates gracefully with users who
 * hand-populate the target. The SCD2 analog of [[AutoCdcScd1TargetTableDurabilitySuite]].
 */
class AutoCdcScd2TargetTableDurabilitySuite
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

  /**
   * Insert a pre-existing open ("current") SCD2 record into a target table, as if a previous
   * AutoCDC run had opened it at sequencing version [[sequence]]: `__START_AT` = `sequence`,
   * `__END_AT` = NULL (still active), and `_cdc_metadata.__RECORD_START_AT` = `sequence`.
   *
   * @param table     Fully-qualified table name (catalog.schema.table).
   * @param colValues Comma-separated SQL literals for the user-defined columns, in declared
   *                  order, excluding the trailing framework columns.
   * @param sequence  Value to seed the interval start and the record-start-at with.
   */
  private def insertPreloadedCurrentRecord(
      table: String, colValues: String, sequence: Long): Unit = {
    val recordStartAt = Scd2BatchProcessor.recordStartAtFieldName
    spark.sql(
      s"INSERT INTO $table SELECT $colValues, " +
      s"CAST($sequence AS BIGINT), CAST(NULL AS BIGINT), " +
      s"named_struct('$recordStartAt', CAST($sequence AS BIGINT))"
    )
  }

  /**
   * Insert a pre-existing closed (historical) SCD2 record into a target table, as if a previous
   * AutoCDC run had opened it at [[startAt]] and later closed it at [[endAt]]: `__START_AT` =
   * `startAt`, `__END_AT` = `endAt` (no longer active), and `_cdc_metadata.__RECORD_START_AT` =
   * `startAt`.
   *
   * @param table     Fully-qualified table name (catalog.schema.table).
   * @param colValues Comma-separated SQL literals for the user-defined columns, in declared
   *                  order, excluding the trailing framework columns.
   * @param startAt   Interval start (and record-start-at) of the pre-existing record.
   * @param endAt     Interval end (exclusive) at which the pre-existing record was closed.
   */
  private def insertPreloadedClosedRecord(
      table: String, colValues: String, startAt: Long, endAt: Long): Unit = {
    val recordStartAt = Scd2BatchProcessor.recordStartAtFieldName
    spark.sql(
      s"INSERT INTO $table SELECT $colValues, " +
      s"CAST($startAt AS BIGINT), CAST($endAt AS BIGINT), " +
      s"named_struct('$recordStartAt', CAST($startAt AS BIGINT))"
    )
  }

  test("pre-loaded current record: a higher-sequence upsert closes it and opens a new record") {
    createScd2Target(s"$catalog.$namespace.target")
    insertPreloadedCurrentRecord(s"$catalog.$namespace.target", "1, 'alice', 5", 5L)

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alicia", 10L)) // > pre-existing seq=5 -> closes it, opens a new record

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

    // The pre-existing record is closed at the incoming event's sequence; the new value is open.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 5L, 5L, 10L, scd2Meta(5L)),
        Row(1, "alicia", 10L, 10L, null, scd2Meta(10L))
      )
    )
  }

  test("pre-loaded current record: a lower-sequence upsert is woven in as a closed prior record") {
    createScd2Target(s"$catalog.$namespace.target")
    insertPreloadedCurrentRecord(s"$catalog.$namespace.target", "1, 'alice', 10", 10L)

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "early", 5L)) // < pre-existing seq=10 -> closed prior record ending at 10

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

    // Unlike SCD1, the late lower-sequence event is not suppressed: it becomes a closed prior
    // record ending where the pre-existing record starts, which stays the open current record.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "early", 5L, 5L, 10L, scd2Meta(5L)),
        Row(1, "alice", 10L, 10L, null, scd2Meta(10L))
      )
    )
  }

  test("pre-loaded closed record: an event landing inside its interval bisects it") {
    // The interop shape unique to SCD2: a hand-loaded *closed* record -- a target row with no aux
    // counterpart -- split by an event landing inside its interval. Pre-load [5, 20) and feed an
    // event at seq=10. The pre-existing record is closed early, at 10, and the incoming event
    // takes over the remainder of the span, [10, 20), so the two records partition the original
    // interval with no row left open.
    createScd2Target(s"$catalog.$namespace.target")
    insertPreloadedClosedRecord(s"$catalog.$namespace.target", "1, 'alice', 5", startAt = 5L,
      endAt = 20L)

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "mid", 10L)) // 5 < 10 < 20 -> bisects the pre-existing [5, 20) record

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

    // The pre-existing record is split at the incoming event's sequence: its value carries into
    // [5, 10), the incoming "mid" opens [10, 20), and no row remains open (endAt=20 was the
    // pre-existing closure).
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 5L, 5L, 10L, scd2Meta(5L)),
        Row(1, "mid", 10L, 10L, 20L, scd2Meta(10L))
      )
    )
  }

  test("pre-loaded target rows merge correctly on the first AutoCDC run, and the " +
    "auxiliary table is created lazily") {
    // Target was populated by some external process; this is the first AutoCDC run.
    createScd2Target(s"$catalog.$namespace.target")
    insertPreloadedCurrentRecord(s"$catalog.$namespace.target", "1, 'alice', 1", 1L)

    assert(
      !spark.catalog.tableExists(auxTableNameFor("target")),
      "Auxiliary table should not exist before the first AutoCDC run"
    )

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "bob", 2L))

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

    // seq=2 > pre-existing seq=1, so the pre-existing record closes at 2 and "bob" opens.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(
        Row(1, "alice", 1L, 1L, 2L, scd2Meta(1L)),
        Row(1, "bob", 2L, 2L, null, scd2Meta(2L))
      )
    )
    assert(
      spark.catalog.tableExists(auxTableNameFor("target")),
      "Auxiliary table should be created lazily on the first AutoCDC run"
    )
  }

  test("a target table created without the framework columns gets them " +
    "auto-added on the first AutoCDC run") {
    // User creates the target without the AutoCDC framework columns. DatasetManager evolves the
    // existing table schema by merging it with the AutoCdcMergeFlow's output schema, which
    // includes __START_AT / __END_AT and the metadata column. The first run therefore proceeds
    // normally, and subsequent reads see the framework columns alongside the user's data columns.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL)"
    )

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData((1, "alice", 1L))

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

    val schema = spark.table(s"$catalog.$namespace.target").schema
    Seq(
      Scd2BatchProcessor.startAtColName,
      Scd2BatchProcessor.endAtColName,
      AutoCdcReservedNames.cdcMetadataColName
    ).foreach { col =>
      assert(
        schema.fieldNames.contains(col),
        s"Target must have $col after first AutoCDC run; got ${schema.fieldNames.toSeq}"
      )
    }
    // Schema evolution appends the framework columns after the user columns in the flow's output
    // order (__START_AT, __END_AT, then the metadata column -- the same order scd2MetadataDdl
    // declares for a pre-created target). Assert by name so the row matches regardless of the
    // physical column order.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target").select(
        "id", "name", "version",
        Scd2BatchProcessor.startAtColName,
        Scd2BatchProcessor.endAtColName,
        AutoCdcReservedNames.cdcMetadataColName
      ),
      Seq(Row(1, "alice", 1L, 1L, null, scd2Meta(1L)))
    )
  }
}
