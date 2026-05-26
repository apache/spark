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
  ChangeArgs,
  ColumnSelection,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Smoke tests for AutoCDC SCD type 1 flows running within a single pipeline: one
 * [[DataflowGraph]] / [[TestPipelineUpdateContext]] executes one or more AutoCDC flows,
 * and the target table contents are asserted at the end. Multi-pipeline scenarios (where
 * multiple pipelines write to the same target) live in [[AutoCdcScd1MultiPipelineSuite]].
 */
class AutoCdcScd1SinglePipelineSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("an upsert event lands a new row in an empty target table") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
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
        sequencing = functions.col("version")
      ))
    }

    runPipeline(ctx)

    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, cdcMeta(None, Some(1L))))
    )
  }

  test("consecutive upsert, delete, and re-upsert events for the same key in one run " +
    "converge to the latest event") {
    val session = spark
    import session.implicits._

    // Target schema deliberately omits `is_delete`: the source carries it as a control
    // column, drives the deleteCondition, and is excluded from the target projection.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, String, Long, Boolean)]
    stream.addData(
      (1, "alice", 1L, false), // initial upsert
      (1, "alice", 2L, true),  // delete
      (1, "alice2", 3L, false) // reinsert
    )

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "auto_cdc_flow",
        target = "target",
        query = dfFlowFunc(stream.toDF().toDF("id", "name", "version", "is_delete")),
        keys = Seq("id"),
        sequencing = functions.col("version"),
        deleteCondition = Some(functions.col("is_delete") === true),
        columnSelection = Some(ColumnSelection.ExcludeColumns(
          Seq(UnqualifiedColumnName("is_delete"))
        ))
      ))
    }

    runPipeline(ctx)

    // After all three events at seqs 1, 2, 3: row "alice2" wins as the highest-sequenced
    // upsert; the delete at seq=2 is superseded by the seq=3 upsert.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice2", 3L, cdcMeta(None, Some(3L))))
    )
  }

  test("two AutoCDC flows targeting separate tables in one pipeline produce independent " +
    "results") {
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

    val streamA = MemoryStream[(Int, Long)]
    val streamB = MemoryStream[(Int, Long)]
    streamA.addData((1, 1L), (2, 1L))
    streamB.addData((10, 1L))

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("t_a", catalog = Some(catalog), database = Some(namespace))
      registerTable("t_b", catalog = Some(catalog), database = Some(namespace))
      registerFlow(autoCdcFlow(
        name = "flow_a",
        target = "t_a",
        query = dfFlowFunc(streamA.toDF().toDF("id", "version")),
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
    runPipeline(ctx)

    checkAnswer(
      spark.table(s"$catalog.$namespace.t_a"),
      Seq(Row(1, 1L, cdcMeta(None, Some(1L))), Row(2, 1L, cdcMeta(None, Some(1L))))
    )
    checkAnswer(
      spark.table(s"$catalog.$namespace.t_b"),
      Seq(Row(10, 1L, cdcMeta(None, Some(1L))))
    )
    assert(spark.catalog.tableExists(auxTableNameFor("t_a")))
    assert(spark.catalog.tableExists(auxTableNameFor("t_b")))
  }

  test("an AutoCDC flow targeting a table whose format does not support row-level " +
    "operations fails with AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE") {
    val session = spark
    import session.implicits._

    // Intentionally use a non-merge-compatible catalog, whose default table format is parquet.
    val catalog = TestGraphRegistrationContext.DEFAULT_CATALOG
    val database = TestGraphRegistrationContext.DEFAULT_DATABASE

    spark.sql(
      s"CREATE TABLE $catalog.$database.target_no_merge " +
      s"(id INT NOT NULL, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )

    val stream = MemoryStream[(Int, Long)]
    stream.addData((1, 1L))

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target_no_merge")
      registerFlow(AutoCdcFlow(
        identifier = fullyQualifiedIdentifier("auto_cdc_flow"),
        destinationIdentifier = fullyQualifiedIdentifier("target_no_merge"),
        func = dfFlowFunc(stream.toDF().toDF("id", "version")),
        queryContext = QueryContext(
          currentCatalog = Some(catalog),
          currentDatabase = Some(database)
        ),
        origin = QueryOrigin.empty,
        changeArgs = ChangeArgs(
          keys = Seq(UnqualifiedColumnName("id")),
          sequencing = functions.col("version"),
          storedAsScdType = ScdType.Type1
        )
      ))
    }

    val ex = intercept[RuntimeException] { runPipeline(ctx) }
    checkErrorInPipelineFailure(
      failure = ex,
      condition = "AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE",
      sqlState = Some("0A000"),
      parameters = Map(
        "tableName" -> s"`$catalog`.`$database`.`target_no_merge`",
        "format" -> "parquet"
      )
    )
  }
}
