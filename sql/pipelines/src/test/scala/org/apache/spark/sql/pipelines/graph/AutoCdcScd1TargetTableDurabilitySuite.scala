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
import org.apache.spark.sql.pipelines.autocdc.Scd1BatchProcessor
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests covering AutoCDC's behavior when the target table is pre-populated by something
 * other than a prior AutoCDC run: pre-loaded rows, missing CDC metadata column on the
 * target, and rows with NULL CDC metadata. These cases verify that AutoCDC interoperates
 * gracefully with users who hand-populate the target table.
 */
class AutoCdcScd1TargetTableDurabilitySuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  test("pre-loaded rows: an event with a lower sequence is suppressed and a higher one " +
    "wins") {
    val session = spark
    import session.implicits._

    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    insertPreloadedRow(s"$catalog.$namespace.target", "1, 'alice', 5", 5L)
    insertPreloadedRow(s"$catalog.$namespace.target", "2, 'bob', 5", 5L)

    val stream = MemoryStream[(Int, String, Long)]
    stream.addData(
      (1, "stale", 2L),  // < pre-existing seq=5 -> ignored
      (2, "bob2", 10L)   // > pre-existing seq=5 -> upserts
    )
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
      Seq(
        Row(1, "alice", 5L, cdcMeta(None, Some(5L))),
        Row(2, "bob2", 10L, cdcMeta(None, Some(10L)))
      )
    )
  }

  test("pre-loaded target rows merge correctly on the first AutoCDC run, and the " +
    "auxiliary table is created lazily") {
    val session = spark
    import session.implicits._

    // Target was populated by some external process; this is the first AutoCDC run.
    spark.sql(
      s"CREATE TABLE $catalog.$namespace.target " +
      s"(id INT NOT NULL, name STRING, version BIGINT NOT NULL, $cdcMetadataDdl)"
    )
    insertPreloadedRow(s"$catalog.$namespace.target", "1, 'alice', 1", 1L)

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
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx)

    // seq=2 > pre-existing seq=1, so "bob" replaces "alice" via the upsert sequence column.
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "bob", 2L, cdcMeta(None, Some(2L))))
    )
    assert(
      spark.catalog.tableExists(auxTableNameFor("target")),
      "Auxiliary table should be created lazily on the first AutoCDC run"
    )
  }

  test("a target table created without the CDC metadata column gets the column " +
    "auto-added on the first AutoCDC run") {
    val session = spark
    import session.implicits._

    // User creates the target without the AutoCDC metadata column. DatasetManager evolves
    // the existing table schema by merging it with the AutoCdcMergeFlow's output schema,
    // which includes the metadata column. The first run therefore proceeds normally, and
    // subsequent reads see the metadata struct alongside the user's data columns.
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
        sequencing = functions.col("version")
      ))
    }
    runPipeline(ctx)

    val schema = spark.table(s"$catalog.$namespace.target").schema
    assert(
      schema.fieldNames.contains(Scd1BatchProcessor.cdcMetadataColName),
      s"Target must have ${Scd1BatchProcessor.cdcMetadataColName} after first AutoCDC run; " +
      s"got ${schema.fieldNames.toSeq}"
    )
    checkAnswer(
      spark.table(s"$catalog.$namespace.target"),
      Seq(Row(1, "alice", 1L, cdcMeta(None, Some(1L))))
    )
  }
}
