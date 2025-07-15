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

package org.apache.spark.sql.connect.pipelines

import java.io.{File, FileWriter}

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.DatasetType
import org.apache.spark.sql.connect.service.{SessionKey, SparkConnectService}
import org.apache.spark.sql.pipelines.utils.{EventVerificationTestHelpers, TestPipelineUpdateContextMixin}
import org.apache.spark.util.Utils
// scalastyle:off println

/**
 * Comprehensive test suite that validates pipeline refresh functionality by running actual
 * pipelines with different refresh parameters and validating the results.
 */
class PipelineRefreshFunctionalSuite
  extends SparkDeclarativePipelinesServerTest
    with TestPipelineUpdateContextMixin
    with EventVerificationTestHelpers {

  private var testDataDir: File = _
  private var streamInputDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create temporary directories for test data
    testDataDir = Utils.createTempDir("pipeline-refresh-test")
    streamInputDir = new File(testDataDir, "stream-input")
    streamInputDir.mkdirs()
  }

  override def afterAll(): Unit = {
    try {
      // Clean up test directories
      Utils.deleteRecursively(testDataDir)
    } finally {
      super.afterAll()
    }
  }

  private def uploadInputFile(filename: String, contents: String): Unit = {
    val file = new File(streamInputDir, filename)
    val writer = new FileWriter(file)
    try {
      writer.write(contents)
    } finally {
      writer.close()
    }
  }


  private def createPipelineWithTransformations(graphId: String): TestPipelineDefinition = {
    new TestPipelineDefinition(graphId) {
      // Create a mv that reads from files
      createTable(
        name = "file_data",
        datasetType = DatasetType.MATERIALIZED_VIEW,
        sql = Some(s"""
          SELECT id, value FROM JSON.`${streamInputDir.getAbsolutePath}/*.json`
        """))

      // Create tables that depend on the mv
      createTable(
        name = "table_a",
        datasetType = DatasetType.TABLE,
        sql = Some("SELECT id, value as odd_value FROM STREAM file_data WHERE id % 2 = 1"))

      createTable(
        name = "table_b",
        datasetType = DatasetType.TABLE,
        sql = Some("SELECT id, value as even_value FROM STREAM file_data WHERE id % 2 = 0"))
    }
  }

  private def createTwoSTPipeline(graphId: String): TestPipelineDefinition = {
    new TestPipelineDefinition(graphId) {
      // Create a mv that reads from files
      createTable(
        name = "file_data",
        datasetType = DatasetType.MATERIALIZED_VIEW,
        sql = Some(s"""
          SELECT id, value FROM JSON.`${streamInputDir.getAbsolutePath}/*.json`
        """))

      // Create tables that depend on the mv
      createTable(
        name = "a",
        datasetType = DatasetType.TABLE,
        sql = Some("SELECT id, value FROM STREAM file_data"))

      createTable(
        name = "b",
        datasetType = DatasetType.TABLE,
        sql = Some("SELECT id, value FROM STREAM file_data"))
    }
  }

  test("pipeline runs selective full_refresh") {
    withRawBlockingStub { implicit stub =>
      uploadInputFile("data.json", """
        |{"id": 1, "value": 1}
        |{"id": 2, "value": 2}
      """.stripMargin)
      val graphId = createDataflowGraph
      val pipeline = createPipelineWithTransformations(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.file_data",
          "spark_catalog.default.table_a",
          "spark_catalog.default.table_b"),
        columnsToVerify = Map(
          "spark_catalog.default.file_data" -> Seq("id", "value"),
          "spark_catalog.default.table_a" -> Seq("id", "odd_value"),
          "spark_catalog.default.table_b" -> Seq("id", "even_value")
        ),
        expectedContent = Map(
          "spark_catalog.default.file_data" -> Set(
            Map("id" -> 1, "value" -> 1),
            Map("id" -> 2, "value" -> 2)
          ),
          "spark_catalog.default.table_a" -> Set(
            Map("id" -> 1, "odd_value" -> 1)
          ),
          "spark_catalog.default.table_b" -> Set(
            Map("id" -> 2, "even_value" -> 2)
          )
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // simulate a full refresh by uploading new data
      uploadInputFile("data.json", """
         |{"id": 1, "value": 1}
         |{"id": 2, "value": 2}
         |{"id": 3, "value": 3}
         |{"id": 4, "value": 4}
      """.stripMargin)

      // Run with full refresh on specific tables
      val fullRefreshTables = List("file_data", "table_a")
      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addAllFullRefresh(fullRefreshTables.asJava)
        .build()

      val capturedEvents = startPipelineAndWaitForCompletion(graphId, Some(startRun))
      // assert that table_b is excluded
      assert(capturedEvents.exists(
        _.getMessage.contains(s"Flow \'spark_catalog.default.table_b\' is EXCLUDED.")))
      // assert that table_a and file_data ran to completion
      assert(capturedEvents.exists(
        _.getMessage.contains(s"Flow spark_catalog.default.table_a has COMPLETED.")))
      assert(capturedEvents.exists(
        _.getMessage.contains(s"Flow spark_catalog.default.file_data has COMPLETED.")))
      // Verify completion event
      assert(capturedEvents.exists(_.getMessage.contains("Run is COMPLETED")))

      verifyMultipleTableContent(
        tableNames = Set("spark_catalog.default.file_data", "spark_catalog.default.table_a"),
        columnsToVerify = Map(
          "spark_catalog.default.file_data" -> Seq("id", "value"),
          "spark_catalog.default.table_a" -> Seq("id", "odd_value"),
          "spark_catalog.default.table_b" -> Seq("id", "even_value")
        ),
        expectedContent = Map(
          "spark_catalog.default.file_data" -> Set(
            Map("id" -> 1, "value" -> 1),
            Map("id" -> 2, "value" -> 2),
            Map("id" -> 3, "value" -> 3),
            Map("id" -> 4, "value" -> 4)
          ),
          "spark_catalog.default.table_a" -> Set(
            Map("id" -> 1, "odd_value" -> 1),
            Map("id" -> 3, "odd_value" -> 3)
          ),
          "spark_catalog.default.table_b" -> Set(
            Map("id" -> 2, "even_value" -> 4) // table_b should not have changed
          )
        )
      )
    }
  }

  test("pipeline runs selective full_refresh and selective refresh") {
    withRawBlockingStub { implicit stub =>
      uploadInputFile("data.json", """
         |{"id": "x", "value": 1}
      """.stripMargin)
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set("spark_catalog.default.a", "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id", "value"),
          "spark_catalog.default.b" -> Seq("id", "value")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(Map("id" -> "x", "value" -> 1)),
          "spark_catalog.default.b" -> Set(Map("id" -> "x", "value" -> 1))
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      uploadInputFile("data.json", """
         |{"id": "x", "value": 2}
      """.stripMargin)

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addAllFullRefresh(List("file_data", "a").asJava)
        .addRefresh("b")
        .build()

      startPipelineAndWaitForCompletion(graphId, Some(startRun))

      // assert that table_b is refreshed
      verifyMultipleTableContent(
        tableNames = Set("spark_catalog.default.a", "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id", "value"),
          "spark_catalog.default.b" -> Seq("id", "value")
        ),
        expectedContent = Map(
          // a should be fully refreshed and only contain the new value
          "spark_catalog.default.a" -> Set(Map("id" -> "x", "value" -> 2)),
          "spark_catalog.default.b" ->
            // b is incrementally refreshed and contain the new value in addition to the old one
            Set(Map("id" -> "x", "value" -> 1), Map("id" -> "x", "value" -> 2))
        )
      )
    }
  }

  private def verifyMultipleTableContent(
    tableNames: Set[String],
    columnsToVerify: Map[String, Seq[String]],
    expectedContent: Map[String, Set[Map[String, Any]]]): Unit = {
    tableNames.foreach { tableName =>
      spark.catalog.refreshTable(tableName)
      val df = spark.table(tableName)
      assert(df.columns.toSet == columnsToVerify(tableName).toSet,
        s"Columns in $tableName do not match expected: ${df.columns.mkString(", ")}")
      val actualContent = df.collect().map(row => {
        columnsToVerify(tableName).map(col => col -> row.getAs[Any](col)).toMap
      }).toSet
      assert(actualContent == expectedContent(tableName),
        s"Content of $tableName does not match expected: $actualContent")
    }
  }
}
