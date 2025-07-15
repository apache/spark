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
      uploadInputFile("data.json",
        """
        |{"id": "x", "value": 1}
        """.stripMargin)
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.file_data",
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.file_data" -> Seq("id", "value"),
          "spark_catalog.default.a" -> Seq("id", "value"),
          "spark_catalog.default.b" -> Seq("id", "value")
        ),
        expectedContent = Map(
          "spark_catalog.default.file_data" -> Set(
            Map("id" -> "x", "value" -> 1)
          ),
          "spark_catalog.default.a" -> Set(
            Map("id" -> "x", "value" -> 1)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> "x", "value" -> 1)
          )
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // simulate a full refresh by uploading new data
      uploadInputFile("data.json",
        """
          |{"id": "x", "value": 2}
        """.stripMargin)

      // Run with full refresh on specific tables
      val fullRefreshTables = List("file_data", "a")
      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addAllFullRefresh(fullRefreshTables.asJava)
        .build()

      val capturedEvents = startPipelineAndWaitForCompletion(graphId, Some(startRun))
      // assert that table_b is excluded
      assert(capturedEvents.exists(
        _.getMessage.contains(s"Flow \'spark_catalog.default.b\' is EXCLUDED.")))
      // assert that table_a and file_data ran to completion
      assert(capturedEvents.exists(
        _.getMessage.contains(s"Flow spark_catalog.default.a has COMPLETED.")))
      assert(capturedEvents.exists(
        _.getMessage.contains(s"Flow spark_catalog.default.file_data has COMPLETED.")))
      // Verify completion event
      assert(capturedEvents.exists(_.getMessage.contains("Run is COMPLETED")))

      verifyMultipleTableContent(
        tableNames = Set("spark_catalog.default.file_data", "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.file_data" -> Seq("id", "value"),
          "spark_catalog.default.a" -> Seq("id", "value"),
          "spark_catalog.default.b" -> Seq("id", "value")
        ),
        expectedContent = Map(
          "spark_catalog.default.file_data" -> Set(
            Map("id" -> "x", "value" -> 2)
          ),
          "spark_catalog.default.a" -> Set(
            Map("id" -> "x", "value" -> 2)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> "x", "value" -> 1) // b should not be refreshed, so it retains the old value
          )
        )
      )
    }
  }

  test("pipeline runs selective full_refresh and selective refresh") {
    withRawBlockingStub { implicit stub =>
      uploadInputFile("data.json",
        """
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

      uploadInputFile("data.json",
        """
          |{"id": "y", "value": 2}
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
          "spark_catalog.default.a" -> Set(Map("id" -> "y", "value" -> 2)),
          "spark_catalog.default.b" ->
            // b contain the new value in addition to the old one
            Set(Map("id" -> "x", "value" -> 1), Map("id" -> "y", "value" -> 2))
        )
      )
    }
  }

  test("pipeline runs refresh by default") {
    withRawBlockingStub { implicit stub =>
      uploadInputFile("data.json",
        """
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

      uploadInputFile("data.json",
        """
          |{"id": "y", "value": 2}
      """.stripMargin)

      // Create a default StartRun command that refreshes all tables
      startPipelineAndWaitForCompletion(graphId)

      // assert that both tables are refreshed
      verifyMultipleTableContent(
        tableNames = Set("spark_catalog.default.a", "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id", "value"),
          "spark_catalog.default.b" -> Seq("id", "value")
        ),
        expectedContent = Map(
          // both tables should contain the new value in addition to the old one
          "spark_catalog.default.a" ->
            Set(Map("id" -> "x", "value" -> 1), Map("id" -> "y", "value" -> 2)),
          "spark_catalog.default.b" ->
            Set(Map("id" -> "x", "value" -> 1), Map("id" -> "y", "value" -> 2))
        )
      )
    }
  }

  test("pipeline runs full_refresh_all") {
    withRawBlockingStub { implicit stub =>
      uploadInputFile("data.json",
        """
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

      uploadInputFile("data.json",
        """
          |{"id": "y", "value": 2}
        """.stripMargin)

      // Create a default StartRun command that refreshes all tables
      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .setFullRefreshAll(true)
        .build()
      startPipelineAndWaitForCompletion(graphId, Some(startRun))

      // assert that all tables are fully refreshed
      verifyMultipleTableContent(
        tableNames = Set("spark_catalog.default.a", "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id", "value"),
          "spark_catalog.default.b" -> Seq("id", "value")
        ),
        // both tables should only contain the new value
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(Map("id" -> "y", "value" -> 2)),
          "spark_catalog.default.b" -> Set(Map("id" -> "y", "value" -> 2))
        )
      )
    }
  }

  private def verifyMultipleTableContent(
    tableNames: Set[String],
    columnsToVerify: Map[String, Seq[String]],
    expectedContent: Map[String, Set[Map[String, Any]]]): Unit = {
    tableNames.foreach { tableName =>
      spark.catalog.refreshTable(tableName) // clear cache for the table
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
