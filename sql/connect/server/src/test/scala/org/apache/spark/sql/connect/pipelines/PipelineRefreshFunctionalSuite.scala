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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.DatasetType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connect.service.{SessionKey, SparkConnectService}
import org.apache.spark.sql.pipelines.utils.{EventVerificationTestHelpers, TestPipelineUpdateContextMixin}

/**
 * Comprehensive test suite that validates pipeline refresh functionality by running actual
 * pipelines with different refresh parameters and validating the results.
 */
class PipelineRefreshFunctionalSuite
  extends SparkDeclarativePipelinesServerTest
    with TestPipelineUpdateContextMixin
    with EventVerificationTestHelpers {

  private val externalSourceTable = TableIdentifier(
    catalog = Some("spark_catalog"),
    database = Some("default"),
    table = "source_data"
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create source directory for streaming input
    spark.sql(s"CREATE TABLE $externalSourceTable AS SELECT * FROM RANGE(1, 2)")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Clean up the source table after each test
    spark.sql(s"DROP TABLE IF EXISTS $externalSourceTable")
  }

  private def createTwoSTPipeline(graphId: String): TestPipelineDefinition = {
    new TestPipelineDefinition(graphId) {
      // Create tables that depend on the mv
      createTable(
        name = "a",
        datasetType = DatasetType.TABLE,
        sql = Some(s"SELECT id FROM STREAM $externalSourceTable"))

      createTable(
        name = "b",
        datasetType = DatasetType.TABLE,
        sql = Some(s"SELECT id FROM STREAM $externalSourceTable"))
    }
  }

  test("pipeline runs selective full_refresh") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 1)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1)
          )
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // spark overwrite the table source_data with new data
      spark.sql("INSERT OVERWRITE TABLE spark_catalog.default.source_data " +
        "SELECT * FROM VALUES (2), (3) AS t(id)")

      // Run with full refresh on specific tables
      val fullRefreshTables = List("a")
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
      // Verify completion event
      assert(capturedEvents.exists(_.getMessage.contains("Run is COMPLETED")))

      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 2), // a should be fully refreshed and only contain the new value
            Map("id" -> 3)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1) // b is refreshed, so it retains the old value
          )
        )
      )
    }
  }

  test("pipeline runs selective full_refresh and selective refresh") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 1)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1)
          )
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // spark overwrite the table source_data with new data
      spark.sql("INSERT OVERWRITE TABLE spark_catalog.default.source_data " +
        "SELECT * FROM VALUES (2), (3) AS t(id)")

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addFullRefresh("a")
        .addRefresh("b")
        .build()

      startPipelineAndWaitForCompletion(graphId, Some(startRun))

      // assert that table_b is refreshed
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 2), // a is fully refreshed and only contain the new value
            Map("id" -> 3)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1), // b is refreshed, so it retains the old value and adds the new one
            Map("id" -> 2),
            Map("id" -> 3)
          )
        )
      )
    }
  }

  test("pipeline runs refresh by default") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 1)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1)
          )
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // spark overwrite the table source_data with new data
      spark.sql("INSERT OVERWRITE TABLE spark_catalog.default.source_data " +
        "SELECT * FROM VALUES (2), (3) AS t(id)")

      // Create a default StartRun command that refreshes all tables
      startPipelineAndWaitForCompletion(graphId)

      // assert that both tables are refreshed
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 1), // a is refreshed by default, retains the old value and adds the new one
            Map("id" -> 2),
            Map("id" -> 3)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1), // b is refreshed by default, retains the old value and adds the new one
            Map("id" -> 2),
            Map("id" -> 3)
          )
        )
      )
    }
  }

  test("pipeline runs full refresh all") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // Verify initial data from file stream
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 1)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 1)
          )
        )
      )

      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // spark overwrite the table source_data with new data
      spark.sql("INSERT OVERWRITE TABLE spark_catalog.default.source_data " +
        "SELECT * FROM VALUES (2), (3) AS t(id)")

      // Create a default StartRun command that refreshes all tables
      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .setFullRefreshAll(true)
        .build()
      startPipelineAndWaitForCompletion(graphId, Some(startRun))

      // assert that both tables are refreshed
      verifyMultipleTableContent(
        tableNames = Set(
          "spark_catalog.default.a",
          "spark_catalog.default.b"),
        columnsToVerify = Map(
          "spark_catalog.default.a" -> Seq("id"),
          "spark_catalog.default.b" -> Seq("id")
        ),
        expectedContent = Map(
          "spark_catalog.default.a" -> Set(
            Map("id" -> 2),
            Map("id" -> 3)
          ),
          "spark_catalog.default.b" -> Set(
            Map("id" -> 2),
            Map("id" -> 3)
          )
        )
      )
    }
  }

  test("validation: cannot specify subset refresh when full_refresh_all is true") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .setFullRefreshAll(true)
        .addRefresh("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(graphId, Some(startRun))
      }
      assert(exception.getMessage.contains(
        "Cannot specify a subset to full refresh when full refresh all is set to true"))
    }
  }

  test("validation: cannot specify subset full_refresh when full_refresh_all is true") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .setFullRefreshAll(true)
        .addFullRefresh("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(graphId, Some(startRun))
      }
      assert(exception.getMessage.contains(
        "Cannot specify a subset to refresh when full refresh all is set to true"))
    }
  }

  test("validation: refresh and full_refresh cannot overlap") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addRefresh("a")
        .addFullRefresh("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(graphId, Some(startRun))
      }
      assert(exception.getMessage.contains(
        "Datasets specified for refresh and full refresh cannot overlap"))
      assert(exception.getMessage.contains("a"))
    }
  }

  test("validation: multiple overlapping tables in refresh and full_refresh not allowed") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addRefresh("a")
        .addRefresh("b")
        .addFullRefresh("a")
        .addFullRefresh("file_data")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(graphId, Some(startRun))
      }
      assert(exception.getMessage.contains(
        "Datasets specified for refresh and full refresh cannot overlap"))
      assert(exception.getMessage.contains("a"))
    }
  }

  test("validation: fully qualified table names in validation") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTwoSTPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = proto.PipelineCommand.StartRun.newBuilder()
        .setDataflowGraphId(graphId)
        .addRefresh("spark_catalog.default.a")
        .addFullRefresh("a")  // This should be treated as the same table
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(graphId, Some(startRun))
      }
      assert(exception.getMessage.contains(
        "Datasets specified for refresh and full refresh cannot overlap"))
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
