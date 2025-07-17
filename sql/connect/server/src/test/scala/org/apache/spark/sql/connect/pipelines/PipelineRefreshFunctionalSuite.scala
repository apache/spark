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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto.{DatasetType, PipelineCommand, PipelineEvent}
import org.apache.spark.sql.QueryTest
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
    table = "source_data")

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create source table to simulate streaming updates
    spark.sql(s"CREATE TABLE $externalSourceTable AS SELECT * FROM RANGE(1, 2)")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Clean up the source table after each test
    spark.sql(s"DROP TABLE IF EXISTS $externalSourceTable")
  }

  private def createTestPipeline(graphId: String): TestPipelineDefinition = {
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
      createTable(
        name = "mv",
        datasetType = DatasetType.MATERIALIZED_VIEW,
        sql = Some(s"SELECT id FROM a"))
    }
  }

  /**
   * Helper method to run refresh tests with common setup and verification logic. This reduces
   * code duplication across the refresh test cases.
   */
  private def runRefreshTest(
      refreshConfigBuilder: String => Option[PipelineCommand.StartRun] = _ => None,
      expectedContentAfterRefresh: Map[String, Set[Map[String, Any]]],
      eventValidation: Option[ArrayBuffer[PipelineEvent] => Unit] = None): Unit = {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTestPipeline(graphId)
      registerPipelineDatasets(pipeline)

      // First run to populate tables
      startPipelineAndWaitForCompletion(graphId)

      // combine above into a map for verification
      val initialContent = Map(
        "spark_catalog.default.a" -> Set(Map("id" -> 1)),
        "spark_catalog.default.b" -> Set(Map("id" -> 1)),
        "spark_catalog.default.mv" -> Set(Map("id" -> 1)))
      // Verify initial content
      initialContent.foreach { case (tableName, expectedRows) =>
        checkTableContent(tableName, expectedRows)
      }
      // Clear cached pipeline execution before starting new run
      SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
        .foreach(_.removeAllPipelineExecutions())

      // Replace source data to simulate a streaming update
      spark.sql(
        "INSERT OVERWRITE TABLE spark_catalog.default.source_data " +
          "SELECT * FROM VALUES (2), (3) AS t(id)")

      // Run with specified refresh configuration
      val capturedEvents = refreshConfigBuilder(graphId) match {
        case Some(startRun) => startPipelineAndWaitForCompletion(startRun)
        case None => startPipelineAndWaitForCompletion(graphId)
      }

      // Additional validation if provided
      eventValidation.foreach(_(capturedEvents))

      // Verify final content with checkTableContent
      expectedContentAfterRefresh.foreach { case (tableName, expectedRows) =>
        checkTableContent(tableName, expectedRows)
      }
    }
  }

  test("pipeline runs selective full_refresh") {
    runRefreshTest(
      refreshConfigBuilder = { graphId =>
        Some(
          PipelineCommand.StartRun
            .newBuilder()
            .setDataflowGraphId(graphId)
            .addAllFullRefreshSelection(List("a").asJava)
            .build())
      },
      expectedContentAfterRefresh = Map(
        "spark_catalog.default.a" -> Set(
          Map("id" -> 2), // a is fully refreshed and only contains the new values
          Map("id" -> 3)),
        "spark_catalog.default.b" -> Set(
          Map("id" -> 1) // b is not refreshed, so it retains the old value
        ),
        "spark_catalog.default.mv" -> Set(
          Map("id" -> 1) // mv is not refreshed, so it retains the old value
        )),
      eventValidation = Some { capturedEvents =>
        // assert that table_b is excluded
        assert(
          capturedEvents.exists(
            _.getMessage.contains(s"Flow \'spark_catalog.default.b\' is EXCLUDED.")))
        // assert that table_a ran to completion
        assert(
          capturedEvents.exists(
            _.getMessage.contains(s"Flow spark_catalog.default.a has COMPLETED.")))
        // assert that mv is excluded
        assert(
          capturedEvents.exists(
            _.getMessage.contains(s"Flow \'spark_catalog.default.mv\' is EXCLUDED.")))
        // Verify completion event
        assert(capturedEvents.exists(_.getMessage.contains("Run is COMPLETED")))
      })
  }

  test("pipeline runs selective full_refresh and selective refresh") {
    runRefreshTest(
      refreshConfigBuilder = { graphId =>
        Some(
          PipelineCommand.StartRun
            .newBuilder()
            .setDataflowGraphId(graphId)
            .addAllFullRefreshSelection(Seq("a", "mv").asJava)
            .addRefreshSelection("b")
            .build())
      },
      expectedContentAfterRefresh = Map(
        "spark_catalog.default.a" -> Set(
          Map("id" -> 2), // a is fully refreshed and only contains the new values
          Map("id" -> 3)),
        "spark_catalog.default.b" -> Set(
          Map("id" -> 1), // b is refreshed, so it retains the old value and adds the new ones
          Map("id" -> 2),
          Map("id" -> 3)),
        "spark_catalog.default.mv" -> Set(
          Map("id" -> 2), // mv is fully refreshed and only contains the new values
          Map("id" -> 3))))
  }

  test("pipeline runs refresh by default") {
    runRefreshTest(expectedContentAfterRefresh =
      Map(
        "spark_catalog.default.a" -> Set(
          Map(
            "id" -> 1
          ), // a is refreshed by default, retains the old value and adds the new ones
          Map("id" -> 2),
          Map("id" -> 3)),
        "spark_catalog.default.b" -> Set(
          Map(
            "id" -> 1
          ), // b is refreshed by default, retains the old value and adds the new ones
          Map("id" -> 2),
          Map("id" -> 3)),
        "spark_catalog.default.mv" -> Set(
          Map("id" -> 1),
          Map("id" -> 2), // mv is refreshed from table a, retains all values
          Map("id" -> 3))))
  }

  test("pipeline runs full refresh all") {
    runRefreshTest(
      refreshConfigBuilder = { graphId =>
        Some(
          PipelineCommand.StartRun
            .newBuilder()
            .setDataflowGraphId(graphId)
            .setFullRefreshAll(true)
            .build())
      },
      // full refresh all causes all tables to lose the initial value
      // and only contain the new values after the source data is updated
      expectedContentAfterRefresh = Map(
        "spark_catalog.default.a" -> Set(Map("id" -> 2), Map("id" -> 3)),
        "spark_catalog.default.b" -> Set(Map("id" -> 2), Map("id" -> 3)),
        "spark_catalog.default.mv" -> Set(Map("id" -> 2), Map("id" -> 3))))
  }

  test("validation: cannot specify subset refresh when full_refresh_all is true") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTestPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = PipelineCommand.StartRun
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFullRefreshAll(true)
        .addRefreshSelection("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(startRun)
      }
      assert(
        exception.getMessage.contains(
          "Cannot specify a subset to refresh when full refresh all is set to true"))
    }
  }

  test("validation: cannot specify subset full_refresh when full_refresh_all is true") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTestPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = PipelineCommand.StartRun
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFullRefreshAll(true)
        .addFullRefreshSelection("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(startRun)
      }
      assert(
        exception.getMessage.contains(
          "Cannot specify a subset to full refresh when full refresh all is set to true"))
    }
  }

  test("validation: refresh and full_refresh cannot overlap") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTestPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = PipelineCommand.StartRun
        .newBuilder()
        .setDataflowGraphId(graphId)
        .addRefreshSelection("a")
        .addFullRefreshSelection("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(startRun)
      }
      assert(
        exception.getMessage.contains(
          "Datasets specified for refresh and full refresh cannot overlap"))
      assert(exception.getMessage.contains("a"))
    }
  }

  test("validation: multiple overlapping tables in refresh and full_refresh not allowed") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTestPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = PipelineCommand.StartRun
        .newBuilder()
        .setDataflowGraphId(graphId)
        .addRefreshSelection("a")
        .addRefreshSelection("b")
        .addFullRefreshSelection("a")
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(startRun)
      }
      assert(
        exception.getMessage.contains(
          "Datasets specified for refresh and full refresh cannot overlap"))
      assert(exception.getMessage.contains("a"))
    }
  }

  test("validation: fully qualified table names in validation") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = createTestPipeline(graphId)
      registerPipelineDatasets(pipeline)

      val startRun = PipelineCommand.StartRun
        .newBuilder()
        .setDataflowGraphId(graphId)
        .addRefreshSelection("spark_catalog.default.a")
        .addFullRefreshSelection("a") // This should be treated as the same table
        .build()

      val exception = intercept[IllegalArgumentException] {
        startPipelineAndWaitForCompletion(startRun)
      }
      assert(
        exception.getMessage.contains(
          "Datasets specified for refresh and full refresh cannot overlap"))
    }
  }

  private def checkTableContent[A <: Map[String, Any]](
      name: String,
      expectedContent: Set[A]): Unit = {
    spark.catalog.refreshTable(name) // clear cache for the table
    val df = spark.table(name)
    QueryTest.checkAnswer(
      df,
      expectedContent
        .map(row => {
          // Convert each row to a Row object
          org.apache.spark.sql.Row.fromSeq(row.values.toSeq)
        })
        .toSeq
        .asJava)
  }
}
