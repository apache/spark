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

import java.util.UUID

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{DatasetType, Expression, PipelineCommand, PipelineCommandResult, Relation, UnresolvedTableValuedFunction}
import org.apache.spark.connect.proto.PipelineCommand.{DefineDataset, DefineFlow}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.service.{SessionKey, SparkConnectService}

class SparkDeclarativePipelinesServerSuite
    extends SparkDeclarativePipelinesServerTest
    with Logging {
  test("CreateDataflowGraph request creates a new graph") {
    withRawBlockingStub { implicit stub =>
      assert(Option(createDataflowGraph(stub)).isDefined)
    }
  }

  test("DefineDataset returns fully qualified name") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      assert(Option(graphId).isDefined)
      val defineDataset = DefineDataset
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setDatasetName("mv")
        .setDatasetType(DatasetType.MATERIALIZED_VIEW)
      val pipelineCmd = PipelineCommand.newBuilder()
        .setDefineDataset(defineDataset)
        .build()
      val res = sendPlan(buildPlanFromPipelineCommand(pipelineCmd)).getPipelineCommandResult
      assert(res !== PipelineCommandResult.getDefaultInstance)
      assert(res.hasDefineEntityResult)
      val graphResult = res.getDefineEntityResult
      assert(graphResult.getFullyQualifiedName == "fullyqualifieddummyname")
    }
  }

  test("DefineFlow returns fully qualified name") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      assert(Option(graphId).isDefined)
      val defineFlow = DefineFlow
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFlowName("mv")
        .setTargetDatasetName("mv")
        .setRelation(Relation
                    .newBuilder()
                    .setUnresolvedTableValuedFunction(
                      UnresolvedTableValuedFunction
                        .newBuilder()
                        .setFunctionName("range")
                        .addArguments(Expression
                          .newBuilder()
                          .setLiteral(Expression.Literal.newBuilder().setInteger(5).build())
                          .build())
                        .build())
                    .build())
        .build()
      val pipelineCmd = PipelineCommand.newBuilder()
        .setDefineFlow(defineFlow)
        .build()
      val res = sendPlan(buildPlanFromPipelineCommand(pipelineCmd)).getPipelineCommandResult
      assert(res !== PipelineCommandResult.getDefaultInstance)
      assert(res.hasDefineEntityResult)
      val graphResult = res.getDefineEntityResult
      assert(graphResult.getFullyQualifiedName == "fullyqualifieddummyname")
    }
  }

  test("create dataflow graph falls back to current database in session") {
    withRawBlockingStub { implicit stub =>
      sendPlan(buildSqlCommandPlan("CREATE DATABASE test_db"))
      sendPlan(buildSqlCommandPlan("USE DATABASE test_db"))
      val graphId = sendPlan(
        buildCreateDataflowGraphPlan(
          proto.PipelineCommand.CreateDataflowGraph
            .newBuilder()
            .build())).getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId
      val definition =
        getDefaultSessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId)
      assert(definition.defaultDatabase == "test_db")
    }
  }

  test("Define a flow for a graph that does not exist") {
    val ex = intercept[Exception] {
      withRawBlockingStub { implicit stub =>
        sendPlan(
          buildPlanFromPipelineCommand(
            PipelineCommand
              .newBuilder()
              .setDefineDataset(
                DefineDataset
                  .newBuilder()
                  .setDataflowGraphId("random-graph-id-that-dne")
                  .setDatasetName("mv")
                  .setDatasetType(DatasetType.MATERIALIZED_VIEW))
              .build()))
      }
    }
    assert(ex.getMessage.contains("DATAFLOW_GRAPH_NOT_FOUND"))

  }

  test(
    "Cross dependency between SQL dataset and non-SQL dataset is valid and can be registered") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setDatasetName("mv")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build()))

      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineFlow(
              DefineFlow
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setFlowName("mv")
                .setTargetDatasetName("mv")
                .setRelation(
                  Relation
                    .newBuilder()
                    .setUnresolvedTableValuedFunction(
                      UnresolvedTableValuedFunction
                        .newBuilder()
                        .setFunctionName("range")
                        .addArguments(Expression
                          .newBuilder()
                          .setLiteral(Expression.Literal.newBuilder().setInteger(5).build())
                          .build())
                        .build())
                    .build()))
            .build()))
      registerGraphElementsFromSql(
        graphId = graphId,
        sql = """
                |CREATE MATERIALIZED VIEW mv2 AS SELECT 2;
                |CREATE FLOW f AS INSERT INTO mv2 BY NAME SELECT * FROM mv
                |""".stripMargin)

      val definition =
        getDefaultSessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId)

      val graph = definition.toDataflowGraph.resolve()

      assert(graph.flows.size == 3)
      assert(graph.tables.size == 2)
      assert(graph.views.isEmpty)

      val mvFlow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "spark_catalog.default.mv").head
      assert(mvFlow.inputs == Set())
      assert(mvFlow.destinationIdentifier.unquotedString == "spark_catalog.default.mv")

      val mv2Flow =
        graph.resolvedFlows
          .filter(_.identifier.unquotedString == "spark_catalog.default.mv2")
          .head
      assert(mv2Flow.inputs == Set())
      assert(mv2Flow.destinationIdentifier.unquotedString == "spark_catalog.default.mv2")

      // flow defined in SQL that connects the non SQL dataset mv to the SQL dataset mv2 should
      // work.
      val namedFlow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "spark_catalog.default.f").head
      assert(namedFlow.inputs.map(_.unquotedString) == Set("spark_catalog.default.mv"))
      assert(namedFlow.destinationIdentifier.unquotedString == "spark_catalog.default.mv2")
    }
  }

  test("simple graph resolution test") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "tableA",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createView(name = "viewB", sql = "SELECT * FROM tableA")
        createTable(
          name = "tableC",
          datasetType = DatasetType.TABLE,
          sql = Some("SELECT * FROM tableA, viewB"))
      }

      val definition =
        getDefaultSessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId)

      registerPipelineDatasets(pipeline)
      val graph = definition.toDataflowGraph
        .resolve()

      assert(graph.flows.size == 3)
      assert(graph.tables.size == 2)
      assert(graph.views.size == 1)

      val tableCFlow =
        graph.resolvedFlows
          .filter(_.identifier.unquotedString == "spark_catalog.default.tableC")
          .head
      assert(
        tableCFlow.inputs.map(_.unquotedString) == Set("viewB", "spark_catalog.default.tableA"))

      val viewBFlow =
        graph.resolvedFlows.filter(_.identifier.unquotedString == "viewB").head
      assert(viewBFlow.inputs.map(_.unquotedString) == Set("spark_catalog.default.tableA"))

      val tableAFlow =
        graph.resolvedFlows
          .filter(_.identifier.unquotedString == "spark_catalog.default.tableA")
          .head
      assert(tableAFlow.inputs == Set())
    }
  }

  test("execute pipeline end-to-end test") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph(stub)

      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "tableA",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createTable(
          name = "tableB",
          datasetType = DatasetType.TABLE,
          sql = Some("SELECT * FROM STREAM tableA"))
        createTable(
          name = "tableC",
          datasetType = DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM tableB"))
      }

      registerPipelineDatasets(pipeline)
      startPipelineAndWaitForCompletion(graphId)
      // Check that each table has the correct data.
      assert(spark.table("spark_catalog.default.tableA").count() == 5)
      assert(spark.table("spark_catalog.default.tableB").count() == 5)
      assert(spark.table("spark_catalog.default.tableC").count() == 5)
    }
  }

  test("create streaming tables, materialized views, and temporary views") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph

      sql(s"CREATE SCHEMA IF NOT EXISTS spark_catalog.`curr`")
      sql(s"CREATE SCHEMA IF NOT EXISTS spark_catalog.`other`")

      val pipeline = new TestPipelineDefinition(graphId) {
        createTable(
          name = "curr.tableA",
          datasetType = proto.DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createTable(
          name = "curr.tableB",
          datasetType = proto.DatasetType.TABLE,
          sql = Some("SELECT * FROM STREAM curr.tableA"))
        createView(name = "viewC", sql = "SELECT * FROM curr.tableB")
        createTable(
          name = "other.tableD",
          datasetType = proto.DatasetType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM viewC"))
      }

      registerPipelineDatasets(pipeline)
      startPipelineAndWaitForCompletion(graphId)

      // Check that each table has the correct data.
      assert(spark.table("spark_catalog.curr.tableA").count() == 5)
      assert(spark.table("spark_catalog.curr.tableB").count() == 5)
      assert(spark.table("spark_catalog.other.tableD").count() == 5)
    }
  }

  test("dataflow graphs are session-specific") {
    withRawBlockingStub { implicit stub =>
      // Create a dataflow graph in the default session
      val graphId1 = createDataflowGraph

      // Register a dataset in the default session
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId1)
                .setDatasetName("session1_table")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build()))

      // Verify the graph exists in the default session
      assert(getDefaultSessionHolder.dataflowGraphRegistry.getAllDataflowGraphs.size == 1)
    }

    // Create a second session with different user/session ID
    val newSessionId = UUID.randomUUID().toString
    val newSessionUserId = "session2_user"

    withRawBlockingStub { implicit stub =>
      // Override the test context to use different session
      val newSessionExecuteRequest = buildExecutePlanRequest(
        buildCreateDataflowGraphPlan(
          proto.PipelineCommand.CreateDataflowGraph
            .newBuilder()
            .setDefaultCatalog("spark_catalog")
            .setDefaultDatabase("default")
            .build())).toBuilder
        .setUserContext(proto.UserContext
          .newBuilder()
          .setUserId(newSessionUserId)
          .build())
        .setSessionId(newSessionId)
        .build()

      val response = stub.executePlan(newSessionExecuteRequest)
      val graphId2 =
        response.next().getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId

      // Register a different dataset in second session
      val session2DefineRequest = buildExecutePlanRequest(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId2)
                .setDatasetName("session2_table")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build())).toBuilder
        .setUserContext(proto.UserContext
          .newBuilder()
          .setUserId(newSessionUserId)
          .build())
        .setSessionId(newSessionId)
        .build()

      stub.executePlan(session2DefineRequest).next()

      // Verify session isolation - each session should only see its own graphs
      val newSessionHolder = SparkConnectService.sessionManager
        .getIsolatedSession(SessionKey(newSessionUserId, newSessionId), None)

      val defaultSessionGraphs =
        getDefaultSessionHolder.dataflowGraphRegistry.getAllDataflowGraphs
      val newSessionGraphs = newSessionHolder.dataflowGraphRegistry.getAllDataflowGraphs

      assert(defaultSessionGraphs.size == 1)
      assert(newSessionGraphs.size == 1)

      assert(
        defaultSessionGraphs.head.toDataflowGraph.tables
          .exists(_.identifier.table == "session1_table"),
        "Session 1 should have its own table")
      assert(
        newSessionGraphs.head.toDataflowGraph.tables
          .exists(_.identifier.table == "session2_table"),
        "Session 2 should have its own table")
    }
  }

  test("dataflow graphs are cleaned up when session is closed") {
    val testUserId = "test_user"
    val testSessionId = UUID.randomUUID().toString

    // Create a session and dataflow graph
    withRawBlockingStub { implicit stub =>
      val createGraphRequest = buildExecutePlanRequest(
        buildCreateDataflowGraphPlan(
          proto.PipelineCommand.CreateDataflowGraph
            .newBuilder()
            .setDefaultCatalog("spark_catalog")
            .setDefaultDatabase("default")
            .build())).toBuilder
        .setUserContext(proto.UserContext
          .newBuilder()
          .setUserId(testUserId)
          .build())
        .setSessionId(testSessionId)
        .build()

      val response = stub.executePlan(createGraphRequest)
      val graphId =
        response.next().getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId

      // Register a dataset
      val defineRequest = buildExecutePlanRequest(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setDatasetName("test_table")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build())).toBuilder
        .setUserContext(proto.UserContext
          .newBuilder()
          .setUserId(testUserId)
          .build())
        .setSessionId(testSessionId)
        .build()

      stub.executePlan(defineRequest).next()

      // Verify the graph exists
      val sessionHolder = SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(testUserId, testSessionId))
        .get

      val graphsBefore = sessionHolder.dataflowGraphRegistry.getAllDataflowGraphs
      assert(graphsBefore.size == 1)

      // Close the session
      SparkConnectService.sessionManager.closeSession(SessionKey(testUserId, testSessionId))

      // Verify the session is no longer available
      val sessionAfterClose = SparkConnectService.sessionManager
        .getIsolatedSessionIfPresent(SessionKey(testUserId, testSessionId))

      assert(sessionAfterClose.isEmpty, "Session should be cleaned up after close")
      // Verify the graph is removed
      val graphsAfter = sessionHolder.dataflowGraphRegistry.getAllDataflowGraphs
      assert(graphsAfter.isEmpty, "Graph should be removed after session close")
    }
  }

  test("multiple dataflow graphs can exist in the same session") {
    withRawBlockingStub { implicit stub =>
      // Create two dataflow graphs in the same session
      val graphId1 = createDataflowGraph
      val graphId2 = createDataflowGraph

      // Register datasets in both graphs
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId1)
                .setDatasetName("graph1_table")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build()))

      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId2)
                .setDatasetName("graph2_table")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build()))

      // Verify both graphs exist in the session
      val sessionHolder = getDefaultSessionHolder
      val graph1 = sessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId1)
      val graph2 = sessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId2)
      // Check that both graphs have their datasets registered
      assert(graph1.toDataflowGraph.tables.exists(_.identifier.table == "graph1_table"))
      assert(graph2.toDataflowGraph.tables.exists(_.identifier.table == "graph2_table"))
    }
  }

  test("dropping a dataflow graph removes it from session") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph

      // Register a dataset
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineDataset(
              DefineDataset
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setDatasetName("test_table")
                .setDatasetType(DatasetType.MATERIALIZED_VIEW))
            .build()))

      // Verify the graph exists
      val sessionHolder = getDefaultSessionHolder
      val graphsBefore = sessionHolder.dataflowGraphRegistry.getAllDataflowGraphs
      assert(graphsBefore.size == 1)

      // Drop the graph
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDropDataflowGraph(PipelineCommand.DropDataflowGraph
              .newBuilder()
              .setDataflowGraphId(graphId))
            .build()))

      // Verify the graph is removed
      val graphsAfter = sessionHolder.dataflowGraphRegistry.getAllDataflowGraphs
      assert(graphsAfter.isEmpty, "Graph should be removed after drop")
    }
  }
}
