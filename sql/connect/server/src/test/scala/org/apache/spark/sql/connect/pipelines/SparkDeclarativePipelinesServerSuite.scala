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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{Expression, OutputType, PipelineCommand, PipelineCommandResult, Relation, UnresolvedTableValuedFunction}
import org.apache.spark.connect.proto.PipelineCommand.{DefineFlow, DefineOutput}
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

  test(
    "create dataflow graph set session catalog and database to pipeline " +
      "default catalog and database") {
    withRawBlockingStub { implicit stub =>
      // Use default spark_catalog and create a test database
      sql("CREATE DATABASE IF NOT EXISTS test_db")
      try {
        val graphId = sendPlan(
          buildCreateDataflowGraphPlan(
            proto.PipelineCommand.CreateDataflowGraph
              .newBuilder()
              .setDefaultCatalog("spark_catalog")
              .setDefaultDatabase("test_db")
              .build())).getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId
        val definition =
          getDefaultSessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId)
        assert(definition.defaultCatalog == "spark_catalog")
        assert(definition.defaultDatabase == "test_db")
        assert(getDefaultSessionHolder.session.catalog.currentCatalog() == "spark_catalog")
        assert(getDefaultSessionHolder.session.catalog.currentDatabase == "test_db")
      } finally {
        sql("DROP DATABASE IF EXISTS test_db")
      }
    }
  }

  test("Define a flow for a graph that does not exist") {
    val ex = intercept[Exception] {
      withRawBlockingStub { implicit stub =>
        sendPlan(
          buildPlanFromPipelineCommand(
            PipelineCommand
              .newBuilder()
              .setDefineOutput(
                DefineOutput
                  .newBuilder()
                  .setDataflowGraphId("random-graph-id-that-dne")
                  .setOutputName("mv")
                  .setOutputType(OutputType.MATERIALIZED_VIEW))
              .build()))
      }
    }
    assert(ex.getMessage.contains("DATAFLOW_GRAPH_NOT_FOUND"))

  }

  gridTest("Define flow 'once' argument not supported")(Seq(true, false)) { onceValue =>
    val ex = intercept[Exception] {
      withRawBlockingStub { implicit stub =>
        val graphId = createDataflowGraph
        sendPlan(
          buildPlanFromPipelineCommand(
            PipelineCommand
              .newBuilder()
              .setDefineFlow(DefineFlow
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setOnce(onceValue))
              .build()))
      }
    }
    assert(ex.getMessage.contains("DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED"))
  }

  test(
    "Cross dependency between SQL dataset and non-SQL dataset is valid and can be registered") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setOutputName("mv")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
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
                .setRelationFlowDetails(
                  DefineFlow.WriteRelationFlowDetails
                    .newBuilder()
                    .setRelation(
                      Relation
                        .newBuilder()
                        .setUnresolvedTableValuedFunction(UnresolvedTableValuedFunction
                          .newBuilder()
                          .setFunctionName("range")
                          .addArguments(Expression
                            .newBuilder()
                            .setLiteral(Expression.Literal.newBuilder().setInteger(5).build())
                            .build())
                          .build())
                        .build())
                    .build())
                .build())
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
          outputType = OutputType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createView(name = "viewB", sql = "SELECT * FROM tableA")
        createTable(
          name = "tableC",
          outputType = OutputType.TABLE,
          sql = Some("SELECT * FROM tableA, viewB"))
      }

      val definition =
        getDefaultSessionHolder.dataflowGraphRegistry.getDataflowGraphOrThrow(graphId)

      registerPipelineOutputs(pipeline)
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
          outputType = OutputType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createTable(
          name = "tableB",
          outputType = OutputType.TABLE,
          sql = Some("SELECT * FROM STREAM tableA"))
        createTable(
          name = "tableC",
          outputType = OutputType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM tableB"))
      }

      registerPipelineOutputs(pipeline)
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
          outputType = proto.OutputType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM RANGE(5)"))
        createTable(
          name = "curr.tableB",
          outputType = proto.OutputType.TABLE,
          sql = Some("SELECT * FROM STREAM curr.tableA"))
        createView(name = "viewC", sql = "SELECT * FROM curr.tableB")
        createTable(
          name = "other.tableD",
          outputType = proto.OutputType.MATERIALIZED_VIEW,
          sql = Some("SELECT * FROM viewC"))
      }

      registerPipelineOutputs(pipeline)
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
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId1)
                .setOutputName("session1_table")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
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
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId2)
                .setOutputName("session2_table")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
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
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setOutputName("test_table")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
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
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId1)
                .setOutputName("graph1_table")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
            .build()))

      sendPlan(
        buildPlanFromPipelineCommand(
          PipelineCommand
            .newBuilder()
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId2)
                .setOutputName("graph2_table")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
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
            .setDefineOutput(
              DefineOutput
                .newBuilder()
                .setDataflowGraphId(graphId)
                .setOutputName("test_table")
                .setOutputType(OutputType.MATERIALIZED_VIEW))
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

  private case class DefineOutputTestCase(
      name: String,
      datasetType: OutputType,
      datasetName: String,
      defaultDatabase: String,
      expectedResolvedDatasetName: String,
      expectedResolvedCatalog: String,
      expectedResolvedNamespace: Seq[String])

  private val defineDatasetDefaultTests = Seq(
    DefineOutputTestCase(
      name = "TEMPORARY_VIEW",
      datasetType = OutputType.TEMPORARY_VIEW,
      datasetName = "tv",
      defaultDatabase = "default",
      expectedResolvedDatasetName = "tv",
      expectedResolvedCatalog = "",
      expectedResolvedNamespace = Seq.empty),
    DefineOutputTestCase(
      name = "TABLE",
      datasetType = OutputType.TABLE,
      datasetName = "`tb`",
      defaultDatabase = "default",
      expectedResolvedDatasetName = "tb",
      expectedResolvedCatalog = "spark_catalog",
      expectedResolvedNamespace = Seq("default")),
    DefineOutputTestCase(
      name = "MV",
      datasetType = OutputType.MATERIALIZED_VIEW,
      datasetName = "mv",
      defaultDatabase = "default",
      expectedResolvedDatasetName = "mv",
      expectedResolvedCatalog = "spark_catalog",
      expectedResolvedNamespace = Seq("default"))).map(tc => tc.name -> tc).toMap

  private val defineDatasetCustomTests = Seq(
    DefineOutputTestCase(
      name = "TEMPORARY_VIEW",
      datasetType = OutputType.TEMPORARY_VIEW,
      datasetName = "tv",
      defaultDatabase = "custom_db",
      expectedResolvedDatasetName = "tv",
      expectedResolvedCatalog = "",
      expectedResolvedNamespace = Seq.empty),
    DefineOutputTestCase(
      name = "TABLE",
      datasetType = OutputType.TABLE,
      datasetName = "`tb`",
      defaultDatabase = "`my_db`",
      expectedResolvedDatasetName = "tb",
      expectedResolvedCatalog = "spark_catalog",
      expectedResolvedNamespace = Seq("`my_db`")),
    DefineOutputTestCase(
      name = "MV",
      datasetType = OutputType.MATERIALIZED_VIEW,
      datasetName = "mv",
      defaultDatabase = "another_db",
      expectedResolvedDatasetName = "mv",
      expectedResolvedCatalog = "spark_catalog",
      expectedResolvedNamespace = Seq("another_db")))
    .map(tc => tc.name -> tc)
    .toMap

  namedGridTest("DefineOutput returns resolved data name for default catalog/schema")(
    defineDatasetDefaultTests) { testCase =>
    withRawBlockingStub { implicit stub =>
      // Build and send the CreateDataflowGraph command with default catalog/db
      val graphId = createDataflowGraph
      assert(Option(graphId).isDefined)

      val defineDataset = DefineOutput
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setOutputName(testCase.datasetName)
        .setOutputType(testCase.datasetType)
      val pipelineCmd = PipelineCommand
        .newBuilder()
        .setDefineOutput(defineDataset)
        .build()
      val res = sendPlan(buildPlanFromPipelineCommand(pipelineCmd)).getPipelineCommandResult

      assert(res !== PipelineCommandResult.getDefaultInstance)
      assert(res.hasDefineOutputResult)
      val graphResult = res.getDefineOutputResult
      val identifier = graphResult.getResolvedIdentifier

      assert(identifier.getCatalogName == testCase.expectedResolvedCatalog)
      assert(identifier.getNamespaceList.asScala == testCase.expectedResolvedNamespace)
      assert(identifier.getTableName == testCase.expectedResolvedDatasetName)
    }
  }

  namedGridTest("DefineOutput returns resolved data name for custom schema")(
    defineDatasetCustomTests) { testCase =>
    withRawBlockingStub { implicit stub =>
      sql(s"CREATE DATABASE IF NOT EXISTS spark_catalog.${testCase.defaultDatabase}")
      try {
        // Build and send the CreateDataflowGraph command with custom catalog/db
        val graphId = sendPlan(
          buildCreateDataflowGraphPlan(
            proto.PipelineCommand.CreateDataflowGraph
              .newBuilder()
              .setDefaultCatalog("spark_catalog")
              .setDefaultDatabase(testCase.defaultDatabase)
              .build())).getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId

        assert(graphId.nonEmpty)

        // Build DefineOutput with the created graphId and dataset info
        val defineDataset = DefineOutput
          .newBuilder()
          .setDataflowGraphId(graphId)
          .setOutputName(testCase.datasetName)
          .setOutputType(testCase.datasetType)
        val pipelineCmd = PipelineCommand
          .newBuilder()
          .setDefineOutput(defineDataset)
          .build()

        val res = sendPlan(buildPlanFromPipelineCommand(pipelineCmd)).getPipelineCommandResult
        assert(res !== PipelineCommandResult.getDefaultInstance)
        assert(res.hasDefineOutputResult)
        val graphResult = res.getDefineOutputResult
        val identifier = graphResult.getResolvedIdentifier

        assert(identifier.getCatalogName == testCase.expectedResolvedCatalog)
        assert(identifier.getNamespaceList.asScala == testCase.expectedResolvedNamespace)
        assert(identifier.getTableName == testCase.expectedResolvedDatasetName)
      } finally {
        sql(s"DROP DATABASE IF EXISTS spark_catalog.${testCase.defaultDatabase}")
      }
    }
  }

  private case class DefineFlowTestCase(
      name: String,
      datasetType: OutputType,
      flowName: String,
      defaultDatabase: String,
      expectedResolvedFlowName: String,
      expectedResolvedCatalog: String,
      expectedResolvedNamespace: Seq[String])

  private val defineFlowDefaultTests = Seq(
    DefineFlowTestCase(
      name = "MV",
      datasetType = OutputType.MATERIALIZED_VIEW,
      flowName = "`mv`",
      defaultDatabase = "`default`",
      expectedResolvedFlowName = "mv",
      expectedResolvedCatalog = "spark_catalog",
      expectedResolvedNamespace = Seq("default")),
    DefineFlowTestCase(
      name = "TV",
      datasetType = OutputType.TEMPORARY_VIEW,
      flowName = "tv",
      defaultDatabase = "default",
      expectedResolvedFlowName = "tv",
      expectedResolvedCatalog = "",
      expectedResolvedNamespace = Seq.empty)).map(tc => tc.name -> tc).toMap

  private val defineFlowCustomTests = Seq(
    DefineFlowTestCase(
      name = "MV custom",
      datasetType = OutputType.MATERIALIZED_VIEW,
      flowName = "mv",
      defaultDatabase = "custom_db",
      expectedResolvedFlowName = "mv",
      expectedResolvedCatalog = "spark_catalog",
      expectedResolvedNamespace = Seq("custom_db")),
    DefineFlowTestCase(
      name = "TV custom",
      datasetType = OutputType.TEMPORARY_VIEW,
      flowName = "tv",
      defaultDatabase = "custom_db",
      expectedResolvedFlowName = "tv",
      expectedResolvedCatalog = "",
      expectedResolvedNamespace = Seq.empty)).map(tc => tc.name -> tc).toMap

  namedGridTest("DefineFlow returns resolved data name for default catalog/schema")(
    defineFlowDefaultTests) { testCase =>
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      assert(graphId.nonEmpty)

      // If the dataset type is TEMPORARY_VIEW, define the dataset explicitly first
      if (testCase.datasetType == OutputType.TEMPORARY_VIEW) {
        val defineDataset = DefineOutput
          .newBuilder()
          .setDataflowGraphId(graphId)
          .setOutputName(testCase.flowName)
          .setOutputType(OutputType.TEMPORARY_VIEW)

        val defineDatasetCmd = PipelineCommand
          .newBuilder()
          .setDefineOutput(defineDataset)
          .build()

        val datasetRes =
          sendPlan(buildPlanFromPipelineCommand(defineDatasetCmd)).getPipelineCommandResult
        assert(datasetRes.hasDefineOutputResult)
      }

      val defineFlow = DefineFlow
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFlowName(testCase.flowName)
        .setTargetDatasetName(testCase.flowName)
        .setRelationFlowDetails(
          DefineFlow.WriteRelationFlowDetails
            .newBuilder()
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
                .build())
            .build())
        .build()
      val pipelineCmd = PipelineCommand
        .newBuilder()
        .setDefineFlow(defineFlow)
        .build()
      val res = sendPlan(buildPlanFromPipelineCommand(pipelineCmd)).getPipelineCommandResult
      assert(res.hasDefineFlowResult)
      val graphResult = res.getDefineFlowResult
      val identifier = graphResult.getResolvedIdentifier

      assert(identifier.getCatalogName == testCase.expectedResolvedCatalog)
      assert(identifier.getNamespaceList.asScala == testCase.expectedResolvedNamespace)
      assert(identifier.getTableName == testCase.expectedResolvedFlowName)
    }
  }

  namedGridTest("DefineFlow returns resolved data name for custom catalog/schema")(
    defineFlowCustomTests) { testCase =>
    withRawBlockingStub { implicit stub =>
      sql(s"CREATE DATABASE IF NOT EXISTS spark_catalog.${testCase.defaultDatabase}")
      try {
        val graphId = sendPlan(
          buildCreateDataflowGraphPlan(
            proto.PipelineCommand.CreateDataflowGraph
              .newBuilder()
              .setDefaultCatalog("spark_catalog")
              .setDefaultDatabase(testCase.defaultDatabase)
              .build())).getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId
        assert(graphId.nonEmpty)

        // If the dataset type is TEMPORARY_VIEW, define the dataset explicitly first
        if (testCase.datasetType == OutputType.TEMPORARY_VIEW) {
          val defineDataset = DefineOutput
            .newBuilder()
            .setDataflowGraphId(graphId)
            .setOutputName(testCase.flowName)
            .setOutputType(OutputType.TEMPORARY_VIEW)

          val defineDatasetCmd = PipelineCommand
            .newBuilder()
            .setDefineOutput(defineDataset)
            .build()

          val datasetRes =
            sendPlan(buildPlanFromPipelineCommand(defineDatasetCmd)).getPipelineCommandResult
          assert(datasetRes.hasDefineOutputResult)
        }

        val defineFlow = DefineFlow
          .newBuilder()
          .setDataflowGraphId(graphId)
          .setFlowName(testCase.flowName)
          .setTargetDatasetName(testCase.flowName)
          .setRelationFlowDetails(
            DefineFlow.WriteRelationFlowDetails
              .newBuilder()
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
                  .build())
              .build())
          .build()
        val pipelineCmd = PipelineCommand
          .newBuilder()
          .setDefineFlow(defineFlow)
          .build()
        val res = sendPlan(buildPlanFromPipelineCommand(pipelineCmd)).getPipelineCommandResult
        assert(res.hasDefineFlowResult)
        val graphResult = res.getDefineFlowResult
        val identifier = graphResult.getResolvedIdentifier

        assert(identifier.getCatalogName == testCase.expectedResolvedCatalog)
        assert(identifier.getNamespaceList.asScala == testCase.expectedResolvedNamespace)
        assert(identifier.getTableName == testCase.expectedResolvedFlowName)
      } finally {
        sql(s"DROP DATABASE IF EXISTS spark_catalog.${testCase.defaultDatabase}")
      }
    }
  }

  test(
    "SPARK-54452: spark.sql() inside a pipeline flow function should return a sql_command_result") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipelineAnalysisContext = proto.PipelineAnalysisContext
        .newBuilder()
        .setDataflowGraphId(graphId)
        .setFlowName("flow1")
        .build()
      val userContext = proto.UserContext
        .newBuilder()
        .addExtensions(com.google.protobuf.Any.pack(pipelineAnalysisContext))
        .setUserId("test_user")
        .build()

      val relation = proto.Plan
        .newBuilder()
        .setCommand(
          proto.Command
            .newBuilder()
            .setSqlCommand(
              proto.SqlCommand
                .newBuilder()
                .setInput(
                  proto.Relation
                    .newBuilder()
                    .setRead(proto.Read
                      .newBuilder()
                      .setNamedTable(
                        proto.Read.NamedTable.newBuilder().setUnparsedIdentifier("table"))
                      .build())
                    .build()))
            .build())
        .build()

      val sparkSqlRequest = proto.ExecutePlanRequest
        .newBuilder()
        .setUserContext(userContext)
        .setPlan(relation)
        .setSessionId(UUID.randomUUID().toString)
        .build()
      val sparkSqlResponse = stub.executePlan(sparkSqlRequest).next()
      assert(sparkSqlResponse.hasSqlCommandResult)
      assert(
        sparkSqlResponse.getSqlCommandResult.getRelation ==
          relation.getCommand.getSqlCommand.getInput)
    }
  }

  test(
    "SPARK-54452: spark.sql() outside a pipeline flow function should return a " +
      "sql_command_result") {
    withRawBlockingStub { implicit stub =>
      val graphId = createDataflowGraph
      val pipelineAnalysisContext = proto.PipelineAnalysisContext
        .newBuilder()
        .setDataflowGraphId(graphId)
        .build()
      val userContext = proto.UserContext
        .newBuilder()
        .addExtensions(com.google.protobuf.Any.pack(pipelineAnalysisContext))
        .setUserId("test_user")
        .build()

      val relation = proto.Plan
        .newBuilder()
        .setCommand(
          proto.Command
            .newBuilder()
            .setSqlCommand(
              proto.SqlCommand
                .newBuilder()
                .setInput(proto.Relation
                  .newBuilder()
                  .setSql(proto.SQL.newBuilder().setQuery("SELECT * FROM RANGE(5)"))
                  .build())
                .build())
            .build())
        .build()

      val sparkSqlRequest = proto.ExecutePlanRequest
        .newBuilder()
        .setUserContext(userContext)
        .setPlan(relation)
        .setSessionId(UUID.randomUUID().toString)
        .build()
      val sparkSqlResponse = stub.executePlan(sparkSqlRequest).next()
      assert(sparkSqlResponse.hasSqlCommandResult)
      assert(
        sparkSqlResponse.getSqlCommandResult.getRelation ==
          relation.getCommand.getSqlCommand.getInput)
    }
  }
}
