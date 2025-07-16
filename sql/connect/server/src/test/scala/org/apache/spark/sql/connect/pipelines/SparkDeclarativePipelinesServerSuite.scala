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

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{DatasetType, Expression, PipelineCommand, Relation, UnresolvedTableValuedFunction}
import org.apache.spark.connect.proto.PipelineCommand.{DefineDataset, DefineFlow}
import org.apache.spark.internal.Logging

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
        DataflowGraphRegistry
          .getDataflowGraphOrThrow(graphId)
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
        DataflowGraphRegistry
          .getDataflowGraphOrThrow(graphId)

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
        DataflowGraphRegistry
          .getDataflowGraphOrThrow(graphId)

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
}
