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

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connect.PythonTestDepsChecker
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.pipelines.Language.Python
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, PipelineUpdateContextImpl, QueryOrigin, QueryOriginType}
import org.apache.spark.sql.pipelines.logging.EventLevel
import org.apache.spark.sql.pipelines.utils.{EventVerificationTestHelpers, TestPipelineUpdateContextMixin}
import org.apache.spark.sql.types.StructType

/**
 * Test suite that starts a Spark Connect server and executes Spark Declarative Pipelines Python
 * code to define tables in the pipeline.
 */
class PythonPipelineSuite
    extends SparkDeclarativePipelinesServerTest
    with TestPipelineUpdateContextMixin
    with EventVerificationTestHelpers {

  def buildGraph(pythonText: String): DataflowGraph = {
    val indentedPythonText = pythonText.linesIterator.map("        " + _).mkString("\n")
    // create a unique identifier to allow identifying the session and dataflow graph
    val customSessionIdentifier = UUID.randomUUID().toString
    val pythonCode =
      s"""
         |from pyspark.sql import SparkSession
         |from pyspark import pipelines as dp
         |from pyspark.pipelines.spark_connect_graph_element_registry import (
         |    SparkConnectGraphElementRegistry,
         |)
         |from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph
         |from pyspark.pipelines.graph_element_registry import (
         |    graph_element_registration_context,
         |)
         |from pyspark.pipelines.add_pipeline_analysis_context import (
         |    add_pipeline_analysis_context
         |)
         |
         |spark = SparkSession.builder \\
         |    .remote("sc://localhost:$serverPort") \\
         |    .config("spark.connect.grpc.channel.timeout", "5s") \\
         |    .config("spark.custom.identifier", "$customSessionIdentifier") \\
         |    .create()
         |
         |dataflow_graph_id = create_dataflow_graph(
         |    spark,
         |    default_catalog=None,
         |    default_database=None,
         |    sql_conf={},
         |)
         |
         |registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
         |with add_pipeline_analysis_context(
         |    spark=spark, dataflow_graph_id=dataflow_graph_id, flow_name=None
         |):
         |    with graph_element_registration_context(registry):
         |$indentedPythonText
         |""".stripMargin

    logInfo(s"Running code: $pythonCode")
    val (exitCode, output) = executePythonCode(pythonCode)

    if (exitCode != 0) {
      throw new RuntimeException(
        s"Python process failed with exit code $exitCode. Output: ${output.mkString("\n")}")
    }
    val activeSessions = SparkConnectService.sessionManager.listActiveSessions

    // get the session holder by finding the session with the custom UUID set in the conf
    val sessionHolder = activeSessions
      .map(info => SparkConnectService.sessionManager.getIsolatedSession(info.key, None))
      .find(_.session.conf.get("spark.custom.identifier") == customSessionIdentifier)
      .getOrElse(
        throw new RuntimeException(s"Session with identifier $customSessionIdentifier not found"))

    // get all dataflow graphs from the session holder
    val dataflowGraphContexts = sessionHolder.dataflowGraphRegistry.getAllDataflowGraphs
    assert(dataflowGraphContexts.size == 1)

    dataflowGraphContexts.head.toDataflowGraph
  }

  def graphIdentifier(name: String): TableIdentifier = {
    TableIdentifier(catalog = Option("spark_catalog"), database = Option("default"), table = name)
  }

  test("basic") {
    val graph = buildGraph("""
        |@dp.table
        |def table1():
        |    return spark.readStream.format("rate").load()
        |""".stripMargin)
      .resolve()
      .validate()
    assert(graph.flows.size == 1)
    assert(graph.tables.size == 1)
  }

  test("failed flow progress event has correct python source code location") {
    // Note that pythonText will be inserted into line 26 of the python script that is run.
    val unresolvedGraph = buildGraph(pythonText = """
        |@dp.table()
        |def table1():
        |    df = spark.createDataFrame([(25,), (30,), (45,)], ["age"])
        |    return df.select("name")
        |""".stripMargin)

    val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph, storageRoot)
    updateContext.pipelineExecution.runPipeline()

    assertFlowProgressEvent(
      updateContext.eventBuffer,
      identifier = graphIdentifier("table1"),
      expectedFlowStatus = FlowStatus.FAILED,
      cond = flowProgressEvent =>
        flowProgressEvent.origin.sourceCodeLocation == Option(
          QueryOrigin(
            language = Option(Python()),
            filePath = Option("<string>"),
            line = Option(34),
            objectName = Option("spark_catalog.default.table1"),
            objectType = Option(QueryOriginType.Flow.toString))),
      errorChecker = ex =>
        ex.getMessage.contains(
          "A column, variable, or function parameter with name `name` cannot be resolved."),
      expectedEventLevel = EventLevel.WARN)
  }

  test("flow progress events have correct python source code location") {
    val unresolvedGraph = buildGraph(pythonText = """
        |@dp.table(
        | comment = 'my table'
        |)
        |def table1():
        |    return spark.readStream.table('mv')
        |
        |@dp.materialized_view
        |def mv2():
        |   return spark.range(26, 29)
        |
        |@dp.materialized_view
        |def mv():
        |   df = spark.createDataFrame([(25,), (30,), (45,)], ["age"])
        |   return df.select("age")
        |
        |@dp.append_flow(
        | target = 'table1'
        |)
        |def standalone_flow1():
        |   return spark.readStream.table('mv2')
        |""".stripMargin)

    val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph, storageRoot)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    Seq(
      FlowStatus.QUEUED,
      FlowStatus.STARTING,
      FlowStatus.PLANNING,
      FlowStatus.RUNNING,
      FlowStatus.COMPLETED).foreach { flowStatus =>
      assertFlowProgressEvent(
        updateContext.eventBuffer,
        identifier = graphIdentifier("mv2"),
        expectedFlowStatus = flowStatus,
        cond = flowProgressEvent =>
          flowProgressEvent.origin.sourceCodeLocation == Option(
            QueryOrigin(
              language = Option(Python()),
              filePath = Option("<string>"),
              line = Option(40),
              objectName = Option("spark_catalog.default.mv2"),
              objectType = Option(QueryOriginType.Flow.toString))),
        expectedEventLevel = EventLevel.INFO)

      assertFlowProgressEvent(
        updateContext.eventBuffer,
        identifier = graphIdentifier("mv"),
        expectedFlowStatus = flowStatus,
        cond = flowProgressEvent =>
          flowProgressEvent.origin.sourceCodeLocation == Option(
            QueryOrigin(
              language = Option(Python()),
              filePath = Option("<string>"),
              line = Option(44),
              objectName = Option("spark_catalog.default.mv"),
              objectType = Option(QueryOriginType.Flow.toString))),
        expectedEventLevel = EventLevel.INFO)
    }

    // Note that streaming flows do not have a PLANNING phase.
    Seq(FlowStatus.QUEUED, FlowStatus.STARTING, FlowStatus.RUNNING, FlowStatus.COMPLETED)
      .foreach { flowStatus =>
        assertFlowProgressEvent(
          updateContext.eventBuffer,
          identifier = graphIdentifier("table1"),
          expectedFlowStatus = flowStatus,
          cond = flowProgressEvent =>
            flowProgressEvent.origin.sourceCodeLocation == Option(
              QueryOrigin(
                language = Option(Python()),
                filePath = Option("<string>"),
                line = Option(34),
                objectName = Option("spark_catalog.default.table1"),
                objectType = Option(QueryOriginType.Flow.toString))),
          expectedEventLevel = EventLevel.INFO)

        assertFlowProgressEvent(
          updateContext.eventBuffer,
          identifier = graphIdentifier("standalone_flow1"),
          expectedFlowStatus = flowStatus,
          cond = flowProgressEvent =>
            flowProgressEvent.origin.sourceCodeLocation == Option(
              QueryOrigin(
                language = Option(Python()),
                filePath = Option("<string>"),
                line = Option(49),
                objectName = Option("spark_catalog.default.standalone_flow1"),
                objectType = Option(QueryOriginType.Flow.toString))),
          expectedEventLevel = EventLevel.INFO)
      }
  }

  test("basic with inverted topological order") {
    // This graph is purposefully in the wrong topological order to test the topological sort
    val graph = buildGraph("""
        |@dp.table()
        |def b():
        |  return spark.readStream.table("a")
        |
        |@dp.table()
        |def c():
        |  return spark.readStream.table("a")
        |
        |@dp.materialized_view()
        |def d():
        |  return spark.read.table("a")
        |
        |@dp.materialized_view()
        |def a():
        |  return spark.range(5)
        |""".stripMargin)
    val resolvedGraph = graph.resolve().validate()
    assert(resolvedGraph.tables.size == 4)
    assert(resolvedGraph.resolvedFlows.size == 4)
  }

  test("flows") {
    val graph = buildGraph("""
      |@dp.table()
      |def a():
      |  return spark.readStream.format("rate").load()
      |
      |@dp.append_flow(target = "a")
      |def supplement():
      |  return spark.readStream.format("rate").load()
      |""".stripMargin).resolve().validate()

    assert(graph.tables.map(_.identifier.table).toSet == Set("a"))
    assert(graph.resolvedFlows.size == 2)
    assert(
      graph.flowsTo(graphIdentifier("a")).map(_.identifier).toSet == Set(
        graphIdentifier("a"),
        graphIdentifier("supplement")))
  }

  test("external sink") {
    val graph = buildGraph("""
        |dp.create_sink(
        |  "myKafkaSink",
        |  format = "kafka",
        |  options = {"kafka.bootstrap.servers": "host1:port1,host2:port2"}
        |)
        |
        |@dp.append_flow(
        |  target = "myKafkaSink"
        |)
        |def mySinkFlow():
        |  return spark.readStream.format("rate").load()
        |""".stripMargin)

    assert(graph.sinks.map(_.identifier) == Seq(TableIdentifier("myKafkaSink")))

    // ensure format and options are properly set
    graph.sinks.filter(_.identifier == TableIdentifier("myKafkaSink")).foreach { sink =>
      assert(sink.format == "kafka")
      assert(sink.options.get("kafka.bootstrap.servers").contains("host1:port1,host2:port2"))
    }

    // ensure the flow is properly linked to the sink
    assert(
      graph
        .flowsTo(TableIdentifier("myKafkaSink"))
        .map(_.identifier) == Seq(TableIdentifier("mySinkFlow")))
  }

  test("referencing internal datasets") {
    val graph = buildGraph("""
      |@dp.materialized_view
      |def src():
      |  return spark.range(5)
      |
      |@dp.materialized_view
      |def a():
      |  return spark.read.table("src")
      |
      |@dp.table
      |def b():
      |  return spark.readStream.table("src")
      |
      |@dp.materialized_view
      |def c():
      |  return spark.sql("SELECT * FROM src")
      |
      |@dp.table
      |def d():
      |  return spark.sql("SELECT * FROM STREAM src")
      |""".stripMargin).resolve().validate()

    assert(
      graph.table.keySet == Set(
        graphIdentifier("src"),
        graphIdentifier("a"),
        graphIdentifier("b"),
        graphIdentifier("c"),
        graphIdentifier("d")))
    Seq("a", "b", "c").foreach { flowName =>
      // dependency is properly tracked
      assert(graph.resolvedFlow(graphIdentifier(flowName)).inputs == Set(graphIdentifier("src")))
    }

    val (streamingFlows, batchFlows) = graph.resolvedFlows.partition(_.df.isStreaming)
    assert(
      batchFlows.map(_.identifier).toSet == Set(
        graphIdentifier("src"),
        graphIdentifier("a"),
        graphIdentifier("c")))
    assert(
      streamingFlows.map(_.identifier).toSet ==
        Set(graphIdentifier("b"), graphIdentifier("d")))
  }

  test("referencing external datasets") {
    sql("CREATE TABLE spark_catalog.default.src AS SELECT * FROM RANGE(5)")
    val graph = buildGraph("""
        |@dp.materialized_view
        |def a():
        |  return spark.read.table("spark_catalog.default.src")
        |
        |@dp.materialized_view
        |def b():
        |  return spark.table("spark_catalog.default.src")
        |
        |@dp.table
        |def c():
        |  return spark.readStream.table("spark_catalog.default.src")
        |
        |@dp.materialized_view
        |def d():
        |  return spark.sql("SELECT * FROM spark_catalog.default.src")
        |
        |@dp.table
        |def e():
        |  return spark.sql("SELECT * FROM STREAM spark_catalog.default.src")
        |""".stripMargin).resolve().validate()

    assert(
      graph.tables.map(_.identifier).toSet == Set(
        graphIdentifier("a"),
        graphIdentifier("b"),
        graphIdentifier("c"),
        graphIdentifier("d"),
        graphIdentifier("e")))
    // dependency is not tracked
    assert(graph.resolvedFlows.forall(_.inputs.isEmpty))
    val (streamingFlows, batchFlows) = graph.resolvedFlows.partition(_.df.isStreaming)
    assert(
      batchFlows.map(_.identifier).toSet == Set(
        graphIdentifier("a"),
        graphIdentifier("b"),
        graphIdentifier("d")))
    assert(
      streamingFlows.map(_.identifier).toSet == Set(graphIdentifier("c"), graphIdentifier("e")))
  }

  test("referencing internal datasets failed") {
    val graph = buildGraph("""
        |@dp.table
        |def a():
        |  return spark.read.table("src")
        |
        |@dp.table
        |def b():
        |  return spark.table("src")
        |
        |@dp.table
        |def c():
        |  return spark.readStream.table("src")
        |
        |@dp.materialized_view
        |def d():
        |  return spark.sql("SELECT * FROM src")
        |
        |@dp.table
        |def e():
        |  return spark.sql("SELECT * FROM STREAM src")
        |""".stripMargin).resolve()

    assert(graph.resolutionFailedFlows.size == 5)
    graph.resolutionFailedFlows.foreach { flow =>
      assert(flow.failure.head.getMessage.contains("[TABLE_OR_VIEW_NOT_FOUND]"))
      assert(flow.failure.head.getMessage.contains("`src`"))
    }
  }

  test("referencing external datasets failed") {
    val graph = buildGraph("""
        |@dp.table
        |def a():
        |  return spark.read.table("spark_catalog.default.src")
        |
        |@dp.materialized_view
        |def b():
        |  return spark.table("spark_catalog.default.src")
        |
        |@dp.materialized_view
        |def c():
        |  return spark.readStream.table("spark_catalog.default.src")
        |
        |@dp.materialized_view
        |def d():
        |  return spark.sql("SELECT * FROM spark_catalog.default.src")
        |
        |@dp.table
        |def e():
        |  return spark.sql("SELECT * FROM STREAM spark_catalog.default.src")
        |""".stripMargin).resolve()
    assert(graph.resolutionFailedFlows.size == 5)
    graph.resolutionFailedFlows.foreach { flow =>
      assert(flow.failure.head.getMessage.contains("[TABLE_OR_VIEW_NOT_FOUND]"))
      assert(flow.failure.head.getMessage.contains("`spark_catalog`.`default`.`src`"))
    }
  }

  test("reading external datasets outside query function works") {
    sql("CREATE TABLE spark_catalog.default.src AS SELECT * FROM RANGE(5)")
    val graph = buildGraph(s"""
        |spark_sql_df = spark.sql("SELECT * FROM spark_catalog.default.src")
        |read_table_df = spark.read.table("spark_catalog.default.src")
        |
        |@dp.materialized_view
        |def mv_from_spark_sql_df():
        |  return spark_sql_df
        |
        |@dp.materialized_view
        |def mv_from_read_table_df():
        |  return read_table_df
        |""".stripMargin).resolve().validate()

    assert(
      graph.resolvedFlows.map(_.identifier).toSet == Set(
        graphIdentifier("mv_from_spark_sql_df"),
        graphIdentifier("mv_from_read_table_df")))
    assert(graph.resolvedFlows.forall(_.inputs.isEmpty))
    assert(graph.resolvedFlows.forall(!_.df.isStreaming))
  }

  test(
    "reading internal datasets outside query function that don't trigger " +
      "eager analysis or execution") {
    val graph = buildGraph("""
        |@dp.materialized_view
        |def src():
        |  return spark.range(5)
        |
        |read_table_df = spark.read.table("src")
        |
        |@dp.materialized_view
        |def mv_from_read_table_df():
        |  return read_table_df
        |
        |""".stripMargin).resolve().validate()
    assert(
      graph.resolvedFlows.map(_.identifier).toSet == Set(
        graphIdentifier("mv_from_read_table_df"),
        graphIdentifier("src")))
    assert(graph.resolvedFlows.forall(!_.df.isStreaming))
    assert(
      graph
        .resolvedFlow(graphIdentifier("mv_from_read_table_df"))
        .inputs
        .contains(graphIdentifier("src")))
  }

  gridTest(
    "reading internal datasets outside query function that trigger " +
      "eager analysis or execution will fail")(
    Seq("""spark.sql("SELECT * FROM src")""", """spark.read.table("src").collect()""")) {
    command =>
      val ex = intercept[RuntimeException] {
        buildGraph(s"""
        |@dp.materialized_view
        |def src():
        |  return spark.range(5)
        |
        |spark_sql_df = $command
        |
        |@dp.materialized_view
        |def mv_from_spark_sql_df():
        |  return spark_sql_df
        |""".stripMargin)
      }
      assert(ex.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"))
      assert(ex.getMessage.contains("`src`"))
  }

  test("create dataset with the same name will fail") {
    val ex = intercept[AnalysisException] {
      buildGraph(s"""
           |@dp.materialized_view
           |def a():
           |  return spark.range(1)
           |
           |@dp.materialized_view(name = "a")
           |def b():
           |  return spark.range(1)
           |""".stripMargin)
    }
    assert(ex.getCondition == "PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT")
  }

  test("create datasets with fully/partially qualified names") {
    val graph = buildGraph(s"""
         |@dp.table
         |def mv_1():
         |  return spark.range(5)
         |
         |@dp.table(name = "schema_a.mv_2")
         |def irrelevant_1():
         |  return spark.range(5)
         |
         |@dp.table(name = "st_1")
         |def irrelevant_2():
         |  return spark.readStream.format("rate").load()
         |
         |@dp.table(name = "schema_b.st_2")
         |def irrelevant_3():
         |  return spark.readStream.format("rate").load()
         |""".stripMargin).resolve()

    // validate these dataset are properly fully qualified
    assert(
      graph.tables.map(_.identifier).toSet == Set(
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("default"),
          table = "mv_1"),
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("schema_a"),
          table = "mv_2"),
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("default"),
          table = "st_1"),
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("schema_b"),
          table = "st_2")))
    assert(
      graph.flows.map(_.identifier).toSet == Set(
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("default"),
          table = "mv_1"),
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("schema_a"),
          table = "mv_2"),
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("default"),
          table = "st_1"),
        TableIdentifier(
          catalog = Option("spark_catalog"),
          database = Option("schema_b"),
          table = "st_2")))
  }

  test("create datasets with three part names") {
    val graphTry = Try {
      buildGraph(s"""
           |@dp.table(name = "some_catalog.some_schema.mv")
           |def irrelevant_1():
           |  return spark.range(5)
           |
           |@dp.table(name = "some_catalog.some_schema.st")
           |def irrelevant_2():
           |  return spark.readStream.format("rate").load()
           |""".stripMargin).resolve()
    }
    assert(graphTry.isSuccess)
    assert(
      graphTry.get.tables.map(_.identifier).toSet == Set(
        TableIdentifier("mv", Some("some_schema"), Some("some_catalog")),
        TableIdentifier("st", Some("some_schema"), Some("some_catalog"))))
    assert(
      graphTry.get.flows.map(_.identifier).toSet == Set(
        TableIdentifier("mv", Some("some_schema"), Some("some_catalog")),
        TableIdentifier("st", Some("some_schema"), Some("some_catalog"))))
  }

  test("temporary views works") {
    // A table is defined since pipeline with only temporary views is invalid.
    val graph = buildGraph(s"""
         |@dp.table
         |def mv_1():
         |  return spark.range(5)
         |@dp.temporary_view
         |def view_1():
         |  return spark.range(5)
         |
         |@dp.temporary_view(name= "view_2")
         |def irrelevant_1():
         |  return spark.read.table("view_1")
         |
         |@dp.temporary_view(name= "view_3")
         |def irrelevant_2():
         |  return spark.read.table("view_1")
         |""".stripMargin).resolve()
    // views are temporary views, so they're not fully qualified.
    assert(
      Set("view_1", "view_2", "view_3").subsetOf(
        graph.flows.map(_.identifier.unquotedString).toSet))
    // dependencies are correctly resolved view_2 reading from view_1
    assert(
      graph.resolvedFlow(TableIdentifier("view_2")).inputs.contains(TableIdentifier("view_1")))
    assert(
      graph.resolvedFlow(TableIdentifier("view_3")).inputs.contains(TableIdentifier("view_1")))
  }

  test("create named flow with multipart name will fail") {
    val ex = intercept[RuntimeException] {
      buildGraph(s"""
           |@dp.table
           |def src():
           |  return spark.readStream.table("src0")
           |
           |@dp.append_flow(name ="some_schema.some_flow", target = "src")
           |def some_flow():
           |  return spark.readStream.format("rate").load()
           |""".stripMargin)
    }
    assert(ex.getMessage.contains("MULTIPART_FLOW_NAME_NOT_SUPPORTED"))
  }

  test("create flow with multipart target and no explicit name succeeds") {
    val graph = buildGraph("""
           |@dp.table()
           |def a():
           |  return spark.readStream.format("rate").load()
           |
           |@dp.append_flow(target = "default.a")
           |def supplement():
           |  return spark.readStream.format("rate").load()
           |""".stripMargin).resolve().validate()

    assert(graph.tables.map(_.identifier) == Seq(graphIdentifier("a")))
    assert(
      graph
        .flowsTo(graphIdentifier("a"))
        .map(_.identifier)
        .toSet == Set(graphIdentifier("a"), graphIdentifier("supplement")))
  }

  test("create named flow with multipart target succeeds") {
    val graph = buildGraph("""
           |@dp.table()
           |def a():
           |  return spark.readStream.format("rate").load()
           |
           |@dp.append_flow(target = "default.a", name = "something")
           |def supplement():
           |  return spark.readStream.format("rate").load()
           |""".stripMargin)

    assert(graph.tables.map(_.identifier) == Seq(graphIdentifier("a")))
    assert(
      graph
        .flowsTo(graphIdentifier("a"))
        .map(_.identifier)
        .toSet == Set(graphIdentifier("a"), graphIdentifier("something")))
  }

  test("groupby and rollup works with internal datasets, referencing with (col, str)") {
    val graph = buildGraph("""
      from pyspark.sql.functions import col, sum, count

      @dp.materialized_view
      def src():
        return spark.range(3)

      @dp.materialized_view
      def groupby_with_col_result():
        return spark.read.table("src").groupBy(col("id")).agg(
          sum("id").alias("sum_id"),
          count("*").alias("cnt")
        )

      @dp.materialized_view
      def groupby_with_str_result():
        return spark.read.table("src").groupBy("id").agg(
          sum("id").alias("sum_id"),
          count("*").alias("cnt")
        )

      @dp.materialized_view
      def rollup_with_col_result():
        return spark.read.table("src").rollup(col("id")).agg(
          sum("id").alias("sum_id"),
          count("*").alias("cnt")
        )

      @dp.materialized_view
      def rollup_with_str_result():
        return spark.read.table("src").rollup("id").agg(
          sum("id").alias("sum_id"),
          count("*").alias("cnt")
        )
    """)

    val updateContext =
      new PipelineUpdateContextImpl(graph, _ => (), storageRoot = storageRoot)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val groupbyDfs =
      Seq(spark.table("groupby_with_col_result"), spark.table("groupby_with_str_result"))

    val rollupDfs =
      Seq(spark.table("rollup_with_col_result"), spark.table("rollup_with_str_result"))

    // groupBy: each variant should have exactly one row per id [0,1,2]
    groupbyDfs.foreach { df =>
      assert(df.select("id").collect().map(_.getLong(0)).toSet == Set(0L, 1L, 2L))
    }

    // rollup: each variant should have groupBy rows + one total row
    rollupDfs.foreach { df =>
      assert(df.count() == 3 + 1) // 3 ids + 1 total
      val totalRow = df.filter("id IS NULL").collect().head
      assert(totalRow.getLong(1) == 3L && totalRow.getLong(2) == 3L)
    }
  }

  test("MV/ST with partition columns works") {
    withTable("mv", "st") {
      val graph = buildGraph("""
            |from pyspark.sql.functions import col
            |
            |@dp.materialized_view(partition_cols = ["id_mod"])
            |def mv():
            |  return spark.range(5).withColumn("id_mod", col("id") % 2)
            |
            |@dp.table(partition_cols = ["id_mod"])
            |def st():
            |  return spark.readStream.table("mv")
            |""".stripMargin)

      val updateContext =
        new PipelineUpdateContextImpl(graph, eventCallback = _ => (), storageRoot = storageRoot)
      updateContext.pipelineExecution.runPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      // check table is created with correct partitioning
      val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]

      Seq("mv", "st").foreach { tableName =>
        val table = catalog.loadTable(Identifier.of(Array("default"), tableName))
        assert(
          table.partitioning().map(_.references().head.fieldNames().head) === Array("id_mod"))

        val rows = spark.table(tableName).collect().map(r => (r.getLong(0), r.getLong(1))).toSet
        val expected = (0 until 5).map(id => (id.toLong, (id % 2).toLong)).toSet
        assert(rows == expected)
      }
    }
  }

  test("create pipeline without table will throw RUN_EMPTY_PIPELINE exception") {
    checkError(
      exception = intercept[AnalysisException] {
        buildGraph(s"""
            |spark.range(1)
            |""".stripMargin)
      },
      condition = "RUN_EMPTY_PIPELINE",
      parameters = Map.empty)
  }

  test("create pipeline with only temp view will throw RUN_EMPTY_PIPELINE exception") {
    checkError(
      exception = intercept[AnalysisException] {
        buildGraph(s"""
            |@dp.temporary_view
            |def view_1():
            |  return spark.range(5)
            |""".stripMargin)
      },
      condition = "RUN_EMPTY_PIPELINE",
      parameters = Map.empty)
  }

  test("create pipeline with only flow will throw RUN_EMPTY_PIPELINE exception") {
    checkError(
      exception = intercept[AnalysisException] {
        buildGraph(s"""
            |@dp.append_flow(target = "a")
            |def flow():
            |  return spark.range(5)
            |""".stripMargin)
      },
      condition = "RUN_EMPTY_PIPELINE",
      parameters = Map.empty)
  }

  test("table with string schema") {
    val graph = buildGraph("""
        |from pyspark.sql.functions import lit
        |
        |@dp.materialized_view(schema="id LONG, name STRING")
        |def table_with_string_schema():
        |    return spark.range(5).withColumn("name", lit("test"))
        |""".stripMargin)
      .resolve()
      .validate()

    assert(graph.flows.size == 1)
    assert(graph.tables.size == 1)

    val table = graph.table(graphIdentifier("table_with_string_schema"))
    assert(table.specifiedSchema.isDefined)
    assert(table.specifiedSchema.get == StructType.fromDDL("id LONG, name STRING"))
  }

  test("table with StructType schema") {
    val graph = buildGraph("""
        |from pyspark.sql.types import StructType, StructField, LongType, StringType
        |from pyspark.sql.functions import lit
        |
        |@dp.materialized_view(schema=StructType([
        |    StructField("id", LongType(), True),
        |    StructField("name", StringType(), True)
        |]))
        |def table_with_struct_schema():
        |    return spark.range(5).withColumn("name", lit("test"))
        |""".stripMargin)
      .resolve()
      .validate()

    assert(graph.flows.size == 1)
    assert(graph.tables.size == 1)

    val table = graph.table(graphIdentifier("table_with_struct_schema"))
    assert(table.specifiedSchema.isDefined)
    assert(table.specifiedSchema.get == StructType.fromDDL("id LONG, name STRING"))
  }

  test("string schema validation error - schema mismatch") {
    val graph = buildGraph("""
        |from pyspark.sql.functions import lit
        |
        |@dp.materialized_view(schema="id LONG, name STRING")
        |def table_with_wrong_schema():
        |    return spark.range(5).withColumn("wrong_column", lit("test"))
        |""".stripMargin)
      .resolve()

    val ex = intercept[AnalysisException] { graph.validate() }
    assert(ex.getMessage.contains("has a user-specified schema that is incompatible"))
    assert(ex.getMessage.contains("table_with_wrong_schema"))
  }

  test("StructType schema validation error - schema mismatch") {
    val graph = buildGraph("""
        |from pyspark.sql.types import StructType, StructField, LongType, StringType
        |from pyspark.sql.functions import lit
        |
        |@dp.materialized_view(schema=StructType([
        |    StructField("id", LongType(), True),
        |    StructField("name", StringType(), True)
        |]))
        |def table_with_wrong_struct_schema():
        |    return spark.range(5).withColumn("different_column", lit("test"))
        |""".stripMargin)
      .resolve()

    val ex = intercept[AnalysisException] { graph.validate() }
    assert(ex.getMessage.contains("has a user-specified schema that is incompatible"))
    assert(ex.getMessage.contains("table_with_wrong_struct_schema"))
  }

  /**
   * Executes Python code in a separate process and returns the exit code.
   *
   * @param pythonCode
   *   The Python code to execute
   * @return
   *   The exit code of the Python process
   */
  private def executePythonCode(pythonCode: String): (Int, Seq[String]) = {
    val pythonExec =
      sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3"))

    // Set up the PYTHONPATH to include PySpark
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env.getOrElse("SPARK_HOME", "."))
    val sourcePath = Paths.get(sparkHome, "python").toAbsolutePath
    val py4jPath = Paths.get(sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).toAbsolutePath
    val pythonPath = PythonUtils.mergePythonPaths(
      sourcePath.toString,
      py4jPath.toString,
      sys.env.getOrElse("PYTHONPATH", ""))

    val pb = new ProcessBuilder(pythonExec, "-c", pythonCode)
    pb.redirectErrorStream(true)
    pb.environment().put("PYTHONPATH", pythonPath)

    val process = pb.start()

    // Read the output
    val reader = new BufferedReader(
      new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
    val output = new ArrayBuffer[String]()
    var line: String = null
    while ({ line = reader.readLine(); line != null }) {
      output += line
      // scalastyle:off println
      println(s"Python output: $line")
      // scalastyle:on println
    }

    // Wait for the process to complete and get the exit code
    process.waitFor(60, TimeUnit.SECONDS)
    val exitCode = process.exitValue()

    // Log the output if the process failed
    if (exitCode != 0) {
      // scalastyle:off println
      Console.err.println(s"Python process failed with exit code $exitCode")
      Console.err.println("Output:")
      output.foreach(line => Console.err.println(line))
      // scalastyle:on println
    }

    (exitCode, output.toSeq)
  }

  test("empty cluster_by list should work and create table with no clustering") {
    withTable("mv", "st") {
      val graph = buildGraph("""
            |from pyspark.sql.functions import col
            |
            |@dp.materialized_view(cluster_by = [])
            |def mv():
            |  return spark.range(5).withColumn("id_mod", col("id") % 2)
            |
            |@dp.table(cluster_by = [])
            |def st():
            |  return spark.readStream.table("mv")
            |""".stripMargin)
      val updateContext =
        new PipelineUpdateContextImpl(graph, eventCallback = _ => (), storageRoot = storageRoot)
      updateContext.pipelineExecution.runPipeline()
      updateContext.pipelineExecution.awaitCompletion()

      // Check tables are created with no clustering transforms
      val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]

      val mvIdentifier = Identifier.of(Array("default"), "mv")
      val mvTable = catalog.loadTable(mvIdentifier)
      val mvTransforms = mvTable.partitioning()
      assert(
        mvTransforms.isEmpty,
        s"MaterializedView should have no transforms, but got: ${mvTransforms.mkString(", ")}")

      val stIdentifier = Identifier.of(Array("default"), "st")
      val stTable = catalog.loadTable(stIdentifier)
      val stTransforms = stTable.partitioning()
      assert(
        stTransforms.isEmpty,
        s"Table should have no transforms, but got: ${stTransforms.mkString(", ")}")
    }
  }

  // List of unsupported SQL commands that should result in a failure.
  private val unsupportedSqlCommandList: Seq[String] = Seq(
    "SET CATALOG some_catalog",
    "USE SCHEMA some_schema",
    "SET `test_conf` = `true`",
    "CREATE TABLE some_table (id INT)",
    "CREATE VIEW some_view AS SELECT * FROM some_table",
    "INSERT INTO some_table VALUES (1)",
    "ALTER TABLE some_table RENAME TO some_new_table",
    "CREATE NAMESPACE some_namespace",
    "DROP VIEW some_view",
    "CREATE MATERIALIZED VIEW some_view AS SELECT * FROM some_table",
    "CREATE STREAMING TABLE some_table AS SELECT * FROM some_table")

  gridTest("Unsupported SQL command outside query function should result in a failure")(
    unsupportedSqlCommandList) { unsupportedSqlCommand =>
    val ex = intercept[RuntimeException] {
      buildGraph(s"""
        |spark.sql("$unsupportedSqlCommand")
        |
        |@dp.materialized_view()
        |def mv():
        |  return spark.range(5)
        |""".stripMargin)
    }
    assert(ex.getMessage.contains("UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND"))
  }

  gridTest("Unsupported SQL command inside query function should result in a failure")(
    unsupportedSqlCommandList) { unsupportedSqlCommand =>
    val ex = intercept[RuntimeException] {
      buildGraph(s"""
        |@dp.materialized_view()
        |def mv():
        |  spark.sql("$unsupportedSqlCommand")
        |  return spark.range(5)
        |""".stripMargin)
    }
    assert(ex.getMessage.contains("UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND"))
  }

  // List of supported SQL commands that should work.
  val supportedSqlCommandList: Seq[String] = Seq(
    "DESCRIBE TABLE spark_catalog.default.src",
    "SHOW TABLES",
    "SHOW TBLPROPERTIES spark_catalog.default.src",
    "SHOW NAMESPACES",
    "SHOW COLUMNS FROM spark_catalog.default.src",
    "SHOW FUNCTIONS",
    "SHOW VIEWS",
    "SHOW CATALOGS",
    "SHOW CREATE TABLE spark_catalog.default.src",
    "SELECT * FROM RANGE(5)",
    "SELECT * FROM spark_catalog.default.src")

  gridTest("Supported SQL command outside query function should work")(supportedSqlCommandList) {
    supportedSqlCommand =>
      sql("CREATE TABLE spark_catalog.default.src AS SELECT * FROM RANGE(5)")
      buildGraph(s"""
        |spark.sql("$supportedSqlCommand")
        |
        |@dp.materialized_view()
        |def mv():
        |  return spark.range(5)
        |""".stripMargin)
  }

  gridTest("Supported SQL command inside query function should work")(supportedSqlCommandList) {
    supportedSqlCommand =>
      sql("CREATE TABLE spark_catalog.default.src AS SELECT * FROM RANGE(5)")
      buildGraph(s"""
        |@dp.materialized_view()
        |def mv():
        |  spark.sql("$supportedSqlCommand")
        |  return spark.range(5)
        |""".stripMargin)
  }

  private val eagerExecutionPythonCommands = Seq(
    "df.collect()",
    "df.first()",
    "df.head(0)",
    "df.toPandas()",
    "spark.readStream.format(\"rate\").load().writeStream" +
      ".format(\"memory\").queryName(\"test_query_name\").start()")

  gridTest("unsupported eager execution inside flow function is blocked")(
    eagerExecutionPythonCommands) { unsupportedEagerExecutionCommand =>
    val ex = intercept[RuntimeException] {
      buildGraph(s"""
        |@dp.materialized_view()
        |def mv():
        |  df = spark.range(5)
        |  $unsupportedEagerExecutionCommand
        |  return df
        |""".stripMargin)
    }
    assert(ex.getMessage.contains("ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION"))
  }

  gridTest("eager execution outside flow function is allowed")(eagerExecutionPythonCommands) {
    unsupportedEagerExecutionCommand =>
      buildGraph(s"""
      |df = spark.range(5)
      |$unsupportedEagerExecutionCommand
      |
      |@dp.materialized_view()
      |def mv():
      |  df = spark.range(5)
      |  return df
      |""".stripMargin)
  }

  private val eagerAnalysisPythonCommands = Seq("df.schema", "df.isStreaming", "df.isLocal()")

  gridTest("eager analysis inside flow function is blocked")(eagerAnalysisPythonCommands) {
    eagerAnalysisPythonCommand =>
      val ex = intercept[RuntimeException] {
        buildGraph(s"""
          |@dp.materialized_view()
          |def mv():
          |  df = spark.range(5)
          |  $eagerAnalysisPythonCommand
          |  return df
          |""".stripMargin)
      }
      assert(ex.getMessage.contains("ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION"))
  }

  gridTest("eager analysis outside flow function is allowed")(eagerAnalysisPythonCommands) {
    eagerAnalysisPythonCommand =>
      buildGraph(s"""
        |df = spark.range(5)
        |$eagerAnalysisPythonCommand
        |
        |@dp.materialized_view()
        |def mv():
        |  df = spark.range(5)
        |  return df
        |""".stripMargin)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (PythonTestDepsChecker.isConnectDepsAvailable) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }
}
