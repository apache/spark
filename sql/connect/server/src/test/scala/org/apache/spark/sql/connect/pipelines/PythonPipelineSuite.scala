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
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.graph.DataflowGraph
import org.apache.spark.sql.pipelines.utils.{EventVerificationTestHelpers, TestPipelineUpdateContextMixin}

/**
 * Test suite that starts a Spark Connect server and executes Spark Declarative Pipelines Python
 * code to define tables in the pipeline.
 */
class PythonPipelineSuite
    extends SparkDeclarativePipelinesServerTest
    with TestPipelineUpdateContextMixin
    with EventVerificationTestHelpers {

  def buildGraph(pythonText: String): DataflowGraph = {
    val indentedPythonText = pythonText.linesIterator.map("    " + _).mkString("\n")
    val pythonCode =
      s"""
         |from pyspark.sql import SparkSession
         |from pyspark import pipelines as sdp
         |from pyspark.pipelines.spark_connect_graph_element_registry import (
         |    SparkConnectGraphElementRegistry,
         |)
         |from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph
         |from pyspark.pipelines.graph_element_registry import (
         |    graph_element_registration_context,
         |)
         |
         |spark = SparkSession.builder \\
         |    .remote("sc://localhost:$serverPort") \\
         |    .config("spark.connect.grpc.channel.timeout", "5s") \\
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
         |with graph_element_registration_context(registry):
         |$indentedPythonText
         |""".stripMargin

    logInfo(s"Running code: $pythonCode")
    val (exitCode, output) = executePythonCode(pythonCode)

    if (exitCode != 0) {
      throw new RuntimeException(
        s"Python process failed with exit code $exitCode. Output: ${output.mkString("\n")}")
    }

    val dataflowGraphContexts = DataflowGraphRegistry.getAllDataflowGraphs
    assert(dataflowGraphContexts.size == 1)

    dataflowGraphContexts.head.toDataflowGraph
  }

  def graphIdentifier(name: String): TableIdentifier = {
    TableIdentifier(catalog = Option("spark_catalog"), database = Option("default"), table = name)
  }

  test("basic") {
    val graph = buildGraph("""
        |@sdp.table
        |def table1():
        |    return spark.readStream.format("rate").load()
        |""".stripMargin)
      .resolve()
      .validate()
    assert(graph.flows.size == 1)
    assert(graph.tables.size == 1)
  }

  test("basic with inverted topological order") {
    // This graph is purposefully in the wrong topological order to test the topological sort
    val graph = buildGraph("""
        |@sdp.table()
        |def b():
        |  return spark.readStream.table("a")
        |
        |@sdp.table()
        |def c():
        |  return spark.readStream.table("a")
        |
        |@sdp.materialized_view()
        |def d():
        |  return spark.read.table("a")
        |
        |@sdp.materialized_view()
        |def a():
        |  return spark.range(5)
        |""".stripMargin)
    val resolvedGraph = graph.resolve().validate()
    assert(resolvedGraph.tables.size == 4)
    assert(resolvedGraph.resolvedFlows.size == 4)
  }

  test("flows") {
    val graph = buildGraph("""
      |@sdp.table()
      |def a():
      |  return spark.readStream.format("rate").load()
      |
      |@sdp.append_flow(target = "a")
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

  test("referencing internal datasets") {
    val graph = buildGraph("""
      |@sdp.materialized_view
      |def src():
      |  return spark.range(5)
      |
      |@sdp.materialized_view
      |def a():
      |  return spark.read.table("src")
      |
      |@sdp.table
      |def b():
      |  return spark.readStream.table("src")
      |""".stripMargin).resolve().validate()

    assert(
      graph.table.keySet == Set(
        graphIdentifier("src"),
        graphIdentifier("a"),
        graphIdentifier("b")))
    Seq("a", "b").foreach { flowName =>
      // dependency is properly tracked
      assert(graph.resolvedFlow(graphIdentifier(flowName)).inputs == Set(graphIdentifier("src")))
    }

    val (streamingFlows, batchFlows) = graph.resolvedFlows.partition(_.df.isStreaming)
    assert(batchFlows.map(_.identifier) == Seq(graphIdentifier("src"), graphIdentifier("a")))
    assert(streamingFlows.map(_.identifier) == Seq(graphIdentifier("b")))
  }

  test("referencing external datasets") {
    sql("CREATE TABLE spark_catalog.default.src AS SELECT * FROM RANGE(5)")
    val graph = buildGraph("""
        |@sdp.materialized_view
        |def a():
        |  return spark.read.table("spark_catalog.default.src")
        |
        |@sdp.materialized_view
        |def b():
        |  return spark.table("spark_catalog.default.src")
        |
        |@sdp.table
        |def c():
        |  return spark.readStream.table("spark_catalog.default.src")
        |""".stripMargin).resolve().validate()

    assert(
      graph.tables.map(_.identifier).toSet == Set(
        graphIdentifier("a"),
        graphIdentifier("b"),
        graphIdentifier("c")))
    // dependency is not tracked
    assert(graph.resolvedFlows.forall(_.inputs.isEmpty))
    val (streamingFlows, batchFlows) = graph.resolvedFlows.partition(_.df.isStreaming)
    assert(batchFlows.map(_.identifier).toSet == Set(graphIdentifier("a"), graphIdentifier("b")))
    assert(streamingFlows.map(_.identifier) == Seq(graphIdentifier("c")))
  }

  test("referencing internal datasets failed") {
    val graph = buildGraph("""
        |@sdp.table
        |def a():
        |  return spark.read.table("src")
        |
        |@sdp.table
        |def b():
        |  return spark.table("src")
        |
        |@sdp.table
        |def c():
        |  return spark.readStream.table("src")
        |""".stripMargin).resolve()

    assert(graph.resolutionFailedFlows.size == 3)
    graph.resolutionFailedFlows.foreach { flow =>
      assert(flow.failure.head.getMessage.contains("[TABLE_OR_VIEW_NOT_FOUND]"))
      assert(flow.failure.head.getMessage.contains("`src`"))
    }
  }

  test("referencing external datasets failed") {
    val graph = buildGraph("""
        |@sdp.table
        |def a():
        |  return spark.read.table("spark_catalog.default.src")
        |
        |@sdp.materialized_view
        |def b():
        |  return spark.table("spark_catalog.default.src")
        |
        |@sdp.materialized_view
        |def c():
        |  return spark.readStream.table("spark_catalog.default.src")
        |""".stripMargin).resolve()
    graph.resolutionFailedFlows.foreach { flow =>
      assert(flow.failure.head.getMessage.contains("[TABLE_OR_VIEW_NOT_FOUND] The table or view"))
    }
  }

  test("create dataset with the same name will fail") {
    val ex = intercept[AnalysisException] {
      buildGraph(s"""
           |@sdp.materialized_view
           |def a():
           |  return spark.range(1)
           |
           |@sdp.materialized_view(name = "a")
           |def b():
           |  return spark.range(1)
           |""".stripMargin)
    }
    assert(ex.getCondition == "PIPELINE_DUPLICATE_IDENTIFIERS.DATASET")
  }

  test("create datasets with fully/partially qualified names") {
    val graph = buildGraph(s"""
         |@sdp.table
         |def mv_1():
         |  return spark.range(5)
         |
         |@sdp.table(name = "schema_a.mv_2")
         |def irrelevant_1():
         |  return spark.range(5)
         |
         |@sdp.table(name = "st_1")
         |def irrelevant_2():
         |  return spark.readStream.format("rate").load()
         |
         |@sdp.table(name = "schema_b.st_2")
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
           |@sdp.table(name = "some_catalog.some_schema.mv")
           |def irrelevant_1():
           |  return spark.range(5)
           |
           |@sdp.table(name = "some_catalog.some_schema.st")
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
         |@sdp.table
         |def mv_1():
         |  return spark.range(5)
         |@sdp.temporary_view
         |def view_1():
         |  return spark.range(5)
         |
         |@sdp.temporary_view(name= "view_2")
         |def irrelevant_1():
         |  return spark.read.table("view_1")
         |
         |@sdp.temporary_view(name= "view_3")
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
           |@sdp.table
           |def src():
           |  return spark.readStream.table("src0")
           |
           |@sdp.append_flow(name ="some_schema.some_flow", target = "src")
           |def some_flow():
           |  return spark.readStream.format("rate").load()
           |""".stripMargin)
    }
    assert(ex.getMessage.contains("MULTIPART_FLOW_NAME_NOT_SUPPORTED"))
  }

  test("create flow with multipart target and no explicit name succeeds") {
    val graph = buildGraph("""
           |@sdp.table()
           |def a():
           |  return spark.readStream.format("rate").load()
           |
           |@sdp.append_flow(target = "default.a")
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
           |@sdp.table()
           |def a():
           |  return spark.readStream.format("rate").load()
           |
           |@sdp.append_flow(target = "default.a", name = "something")
           |def supplement():
           |  return spark.readStream.format("rate").load()
           |""".stripMargin)

    assert(graph.tables.map(_.identifier) == Seq(graphIdentifier("a")))
    assert(
      graph
        .flowsTo(graphIdentifier("a"))
        .map(_.identifier) == Seq(graphIdentifier("a"), graphIdentifier("something")))
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
            |@sdp.temporary_view
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
            |@sdp.append_flow(target = "a")
            |def flow():
            |  return spark.range(5)
            |""".stripMargin)
      },
      condition = "RUN_EMPTY_PIPELINE",
      parameters = Map.empty)
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
}
