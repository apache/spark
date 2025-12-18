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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

class SystemMetadataSuite
    extends ExecutionTest
    with SystemMetadataTestHelpers
    with SharedSparkSession {

    gridTest(
      "flow checkpoint for ST wrote to the expected location when fullRefresh ="
    ) (Seq(false, true))
    { fullRefresh =>
      val session = spark
      import session.implicits._

      // create a pipeline with only a single ST
      val graph = new TestGraphRegistrationContext(spark) {
        implicit val sparkSession: SparkSession = spark
        val mem: MemoryStream[Int] = MemoryStream[Int]
        mem.addData(1, 2, 3)
        registerView("a", query = dfFlowFunc(mem.toDF()))
        registerTable("st")
        registerFlow("st", "st", query = readStreamFlowFunc("a"))
      }.toDataflowGraph

      val updateContext1 = TestPipelineUpdateContext(
        unresolvedGraph = graph,
        spark = spark,
        storageRoot = storageRoot,
        failOnErrorEvent = true
      )

      // run first update
      updateContext1.pipelineExecution.startPipeline()
      updateContext1.pipelineExecution.awaitCompletion()

      val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
      val executionGraph1 = graphExecution1.graphForExecution

      val stIdentifier = fullyQualifiedIdentifier("st")

      // assert checkpoint v0
      assertFlowCheckpointDirExists(
        tableOrSinkElement = executionGraph1.table(stIdentifier),
        flowElement = executionGraph1.flow(stIdentifier),
        expectedCheckpointVersion = 0,
        graphExecution = graphExecution1,
        updateContext = updateContext1
      )

      // run second update, either refresh (reuse) or full refresh (increment)
      val updateContext2 = TestPipelineUpdateContext(
        unresolvedGraph = graph,
        spark = spark,
        storageRoot = storageRoot,
        refreshTables = if (!fullRefresh) AllTables else NoTables,
        fullRefreshTables = if (fullRefresh) AllTables else NoTables,
        failOnErrorEvent = true
      )

      updateContext2.pipelineExecution.startPipeline()
      updateContext2.pipelineExecution.awaitCompletion()

      val graphExecution2 = updateContext2.pipelineExecution.graphExecution.get
      val executionGraph2 = graphExecution2.graphForExecution

      // checkpoint reused in refresh, incremented in full refresh
      val expectedVersion = if (fullRefresh) 1 else 0
      assertFlowCheckpointDirExists(
        tableOrSinkElement = executionGraph2.table(stIdentifier),
        flowElement = executionGraph1.flow(stIdentifier),
        expectedCheckpointVersion = expectedVersion,
        graphExecution = graphExecution2,
        updateContext2
      )
    }

  test(
    "flow checkpoint for ST (with flow name different from table name) wrote to the expected " +
    "location"
  ) {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      implicit val sparkSession: SparkSession = spark
      val mem: MemoryStream[Int] = MemoryStream[Int]
      mem.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem.toDF()))
      registerTable("st")
      registerFlow("st", "st_flow", query = readStreamFlowFunc("a"))
    }.toDataflowGraph

    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRoot = storageRoot
    )

    // start an update in continuous mode, checkpoints are only created to streaming query
    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()

    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    val stIdentifier = fullyQualifiedIdentifier("st")
    val stFlowIdentifier = fullyQualifiedIdentifier("st_flow")

    // assert that the checkpoint dir for the ST is created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.table(stIdentifier),
      flowElement = executionGraph1.flow(stFlowIdentifier),
      // the default checkpoint version is 0
      expectedCheckpointVersion = 0,
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )

    // start another update in full refresh, expected a new checkpoint dir to be created
    // with version number incremented to 1
    val updateContext2 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRoot = storageRoot,
      fullRefreshTables = AllTables
    )

    updateContext2.pipelineExecution.startPipeline()
    updateContext2.pipelineExecution.awaitCompletion()
    val graphExecution2 = updateContext2.pipelineExecution.graphExecution.get
    val executionGraph2 = graphExecution2.graphForExecution

    // due to full refresh, assert that new checkpoint dir is created with version number
    // incremented to 1
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.table(stIdentifier),
      flowElement = executionGraph1.flow(stFlowIdentifier),
      // new checkpoint directory is created
      expectedCheckpointVersion = 1,
      graphExecution = graphExecution2,
      updateContext2
    )
  }

  test("flow checkpoint for sink wrote to the expected location for full refresh") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      implicit val sparkSession: SparkSession = spark
      val mem: MemoryStream[Int] = MemoryStream[Int]
      mem.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem.toDF()))
      registerSink("sink", format = "console")
      registerFlow("sink", "sink", query = readStreamFlowFunc("a"))
    }.toDataflowGraph
    val sinkIdentifier = TableIdentifier("sink")

    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRoot = storageRoot
    )

    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()
    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    // assert that the checkpoint dir for the sink is created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier),
      flowElement = executionGraph1.flow(sinkIdentifier),
      // the default checkpoint version is 0
      expectedCheckpointVersion = 0,
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )

    // start another update in full refresh, expected a new checkpoint dir to be created
    // with version number incremented to 1
    val updateContext2 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRoot = storageRoot,
      fullRefreshTables = AllTables
    )

    updateContext2.pipelineExecution.startPipeline()
    updateContext2.pipelineExecution.awaitCompletion()
    val graphExecution2 = updateContext2.pipelineExecution.graphExecution.get
    val executionGraph2 = graphExecution2.graphForExecution

    // due to full refresh, assert that new checkpoint dir is created with version number
    // incremented to 1
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.sink(sinkIdentifier),
      flowElement = executionGraph2.flow(sinkIdentifier),
      // new checkpoint directory is created with version number incremented to 1
      expectedCheckpointVersion = 1,
      graphExecution = graphExecution2,
      updateContext2
    )
  }

  test("checkpoint dirs for tables with same name but different schema don't collide") {
    val session = spark
    import session.implicits._

    // create a pipeline with only a single ST
    val graph = new TestGraphRegistrationContext(spark) {
      implicit val sparkSession: SparkSession = spark
      val mem: MemoryStream[Int] = MemoryStream[Int]
      mem.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem.toDF()))
      registerTable("st")
      registerFlow("st", "st", query = readStreamFlowFunc("a"))
      registerTable("schema2.st")
      registerFlow("schema2.st", "schema2.st", query = readStreamFlowFunc("a"))
    }.toDataflowGraph

    val updateContext = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRoot = storageRoot,
      failOnErrorEvent = true
    )

    val stFlow = graph.flow(fullyQualifiedIdentifier("st"))
    val schema2StFlow = graph.flow(fullyQualifiedIdentifier("st", database = Option("schema2")))
    val stSystemMetadata = FlowSystemMetadata(updateContext, stFlow, graph)
    val schema2StSystemMetadata = FlowSystemMetadata(updateContext, schema2StFlow, graph)
    assert(
      stSystemMetadata.flowCheckpointsDirOpt() != schema2StSystemMetadata.flowCheckpointsDirOpt()
    )
  }
}

trait SystemMetadataTestHelpers {
  this: ExecutionTest =>

  /** Return the expected checkpoint directory location for a table or sink.
   *  These directories have "<expectedStorageName>/<expectedCheckpointVersion>" as their suffix. */
  private def getExpectedFlowCheckpointDirForTableOrSink(
      tableOrSinkElement: GraphElement,
      flowElement: Flow,
      expectedCheckpointVersion: Int,
      updateContext: PipelineUpdateContext
  ): Path = {
    val expectedRawCheckPointDir = tableOrSinkElement match {
      case t if t.isInstanceOf[Table] || t.isInstanceOf[Sink] =>
        val tableId = t.identifier.nameParts.mkString(Path.SEPARATOR)
        new Path(updateContext.storageRoot)
        .suffix(s"/_checkpoints/$tableId/${flowElement.identifier.table}")
        .toString
      case _ =>
        fail(
          s"unexpected table element type for assertFlowCheckpointDirForTableOrSink: " +
          tableOrSinkElement.getClass.getSimpleName
        )
    }

    new Path("file://", expectedRawCheckPointDir).suffix(s"/$expectedCheckpointVersion")
  }

  /** Return the actual checkpoint directory location used by the table or sink.
   * We use a Flow object as input since the checkpoints for a table or sink are associated with
   * each of their incoming flows.
   */
  private def getActualFlowCheckpointDirForTableOrSink(
      flowElement: Flow,
      graphExecution: GraphExecution
  ): Path = {
    // spark flow stream takes a while to be created, so we need to poll for it
    val flowStream = graphExecution
      .flowExecutions(flowElement.identifier)
      .asInstanceOf[StreamingFlowExecution]
      .getStreamingQuery

    // we grab the checkpoint location from the actual spark stream query executed in the update
    // execution, which is the best source of truth.
    new Path(
      flowStream.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot
    )
  }

  /** Assert the flow checkpoint directory exists in the filesystem. */
  protected def assertFlowCheckpointDirExists(
      tableOrSinkElement: GraphElement,
      flowElement: Flow,
      expectedCheckpointVersion: Int,
      graphExecution: GraphExecution,
      updateContext: PipelineUpdateContext
  ): Path = {
    val expectedCheckPointDir = getExpectedFlowCheckpointDirForTableOrSink(
      tableOrSinkElement = tableOrSinkElement,
      flowElement = flowElement,
      expectedCheckpointVersion = expectedCheckpointVersion,
      updateContext
    )
    val actualCheckpointDir = getActualFlowCheckpointDirForTableOrSink(
      flowElement = flowElement,
      graphExecution = graphExecution
    )
    val fs = expectedCheckPointDir.getFileSystem(spark.sessionState.newHadoopConf())
    assert(actualCheckpointDir == expectedCheckPointDir)
    assert(fs.exists(actualCheckpointDir))
    actualCheckpointDir
  }

  /** Assert the flow checkpoint directory does not exist in the filesystem. */
  protected def assertFlowCheckpointDirNotExists(
      tableOrSinkElement: GraphElement,
      flowElement: Flow,
      expectedCheckpointVersion: Int,
      graphExecution: GraphExecution,
      updateContext: PipelineUpdateContext
  ): Path = {
    val expectedCheckPointDir = getExpectedFlowCheckpointDirForTableOrSink(
      tableOrSinkElement = tableOrSinkElement,
      flowElement = flowElement,
      expectedCheckpointVersion = expectedCheckpointVersion,
      updateContext
    )
    val actualCheckpointDir = getActualFlowCheckpointDirForTableOrSink(
      flowElement = flowElement,
      graphExecution = graphExecution
    )
    val fs = expectedCheckPointDir.getFileSystem(spark.sessionState.newHadoopConf())
    assert(actualCheckpointDir != expectedCheckPointDir)
    assert(!fs.exists(expectedCheckPointDir))
    actualCheckpointDir
  }
}
