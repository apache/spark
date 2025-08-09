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

import java.nio.file.Files

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

class SystemMetadataSuite
    extends ExecutionTest
    with SystemMetadataTestHelpers
    with SharedSparkSession {
  test("flow checkpoint for ST wrote to the expected location") {
    val session = spark
    import session.implicits._

    // create a pipeline with only a single ST
    val graph = new TestGraphRegistrationContext(spark) {
      val mem: MemoryStream[Int] = MemoryStream[Int]
      mem.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem.toDF()))
      registerTable("st")
      registerFlow("st", "st", query = readStreamFlowFunc("a"))
    }.toDataflowGraph

    val testStorageRoot = Files.createTempDirectory("TestUpdateContext").normalize.toString
    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot)
    )

    // start an update in continuous mode, checkpoints are only created to streaming query
    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()

    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    val stIdentifier = fullyQualifiedIdentifier("st")

    val expectedStorageSuffix = "st"

    // assert that the checkpoint dir for the ST is created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.table(stIdentifier),
      flowElement = executionGraph1.flow(stIdentifier),
      expectedStorageName = expectedStorageSuffix,
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
      storageRootOpt = Option(testStorageRoot),
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
      flowElement = executionGraph1.flow(stIdentifier),
      expectedStorageName = expectedStorageSuffix,
      // new checkpoint directory is created
      expectedCheckpointVersion = 1,
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
      val mem: MemoryStream[Int] = MemoryStream[Int]
      mem.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem.toDF()))
      registerTable("st")
      registerFlow("st", "st_flow", query = readStreamFlowFunc("a"))
    }.toDataflowGraph

    val testStorageRoot = Files.createTempDirectory("TestUpdateContext").normalize.toString
    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot)
    )

    // start an update in continuous mode, checkpoints are only created to streaming query
    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()

    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    val stIdentifier = fullyQualifiedIdentifier("st")

    val expectedStorageSuffix = "st_flow"

    // assert that the checkpoint dir for the ST is created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.table(stIdentifier),
      flowElement = executionGraph1.flow(stIdentifier),
      expectedStorageName = expectedStorageSuffix,
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
      storageRootOpt = Option(testStorageRoot),
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
      flowElement = executionGraph1.flow(stIdentifier),
      expectedStorageName = expectedStorageSuffix,
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
      val mem: MemoryStream[Int] = MemoryStream[Int]
      mem.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem.toDF()))
      registerSink("sink", format = "console")
      registerFlow("sink", "sink", query = readStreamFlowFunc("a"))
    }.toDataflowGraph
    val sinkIdentifier = fullyQualifiedIdentifier("sink")

    val testStorageRoot = Files.createTempDirectory("TestUpdateContext").normalize.toString
    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot)
    )

    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()
    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    val expectedStorageSuffix = "sink"

    // assert that the checkpoint dir for the sink is created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier),
      flowElement = executionGraph1.flow(sinkIdentifier),
      expectedStorageName = expectedStorageSuffix,
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
      storageRootOpt = Option(testStorageRoot),
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
      expectedStorageName = expectedStorageSuffix,
      // new checkpoint directory is created with version number incremented to 1
      expectedCheckpointVersion = 1,
      graphExecution = graphExecution2,
      updateContext2
    )

  }

  // This test creates three sinks, and selectively full-refreshes two of them. We check that
  // the checkpoints are updated for only the two sinks that are refreshed.
  test("flow checkpoint for sink wrote to the expected location for selective refresh") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      // Create external sink
      val mem1: MemoryStream[Int] = MemoryStream[Int]
      mem1.addData(1, 2, 3)
      registerView("a", query = dfFlowFunc(mem1.toDF()))
      registerSink("sink", format = "console")
      registerFlow("sink", "sink", query = readStreamFlowFunc("a"))

      val mem2 = MemoryStream[Int]
      mem2.addData(1, 2, 3)
      registerView("b", query = dfFlowFunc(mem2.toDF()))
      registerSink("sink_1", "memory")
      registerFlow("sink_1", "sink_1", query = readStreamFlowFunc("b"))

      val mem3 = MemoryStream[Int]
      mem3.addData(1, 2, 3)
      registerView("c", query = dfFlowFunc(mem3.toDF()))
      registerSink("sink_2", "memory")
      registerFlow("sink_2", "sink_2", query = readStreamFlowFunc("c"))
    }.toDataflowGraph

    val sinkIdentifier = fullyQualifiedIdentifier("sink")
    val sinkIdentifier1 = fullyQualifiedIdentifier("sink_1")
    val sinkIdentifier2 = fullyQualifiedIdentifier("sink_2")

    val testStorageRoot = Files.createTempDirectory("TestUpdateContext").normalize.toString
    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot)
    )

    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()
    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    val expectedSinkStorageSuffix = "sink"
    val expectedSink1StorageSuffix = "sink_1"
    val expectedSink2StorageSuffix = "sink_2"

    // Assert that the checkpoint dir for the sinks are created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier),
      flowElement = executionGraph1.flow(sinkIdentifier),
      expectedStorageName = expectedSinkStorageSuffix,
      expectedCheckpointVersion = 0, // the default checkpoint version is 0
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier1),
      flowElement = executionGraph1.flow(sinkIdentifier1),
      expectedStorageName = expectedSink1StorageSuffix,
      expectedCheckpointVersion = 0, // the default checkpoint version is 0
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier2),
      flowElement = executionGraph1.flow(sinkIdentifier2),
      expectedStorageName = expectedSink2StorageSuffix,
      expectedCheckpointVersion = 0, // the default checkpoint version is 0
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )

    // Start another update with full refresh for selected sinks, expect a new checkpoint dir to be
    // created for selected sinks with version number incremented to 1
    val updateContext2 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot),
      fullRefreshTables = SomeTables(Set(sinkIdentifier, sinkIdentifier2))
    )

    updateContext2.pipelineExecution.startPipeline()
    updateContext2.pipelineExecution.awaitCompletion()
    val graphExecution2 = updateContext2.pipelineExecution.graphExecution.get
    val executionGraph2 = graphExecution2.graphForExecution

    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.sink(sinkIdentifier),
      flowElement = executionGraph2.flow(sinkIdentifier),
      expectedStorageName = expectedSinkStorageSuffix,
      expectedCheckpointVersion = 1, // version number incremented to 1 due to full refresh
      graphExecution = graphExecution2,
      updateContext = updateContext2
    )
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.sink(sinkIdentifier1),
      flowElement = executionGraph2.flow(sinkIdentifier1),
      expectedStorageName = expectedSink1StorageSuffix,
      expectedCheckpointVersion = 0, // version number unchanged
      graphExecution = graphExecution2,
      updateContext = updateContext2
    )
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.sink(sinkIdentifier2),
      flowElement = executionGraph2.flow(sinkIdentifier2),
      expectedStorageName = expectedSink2StorageSuffix,
      expectedCheckpointVersion = 1, // version number incremented to 1 due to full refresh
      graphExecution = graphExecution2,
      updateContext = updateContext2
    )

  }

  // This test creates a sink with two incoming flows, and we selectively full-refresh the sink.
  // We check that for all incoming flows are refreshed.
  test("all flow checkpoints for a sink are updated in selective refresh") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      val (mem1, mem2) = (MemoryStream[Int], MemoryStream[Int])
      mem1.addData(1, 2, 3)
      mem2.addData(4, 5, 6)
      registerView("a", query = dfFlowFunc(mem2.toDF()))
      registerView("b", query = dfFlowFunc(mem2.toDF()))
      registerSink(
        "sink",
        "memory"
      )
      registerFlow("sink", "sink_a", query = readStreamFlowFunc("a"))
      registerFlow("sink", "sink_a", query = readStreamFlowFunc("b"))
    }.toDataflowGraph

    val sinkIdentifier = fullyQualifiedIdentifier("sink")
    val sinkFlowIdentifier1 = fullyQualifiedIdentifier("sink_a")
    val sinkFlowIdentifier2 = fullyQualifiedIdentifier("sink_b")

    val testStorageRoot = Files.createTempDirectory("TestUpdateContext").normalize.toString
    val updateContext1 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot)
    )

    updateContext1.pipelineExecution.startPipeline()
    updateContext1.pipelineExecution.awaitCompletion()
    val graphExecution1 = updateContext1.pipelineExecution.graphExecution.get
    val executionGraph1 = graphExecution1.graphForExecution

    val expectedSinkStorageSuffix1 = "sink_a"
    val expectedSinkStorageSuffix2 = "sink_b"

    // Assert that the checkpoint dir for the sinks are created as expected
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier),
      flowElement = executionGraph1.flow(sinkFlowIdentifier1),
      expectedStorageName = expectedSinkStorageSuffix1,
      expectedCheckpointVersion = 0, // the default checkpoint version is 0
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph1.sink(sinkIdentifier),
      flowElement = executionGraph1.flow(sinkFlowIdentifier2),
      expectedStorageName = expectedSinkStorageSuffix2,
      expectedCheckpointVersion = 0, // the default checkpoint version is 0
      graphExecution = graphExecution1,
      updateContext = updateContext1
    )

    // Start another update with full refresh for selected sinks, expect a new checkpoint dir to be
    // created for the sink's flows with version number incremented to 1
    val updateContext2 = TestPipelineUpdateContext(
      unresolvedGraph = graph,
      spark = spark,
      storageRootOpt = Option(testStorageRoot),
      fullRefreshTables = SomeTables(Set(sinkIdentifier))
    )

    updateContext2.pipelineExecution.startPipeline()
    updateContext2.pipelineExecution.awaitCompletion()
    val graphExecution2 = updateContext2.pipelineExecution.graphExecution.get
    val executionGraph2 = graphExecution2.graphForExecution

    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.sink(sinkIdentifier),
      flowElement = executionGraph2.flow(sinkFlowIdentifier1),
      expectedStorageName = expectedSinkStorageSuffix1,
      expectedCheckpointVersion = 1, // version number incremented to 1 due to full refresh
      graphExecution = graphExecution2,
      updateContext2
    )
    assertFlowCheckpointDirExists(
      tableOrSinkElement = executionGraph2.sink(sinkIdentifier),
      flowElement = executionGraph2.flow(sinkFlowIdentifier2),
      expectedStorageName = expectedSinkStorageSuffix2,
      expectedCheckpointVersion = 1, // version number incremented to 1 due to full refresh
      graphExecution = graphExecution2,
      updateContext2
    )
  }
}

trait SystemMetadataTestHelpers {
  this: ExecutionTest =>

  /** Return the expected checkpoint directory location for a table or sink.
   *  These directories have "<expectedStorageName>/<expectedCheckpointVersion>" as their suffix. */
  private def getExpectedFlowCheckpointDirForTableOrSink(
      tableOrSinkElement: GraphElement,
      expectedStorageName: String,
      expectedCheckpointVersion: Int,
      updateContext: PipelineUpdateContext
  ): Path = {
    val expectedRawCheckPointDir = tableOrSinkElement match {
      case t: Table => t.normalizedPath.get + s"/checkpoints/$expectedStorageName"
      case _: Sink =>
        new Path(updateContext.storageRootOpt.get)
          .suffix(s"/checkpoints/$expectedStorageName")
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
      expectedStorageName: String,
      expectedCheckpointVersion: Int,
      graphExecution: GraphExecution,
      updateContext: PipelineUpdateContext
  ): Path = {
    val expectedCheckPointDir = getExpectedFlowCheckpointDirForTableOrSink(
      tableOrSinkElement = tableOrSinkElement,
      expectedStorageName = expectedStorageName,
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
      expectedStorageName: String,
      expectedCheckpointVersion: Int,
      graphExecution: GraphExecution,
      updateContext: PipelineUpdateContext
  ): Path = {
    val expectedCheckPointDir = getExpectedFlowCheckpointDirForTableOrSink(
      tableOrSinkElement = tableOrSinkElement,
      expectedStorageName = expectedStorageName,
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
