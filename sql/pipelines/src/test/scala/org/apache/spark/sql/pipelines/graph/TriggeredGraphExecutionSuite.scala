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

import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.{functions, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.{DataFrame, Dataset}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.common.{FlowStatus, RunState}
import org.apache.spark.sql.pipelines.graph.TriggeredGraphExecution.StreamState
import org.apache.spark.sql.pipelines.logging.EventLevel
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class TriggeredGraphExecutionSuite extends ExecutionTest with SharedSparkSession {

  /** Returns a Dataset of Longs from the table with the given identifier. */
  private def getTable(identifier: TableIdentifier): Dataset[Long] = {
    val session = spark
    import session.implicits._

    spark.read.table(identifier.toString).as[Long]
  }

  /** Return flows with expected stream state. */
  private def getFlowsWithState(
      graphExecution: TriggeredGraphExecution,
      state: StreamState
  ): Set[TableIdentifier] = {
    graphExecution.pipelineState.collect {
      case (flowIdentifier, flowState) if flowState == state => flowIdentifier
    }.toSet
  }

  test("basic graph resolution and execution") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(Seq(1, 2).toDF("x")))
      registerMaterializedView("b", query = readFlowFunc("a"))
    }
    val unresolvedGraph = pipelineDef.toDataflowGraph
    val resolvedGraph = unresolvedGraph.resolve()
    assert(resolvedGraph.flows.size == 2)
    assert(unresolvedGraph.flows.size == 2)
    assert(unresolvedGraph.tables.size == 2)
    val bFlow =
      resolvedGraph.resolvedFlows.filter(_.identifier == fullyQualifiedIdentifier("b")).head
    assert(bFlow.inputs == Set(fullyQualifiedIdentifier("a")))

    val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    // start with queued
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("a"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.PLANNING),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("b"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.PLANNING),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
  }

  test("graph materialization with streams") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(Seq(1, 2).toDF("x")))
      registerMaterializedView("b", query = readFlowFunc("a"))
      registerView("c", query = readStreamFlowFunc("a"))
      registerTable("d", query = Option(readStreamFlowFunc("c")))
    }

    val unresolvedGraph = pipelineDef.toDataflowGraph
    val resolvedGraph = unresolvedGraph.resolve()
    assert(resolvedGraph.flows.size == 4)
    assert(resolvedGraph.tables.size == 3)
    assert(resolvedGraph.views.size == 1)

    val bFlow =
      resolvedGraph.resolvedFlows.filter(_.identifier == fullyQualifiedIdentifier("b")).head
    assert(bFlow.inputs == Set(fullyQualifiedIdentifier("a")))

    val cFlow =
      resolvedGraph.resolvedFlows
        .filter(_.identifier == fullyQualifiedIdentifier("c", isTemporaryView = true))
        .head
    assert(cFlow.inputs == Set(fullyQualifiedIdentifier("a")))

    val dFlow =
      resolvedGraph.resolvedFlows.filter(_.identifier == fullyQualifiedIdentifier("d")).head
    assert(dFlow.inputs == Set(fullyQualifiedIdentifier("c", isTemporaryView = true)))

    val updateContext = TestPipelineUpdateContext(spark, unresolvedGraph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("a"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.PLANNING),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("b"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.PLANNING),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("d"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )

    // no flow progress event for c, as it is a temporary view
    assertNoFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("c", isTemporaryView = true),
      flowStatus = FlowStatus.STARTING
    )
    checkAnswer(
      spark.read.table(fullyQualifiedIdentifier("a").toString),
      Seq(Row(1), Row(2))
    )
  }

  test("three hop pipeline") {
    val session = spark
    import session.implicits._

    // Construct pipeline
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      private val ints = MemoryStream[Int]
      ints.addData(1 until 10: _*)
      registerView("input", query = dfFlowFunc(ints.toDF()))
      registerTable(
        "eights",
        query = Option(sqlFlowFunc(spark, "SELECT value * 2 as value FROM STREAM fours"))
      )
      registerTable(
        "fours",
        query = Option(sqlFlowFunc(spark, "SELECT value * 2 as value FROM STREAM evens"))
      )
      registerTable(
        "evens",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM STREAM input WHERE value % 2 = 0"))
      )
    }
    val graph = pipelineDef.toDataflowGraph

    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("evens"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("eights"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("fours"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )

    val graphExecution = updateContext.pipelineExecution.graphExecution.get
    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("eights"),
        fullyQualifiedIdentifier("fours"),
        fullyQualifiedIdentifier("evens")
      )
    )
    assert(getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.SKIPPED).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.CANCELED).isEmpty)

    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("evens")), 2L, 4L, 6L, 8L)
    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("fours")), 4L, 8L, 12L, 16L)
    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("eights")), 8L, 16L, 24L, 32L)
  }

  test("all events are emitted even if there is no data") {
    val session = spark
    import session.implicits._

    // Construct pipeline
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      private val ints = MemoryStream[Int]
      registerView("input", query = dfFlowFunc(ints.toDF()))
      registerTable(
        "evens",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM STREAM input WHERE value % 2 = 0"))
      )
    }
    val graph = pipelineDef.toDataflowGraph

    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graphExecution = updateContext.pipelineExecution.graphExecution.get
    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("evens")
      )
    )
    assert(getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.SKIPPED).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.CANCELED).isEmpty)

    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("evens"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )

    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("evens")))
  }

  test("stream failure causes its downstream to be skipped") {
    val session = spark
    import session.implicits._

    spark.sql("CREATE TABLE src USING PARQUET AS SELECT * FROM RANGE(10)")

    // A UDF which fails immediately
    val failUDF = functions.udf((_: String) => {
      throw new RuntimeException("Test error")
      true
    })

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      private val memoryStream = MemoryStream[Int]
      memoryStream.addData(1, 2)
      registerView("input_view", query = dfFlowFunc(memoryStream.toDF()))
      registerTable(
        "input_table",
        query = Option(readStreamFlowFunc("input_view"))
      )
      registerTable(
        "branch_1",
        query = Option(readStreamFlowFunc("input_table"))
      )
      registerTable(
        "branch_2",
        query = Option(dfFlowFunc(spark.readStream.table("src").filter(failUDF($"id"))))
      )
      registerTable("x", query = Option(readStreamFlowFunc("branch_2")))
      registerView("y", query = readStreamFlowFunc("x"))
      registerTable("z", query = Option(readStreamFlowFunc("x")))
    }
    val graph = pipelineDef.toDataflowGraph
    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graphExecution = updateContext.pipelineExecution.graphExecution.get
    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("input_table"),
        fullyQualifiedIdentifier("branch_1")
      )
    )
    assert(
      getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR) == Set(
        fullyQualifiedIdentifier("branch_2")
      )
    )
    assert(
      getFlowsWithState(graphExecution, StreamState.SKIPPED) == Set(
        fullyQualifiedIdentifier("x"),
        fullyQualifiedIdentifier("z")
      )
    )
    assert(getFlowsWithState(graphExecution, StreamState.CANCELED).isEmpty)

    // `input_table` and `branch_1` should succeed, while `branch_2` should fail.
    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("input_table"),
      expectedFlowStatus = FlowStatus.COMPLETED,
      expectedEventLevel = EventLevel.INFO
    )
    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("branch_1"),
      expectedFlowStatus = FlowStatus.COMPLETED,
      expectedEventLevel = EventLevel.INFO
    )
    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("branch_2"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.ERROR,
      errorChecker = _.getMessage.contains("Test error")
    )
    // all the downstream flows of `branch_2` should be skipped.
    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("x"),
      expectedFlowStatus = FlowStatus.SKIPPED,
      expectedEventLevel = EventLevel.INFO
    )
    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("z"),
      expectedFlowStatus = FlowStatus.SKIPPED,
      expectedEventLevel = EventLevel.INFO
    )

    // since flow `x` and `z` are skipped, we should not see any progress events for them.
    assertNoFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("x"),
      flowStatus = FlowStatus.STARTING
    )
    assertNoFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("z"),
      flowStatus = FlowStatus.STARTING
    )

    // b,c should have valid data as their upstream has no failures.
    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("input_table")), 1L, 2L)
    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("branch_1")), 1L, 2L)

    // the pipeline run should be marked as failed since `branch_2` has failed.
    assertRunProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      state = RunState.FAILED,
      expectedEventLevel = EventLevel.ERROR,
      msgChecker = msg =>
        msg.contains(
          "Run is FAILED since flow 'spark_catalog.test_db.branch_2' has failed more than " +
          "2 times"
        )
    )
  }

  test("stream failure on deletes and updates gives clear error") {
    spark.sql("CREATE TABLE src1 USING PARQUET AS SELECT * FROM RANGE(10)")
    spark.sql("CREATE TABLE src2 USING PARQUET AS SELECT * FROM RANGE(10)")

    // Pipeline fails due to udf failure
    val pipelineDef1 = new TestGraphRegistrationContext(spark) {
      registerView(
        "input_view",
        query = dfFlowFunc(
          spark.readStream
            .table("src1")
            .unionAll(spark.readStream.table("src2"))
        )
      )
      registerTable(
        "input_table",
        query = Option(readStreamFlowFunc("input_view"))
      )
    }

    val graph1 = pipelineDef1.toDataflowGraph
    val updateContext1 = TestPipelineUpdateContext(spark, graph1)
    updateContext1.pipelineExecution.runPipeline()
    updateContext1.pipelineExecution.awaitCompletion()
    assertFlowProgressEvent(
      eventBuffer = updateContext1.eventBuffer,
      identifier = fullyQualifiedIdentifier("input_table"),
      expectedFlowStatus = FlowStatus.COMPLETED,
      expectedEventLevel = EventLevel.INFO
    )

    val pipelineDef2 = new TestGraphRegistrationContext(spark) {
      registerView(
        "input_view",
        query = dfFlowFunc(spark.readStream.table("src2"))
      )
      registerTable(
        "input_table",
        query = Option(readStreamFlowFunc("input_view"))
      )
    }
    val graph2 = pipelineDef2.toDataflowGraph
    val updateContext2 = TestPipelineUpdateContext(spark, graph2)
    updateContext2.pipelineExecution.runPipeline()
    updateContext2.pipelineExecution.awaitCompletion()

    assertFlowProgressEvent(
      eventBuffer = updateContext2.eventBuffer,
      identifier = fullyQualifiedIdentifier("input_table"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.ERROR,
      msgChecker = _.contains(
        s"Flow '${eventLogName("input_table")}' had streaming sources added or removed."
      )
    )
  }

  test("user-specified schema is applied to table") {
    val session = spark
    import session.implicits._

    val specifiedSchema = new StructType().add("x", "int", nullable = true)
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView(
        "specified_schema",
        query = dfFlowFunc(Seq(1, 2).toDF("x")),
        specifiedSchema = Option(specifiedSchema)
      )

      registerMaterializedView(
        "specified_schema_stream",
        query = dfFlowFunc(Seq(1, 2).toDF("x")),
        specifiedSchema = Option(specifiedSchema)
      )

      registerMaterializedView(
        "specified_schema_downstream",
        query = readFlowFunc("specified_schema"),
        specifiedSchema = Option(specifiedSchema)
      )

      registerMaterializedView(
        "specified_schema_downbatch",
        query = readFlowFunc("specified_schema_stream"),
        specifiedSchema = Option(specifiedSchema)
      )
    }
    val ctx = TestPipelineUpdateContext(
      spark,
      pipelineDef.toDataflowGraph,
      fullRefreshTables = AllTables,
      resetCheckpointFlows = AllFlows
    )
    ctx.pipelineExecution.runPipeline()
    ctx.pipelineExecution.awaitCompletion()

    Seq(
      fullyQualifiedIdentifier("specified_schema"),
      fullyQualifiedIdentifier("specified_schema_stream"),
      fullyQualifiedIdentifier("specified_schema_downstream"),
      fullyQualifiedIdentifier("specified_schema_downbatch")
    ).foreach { tableIdentifier =>
      val catalogId =
        Identifier.of(Array(tableIdentifier.database.get), tableIdentifier.identifier)
      assert(
        spark.sessionState.catalogManager.currentCatalog
          .asInstanceOf[TableCatalog]
          .loadTable(catalogId)
          .columns() sameElements CatalogV2Util.structTypeToV2Columns(specifiedSchema),
        s"Table $catalogId's schema in the catalog does not match the specified schema"
      )
      assert(
        spark.table(tableIdentifier).schema == specifiedSchema,
        s"Table $tableIdentifier's schema in storage does not match the specified schema"
      )
    }
  }

  test("stopping a pipeline mid-execution") {
    val session = spark
    import session.implicits._

    // A UDF which adds a delay
    val delayUDF = functions.udf((_: String) => {
      Thread.sleep(5 * 1000)
      true
    })
    spark.sql("CREATE TABLE src1 USING PARQUET AS SELECT * FROM RANGE(10)")

    // Construct pipeline
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      private val memoryStream = MemoryStream[Int]
      memoryStream.addData(1, 2)
      registerView("input_view", query = dfFlowFunc(memoryStream.toDF()))
      registerTable(
        "input_table",
        query = Option(readStreamFlowFunc("input_view"))
      )
      registerTable(
        "query_with_delay",
        query = Option(dfFlowFunc(spark.readStream.table("src1").filter(delayUDF($"id"))))
      )
      registerTable("x", query = Option(readStreamFlowFunc("query_with_delay")))
    }

    val graph = pipelineDef.toDataflowGraph
    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.startPipeline()

    val graphExecution = updateContext.pipelineExecution.graphExecution.get

    eventually(timeout(Span(60, Seconds))) {
      assert(
        graphExecution.pipelineState(
          fullyQualifiedIdentifier("input_table")
        ) == StreamState.SUCCESSFUL
      )
      assert(
        graphExecution.pipelineState(
          fullyQualifiedIdentifier("query_with_delay")
        ) == StreamState.RUNNING
      )
      graphExecution.stop()
    }
    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("input_table")
      )
    )
    assert(getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR).isEmpty)
    assert(
      getFlowsWithState(graphExecution, StreamState.SKIPPED) == Set(fullyQualifiedIdentifier("x"))
    )
    assert(
      getFlowsWithState(graphExecution, StreamState.CANCELED) == Set(
        fullyQualifiedIdentifier("query_with_delay")
      )
    )
    assert(
      latestFlowStatuses(updateContext.eventBuffer) == Map(
        eventLogName("input_table") -> FlowStatus.COMPLETED,
        eventLogName("query_with_delay") -> FlowStatus.STOPPED,
        eventLogName("x") -> FlowStatus.SKIPPED
      )
    )

    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("input_table")), 1L, 2L)
  }

  test("two hop pipeline with partitioned graph") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("integer_input", query = dfFlowFunc(Seq(1, 2, 3, 4).toDF("value")))
      registerMaterializedView(
        "double",
        query = sqlFlowFunc(spark, "SELECT value * 2 as value FROM integer_input")
      )
      registerMaterializedView(
        "string_input",
        query = dfFlowFunc(Seq("a", "b", "c", "d").toDF("value"))
      )
      registerMaterializedView(
        "append_x",
        query = sqlFlowFunc(spark, "SELECT CONCAT(value, 'x') as value FROM string_input")
      )
    }

    val graph = pipelineDef.toDataflowGraph
    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graphExecution = updateContext.pipelineExecution.graphExecution.get

    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("integer_input"),
        fullyQualifiedIdentifier("string_input"),
        fullyQualifiedIdentifier("double"),
        fullyQualifiedIdentifier("append_x")
      )
    )
    assert(getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.SKIPPED).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.CANCELED).isEmpty)

    assert(
      latestFlowStatuses(updateContext.eventBuffer) == Map(
        eventLogName("integer_input") -> FlowStatus.COMPLETED,
        eventLogName("string_input") -> FlowStatus.COMPLETED,
        eventLogName("double") -> FlowStatus.COMPLETED,
        eventLogName("append_x") -> FlowStatus.COMPLETED
      )
    )

    checkDatasetUnorderly(getTable(fullyQualifiedIdentifier("double")), 2L, 4L, 6L, 8L)
    checkAnswer(
      spark.read.table(fullyQualifiedIdentifier("append_x").toString),
      Seq(Row("ax"), Row("bx"), Row("cx"), Row("dx"))
    )
  }

  test("multiple hop pipeline with merge from multiple sources") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("integer_input", query = dfFlowFunc(Seq(1, 2, 3, 4).toDF("nums")))
      registerMaterializedView(
        "double",
        query = sqlFlowFunc(spark, "SELECT nums * 2 as nums FROM integer_input")
      )
      registerMaterializedView(
        "string_input",
        query = dfFlowFunc(Seq("a", "b", "c", "d").toDF("text"))
      )
      registerMaterializedView(
        "append_x",
        query = sqlFlowFunc(spark, "SELECT CONCAT(text, 'x') as text FROM string_input")
      )
      registerMaterializedView(
        "merged",
        query = sqlFlowFunc(
          spark,
          "SELECT * FROM double FULL OUTER JOIN append_x ON nums::STRING = text"
        )
      )
    }

    val graph = pipelineDef.toDataflowGraph
    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graphExecution = updateContext.pipelineExecution.graphExecution.get

    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("integer_input"),
        fullyQualifiedIdentifier("string_input"),
        fullyQualifiedIdentifier("double"),
        fullyQualifiedIdentifier("append_x"),
        fullyQualifiedIdentifier("merged")
      )
    )

    assert(getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.SKIPPED).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.CANCELED).isEmpty)

    assert(
      latestFlowStatuses(updateContext.eventBuffer) == Map(
        eventLogName("integer_input") -> FlowStatus.COMPLETED,
        eventLogName("string_input") -> FlowStatus.COMPLETED,
        eventLogName("double") -> FlowStatus.COMPLETED,
        eventLogName("append_x") -> FlowStatus.COMPLETED,
        eventLogName("merged") -> FlowStatus.COMPLETED
      )
    )

    val expectedSchema = new StructType().add("nums", IntegerType).add("text", StringType)
    assert(spark.read.table(fullyQualifiedIdentifier("merged").toString).schema == expectedSchema)
    checkAnswer(
      spark.read.table(fullyQualifiedIdentifier("merged").toString),
      Seq(
        Row(2, null),
        Row(4, null),
        Row(6, null),
        Row(8, null),
        Row(null, "ax"),
        Row(null, "bx"),
        Row(null, "cx"),
        Row(null, "dx")
      )
    )
  }

  test("multiple hop pipeline with split and merge from single source") {
    val session = spark
    import session.implicits._

    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerMaterializedView(
        "input_table",
        query = dfFlowFunc(
          Seq((1, 1), (1, 2), (2, 3), (2, 4)).toDF("x", "y")
        )
      )
      registerMaterializedView(
        "left_split",
        query = sqlFlowFunc(
          spark,
          "SELECT x FROM input_table WHERE x IS NOT NULL"
        )
      )
      registerMaterializedView(
        "right_split",
        query = sqlFlowFunc(spark, "SELECT y FROM input_table WHERE y IS NOT NULL")
      )
      registerMaterializedView(
        "merged",
        query = sqlFlowFunc(
          spark,
          "SELECT * FROM left_split FULL OUTER JOIN right_split ON x = y"
        )
      )
    }

    val graph = pipelineDef.toDataflowGraph
    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    val graphExecution = updateContext.pipelineExecution.graphExecution.get

    assert(
      getFlowsWithState(graphExecution, StreamState.SUCCESSFUL) == Set(
        fullyQualifiedIdentifier("input_table"),
        fullyQualifiedIdentifier("left_split"),
        fullyQualifiedIdentifier("right_split"),
        fullyQualifiedIdentifier("merged")
      )
    )

    assert(getFlowsWithState(graphExecution, StreamState.TERMINATED_WITH_ERROR).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.SKIPPED).isEmpty)
    assert(getFlowsWithState(graphExecution, StreamState.CANCELED).isEmpty)

    assert(
      latestFlowStatuses(updateContext.eventBuffer) == Map(
        eventLogName("input_table") -> FlowStatus.COMPLETED,
        eventLogName("left_split") -> FlowStatus.COMPLETED,
        eventLogName("right_split") -> FlowStatus.COMPLETED,
        eventLogName("merged") -> FlowStatus.COMPLETED
      )
    )

    val expectedSchema = new StructType().add("x", IntegerType).add("y", IntegerType)
    assert(spark.read.table(fullyQualifiedIdentifier("merged").toString).schema == expectedSchema)
    checkAnswer(
      spark.read.table(fullyQualifiedIdentifier("merged").toString),
      Seq(
        Row(1, 1),
        Row(1, 1),
        Row(2, 2),
        Row(2, 2),
        Row(null, 3),
        Row(null, 4)
      )
    )
  }

  test("test default flow retry is 2 and event WARN/ERROR levels accordingly") {
    val session = spark
    import session.implicits._

    val fail = functions.udf((x: Int) => {
      throw new RuntimeException("Intentionally failing UDF.")
      x
    })
    spark.sql("CREATE TABLE src USING parquet AS SELECT id AS value FROM RANGE(10)")
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(spark.readStream.table("src").select(fail($"value") as "value")))
      )
    }
    val graph = pipelineDef.toDataflowGraph
    val updateContext = TestPipelineUpdateContext(spark, graph)
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()

    // assert the flow failure event log tracked 3 times, 2 retries and 1 original attempt
    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("a"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.INFO,
      expectedNumOfEvents = Option(3)
    )

    assertFlowProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("a"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.ERROR,
      expectedNumOfEvents = Option(1),
      msgChecker = _.contains("has FAILED more than 2 times and will not be restarted")
    )

    assertRunProgressEvent(
      eventBuffer = updateContext.eventBuffer,
      state = RunState.FAILED,
      expectedEventLevel = EventLevel.ERROR,
      msgChecker = msg =>
        msg.contains(
          "Run is FAILED since flow 'spark_catalog.test_db.a' has failed more than 2 times"
        )
    )
  }

  test("partial graph updates") {
    val session = spark
    import session.implicits._

    val ints: MemoryStream[Int] = MemoryStream[Int]
    val pipelineDef = new TestGraphRegistrationContext(spark) {
      ints.addData(1, 2, 3)
      registerTable("source", query = Option(dfFlowFunc(ints.toDF())))
      registerTable("all", query = Option(readStreamFlowFunc("source")))
      registerView(
        "evens",
        query = sqlFlowFunc(spark, "SELECT * FROM all WHERE value % 2 = 0")
      )
      registerMaterializedView(
        "max_evens",
        query = sqlFlowFunc(spark, "SELECT MAX(value) FROM evens")
      )
    }
    val graph1 = pipelineDef.toDataflowGraph

    // First update, which excludes "max_evens" table.
    val updateContext1 = TestPipelineUpdateContext(
      spark = spark,
      unresolvedGraph = graph1,
      refreshTables = SomeTables(
        Set(fullyQualifiedIdentifier("source"), fullyQualifiedIdentifier("all"))
      ),
      resetCheckpointFlows = NoFlows
    )
    updateContext1.pipelineExecution.runPipeline()
    updateContext1.pipelineExecution.awaitCompletion()

    def readTable(tableName: String): DataFrame = {
      spark.read.table(fullyQualifiedIdentifier(tableName).toString)
    }

    // only `source` and `all_tables` flows are executed and their table are refreshed and have data
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext1.eventBuffer,
      identifier = fullyQualifiedIdentifier("source"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext1.eventBuffer,
      identifier = fullyQualifiedIdentifier("all"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    checkAnswer(readTable("source"), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(readTable("all"), Seq(Row(1), Row(2), Row(3)))
    // `max_evens` flow shouldn't be executed and the table is not created
    assertFlowProgressEvent(
      eventBuffer = updateContext1.eventBuffer,
      identifier = fullyQualifiedIdentifier("max_evens"),
      expectedFlowStatus = FlowStatus.EXCLUDED,
      expectedEventLevel = EventLevel.INFO
    )
    assert(!spark.catalog.tableExists(fullyQualifiedIdentifier("max_evens").toString))

    // Second update, which excludes "all" table.
    ints.addData(4)
    val updateContext2 = TestPipelineUpdateContext(
      spark = spark,
      unresolvedGraph = graph1,
      refreshTables = SomeTables(
        Set(fullyQualifiedIdentifier("source"), fullyQualifiedIdentifier("max_evens"))
      ),
      resetCheckpointFlows = NoFlows
    )
    updateContext2.pipelineExecution.runPipeline()
    updateContext2.pipelineExecution.awaitCompletion()
    checkAnswer(readTable("source"), Seq(Row(1), Row(2), Row(3), Row(4)))
    // `all` flow should not be executed and 'all' table is not refreshed and has old data
    checkAnswer(readTable("all"), Seq(Row(1), Row(2), Row(3)))
    checkAnswer(readTable("max_evens"), Seq(Row(2)))
    assertFlowProgressEvent(
      eventBuffer = updateContext2.eventBuffer,
      identifier = fullyQualifiedIdentifier("all"),
      expectedFlowStatus = FlowStatus.EXCLUDED,
      expectedEventLevel = EventLevel.INFO
    )
    assertNoFlowProgressEvent(
      eventBuffer = updateContext2.eventBuffer,
      identifier = fullyQualifiedIdentifier("all"),
      flowStatus = FlowStatus.STARTING
    )

    // `max_evens` and `source` flows should be executed and the table is created with new data
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext2.eventBuffer,
      identifier = fullyQualifiedIdentifier("max_evens"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.PLANNING),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
    assertFlowProgressStatusInOrder(
      eventBuffer = updateContext2.eventBuffer,
      identifier = fullyQualifiedIdentifier("source"),
      expectedFlowProgressStatus = Seq(
        (EventLevel.INFO, FlowStatus.QUEUED),
        (EventLevel.INFO, FlowStatus.STARTING),
        (EventLevel.INFO, FlowStatus.RUNNING),
        (EventLevel.INFO, FlowStatus.COMPLETED)
      )
    )
  }

  test("flow fails to resolve") {
    val graph = new TestGraphRegistrationContext(spark) {
      registerTable("table1", query = Option(sqlFlowFunc(spark, "SELECT * FROM nonexistent_src1")))
      registerTable("table2", query = Option(sqlFlowFunc(spark, "SELECT * FROM nonexistent_src2")))
      registerTable("table3", query = Option(sqlFlowFunc(spark, "SELECT * FROM table1")))
    }.toDataflowGraph

    val updateContext = TestPipelineUpdateContext(spark = spark, unresolvedGraph = graph)
    updateContext.pipelineExecution.runPipeline()

    assertFlowProgressEvent(
      updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("table1"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.WARN,
      msgChecker = _.contains("Failed to resolve flow: 'spark_catalog.test_db.table1'"),
      errorChecker = _.getMessage.contains(
        "The table or view `spark_catalog`.`test_db`.`nonexistent_src1` cannot be found"
      )
    )

    assertFlowProgressEvent(
      updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("table2"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.WARN,
      msgChecker = _.contains("Failed to resolve flow: 'spark_catalog.test_db.table2'"),
      errorChecker = _.getMessage.contains(
        "The table or view `spark_catalog`.`test_db`.`nonexistent_src2` cannot be found"
      )
    )

    assertFlowProgressEvent(
      updateContext.eventBuffer,
      identifier = fullyQualifiedIdentifier("table3"),
      expectedFlowStatus = FlowStatus.FAILED,
      expectedEventLevel = EventLevel.WARN,
      msgChecker = _.contains(
        "Failed to resolve flow due to upstream failure: 'spark_catalog.test_db.table3'"
      ),
      errorChecker = { ex =>
        ex.getMessage.contains(
          "Failed to read dataset 'spark_catalog.test_db.table1'. Dataset is defined in the " +
          "pipeline but could not be resolved."
        )
      }
    )
  }
}
