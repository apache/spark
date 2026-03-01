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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * TODO
 */
class DistributedAnalysisSuite extends PipelineTest with SharedSparkSession {
  case class DummyFlowInfo(
      destinationTable: String,
      eagerDeps: Seq[String] = Seq.empty,
      lazyDeps: Seq[String] = Seq.empty,
      failsTerminally: Boolean = false
  ) {
    val flowName: String = destinationTable.replace("table", "flow")

    def register(registrationContext: TestGraphRegistrationContext): Unit = {
      val flowFunction = if (eagerDeps.nonEmpty) {
        FlowAnalysis.createQueryFunctionResultPollingFlowFunction(
          fullyQualifiedIdentifier(flowName),
          registrationContext
        )
      } else {
        val plan = if (lazyDeps.nonEmpty) {
          spark.sessionState.sqlParser
            .parsePlan(eagerDeps.map(t => s"select * from $t").mkString(" union "))
        } else {
          spark.range(4).logicalPlan
        }
        FlowAnalysis.createFlowFunctionFromLogicalPlan(plan)
      }
      registrationContext.registerFlow(destinationTable, flowName, flowFunction)
    }
  }

  class TestDistributedAnalyzer(val dummyFlows: Seq[DummyFlowInfo], val spark: SparkSession) {
    val registrationContext = new TestGraphRegistrationContext(spark)
    dummyFlows.foreach { f =>
      f.register(registrationContext)
      registrationContext.registerTable(f.destinationTable)
    }
    private val dummyFlowsById = dummyFlows.map { f =>
      (fullyQualifiedIdentifier(f.flowName), f)
    }.toMap

    val unresolvedGraph: DataflowGraph = registrationContext.toDataflowGraph
    val analysisContext = new GraphAnalysisContext()
    unresolvedGraph.flows.foreach(analysisContext.toBeResolvedFlows.add)
    val transformer = new DataflowGraphTransformer(unresolvedGraph)

    val nodeProcessor = new CoreDataflowNodeProcessor(unresolvedGraph, analysisContext)

    // Signals received by the client, but not yet processed
    val clientSignalQueue = new mutable.Queue[TableIdentifier]()

    def transformerIteration(): Unit = {
      Option(analysisContext.toBeResolvedFlows.pollFirst()).foreach { flow =>
        transformer.transformFlowAndMaybeDestination(
          flow,
          nodeProcessor.processNode,
          analysisContext
        )
      }
    }

    def analyze(flowIdentifier: TableIdentifier, plan: LogicalPlan): Unit = {
      analysisContext.analyze(flowIdentifier, plan, unresolvedGraph, spark)
    }

    def registerQueryFunctionResult(
        flowIdentifier: TableIdentifier,
        result: QueryFunctionResult): Unit = {
      registrationContext.registerQueryFunctionResult(flowIdentifier, result)
      analysisContext.markFlowPlanRegistered(flowIdentifier)
    }

    def runToCompletion(randomSeed: Integer): Unit = {
      val r = new scala.util.Random(randomSeed)

      var numIters = 0
      while (numIters < 200
        && analysisContext.toBeResolvedFlows.isEmpty
        && clientSignalQueue.isEmpty
        && analysisContext.flowClientSignalQueue.isEmpty) {
        numIters += 1

        r.nextInt(3) match {
          case 0 => transformerIteration()
          case 1 => // Simulate the signal-sending RPC thread
            if (!analysisContext.flowClientSignalQueue.isEmpty) {
              val flowToRetryId = analysisContext.flowClientSignalQueue.poll()
              dummyFlowsById(flowToRetryId).eagerDeps.foreach { tableNames =>
                try {
                  tableNames.foreach { tableName =>
                    val plan = spark.sessionState.sqlParser.parsePlan(s"select * from $tableName")
                    analyze(flowToRetryId, plan)
                  }
                  clientSignalQueue.append(flowToRetryId)
                } catch {
                  case _: UnresolvedDatasetException =>
                }
              }
            }
          case 2 => // Simulate the client
            if (clientSignalQueue.nonEmpty) {
              val flowId = clientSignalQueue.dequeue()
              val upstreamTables = dummyFlowsById(flowId).eagerDeps
              val dummyFlow = dummyFlowsById(flowId)
              val result = if (dummyFlow.failsTerminally) {
                QueryFunctionTerminalFailure
              } else {
                QueryFunctionSuccess(
                  spark.sessionState.sqlParser
                    .parsePlan(s"select * from $upstreamTables")
                    .logicalPlan
                )
              }
              registerQueryFunctionResult(flowId, result)
            }
        }

        assert(
          analysisContext.toBeResolvedFlows.size +
          analysisContext.resolvedFlowsMap.size +
          analysisContext.failedUnregisteredFlows.size +
          analysisContext.failedDependentFlows.size
          == unresolvedGraph.flows.size
        )
      }
    }
  }

  test("single node external .columns") {
    val externalTableId = fullyQualifiedIdentifier("external")
    spark.sql(s"CREATE TABLE $externalTableId AS SELECT * FROM RANGE(3)")
    val dummyFlows = Seq(
      DummyFlowInfo("table1", eagerDeps = Seq(externalTableId.quotedString))
    )
    val testDistributedAnalyzer = new TestDistributedAnalyzer(dummyFlows, spark)

    // transformer processes node, flow function should fail because no relation for node, get
    // popped back on queue
    testDistributedAnalyzer.transformerIteration()

    // define query function result
    testDistributedAnalyzer.registerQueryFunctionResult(
      fullyQualifiedIdentifier("table1"),
      QueryFunctionSuccess(spark.range(4).logicalPlan)
    )

    // transformer processes node again. flow function should succeed.
    testDistributedAnalyzer.transformerIteration()
  }

  test("two nodes with second has .columns") {
    // flow 1 -> table -> flow 2 -> table
    // flow 2 can't be resolved immediately because it analyzes flow 1
    val dummyFlows = Seq(
      DummyFlowInfo("table2", eagerDeps = Seq("table1")),
      DummyFlowInfo("table1")
    )

    val testDistributedAnalyzer = new TestDistributedAnalyzer(dummyFlows, spark)
    val analysisContext = testDistributedAnalyzer.analysisContext
    val selectFromTable1Plan = spark.sessionState.sqlParser.parsePlan("select * from table1")

    val flow1Id = fullyQualifiedIdentifier("flow1")
    val table1Id = fullyQualifiedIdentifier("table1")
    val flow2Id = fullyQualifiedIdentifier("flow2")

    // Attempt to analyze table1 on behalf of flow2. Should fail and record dependency.
    intercept[UnresolvedDatasetException](
      testDistributedAnalyzer.analyze(flow2Id, selectFromTable1Plan)
    )
    val failedDependentFlows = analysisContext.failedDependentFlows
    assert(failedDependentFlows.size() == 1)
    assert(failedDependentFlows.get(table1Id).map(_.identifier).toSet == Set(flow2Id))

    // transformer processes flow2, flow function should fail because no relation for node, get
    // added to unregistered list
    testDistributedAnalyzer.transformerIteration()
    assert(analysisContext.failedUnregisteredFlows.containsKey(flow2Id))

    // transformer processes flow1, flow function should succeed
    // flow2 shouldn't be added to the queue, because its plan is still unregistered
    testDistributedAnalyzer.transformerIteration()
    assert(analysisContext.toBeResolvedFlows.isEmpty)
    assert(analysisContext.resolvedFlowsMap.containsKey(flow1Id))

    // detects flow1 is resolved, sends signal to retry flow2
    assert(
      testDistributedAnalyzer.analysisContext.flowClientSignalQueue.toArray.toSeq == Seq(flow2Id)
    )
    testDistributedAnalyzer.analysisContext.flowClientSignalQueue.clear()

    testDistributedAnalyzer.analyze(flow2Id, selectFromTable1Plan)

    // define query function result
    testDistributedAnalyzer.registerQueryFunctionResult(
      flow2Id,
      QueryFunctionSuccess(selectFromTable1Plan)
    )
    assert(analysisContext.failedUnregisteredFlows.isEmpty)
    assert(analysisContext.toBeResolvedFlows.size == 1)

    // transformer processes node again. flow function should succeed.
    testDistributedAnalyzer.transformerIteration()
    assert(analysisContext.toBeResolvedFlows.isEmpty)
    assert(analysisContext.failedUnregisteredFlows.isEmpty)
    assert(analysisContext.failedDependentFlows.isEmpty)
    assert(analysisContext.resolvedFlowsMap.size == 2)
  }

  test("random orderings") {
    // flow 1 -> table -> flow 2 -> table
    // flow 2 can't be resolved immediately because it analyzes flow 1
    val dummyFlows = Seq(
      DummyFlowInfo("table2", eagerDeps = Seq("table1")),
      DummyFlowInfo("table1")
    )

    val testDistributedAnalyzer = new TestDistributedAnalyzer(dummyFlows, spark)

    testDistributedAnalyzer.runToCompletion(4367)
    assert(testDistributedAnalyzer.analysisContext.failedUnregisteredFlows.isEmpty)
    assert(testDistributedAnalyzer.analysisContext.failedDependentFlows.isEmpty)
    assert(testDistributedAnalyzer.analysisContext.resolvedFlowsMap.size == 2)
  }

  test("query function fails after eager analysis") {
    val dummyFlows = Seq(
      DummyFlowInfo("table2", eagerDeps = Seq("table1"), failsTerminally = true),
      DummyFlowInfo("table1")
    )
    val testDistributedAnalyzer = new TestDistributedAnalyzer(dummyFlows, spark)
    testDistributedAnalyzer.runToCompletion(4367)
    assert(testDistributedAnalyzer.analysisContext.resolvedFlowsMap.size == 0)
  }
}
