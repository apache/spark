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

import org.apache.spark.connect.{proto => sc}
import org.apache.spark.connect.proto.{PipelineCommand, PipelineEvent}
import org.apache.spark.sql.connect.{SparkConnectServerTest, SparkConnectTestUtils}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.{SessionKey, SparkConnectService}
import org.apache.spark.sql.pipelines.utils.PipelineTest

class SparkDeclarativePipelinesServerTest extends SparkConnectServerTest {

  override def afterEach(): Unit = {
    SparkConnectService.sessionManager
      .getIsolatedSessionIfPresent(SessionKey(defaultUserId, defaultSessionId))
      .foreach(_.removeAllPipelineExecutions())
    DataflowGraphRegistry.dropAllDataflowGraphs()
    PipelineTest.cleanupMetastore(spark)
    super.afterEach()
  }

  def buildPlanFromPipelineCommand(command: sc.PipelineCommand): sc.Plan = {
    sc.Plan
      .newBuilder()
      .setCommand(sc.Command.newBuilder().setPipelineCommand(command).build())
      .build()
  }

  def buildCreateDataflowGraphPlan(
      createDataflowGraph: sc.PipelineCommand.CreateDataflowGraph): sc.Plan = {
    buildPlanFromPipelineCommand(
      sc.PipelineCommand
        .newBuilder()
        .setCreateDataflowGraph(createDataflowGraph)
        .build())
  }

  def createDataflowGraph(implicit
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub): String = {
    sendPlan(
      buildCreateDataflowGraphPlan(
        sc.PipelineCommand.CreateDataflowGraph
          .newBuilder()
          // Currently, the default catalog cannot be changed because the in-memory catalog doesn't
          // support streaming.
          .setDefaultCatalog("spark_catalog")
          .setDefaultDatabase("default")
          .build()))(
      stub).getPipelineCommandResult.getCreateDataflowGraphResult.getDataflowGraphId
  }

  def buildStartRunPlan(startRun: sc.PipelineCommand.StartRun): sc.Plan = {
    buildPlanFromPipelineCommand(
      sc.PipelineCommand
        .newBuilder()
        .setStartRun(startRun)
        .build())
  }

  def sendPlan(plan: sc.Plan)(implicit
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub)
      : sc.ExecutePlanResponse = {
    val iter = stub.executePlan(buildExecutePlanRequest(plan))
    if (iter.hasNext) {
      iter.next()
    } else {
      throw new IllegalStateException(s"Invalid response: $iter")
    }
  }

  def registerPipelineDatasets(testPipelineDefinition: TestPipelineDefinition)(implicit
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub): Unit = {
    (testPipelineDefinition.viewDefs ++ testPipelineDefinition.tableDefs).foreach { tv =>
      sendPlan(
        buildPlanFromPipelineCommand(
          sc.PipelineCommand
            .newBuilder()
            .setDefineDataset(tv)
            .build()))
    }

    testPipelineDefinition.flowDefs.foreach { flow =>
      sendPlan(
        buildPlanFromPipelineCommand(
          sc.PipelineCommand
            .newBuilder()
            .setDefineFlow(flow)
            .build()))
    }
  }

  def registerGraphElementsFromSql(
      graphId: String,
      sql: String,
      sqlFileName: String = "table.sql")(implicit
      stub: sc.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub): Unit = {
    sendPlan(
      buildPlanFromPipelineCommand(
        sc.PipelineCommand
          .newBuilder()
          .setDefineSqlGraphElements(
            sc.PipelineCommand.DefineSqlGraphElements
              .newBuilder()
              .setDataflowGraphId(graphId)
              .setSqlFilePath(sqlFileName)
              .setSqlText(sql))
          .build()))
  }

  def createPlanner(): SparkConnectPlanner =
    new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))

  def startPipelineAndWaitForCompletion(graphId: String): ArrayBuffer[PipelineEvent] = {
    val defaultStartRunCommand =
      PipelineCommand.StartRun.newBuilder().setDataflowGraphId(graphId).build()
    startPipelineAndWaitForCompletion(defaultStartRunCommand)
  }

  def startPipelineAndWaitForCompletion(
      startRunCommand: PipelineCommand.StartRun): ArrayBuffer[PipelineEvent] = {
    withClient { client =>
      val capturedEvents = new ArrayBuffer[PipelineEvent]()
      val startRunRequest = buildStartRunPlan(startRunCommand)
      val responseIterator = client.execute(startRunRequest)
      // The response iterator will be closed when the pipeline is completed.
      while (responseIterator.hasNext) {
        val response = responseIterator.next()
        if (response.hasPipelineEventResult) {
          capturedEvents.append(response.getPipelineEventResult.getEvent)
        }
      }
      return capturedEvents
    }
    ArrayBuffer.empty[PipelineEvent]
  }
}
