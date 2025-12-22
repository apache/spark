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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque, ConcurrentLinkedQueue}

import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala
import scala.util.Failure

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{DataFrame, SparkSession}

class GraphAnalysisContext {
  val toBeResolvedFlows = new ConcurrentLinkedDeque[Flow]()

  // Map of input identifier to resolved [[Input]].
  private val resolvedInputsHashMap = new ConcurrentHashMap[TableIdentifier, Input]()

  // Destination identifier to boolean indicating whether the destination has been resolved
  val resolvedFlowDestinationsMap = new ConcurrentHashMap[TableIdentifier, Boolean]()

  // Map & queue of resolved flows identifiers
  // queue is there to track the topological order while map is used to store the id -> flow
  // mapping
  val resolvedFlowsMap = new ConcurrentHashMap[TableIdentifier, ResolvedFlow]()
  val resolvedFlowsQueue = new ConcurrentLinkedQueue[ResolvedFlow]()

  // List of resolved tables, sinks and flows
  val resolvedTables = new ConcurrentLinkedQueue[Table]()
  val resolvedViews = new ConcurrentLinkedQueue[View]()
  val resolvedSinks = new ConcurrentLinkedQueue[Sink]()

  // Flows that failed due to the client not yet having registered a plan. Keyed by flow identifier.
  val failedUnregisteredFlows = new ConcurrentHashMap[TableIdentifier, ResolutionFailedFlow]()

  // Dataset identifier to list of flows that failed resolution due to missing this dataset
  val failedDependentFlows = new ConcurrentHashMap[TableIdentifier, Seq[ResolutionFailedFlow]]()
  val failedFlowsQueue = new ConcurrentLinkedQueue[ResolutionFailedFlow]()

  // Queue of flow identifiers that have had their upstream inputs resolved and should have their
  // flow function retried on the client
  val flowClientSignalQueue = new ConcurrentLinkedQueue[TableIdentifier]()

  def putResolvedInput(input: Input): Unit = {
    resolvedInputsHashMap.put(input.identifier, input)
  }

  def resolvedInputsByIdentifier: Map[TableIdentifier, Input] = resolvedInputsHashMap.asScala.toMap

  def registerFailedDependentFlow(
      inputDatasetIdentifier: TableIdentifier,
      failedFlow: ResolutionFailedFlow): Unit = {
    failedDependentFlows.compute(
      inputDatasetIdentifier,
      (_, flows) => {
        if (flows == null) {
          Seq(failedFlow)
        } else {
          flows :+ failedFlow
        }
      }
    )
  }

  def markFlowPlanRegistered(flowIdentifier: TableIdentifier): Unit = {
    // scalastyle:off println
    println(s"INSTRUMENTATION: markFlowPlanRegistered - looking for flow $flowIdentifier")
    println(s"INSTRUMENTATION: failedUnregisteredFlows size: ${failedUnregisteredFlows.size()}")
    // scalastyle:on println
    val flow = failedUnregisteredFlows.remove(flowIdentifier)
    // Flow could be null if it failed on the client side and we never tried to execute the flow
    // function on the server side.
    if (flow != null) {
      // scalastyle:off println
      println(s"INSTRUMENTATION: Found flow $flowIdentifier, adding to toBeResolvedFlows queue")
      // scalastyle:on println
      toBeResolvedFlows.addFirst(flow)
    } else {
      // scalastyle:off println
      println(s"INSTRUMENTATION: Flow $flowIdentifier not found in failedUnregisteredFlows")
      // scalastyle:on println
    }
  }

  def analyze(
      flowIdentifier: TableIdentifier,
      logicalPlan: LogicalPlan,
      unresolvedGraph: DataflowGraph,
      session: SparkSession): DataFrame = {
    val unresolvedFlow = unresolvedGraph.flow(flowIdentifier).asInstanceOf[UnresolvedFlow]
    val flowAnalysisContext = FlowAnalysisContext(
      allInputs = unresolvedGraph.inputIdentifiers,
      availableInputs = resolvedInputsByIdentifier.values.toSeq,
      queryContext = unresolvedFlow.queryContext,
      spark = session
    )

    try {
      FlowAnalysis.analyze(flowAnalysisContext, plan = logicalPlan)
    } catch {
      case e: UnresolvedDatasetException =>
        val flowFunctionResult =
          FlowFunctionResult.fromFlowAnalysisContext(flowAnalysisContext, Failure(e), Map.empty)

        val resolutionFailedFlow = new ResolutionFailedFlow(unresolvedFlow, flowFunctionResult)
        registerFailedDependentFlow(e.identifier, resolutionFailedFlow)
        throw e
    }
  }
}
