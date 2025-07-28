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

import org.apache.spark.sql.pipelines.common.RunState
import org.apache.spark.sql.pipelines.logging.{
  ConstructPipelineEvent,
  EventLevel,
  PipelineEvent,
  PipelineEventOrigin,
  RunProgress
}

/**
 * Executes a [[DataflowGraph]] by resolving the graph, materializing datasets, and running the
 * flows.
 *
 * @param context The context for this pipeline update.
 */
class PipelineExecution(context: PipelineUpdateContext) {

  /** [Visible for testing] */
  private[pipelines] var graphExecution: Option[TriggeredGraphExecution] = None

  def executionStarted: Boolean = synchronized { graphExecution.nonEmpty }

  /**
   * Starts the pipeline execution by initializing the graph and starting the graph execution
   * thread. This function does not block on the completion of the graph execution thread.
   */
  def startPipeline(): Unit = synchronized {
    // Initialize the graph.
    val resolvedGraph = resolveGraph()
    val initializedGraph = DatasetManager.materializeDatasets(resolvedGraph, context)

    // Execute the graph.
    graphExecution = Option(
      new TriggeredGraphExecution(initializedGraph, context, onCompletion = terminationReason => {
        context.eventCallback(constructTerminationEvent(terminationReason))
      })
    )
    graphExecution.foreach(_.start())
  }

  /** Starts pipeline execution and waits for it to complete before returning. */
  def runPipeline(): Unit = synchronized {
    try {
      startPipeline()
      context.pipelineExecution.awaitCompletion()
    } catch {
      case e: Throwable =>
        context.eventCallback(
          ConstructPipelineEvent(
            origin = PipelineEventOrigin(
              flowName = None,
              datasetName = None,
              sourceCodeLocation = None
            ),
            message = "Pipeline execution failed.",
            details = RunProgress(RunState.FAILED),
            exception = Option(e),
            level = EventLevel.ERROR
          )
        )
    }
  }

  /** Validates that the pipeline graph can be successfully resolved and validates it. */
  def dryRunPipeline(): Unit = synchronized {
    resolveGraph()
    context.eventCallback(
      constructTerminationEvent(RunCompletion())
    )
  }

  private def constructTerminationEvent(
      terminationReason: RunTerminationReason
  ): PipelineEvent = {
    ConstructPipelineEvent(
      origin = PipelineEventOrigin(
        flowName = None,
        datasetName = None,
        sourceCodeLocation = None
      ),
      level = EventLevel.INFO,
      message = terminationReason.message,
      details = RunProgress(terminationReason.terminalState),
      exception = terminationReason.cause
    )
  }

  private def resolveGraph(): DataflowGraph = {
    try {
      context.unresolvedGraph.resolve().validate()
    } catch {
      case e: UnresolvedPipelineException =>
        handleInvalidPipeline(e)
        throw e
    }
  }

  /** Waits for the execution to complete. Only used in tests */
  private[sql] def awaitCompletion(): Unit = {
    graphExecution.foreach(_.awaitCompletion())
  }

  /**
   * Emits FlowProgress.FAILED events for each flow that failed to resolve. Downstream flow failures
   * (flows that failed to resolve when reading from other flows that also failed to resolve) are
   * written to the event log first at WARN level, while upstream flow failures which are expected
   * to be "real" failures are written at ERROR level and come afterwards. This makes the real
   * errors show up first in the UI.
   *
   * @param e The exception that was raised while executing a stage
   */
  private def handleInvalidPipeline(e: UnresolvedPipelineException): Unit = {
    e.downstreamFailures.foreach { failure =>
      val (flowIdentifier, ex) = failure
      val flow = e.graph.resolutionFailedFlow(flowIdentifier)
      context.flowProgressEventLogger.recordFailed(
        flow = flow,
        exception = ex,
        logAsWarn = true,
        messageOpt = Option(
          s"Failed to resolve flow due to upstream failure: '${flow.displayName}'."
        )
      )
    }
    e.directFailures.foreach { failure =>
      val (flowIdentifier, ex) = failure
      val flow = e.graph.resolutionFailedFlow(flowIdentifier)
      context.flowProgressEventLogger.recordFailed(
        flow = flow,
        exception = ex,
        logAsWarn = true,
        messageOpt = Option(s"Failed to resolve flow: '${flow.displayName}'.")
      )
    }
  }

  /**
   * Stops the pipeline execution by stopping the graph execution thread.
   */
  def stopPipeline(): Unit = synchronized {
    graphExecution
      .getOrElse(
        throw new IllegalStateException(
          "Pipeline execution has not started yet."
        )
      )
      .stop()
  }
}
