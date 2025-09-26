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

package org.apache.spark.sql.pipelines.logging

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.graph.{FlowExecution, ResolutionCompletedFlow, ResolvedFlow}

/**
 * This class should be used for all flow progress events logging, it controls the level at which
 * events are logged. It uses execution mode, flow name and previous flow statuses to infer the
 * level at which an event is to be logged. Below is a more details description of how flow
 * progress events for batch/streaming flows will be logged:
 *
 * For batch & streaming flows in triggered execution mode:
 *  - All flow progress events other than errors/warnings will be logged at INFO level (including
 *    flow progress events with metrics) and error/warning messages will be logged at their level.
 *
 * @param eventCallback Callback to invoke on the flow progress events.
 */
class FlowProgressEventLogger(eventCallback: PipelineEvent => Unit) extends Logging {

  /**
   * This map stores flow identifier to a boolean representing whether flow is running.
   * - For a flow which is queued and has not yet run, there will be no entry present in the map.
   * - For a flow which has started running but failed will have a value of false or will not be
   * present in the map.
   * - Flow which has started running with no failures will have a value of true.
   */
  private val runningFlows = new ConcurrentHashMap[TableIdentifier, Boolean]().asScala

  /** This map stores idle flows, it's a map of flow name to idle status (IDLE|SKIPPED). */
  private val knownIdleFlows = new ConcurrentHashMap[TableIdentifier, FlowStatus]().asScala

  /**
   * Records flow progress events with flow status as QUEUED. This event will always be logged at
   * INFO level, since flows are only queued once.
   */
  def recordQueued(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = s"Flow ${flow.displayName} is QUEUED.",
        details = FlowProgress(FlowStatus.QUEUED)
      )
    )
  }

  /**
   * Records flow progress events with flow status as PLANNING for batch flows.
   */
  def recordPlanningForBatchFlow(batchFlow: ResolvedFlow): Unit = synchronized {
    if (batchFlow.df.isStreaming) return
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(batchFlow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(batchFlow.origin)
        ),
        level = EventLevel.INFO,
        message = s"Flow ${batchFlow.displayName} is PLANNING.",
        details = FlowProgress(FlowStatus.PLANNING)
      )
    )
    knownIdleFlows.remove(batchFlow.identifier)
  }

  /**
   * Records flow progress events with flow status as STARTING. For batch flows in continuous mode,
   * event will be logged at INFO if the recent flow run had failed otherwise the event will be
   * logged at METRICS. All other cases will be logged at INFO.
   */
  def recordStart(flowExecution: FlowExecution): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flowExecution.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flowExecution.getOrigin)
        ),
        level = EventLevel.INFO,
        message = s"Flow ${flowExecution.displayName} is STARTING.",
        details = FlowProgress(FlowStatus.STARTING)
      )
    )
    knownIdleFlows.remove(flowExecution.identifier)
  }

  /** Records flow progress events with flow status as RUNNING. */
  def recordRunning(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = s"Flow ${flow.displayName} is RUNNING.",
        details = FlowProgress(FlowStatus.RUNNING)
      )
    )
    runningFlows.put(flow.identifier, true)
    knownIdleFlows.remove(flow.identifier)
  }

  /**
   * Records flow progress events with failure flow status. By default failed flow progress events
   * are logged at ERROR level, logAsWarn serve as a way to log the event as a WARN.
   */
  def recordFailed(
      flow: ResolutionCompletedFlow,
      exception: Throwable,
      logAsWarn: Boolean,
      messageOpt: Option[String] = None
  ): Unit = synchronized {
    val eventLogMessage = messageOpt.getOrElse(s"Flow '${flow.displayName}' has FAILED.")

    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = if (logAsWarn) EventLevel.WARN else EventLevel.ERROR,
        message = eventLogMessage,
        details = FlowProgress(FlowStatus.FAILED),
        exception = Option(exception)
      )
    )
    // Since the flow failed, remove the flow from runningFlows.
    runningFlows.remove(flow.identifier)
    knownIdleFlows.remove(flow.identifier)
  }

  /**
   * Records flow progress events with flow status as SKIPPED at WARN level, this version of
   * record skipped should be used when the flow is skipped because of upstream flow failures.
   */
  def recordSkippedOnUpStreamFailure(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.WARN,
        message = s"Flow '${flow.displayName}' SKIPPED due to upstream failure(s).",
        details = FlowProgress(FlowStatus.SKIPPED)
      )
    )
    runningFlows.remove(flow.identifier)
    // Even though this is skipped  it is a skipped because of a failure so this is not marked as
    // a idle flow.
    knownIdleFlows.remove(flow.identifier)
  }

  /**
   * Records flow progress events with flow status as SKIPPED. For flows skipped because of
   * upstream failures use [[recordSkippedOnUpStreamFailure]] function.
   */
  def recordSkipped(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = {
          s"Flow '${flow.displayName}' has been processed by a previous iteration " +
          s"and will not be rerun."
        },
        details = FlowProgress(FlowStatus.SKIPPED)
      )
    )
    knownIdleFlows.put(flow.identifier, FlowStatus.SKIPPED)
  }

  /** Records flow progress events with flow status as EXCLUDED at INFO level.  */
  def recordExcluded(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = s"Flow '${flow.displayName}' is EXCLUDED.",
        details = FlowProgress(FlowStatus.EXCLUDED)
      )
    )
    knownIdleFlows.remove(flow.identifier)
  }

  /**
   * Records flow progress events with flow status as STOPPED. This event will always be logged at
   * INFO level, since flows wouldn't run after they are stopped.
   */
  def recordStop(
      flow: ResolvedFlow,
      message: Option[String] = None,
      cause: Option[Throwable] = None
  ): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = message.getOrElse(s"Flow '${flow.displayName}' has STOPPED."),
        details = FlowProgress(FlowStatus.STOPPED),
        exception = cause
      )
    )
    // Once a flow is stopped, remove it from running and idle.
    runningFlows.remove(flow.identifier)
    knownIdleFlows.remove(flow.identifier)
  }

  /** Records flow progress events with flow status as IDLE. */
  def recordIdle(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = s"Flow '${flow.displayName}' is IDLE, waiting for new data.",
        details = FlowProgress(FlowStatus.IDLE)
      )
    )
    knownIdleFlows.put(flow.identifier, FlowStatus.IDLE)
  }

  /**
   * Records flow progress events with flow status as COMPLETED. For batch flows in continuous
   * mode, events will be logged at METRICS since a completed status is always preceded by running
   * status.
   *
   * Note that flow complete events for batch flows are expected to contain quality stats where as
   * for streaming flows quality stats are not expected and hence not added to the flow progress
   * event.
   */
  def recordCompletion(flow: ResolvedFlow): Unit = synchronized {
    eventCallback(
      ConstructPipelineEvent(
        origin = PipelineEventOrigin(
          flowName = Option(flow.displayName),
          datasetName = None,
          sourceCodeLocation = Option(flow.origin)
        ),
        level = EventLevel.INFO,
        message = s"Flow ${flow.displayName} has COMPLETED.",
        details = FlowProgress(FlowStatus.COMPLETED)
      )
    )
    knownIdleFlows.remove(flow.identifier)
  }
}
