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

package org.apache.spark.sql.pipelines.utils

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.pipelines.common.{FlowStatus, RunState}
import org.apache.spark.sql.pipelines.graph.{
  AllFlows,
  AllTables,
  DataflowGraph,
  FlowFilter,
  NoTables,
  PipelineUpdateContext,
  TableFilter
}
import org.apache.spark.sql.pipelines.logging.{
  EventLevel,
  FlowProgress,
  FlowProgressEventLogger,
  PipelineEvent,
  RunProgress
}

trait ExecutionTest
    extends PipelineTest
    with TestPipelineUpdateContextMixin
    with EventVerificationTestHelpers

trait TestPipelineUpdateContextMixin {

  /**
   * A test implementation of the PipelineUpdateContext trait.
   * @param spark The Spark session to use.
   * @param unresolvedGraph The unresolved dataflow graph.
   * @param fullRefreshTables Set of tables to be fully refreshed.
   * @param refreshTables  Set of tables to be refreshed.
   * @param resetCheckpointFlows Set of flows to be reset.
   */
  case class TestPipelineUpdateContext(
      spark: SparkSession,
      unresolvedGraph: DataflowGraph,
      fullRefreshTables: TableFilter = NoTables,
      refreshTables: TableFilter = AllTables,
      resetCheckpointFlows: FlowFilter = AllFlows
  ) extends PipelineUpdateContext {
    val eventBuffer = new PipelineRunEventBuffer()

    override val eventCallback: PipelineEvent => Unit = eventBuffer.addEvent

    override def flowProgressEventLogger: FlowProgressEventLogger = {
      new FlowProgressEventLogger(eventCallback = eventCallback)
    }
  }
}

trait EventVerificationTestHelpers {

  /**
   * Asserts that there is a FlowProgress event in the event log with the specified flow name
   * and status and matching the specified error condition.
   *
   * @param identifier Flow identifier to look for events for
   * @param expectedFlowStatus Expected FlowStatus
   * @param expectedEventLevel Expected EventLevel of the event.
   * @param errorChecker Condition that the event's exception, if any, must pass in order for this
   *                     function to return true.
   * @param msgChecker Condition that the event's msg must pass in order for the function to return
   *                   true.
   * @param cond Predicate to filter the flow progress events by. Useful for more complex event
   *             verification.
   */
  protected def assertFlowProgressEvent(
      eventBuffer: PipelineRunEventBuffer,
      identifier: TableIdentifier,
      expectedFlowStatus: FlowStatus,
      expectedEventLevel: EventLevel,
      errorChecker: Throwable => Boolean = _ => true,
      msgChecker: String => Boolean = _ => true,
      cond: PipelineEvent => Boolean = _ => true,
      expectedNumOfEvents: Option[Int] = None
  ): Unit = {
    // Get all events for the flow. This list is logged if the assertion
    // fails to help with debugging. Only minimal filtering is done here
    // so we have a complete list of events to look at for debugging.
    val flowEvents = eventBuffer.getEvents.filter(_.details.isInstanceOf[FlowProgress]).filter {
      event =>
        event.origin.flowName == Option(identifier.unquotedString)
    }

    var matchingEvents = flowEvents.filter(e => msgChecker(e.message))
    matchingEvents = matchingEvents
      .filter {
        _.details.isInstanceOf[FlowProgress]
      }
      .filter {
        cond
      }
      .filter { e =>
        e.details.asInstanceOf[FlowProgress].status == expectedFlowStatus &&
        (e.error.isEmpty || errorChecker(e.error.get))
      }

    assert(
      matchingEvents.nonEmpty,
      s"Could not find a matching event for $identifier. Logs for $identifier are $flowEvents"
    )
    assert(
      expectedNumOfEvents.forall(_ == matchingEvents.size),
      s"Found ${matchingEvents.size} events for $identifier but expected " +
      s"$expectedNumOfEvents events. Logs for $identifier are $flowEvents"
    )
  }

  /**
   * Asserts emitted flow progress event logs for a given flow have the expected sequence of
   * event levels and flow statuses in the expected order.
   *
   * @param eventBuffer The event buffer containing the events.
   * @param identifier The identifier of the flow to check.
   * @param expectedFlowProgressStatus A sequence of tuples containing the expected event level
   *                                   and flow status in the expected order.
   */
  protected def assertFlowProgressStatusInOrder(
      eventBuffer: PipelineRunEventBuffer,
      identifier: TableIdentifier,
      expectedFlowProgressStatus: Seq[(EventLevel, FlowStatus)]
  ): Unit = {
    // Get all events for the flow. This list is logged if the assertion
    // fails to help with debugging. Only minimal filtering is done here
    // so we have a complete list of events to look at for debugging.
    val actualFlowProgressStatus = eventBuffer.getEvents
      .filter(_.details.isInstanceOf[FlowProgress])
      .filter { event =>
        event.origin.flowName == Option(identifier.unquotedString)
      }
      .map { event =>
        val flowProgress = event.details.asInstanceOf[FlowProgress]
        (event.level, flowProgress.status)
      }

    assert(
      actualFlowProgressStatus == expectedFlowProgressStatus,
      s"Expected flow progress status for $identifier to be " +
      s"$expectedFlowProgressStatus but got $actualFlowProgressStatus. " +
      s"Logs for $identifier are " +
      s"${eventBuffer.getEvents.filter(_.origin.flowName == Option(identifier.unquotedString))}. " +
      s"All events in the buffer are ${eventBuffer.getEvents.mkString("\n")}"
    )
  }

  /**
   * Asserts that there is no `FlowProgress` event
   * in the event log with the specified flow name and status and metrics checkers.
   */
  protected def assertNoFlowProgressEvent(
      eventBuffer: PipelineRunEventBuffer,
      identifier: TableIdentifier,
      flowStatus: FlowStatus
  ): Unit = {
    val flowEvents = eventBuffer.getEvents
      .filter(_.details.isInstanceOf[FlowProgress])
      .filter(_.origin.flowName == Option(identifier.unquotedString))
    assert(
      !flowEvents.filter(_.details.isInstanceOf[FlowProgress]).exists { e =>
        e.details.asInstanceOf[FlowProgress].status == flowStatus
      },
      s"Found a matching event for flow $identifier. Logs for $identifier are $flowEvents"
    )
  }

  /** Returns a map of flow names to their latest FlowStatus. */
  protected def latestFlowStatuses(eventBuffer: PipelineRunEventBuffer): Map[String, FlowStatus] = {
    eventBuffer.getEvents
      .filter(_.details.isInstanceOf[FlowProgress])
      .groupBy(_.origin.flowName.get)
      .view
      .mapValues { events: Seq[PipelineEvent] =>
        events.reverse
          .map(_.details)
          .collectFirst { case fp: FlowProgress => fp.status }
      }
      .collect { case (k, Some(v)) => k -> v }
      .toMap
  }

  /**
   * Asserts that there is a planning event in the event log with the specified flow name.
   */
  protected def assertPlanningEvent(
      eventBuffer: PipelineRunEventBuffer,
      identifier: TableIdentifier
  ): Unit = {
    val flowEventLogName = identifier.unquotedString
    val expectedPlanningMessage = s"Flow '$flowEventLogName' is PLANNING."
    val foundPlanningEvent = eventBuffer.getEvents
      .filter { e =>
        val matchName = e.origin.flowName.contains(flowEventLogName)
        val matchDetails = e.details match {
          case fp: FlowProgress => fp.status == FlowStatus.PLANNING
          case _ => false
        }
        matchName && matchDetails
      }
    assert(
      foundPlanningEvent.nonEmpty &&
      foundPlanningEvent.head.message.contains(expectedPlanningMessage),
      s"Planning event not found for flow $flowEventLogName"
    )
  }

  /**
   * Asserts that there is a RunProgress event in the event log with the specified id
   * and state and matching the specified error condition.
   *
   * @param state        Expected RunState
   * @param expectedEventLevel Expected EventLevel of the event.
   * @param errorChecker Condition that the event's exception, if any, must pass in order for this
   *                     function to return true.
   * @param msgChecker   Condition that the event's msg must pass in order for the function to
   *                     return true.
   */
  protected def assertRunProgressEvent(
      eventBuffer: PipelineRunEventBuffer,
      state: RunState,
      expectedEventLevel: EventLevel,
      errorChecker: Option[Throwable] => Boolean = null,
      msgChecker: String => Boolean = _ => true): Unit = {
    val errorCheckerWithDefault = Option(errorChecker).getOrElse {
      if (state == RunState.FAILED) { (errorOpt: Option[Throwable]) =>
        errorOpt.nonEmpty
      } else { (errorOpt: Option[Throwable]) =>
        errorOpt.isEmpty
      }
    }

    val runEvents = eventBuffer.getEvents.filter(_.details.isInstanceOf[RunProgress])
    val expectedEvent = eventBuffer.getEvents.find { e =>
      val matchingState = e.details match {
        case RunProgress(up) => up == state
        case _ => false
      }
      matchingState && errorCheckerWithDefault(e.error) && msgChecker(e.message)
    }
    assert(
      expectedEvent.isDefined,
      s"Could not find a matching event with run state $state. " +
      s"Logs are $runEvents"
    )
  }
}
