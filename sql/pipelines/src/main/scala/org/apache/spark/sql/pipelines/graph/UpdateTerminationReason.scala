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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.common.RunState

sealed trait UpdateTerminationReason {

  /** Terminal state for the given update. */
  def terminalState: RunState

  /**
   * User visible message associated with update termination. This will also be set as the message
   * in the associated terminal update progress log.
   */
  def message: String

  /**
   * Exception associated with the given update termination. This exception will be
   * included in the error details in the associated terminal update progress event.
   */
  def cause: Option[Throwable]

  /**
   * Whether this termination reason should override the partial execution failure thrown during
   * flow execution.
   */
  def overridesPartialExecutionFailure: Boolean = false
}

/**
 * Helper exception class that indicates that an update has to be terminated and
 * tracks the associated termination reason.
 */
case class UpdateTerminationException(reason: UpdateTerminationReason) extends Exception

// ===============================================================
// ============ Graceful update termination states ===============
// ===============================================================

/** Indicates that a triggered update has successfully completed execution. */
case class UpdateCompletion() extends UpdateTerminationReason {
  override def terminalState: RunState = RunState.COMPLETED
  override def message: String = s"Update is $terminalState."
  override def cause: Option[Throwable] = None
}

/**
 * Indicates that the update is being terminated since the schema for a given flow
 * changed during execution.
 */
case class UpdateSchemaChange(flowName: String, override val cause: Option[Throwable])
    extends UpdateTerminationReason {
  override def terminalState: RunState = RunState.CANCELED
  override def message: String =
    s"Update has been cancelled due to a schema change in $flowName, " +
    s"and will be restarted."

  override def overridesPartialExecutionFailure: Boolean = true
}

// ===============================================================
// ======================= Update failures =======================
// ===============================================================

/** Indicates that an update entered the failed state.. */
abstract sealed class UpdateFailure extends UpdateTerminationReason {

  /** Whether or not this failure is considered fatal / irrecoverable. */
  def isFatal: Boolean

  override def terminalState: RunState = RunState.FAILED
}

/** Indicates that update has failed due to a query execution failure. */
case class QueryExecutionFailure(
    flowName: String,
    maxRetries: Int,
    override val cause: Option[Throwable])
    extends UpdateFailure {
  override def isFatal: Boolean = false

  override def message: String =
    if (maxRetries == 0) {
      s"Update is $terminalState since flow '$flowName' has failed."
    } else {
      s"Update is $terminalState since flow '$flowName' has failed more " +
      s"than $maxRetries times."
    }
}

/** Abstract class used to identify failures related to failures stopping an operation/timeouts. */
abstract class FailureStoppingOperation extends UpdateFailure {

  /** Name of the operation that failed to stop. */
  def operation: String
}

/** Indicates that there was a failure while stopping the flow. */
case class FailureStoppingFlow(flowIdentifiers: Seq[TableIdentifier])
    extends FailureStoppingOperation {
  override def isFatal: Boolean = false
  override def operation: String = "flow execution"
  override def message: String = {
    if (flowIdentifiers.nonEmpty) {
      val flowNamesToPrint = flowIdentifiers.map(_.toString).sorted.take(5).mkString(", ")
      s"Update is $terminalState since following flows have failed to stop: " +
      s"$flowNamesToPrint."
    } else {
      s"Update is $terminalState since stopping flow execution has failed."
    }
  }
  override def cause: Option[Throwable] = None
}

/**
 * Update could not be associated with a proper root cause.
 * This is not expected and likely indicates a bug.
 */
case class UnexpectedUpdateFailure() extends UpdateFailure {
  override def isFatal: Boolean = false
  override def message: String =
    s"Update $terminalState unexpectedly."
  override def cause: Option[Throwable] = None
}
