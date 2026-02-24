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

sealed trait RunTerminationReason {

  /** Terminal state for the given run. */
  def terminalState: RunState

  /**
   * User visible message associated with run termination. This will also be set as the message
   * in the associated terminal run progress log.
   */
  def message: String

  /**
   * Exception associated with the given run termination. This exception will be
   * included in the error details in the associated terminal run progress event.
   */
  def cause: Option[Throwable]
}

/**
 * Helper exception class that indicates that a run has to be terminated and
 * tracks the associated termination reason.
 */
case class RunTerminationException(reason: RunTerminationReason) extends Exception

// ===============================================================
// ============ Graceful run termination states ==================
// ===============================================================

/** Indicates that a triggered run has successfully completed execution. */
case class RunCompletion() extends RunTerminationReason {
  override def terminalState: RunState = RunState.COMPLETED
  override def message: String = s"Run is $terminalState."
  override def cause: Option[Throwable] = None
}

// ===============================================================
// ======================= Run failures ==========================
// ===============================================================

/** Indicates that an run entered the failed state.. */
abstract sealed class RunFailure extends RunTerminationReason {

  /** Whether or not this failure is considered fatal / irrecoverable. */
  def isFatal: Boolean

  override def terminalState: RunState = RunState.FAILED
}

/** Indicates that run has failed due to a query execution failure. */
case class QueryExecutionFailure(
    flowName: String,
    maxRetries: Int,
    override val cause: Option[Throwable])
    extends RunFailure {
  override def isFatal: Boolean = false

  override def message: String =
    if (maxRetries == 0) {
      s"Run is $terminalState since flow '$flowName' has failed."
    } else {
      s"Run is $terminalState since flow '$flowName' has failed more " +
      s"than $maxRetries times."
    }
}

/** Abstract class used to identify failures related to failures stopping an operation/timeouts. */
abstract class FailureStoppingOperation extends RunFailure {

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
      s"Run is $terminalState since following flows have failed to stop: " +
      s"$flowNamesToPrint."
    } else {
      s"Run is $terminalState since stopping flow execution has failed."
    }
  }
  override def cause: Option[Throwable] = None
}

/**
 * Run could not be associated with a proper root cause.
 * This is not expected and likely indicates a bug.
 */
case class UnexpectedRunFailure() extends RunFailure {
  override def isFatal: Boolean = false
  override def message: String =
    s"Run $terminalState unexpectedly."
  override def cause: Option[Throwable] = None
}
