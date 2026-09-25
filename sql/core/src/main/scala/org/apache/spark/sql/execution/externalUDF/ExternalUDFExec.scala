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

package org.apache.spark.sql.execution.externalUDF

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.udf.worker.{ExecutionError, UDFWorkerSpecification}
import org.apache.spark.udf.worker.core.{Termination, WorkerSecurityScope, WorkerSession}

/**
 * :: Experimental ::
 * Base trait for physical plan nodes that execute UDFs in an external
 * worker process via the language-agnostic UDF worker framework.
 *
 * Dispatchers are obtained via [[SparkEnv#getExternalUDFDispatcher]],
 * which uses the [[UDFDispatcherManager]] registered on the
 * environment. This avoids serializing the manager as part of the
 * physical plan.
 */
@Experimental
trait ExternalUDFExec extends UnaryExecNode {

  /**
   * Specification describing how to create and communicate with the UDF worker.
   * There is exactly one specification per [[ExternalUDFExec]] node.
   */
  def workerSpec: UDFWorkerSpecification

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  protected def externalUdfMetrics: Map[String, SQLMetric] = Map(
    // TODO [SPARK-57324]: Emit the correct metrics here
  )

  override lazy val metrics: Map[String, SQLMetric] = externalUdfMetrics

  // ---------------------------------------------------------------------------
  // Session lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a [[WorkerSession]] via [[SparkEnv#getExternalUDFDispatcher]].
   * Finalizes the session on task completion (which fires on both success and
   * failure). [[WorkerSession#close]] is the single finalizer: it fetches the
   * `FinishResponse` if processing completed, or cancels anything still in
   * flight and waits for the `CancelResponse`. The provided function receives
   * the session and must return the result iterator. It may use the session
   * but MUST NOT close it.
   */
  protected def withUDFWorkerSession(
      taskContext: TaskContext,
      securityScope: Option[WorkerSecurityScope] = None)(
      f: WorkerSession => Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    val dispatcher = SparkEnv.get.getExternalUDFDispatcher(
      workerSpec)
    val session = dispatcher.createSession(securityScope)

    // Finalize the session when the task ends. The completion listener fires on
    // both success and failure, and close() is the single finalizer that
    // resolves to whichever terminator the stream reached:
    //
    //  - Task completed and the result iterator was fully consumed: process()
    //    sent Finish (input exhausted) and the data drained. close() then
    //    typically returns the Finished termination -- but a failure during the
    //    finish/cleanup phase (raised after the data drained, so it reached no one
    //    through the iterator) is surfaced from the completion listener below.
    //  - Task failed, was killed, or stopped before draining (e.g. a downstream
    //    LIMIT or exception): the stream has not finished, so close() sends a
    //    Cancel, the worker runs its cleanup, and its CancelResponse is returned
    //    as a Cancelled termination. An empty Cancel is enough here -- there is
    //    no extra information to convey to the worker on cancellation -- so we
    //    rely on close()'s default and pass no cancel thunk.
    //  - The stream died without a terminator (transport failure / timeout):
    //    close() returns a best-effort TransportFailed termination rather than
    //    raising it (a thread interrupt may still propagate); the underlying
    //    failure has already surfaced through the result iterator.
    //
    registerWorkerSessionCompletionListener(taskContext, session)

    f(session)
  }

  /** Registers the single session finalizer and surfaces close-only worker failures. */
  protected final def registerWorkerSessionCompletionListener(
      taskContext: TaskContext,
      session: WorkerSession): Unit = {
    taskContext.addTaskCompletionListener[Unit] { context =>
      val termination = session.close()
      // Preserve an existing task failure as the primary error. A data-phase worker failure is
      // already surfaced by the result iterator; only a close-only failure needs to fail an
      // otherwise successful task here. SPARK-57324 tracks consuming the remaining termination
      // data, including metrics and callback payloads.
      if (context.getTaskFailure.isEmpty) {
        throwOnFailedTermination(termination)
      }
    }
  }

  private def throwOnFailedTermination(termination: Termination): Unit = termination match {
    case Termination.Finished(response) if response.hasError =>
      throw executionError("finish callback", response.getError)
    case Termination.Cancelled(response) if response.hasError =>
      throw executionError("cancel callback", response.getError)
    case Termination.Failed(error) =>
      throw executionError("session finalization", error)
    case Termination.TransportFailed(cause) =>
      throw SparkException.internalError(
        "External UDF worker transport failed during session finalization.", cause)
    case Termination.Interrupted(cause) =>
      throw SparkException.internalError(
        "External UDF worker session finalization was interrupted.", cause)
    case _ =>
  }

  private def executionError(phase: String, error: ExecutionError): SparkException = {
    SparkException.internalError(
      s"External UDF worker $phase failed: ${describeExecutionError(error)}")
  }

  private def describeExecutionError(error: ExecutionError): String = error.getKindCase match {
    case ExecutionError.KindCase.USER =>
      val userError = error.getUser
      val errorClass = if (userError.hasErrorClass) s"[${userError.getErrorClass}] " else ""
      s"$errorClass${userError.getMessage}"
    case ExecutionError.KindCase.WORKER =>
      s"WorkerError: ${error.getWorker.getMessage}"
    case ExecutionError.KindCase.PROTOCOL =>
      s"ProtocolError: ${error.getProtocol.getMessage}"
    case ExecutionError.KindCase.KIND_NOT_SET =>
      "ExecutionError without kind"
  }
}
