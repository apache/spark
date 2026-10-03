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

import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.udf.worker.{ExecutionMetrics, UDFWorkerSpecification}
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

  protected val externalUdfMetrics: Map[String, SQLMetric] =
    ExternalUDFMetrics.create(sparkContext)

  override lazy val metrics: Map[String, SQLMetric] = externalUdfMetrics

  // ---------------------------------------------------------------------------
  // Session lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a [[WorkerSession]] with [[createUDFWorkerSession]] and finalizes it
   * on task completion (which fires on both success and failure).
   * [[WorkerSession#close]] is the single finalizer: it fetches the
   * `FinishResponse` if processing completed, or cancels anything still in
   * flight and waits for the `CancelResponse`. For the UDF sessions used here,
   * exhausting the data iterator completes execution and surfaces execution or
   * finish errors. Therefore, `close` cleans up the session, and the termination
   * it returns is used only for metrics; it does not determine whether the Spark
   * task succeeds. The provided function receives the session and must return the
   * result iterator. It may use the session but MUST NOT close it.
   */
  protected def withUDFWorkerSession(
      taskContext: TaskContext,
      securityScope: Option[WorkerSecurityScope] = None)(
      f: WorkerSession => Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    val session = createUDFWorkerSession(securityScope)

    // Finalize the session when the task ends. The completion listener fires on
    // both success and failure, and close() is the single finalizer that
    // resolves to whichever terminator the stream reached:
    //
    //  - Task completed and the result iterator was fully consumed: process()
    //    sent Finish (input exhausted), drained the output, and surfaced any
    //    execution or finish error. close() observes the settled termination and
    //    finishes cleaning up the request side.
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
    // For these UDF sessions, exhausting the data iterator covers execution and
    // surfaces its errors. close() cleans up protocol state and releases or
    // invalidates the worker handle; a clean terminal response also carries the
    // execution's final or partial metrics.
    //
    taskContext.addTaskCompletionListener[Unit] { _ =>
      recordTerminalMetrics(session.close())
    }

    f(session)
  }

  protected def createUDFWorkerSession(
      securityScope: Option[WorkerSecurityScope]): WorkerSession = {
    SparkEnv.get.getExternalUDFDispatcher(workerSpec).createSession(securityScope)
  }

  private def recordTerminalMetrics(termination: Termination): Unit = {
    val reported = termination match {
      case Termination.Finished(response) if response.hasExecutionMetrics =>
        Some(response.getExecutionMetrics)
      case Termination.Cancelled(response) if response.hasExecutionMetrics =>
        Some(response.getExecutionMetrics)
      case _ => None
    }
    reported.foreach(ExternalUDFMetrics.update(metrics, _))
  }
}

private[externalUDF] object ExternalUDFMetrics {
  private val sizeMetrics = Map(
    "bytesIn" -> "data received by the external UDF worker",
    "bytesOut" -> "data returned by the external UDF worker")

  private val countMetrics = Map(
    "rowsIn" -> "rows received by the external UDF worker",
    "rowsOut" -> "rows returned by the external UDF worker",
    "batchesIn" -> "batches received by the external UDF worker",
    "batchesOut" -> "batches returned by the external UDF worker")

  private val timingMetrics = Map(
    "initWallNanos" -> "external UDF worker initialization time",
    "processingWallNanos" -> "external UDF worker processing time",
    "receiveWallNanos" -> "time the external UDF worker waited for requests",
    "sendWallNanos" -> "time the external UDF worker was blocked sending responses",
    "workWallNanos" -> "external UDF worker execution time",
    "workCpuNanos" -> "external UDF worker CPU time",
    "finishWallNanos" -> "external UDF worker finish time")

  def create(sc: SparkContext): Map[String, SQLMetric] = {
    sizeMetrics.map { case (name, description) =>
      name -> SQLMetrics.createSizeMetric(sc, description)
    } ++ countMetrics.map { case (name, description) =>
      name -> SQLMetrics.createMetric(sc, description)
    } ++ timingMetrics.map { case (name, description) =>
      name -> SQLMetrics.createNanoTimingMetric(sc, description)
    }
  }

  def update(target: Map[String, SQLMetric], reported: ExecutionMetrics): Unit = {
    def updateIfPresent(name: String, present: Boolean, value: => Long): Unit = {
      if (present) {
        target(name) += value
      }
    }

    updateIfPresent("bytesIn", reported.hasBytesIn, reported.getBytesIn)
    updateIfPresent("bytesOut", reported.hasBytesOut, reported.getBytesOut)
    updateIfPresent("rowsIn", reported.hasRowsIn, reported.getRowsIn)
    updateIfPresent("rowsOut", reported.hasRowsOut, reported.getRowsOut)
    updateIfPresent("batchesIn", reported.hasBatchesIn, reported.getBatchesIn)
    updateIfPresent("batchesOut", reported.hasBatchesOut, reported.getBatchesOut)
    updateIfPresent("initWallNanos", reported.hasInitWallNanos, reported.getInitWallNanos)
    updateIfPresent(
      "processingWallNanos", reported.hasProcessingWallNanos, reported.getProcessingWallNanos)
    updateIfPresent("receiveWallNanos", reported.hasReceiveWallNanos, reported.getReceiveWallNanos)
    updateIfPresent("sendWallNanos", reported.hasSendWallNanos, reported.getSendWallNanos)
    updateIfPresent("workWallNanos", reported.hasWorkWallNanos, reported.getWorkWallNanos)
    updateIfPresent("workCpuNanos", reported.hasWorkCpuNanos, reported.getWorkCpuNanos)
    updateIfPresent("finishWallNanos", reported.hasFinishWallNanos, reported.getFinishWallNanos)
  }
}
