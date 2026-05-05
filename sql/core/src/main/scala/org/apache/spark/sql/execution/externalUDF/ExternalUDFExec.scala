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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{WorkerSecurityScope, WorkerSession}

/**
 * :: Experimental ::
 * Base trait for physical plan nodes that execute UDFs in an external
 * worker process via the language-agnostic UDF worker framework.
 *
 * Sessions are created via [[SparkEnv#createExternalUDFSession]],
 * which uses the [[UDFWorkerManager]] registered on the
 * environment. This avoids serializing the factory as part of the
 * physical plan.
 */
@Experimental
trait ExternalUDFExec extends UnaryExecNode {

  /** Specification describing how to create and communicate with the UDF worker. */
  def workerSpec: UDFWorkerSpecification

  /** Output attributes produced by this UDF execution. */
  def resultAttributes: Seq[Attribute]

  override def output: Seq[Attribute] = resultAttributes

  override def producedAttributes: AttributeSet =
    AttributeSet(resultAttributes)

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  protected def externalUdfMetrics: Map[String, SQLMetric] = Map(
    // TODO [SPARK-55278]: Emit the correct metrics here
  )

  override lazy val metrics: Map[String, SQLMetric] = externalUdfMetrics

  // ---------------------------------------------------------------------------
  // Session lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Creates a [[WorkerSession]] via [[SparkEnv#createExternalUDFSession]]
   * and registers cancellation on task failure. The provided function
   * receives the session and must return the result iterator. Moreover,
   * the function MUST close the session once all input data has been sent.
   */
  protected def withUDFWorkerSession(
      taskContext: TaskContext,
      securityScope: Option[WorkerSecurityScope] = None)(
      f: WorkerSession => Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    val session = SparkEnv.get.createExternalUDFSession(
      workerSpec, securityScope)

    logInfo(s"Task ${taskContext.taskAttemptId()} bound to" +
      s" UDF worker session ${session.sessionId}")

    // Make sure to cancel the session, if the task fails
    taskContext.addTaskFailureListener { (_, _) =>
      session.cancel()
    }

    f(session)
  }
}
