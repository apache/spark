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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Exception raised when a flow tries to read from a dataset that exists but is unresolved
 *
 * @param identifier The identifier of the dataset
 */
case class UnresolvedDatasetException(identifier: TableIdentifier)
    extends AnalysisException(
      s"Failed to read dataset '${identifier.unquotedString}'. Dataset is defined in the " +
      s"pipeline but could not be resolved."
    )

/**
 * Exception raised when a flow fails to read from a table defined within the pipeline
 *
 * @param name The name of the table
 * @param cause The cause of the failure
 */
case class LoadTableException(name: String, cause: Option[Throwable])
    extends SparkException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Map("message" -> s"Failed to load table '$name'"),
      cause = cause.orNull
    )

object PipelinesErrors extends Logging {

  /**
   * Gets the exception chain for a given exception by repeatedly calling getCause.
   *
   * @param originalErr The error on which getCause is repeatedly called
   * @return An ArrayBuffer containing the original error and all the causes in its exception chain.
   *         For a given exception in the ArrayBuffer, the next element is its cause.
   */
  private def getExceptionChain(originalErr: Throwable): ArrayBuffer[Throwable] = {
    val exceptionChain = ArrayBuffer[Throwable]()
    var lastException = originalErr
    while (lastException != null) {
      exceptionChain += lastException
      lastException = lastException.getCause
    }
    exceptionChain
  }

  /**
   * Checks whether a throwable or any of its nested causes meets some condition
   * @param throwable A Throwable to inspect
   * @param check Function to run on each cause
   * @return Whether or not `throwable` or any of its nested causes satisfy the check
   */
  private def checkCauses(throwable: Throwable, check: Throwable => Boolean): Boolean = {
    getExceptionChain(throwable).exists(check)
  }

  /**
   * Checks an error for streaming specific handling. This is a pretty messy signature as a result
   * of unifying some divergences between the triggered caller in TriggeredGraphExecution and the
   * continuous caller in StreamWatchdog.
   *
   * @param ex the error to check
   * @param env the update context
   * @param graphExecution the graph execution
   * @param flow the resolved logical flow
   * @param shouldRethrow whether to throw an UpdateTerminationException wrapping `ex`. This is set
   *                      to true for ContinuousFlowExecution so we can eagerly stop the execution.
   * @param prevFailureCount the number of failures that have occurred so far
   * @param maxRetries the max retries that were available (whether or not they're exhausted now)
   */
  def checkStreamingErrorsAndRetry(
      ex: Throwable,
      env: PipelineUpdateContext,
      graphExecution: GraphExecution,
      flow: ResolvedFlow,
      shouldRethrow: Boolean,
      prevFailureCount: Int,
      maxRetries: Int,
      onRetry: => Unit
  ): Unit = {
    if (PipelinesErrors.checkCauses(
        throwable = ex,
        check = ex => {
          ex.isInstanceOf[AssertionError] &&
          ex.getMessage != null &&
          ex.getMessage.contains("sources in the checkpoint offsets and now there are") &&
          ex.getMessage.contains("sources requested by the query. Cannot continue.")
        }
      )) {
      val message = s"""
         |Flow '${flow.displayName}' had streaming sources added or removed. Please perform a
         |full refresh in order to rebuild '${flow.displayName}' against the current set of
         |sources.
         |""".stripMargin

      env.flowProgressEventLogger.recordFailed(
        flow = flow,
        exception = ex,
        logAsWarn = false,
        messageOpt = Option(message)
      )
    } else if (flow.once && ex == null) {
      // No need to do anything if this is a ONCE flow with no exception. That just means it's done.
    } else {
      val actionFromError = GraphExecution.determineFlowExecutionActionFromError(
        ex = ex,
        flowDisplayName = flow.displayName,
        currentNumTries = prevFailureCount + 1,
        maxAllowedRetries = maxRetries
      )
      actionFromError match {
        // Simply retry
        case GraphExecution.RetryFlowExecution => onRetry
        // Schema change exception
        case GraphExecution.StopFlowExecution(reason) =>
          val msg = reason.failureMessage
          if (reason.warnInsteadOfError) {
            logWarning(msg, reason.cause)
            env.flowProgressEventLogger.recordStop(
              flow = flow,
              message = Option(msg),
              cause = Option(reason.cause)
            )
          } else {
            logError(reason.failureMessage, reason.cause)
            env.flowProgressEventLogger.recordFailed(
              flow = flow,
              exception = reason.cause,
              logAsWarn = false,
              messageOpt = Option(msg)
            )
          }
          if (shouldRethrow) {
            throw RunTerminationException(reason.runTerminationReason)
          }
      }
    }
  }
}

/**
 * Exception raised when a pipeline has one or more flows that cannot be resolved
 *
 * @param directFailures     Mapping between the name of flows that failed to resolve (due to an
 *                           error in that flow) and the error that occurred when attempting to
 *                           resolve them
 * @param downstreamFailures Mapping between the name of flows that failed to resolve (because they
 *                           failed to read from other unresolved flows) and the error that occurred
 *                           when attempting to resolve them
 */
case class UnresolvedPipelineException(
    graph: DataflowGraph,
    directFailures: Map[TableIdentifier, Throwable],
    downstreamFailures: Map[TableIdentifier, Throwable],
    additionalHint: Option[String] = None)
    extends AnalysisException(
      s"""
       |Failed to resolve flows in the pipeline.
       |
       |A flow can fail to resolve because the flow itself contains errors or because it reads
       |from an upstream flow which failed to resolve.
       |${additionalHint.getOrElse("")}
       |Flows with errors: ${directFailures.keys.map(_.unquotedString).toSeq.sorted.mkString(", ")}
       |Flows that failed due to upstream errors: ${downstreamFailures.keys
           .map(_.unquotedString)
           .toSeq
           .sorted
           .mkString(", ")}
       |
       |To view the exceptions that were raised while resolving these flows, look for flow
       |failures that precede this log.""".stripMargin
    )

/**
 * Raised when there's a circular dependency in the current pipeline. That is, a downstream
 * table is referenced while creating a upstream table.
 */
case class CircularDependencyException(
    downstreamTable: TableIdentifier,
    upstreamDataset: TableIdentifier)
    extends AnalysisException(
      s"The downstream table '${downstreamTable.unquotedString}' is referenced when " +
      s"creating the upstream table or view '${upstreamDataset.unquotedString}'. " +
      s"Circular dependencies are not supported in a pipeline. Please remove the dependency " +
      s"between '${upstreamDataset.unquotedString}' and '${downstreamTable.unquotedString}'."
    )
