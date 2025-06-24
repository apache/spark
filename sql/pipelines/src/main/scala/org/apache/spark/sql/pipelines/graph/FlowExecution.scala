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

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.pipelines.graph.QueryOrigin.ExceptionHelpers
import org.apache.spark.sql.pipelines.util.SparkSessionUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.util.ThreadUtils

/**
 * A flow's execution may complete for two reasons:
 * 1. it may finish performing all of its necessary work, or
 * 2. it may be interrupted by a request from a user to stop it.
 *
 * We use this result to disambiguate these two cases, using 'ExecutionResult.FINISHED'
 * for the former and 'ExecutionResult.STOPPED' for the latter.
 */
sealed trait ExecutionResult
object ExecutionResult {
  case object FINISHED extends ExecutionResult
  case object STOPPED extends ExecutionResult
}

/** A `FlowExecution` specifies how to execute a flow and manages its execution. */
trait FlowExecution {

  /** Identifier of this physical flow */
  def identifier: TableIdentifier

  /**
   * Returns a user-visible name for the flow.
   */
  final def displayName: String = identifier.unquotedString

  /**
   * SparkSession to execute this physical flow with.
   *
   * The default value for streaming flows is the pipeline's spark session because the source
   * dataframe is resolved using the pipeline's spark session, and a new session will be started
   * implicitly by the streaming query.
   *
   * The default value for batch flows is a cloned spark session from the pipeline's spark session.
   *
   * Please make sure that the execution thread runs in a different spark session than the
   * pipeline's spark session.
   */
  protected def spark: SparkSession = updateContext.spark

  /**
   * Origin to use when recording events for this flow.
   */
  def getOrigin: QueryOrigin

  /**
   * Returns true if and only if this `FlowExecution` has been completed with
   * either success or an exception.
   */
  def isCompleted: Boolean = _future.exists(_.isCompleted)

  /** Returns true iff this `FlowExecution` executes using Spark Structured Streaming. */
  def isStreaming: Boolean

  /** Retrieves the future that can be used to track execution status. */
  def getFuture: Future[ExecutionResult] = {
    _future.getOrElse(
      throw new IllegalStateException(s"FlowExecution $identifier has not been executed.")
    )
  }

  /** Tracks the currently running future. */
  private final var _future: Option[Future[ExecutionResult]] = None

  /** Context about this pipeline update. */
  def updateContext: PipelineUpdateContext

  /** The thread execution context for the current `FlowExecution`. */
  implicit val executionContext: ExecutionContext = {
    ExecutionContext.fromExecutor(FlowExecution.threadPool)
  }

  /**
   * Stops execution of this `FlowExecution`. If you override this, please be sure to
   * call `super.stop()` at the beginning of your method, so we can properly handle errors
   * when a user tries to stop a flow.
   */
  def stop(): Unit = {
    stopped.set(true)
  }

  /** Returns an optional exception that occurred during execution, if any. */
  def exception: Option[Throwable] = _future.flatMap(_.value).flatMap(_.failed.toOption)

  /**
   * Executes this FlowExecution synchronously to perform its intended update.
   * This method should be overridden by subclasses to provide the actual execution logic.
   *
   * @return a Future that completes when the execution is finished or stopped.
   */
  def executeInternal(): Future[Unit]

  /**
   * Executes this FlowExecution asynchronously to perform its intended update. A future that can be
   * used to track execution status is saved, and can be retrieved with `getFuture`.
   */
  final def executeAsync(): Unit = {
    if (_future.isDefined) {
      throw new IllegalStateException(
        s"FlowExecution ${identifier.unquotedString} has already been executed."
      )
    }

    val queryOrigin = QueryOrigin(filePath = getOrigin.filePath)

    _future = try {
      Option(
        executeInternal()
          .transform {
            case Success(_) => Success(ExecutionResult.FINISHED)
            case Failure(e) => Failure(e)
          }
          .map(_ => ExecutionResult.FINISHED)
          .recover {
            case _: Throwable if stopped.get() =>
              ExecutionResult.STOPPED
          }
      )
    } catch {
      case NonFatal(e) =>
        // Add query origin to exceptions raised while starting a flow
        throw e.addOrigin(queryOrigin)
    }
  }

  /** The destination that this `FlowExecution` is writing to. */
  def destination: Output

  /** Whether this `FlowExecution` has been stopped. Set by `FlowExecution.stop()`. */
  private val stopped: AtomicBoolean = new AtomicBoolean(false)
}

object FlowExecution {

  /** A thread pool used to execute `FlowExecutions`. */
  private val threadPool: ThreadPoolExecutor = {
    ThreadUtils.newDaemonCachedThreadPool("FlowExecution")
  }
}

/** A 'FlowExecution' that processes data statefully using Structured Streaming. */
trait StreamingFlowExecution extends FlowExecution with Logging {

  /** The `ResolvedFlow` that this `StreamingFlowExecution` is executing. */
  def flow: ResolvedFlow

  /** Structured Streaming checkpoint. */
  def checkpointPath: String

  /** Structured Streaming trigger. */
  def trigger: Trigger

  def isStreaming: Boolean = true

  /** Spark confs that must be set when starting this flow. */
  protected def sqlConf: Map[String, String]

  /** Starts a stream and returns its streaming query. */
  protected def startStream(): StreamingQuery

  /**
   * Executes this `StreamingFlowExecution` by starting its stream with the correct scheduling pool
   * and confs.
   */
  override final def executeInternal(): Future[Unit] = {
    logInfo(
      log"Starting ${MDC(LogKeys.TABLE_NAME, identifier)} with " +
      log"checkpoint location ${MDC(LogKeys.CHECKPOINT_PATH, checkpointPath)}"
    )
    val streamingQuery = SparkSessionUtils.withSqlConf(spark, sqlConf.toList: _*)(startStream())
    Future(streamingQuery.awaitTermination())
  }
}

/** A `StreamingFlowExecution` that writes a streaming `DataFrame` to a `Table`. */
class StreamingTableWrite(
    val identifier: TableIdentifier,
    val flow: ResolvedFlow,
    val graph: DataflowGraph,
    val updateContext: PipelineUpdateContext,
    val checkpointPath: String,
    val trigger: Trigger,
    val destination: Table,
    val sqlConf: Map[String, String]
) extends StreamingFlowExecution {

  override def getOrigin: QueryOrigin = flow.origin

  def startStream(): StreamingQuery = {
    val data = graph.reanalyzeFlow(flow).df
    val dataStreamWriter = data.writeStream
      .queryName(displayName)
      .option("checkpointLocation", checkpointPath)
      .trigger(trigger)
      .outputMode(OutputMode.Append())
    if (destination.format.isDefined) {
      dataStreamWriter.format(destination.format.get)
    }
    dataStreamWriter.toTable(destination.identifier.unquotedString)
  }
}

/** A `FlowExecution` that writes a batch `DataFrame` to a `Table`. */
class BatchTableWrite(
    val identifier: TableIdentifier,
    val flow: ResolvedFlow,
    val graph: DataflowGraph,
    val destination: Table,
    val updateContext: PipelineUpdateContext,
    val sqlConf: Map[String, String]
) extends FlowExecution {

  override def isStreaming: Boolean = false
  override def getOrigin: QueryOrigin = flow.origin

  def executeInternal(): scala.concurrent.Future[Unit] =
    SparkSessionUtils.withSqlConf(spark, sqlConf.toList: _*) {
      updateContext.flowProgressEventLogger.recordRunning(flow = flow)
      val data = graph.reanalyzeFlow(flow).df
      Future {
        val dataFrameWriter = data.write
        if (destination.format.isDefined) {
          dataFrameWriter.format(destination.format.get)
        }
        dataFrameWriter
          .mode("append")
          .saveAsTable(destination.identifier.unquotedString)
      }
    }
}
