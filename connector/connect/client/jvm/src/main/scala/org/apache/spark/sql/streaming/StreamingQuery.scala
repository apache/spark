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

package org.apache.spark.sql.streaming

import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.connect.proto.Command
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.connect.proto.StreamingQueryCommand
import org.apache.spark.connect.proto.StreamingQueryCommandResult
import org.apache.spark.connect.proto.StreamingQueryManagerCommandResult.StreamingQueryInstance
import org.apache.spark.sql.SparkSession

/**
 * A handle to a query that is executing continuously in the background as new data arrives. All
 * these methods are thread-safe.
 * @since 3.5.0
 */
@Evolving
trait StreamingQuery {
  // This is a copy of StreamingQuery in sql/core/.../streaming/StreamingQuery.scala

  /**
   * Returns the user-specified name of the query, or null if not specified. This name can be
   * specified in the `org.apache.spark.sql.streaming.DataStreamWriter` as
   * `dataframe.writeStream.queryName("query").start()`. This name, if set, must be unique across
   * all active queries.
   *
   * @since 3.5.0
   */
  def name: String

  /**
   * Returns the unique id of this query that persists across restarts from checkpoint data. That
   * is, this id is generated when a query is started for the first time, and will be the same
   * every time it is restarted from checkpoint data. Also see [[runId]].
   *
   * @since 3.5.0
   */
  def id: UUID

  /**
   * Returns the unique id of this run of the query. That is, every start/restart of a query will
   * generate a unique runId. Therefore, every time a query is restarted from checkpoint, it will
   * have the same [[id]] but different [[runId]]s.
   */
  def runId: UUID

  /**
   * Returns the `SparkSession` associated with `this`.
   *
   * @since 3.5.0
   */
  def sparkSession: SparkSession

  /**
   * Returns `true` if this query is actively running.
   *
   * @since 3.5.0
   */
  def isActive: Boolean

  /**
   * Returns the [[StreamingQueryException]] if the query was terminated by an exception.
   * @since 3.5.0
   */
  def exception: Option[StreamingQueryException]

  /**
   * Returns the current status of the query.
   *
   * @since 3.5.0
   */
  def status: StreamingQueryStatus

  /**
   * Returns an array of the most recent [[StreamingQueryProgress]] updates for this query. The
   * number of progress updates retained for each stream is configured by Spark session
   * configuration `spark.sql.streaming.numRecentProgressUpdates`.
   *
   * @since 3.5.0
   */
  def recentProgress: Array[StreamingQueryProgress]

  /**
   * Returns the most recent [[StreamingQueryProgress]] update of this streaming query.
   *
   * @since 3.5.0
   */
  def lastProgress: StreamingQueryProgress

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception. If
   * the query has terminated with an exception, then the exception will be thrown.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * immediately (if the query was terminated by `stop()`), or throw the exception immediately (if
   * the query has terminated with exception).
   *
   * @throws StreamingQueryException
   *   if the query has terminated with an exception.
   * @since 3.5.0
   */
  @throws[StreamingQueryException]
  def awaitTermination(): Unit

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception. If
   * the query has terminated with an exception, then the exception will be thrown. Otherwise, it
   * returns whether the query has terminated or not within the `timeoutMs` milliseconds.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * `true` immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   *
   * @throws StreamingQueryException
   *   if the query has terminated with an exception
   * @since 3.5.0
   */
  @throws[StreamingQueryException]
  def awaitTermination(timeoutMs: Long): Boolean

  /**
   * Blocks until all available data in the source has been processed and committed to the sink.
   * This method is intended for testing. Note that in the case of continually arriving data, this
   * method may block forever. Additionally, this method is only guaranteed to block until data
   * that has been synchronously appended data to a
   * `org.apache.spark.sql.execution.streaming.Source` prior to invocation. (i.e. `getOffset` must
   * immediately reflect the addition).
   * @since 3.5.0
   */
  def processAllAvailable(): Unit

  /**
   * Stops the execution of this query if it is running. This waits until the termination of the
   * query execution threads or until a timeout is hit.
   *
   * By default stop will block indefinitely. You can configure a timeout by the configuration
   * `spark.sql.streaming.stopTimeout`. A timeout of 0 (or negative) milliseconds will block
   * indefinitely. If a `TimeoutException` is thrown, users can retry stopping the stream. If the
   * issue persists, it is advisable to kill the Spark application.
   *
   * @since 3.5.0
   */
  @throws[TimeoutException]
  def stop(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   * @since 3.5.0
   */
  def explain(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @param extended
   *   whether to do extended explain or not
   * @since 3.5.0
   */
  def explain(extended: Boolean): Unit
}

class RemoteStreamingQuery(
    override val id: UUID,
    override val runId: UUID,
    override val name: String,
    override val sparkSession: SparkSession)
    extends StreamingQuery {

  override def isActive: Boolean = {
    executeQueryCmd(_.setStatus(true)).getStatus.getIsActive
  }

  override def awaitTermination(): Unit = {
    executeQueryCmd(_.getAwaitTerminationBuilder.build())
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    executeQueryCmd(
      _.getAwaitTerminationBuilder.setTimeoutMs(timeoutMs)).getAwaitTermination.getTerminated
  }

  override def status: StreamingQueryStatus = {
    val statusResp = executeQueryCmd(_.setStatus(true)).getStatus
    new StreamingQueryStatus(
      message = statusResp.getStatusMessage,
      isDataAvailable = statusResp.getIsDataAvailable,
      isTriggerActive = statusResp.getIsTriggerActive)
  }

  override def recentProgress: Array[StreamingQueryProgress] = {
    executeQueryCmd(_.setRecentProgress(true)).getRecentProgress.getRecentProgressJsonList.asScala
      .map(StreamingQueryProgress.fromJson)
      .toArray
  }

  override def lastProgress: StreamingQueryProgress = {
    executeQueryCmd(
      _.setLastProgress(true)).getRecentProgress.getRecentProgressJsonList.asScala.headOption
      .map(StreamingQueryProgress.fromJson)
      .orNull
  }

  override def processAllAvailable(): Unit = {
    executeQueryCmd(_.setProcessAllAvailable(true))
  }

  override def stop(): Unit = {
    executeQueryCmd(_.setStop(true))
  }

  override def explain(): Unit = {
    explain(extended = false)
  }

  override def explain(extended: Boolean): Unit = {
    val explainCmd = StreamingQueryCommand.ExplainCommand
      .newBuilder()
      .setExtended(extended)
      .build()

    val explain = executeQueryCmd(_.setExplain(explainCmd)).getExplain.getResult

    // scalastyle:off println
    println(explain)
    // scalastyle:on println
  }

  override def exception: Option[StreamingQueryException] = {
    try {
      // When exception field is set to false, the server throws a StreamingQueryException
      // to the client.
      executeQueryCmd(_.setException(false))
    } catch {
      case e: StreamingQueryException => return Some(e)
    }

    None
  }

  private def executeQueryCmd(
      setCmdFn: StreamingQueryCommand.Builder => Unit // Sets the command field, like stop().
  ): StreamingQueryCommandResult = {

    val cmdBuilder = Command.newBuilder()
    val queryCmdBuilder = cmdBuilder.getStreamingQueryCommandBuilder

    // Set queryId.
    queryCmdBuilder.getQueryIdBuilder
      .setId(id.toString)
      .setRunId(runId.toString)

    // Set command.
    setCmdFn(queryCmdBuilder)

    val resp = sparkSession.execute(cmdBuilder.build()).head

    if (!resp.hasStreamingQueryCommandResult) {
      throw new RuntimeException("Unexpected missing response for streaming query command")
    }

    resp.getStreamingQueryCommandResult
  }
}

object RemoteStreamingQuery {

  def fromStartCommandResponse(
      sparkSession: SparkSession,
      response: ExecutePlanResponse): RemoteStreamingQuery = {

    if (!response.hasWriteStreamOperationStartResult) {
      throw new RuntimeException("Unexpected: No result in response for start stream command")
    }

    val result = response.getWriteStreamOperationStartResult

    new RemoteStreamingQuery(
      id = UUID.fromString(result.getQueryId.getId),
      runId = UUID.fromString(result.getQueryId.getRunId),
      name = if (result.getName.isEmpty) null else result.getName,
      sparkSession = sparkSession)
  }

  def fromStreamingQueryInstanceResponse(
      sparkSession: SparkSession,
      q: StreamingQueryInstance): RemoteStreamingQuery = {

    val name = if (q.hasName) {
      q.getName
    } else {
      null
    }
    new RemoteStreamingQuery(
      UUID.fromString(q.getId.getId),
      UUID.fromString(q.getId.getRunId),
      name,
      sparkSession)
  }
}
