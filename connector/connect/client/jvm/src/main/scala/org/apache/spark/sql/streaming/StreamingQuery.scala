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

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto.Command
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.connect.proto.StreamingQueryCommand
import org.apache.spark.connect.proto.StreamingQueryCommandResult
import org.apache.spark.connect.proto.StreamingQueryManagerCommandResult.StreamingQueryInstance
import org.apache.spark.sql.{api, SparkSession}

/** @inheritdoc */
trait StreamingQuery extends api.StreamingQuery {

  /** @inheritdoc */
  override def sparkSession: SparkSession
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
