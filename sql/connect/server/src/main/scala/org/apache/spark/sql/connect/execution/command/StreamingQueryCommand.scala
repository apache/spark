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
package org.apache.spark.sql.connect.execution.command

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.planner.InvalidInputErrors
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryProgress}
import org.apache.spark.util.Utils

case class StreamingQueryCommand(
    command: proto.StreamingQueryCommand,
    sparkSessionTags: Set[String],
    responseBuilder: proto.StreamingQueryCommandResult.Builder)
    extends ConnectLeafRunnableCommand
    with Logging {

  override def run(session: SparkSession): Seq[Row] = {
    responseBuilder.setQueryId(command.getQueryId)

    val id = command.getQueryId.getId
    val runId = command.getQueryId.getRunId

    // Find the query in connect service level cache, otherwise check session's active streams.
    val query = SparkConnectService.streamingSessionManager
      // Common case: query is cached in the cache.
      .getCachedQuery(id, runId, sparkSessionTags, session)
      .map(_.query)
      .orElse { // Else try to find it in active streams. Mostly will not be found here either.
        Option(session.streams.get(id))
      } match {
      case Some(query) if query.runId.toString == runId =>
        query
      case Some(query) =>
        throw InvalidInputErrors.streamingQueryRunIdMismatch(id, runId, query.runId.toString)
      case None =>
        throw InvalidInputErrors.streamingQueryNotFound(id)
    }
    command.getCommandCase match {
      case proto.StreamingQueryCommand.CommandCase.STATUS =>
        val queryStatus = query.status

        val statusResult = proto.StreamingQueryCommandResult.StatusResult
          .newBuilder()
          .setStatusMessage(queryStatus.message)
          .setIsDataAvailable(queryStatus.isDataAvailable)
          .setIsTriggerActive(queryStatus.isTriggerActive)
          .setIsActive(query.isActive)
          .build()

        responseBuilder.setStatus(statusResult)

      case proto.StreamingQueryCommand.CommandCase.LAST_PROGRESS |
          proto.StreamingQueryCommand.CommandCase.RECENT_PROGRESS =>
        val progressReports = if (command.getLastProgress) {
          Option(query.lastProgress).toSeq
        } else {
          query.recentProgress.toSeq
        }
        responseBuilder.setRecentProgress(
          proto.StreamingQueryCommandResult.RecentProgressResult
            .newBuilder()
            .addAllRecentProgressJson(
              progressReports.map(StreamingQueryProgress.jsonString).asJava)
            .build())

      case proto.StreamingQueryCommand.CommandCase.STOP =>
        query.stop()

      case proto.StreamingQueryCommand.CommandCase.PROCESS_ALL_AVAILABLE =>
        // This might take a long time, Spark-connect client keeps this connection alive.
        query.processAllAvailable()

      case proto.StreamingQueryCommand.CommandCase.EXPLAIN =>
        val result = query match {
          case q: StreamingQueryWrapper =>
            q.streamingQuery.explainInternal(command.getExplain.getExtended)
          case _ =>
            throw SparkException.internalError(s"Unexpected type for streaming query: $query")
        }
        val explain = proto.StreamingQueryCommandResult.ExplainResult
          .newBuilder()
          .setResult(result)
          .build()
        responseBuilder.setExplain(explain)

      case proto.StreamingQueryCommand.CommandCase.EXCEPTION =>
        val result = query.exception
        if (result.isDefined) {
          // Throw StreamingQueryException directly and rely on error translation on the
          // client-side to reconstruct the exception. Keep the remaining implementation
          // for backward-compatibility
          if (!command.getException) {
            throw result.get
          }
          val e = result.get
          val exception_builder = proto.StreamingQueryCommandResult.ExceptionResult
            .newBuilder()
          exception_builder
            .setExceptionMessage(e.toString())
            .setErrorClass(e.getCondition)

          val stackTrace = Option(Utils.stackTraceToString(e))
          stackTrace.foreach { s =>
            exception_builder.setStackTrace(s)
          }
          responseBuilder.setException(exception_builder.build())
        }

      case proto.StreamingQueryCommand.CommandCase.AWAIT_TERMINATION =>
        val timeout = if (command.getAwaitTermination.hasTimeoutMs) {
          Some(command.getAwaitTermination.getTimeoutMs)
        } else {
          None
        }
        val terminated = handleStreamingAwaitTermination(query, timeout)
        responseBuilder.getAwaitTerminationBuilder.setTerminated(terminated)

      case other =>
        throw InvalidInputErrors.invalidOneOfField(other, command.getDescriptorForType)
    }

    Seq.empty
  }

  override def handleConnectResponse(
      responseObserver: StreamObserver[proto.ExecutePlanResponse],
      sessionId: String,
      serverSessionId: String): Unit = {
    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(serverSessionId)
        .setStreamingQueryCommandResult(responseBuilder.build())
        .build())
  }

  /**
   * A helper function to handle streaming awaitTermination(). awaitTermination() can be a long
   * running command. In this function, we periodically check if the RPC call has been cancelled.
   * If so, we can stop the operation and release resources early.
   * @param query
   *   the query waits to be terminated
   * @param timeoutOptionMs
   *   optional. Timeout to wait for termination. If None, no timeout is set
   * @return
   *   if the query has terminated
   */
  private def handleStreamingAwaitTermination(
      query: StreamingQuery,
      timeoutOptionMs: Option[Long]): Boolean = {
    // How often to check if RPC is cancelled and call awaitTermination()
    val awaitTerminationIntervalMs = 10000
    val startTimeMs = System.currentTimeMillis()

    val timeoutTotalMs = timeoutOptionMs.getOrElse(Long.MaxValue)
    var timeoutLeftMs = timeoutTotalMs
    require(timeoutLeftMs > 0, "Timeout has to be positive")

    val grpcContext = io.grpc.Context.current
    while (!grpcContext.isCancelled) {
      val awaitTimeMs = math.min(awaitTerminationIntervalMs, timeoutLeftMs)

      val terminated = query.awaitTermination(awaitTimeMs)
      if (terminated) {
        return true
      }

      timeoutLeftMs = timeoutTotalMs - (System.currentTimeMillis() - startTimeMs)
      if (timeoutLeftMs <= 0) {
        return false
      }
    }

    // gRPC is cancelled
    logWarning("RPC context is cancelled when executing awaitTermination()")
    throw new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED)
  }
}
