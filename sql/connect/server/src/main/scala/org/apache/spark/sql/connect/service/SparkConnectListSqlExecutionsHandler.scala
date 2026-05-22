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

package org.apache.spark.sql.connect.service

import scala.util.Try

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.ui.SparkConnectServerAppStatusStore
import org.apache.spark.sql.execution.ui.SQLExecutionUIData

class SparkConnectListSqlExecutionsHandler(
    responseObserver: StreamObserver[proto.ListSqlExecutionsResponse])
    extends Logging {

  import SparkConnectListSqlExecutionsHandler._

  def handle(request: proto.ListSqlExecutionsRequest): Unit = {
    val key = SessionKey(request.getUserContext.getUserId, request.getSessionId)

    val responseBuilder = proto.ListSqlExecutionsResponse
      .newBuilder()
      .setSessionId(request.getSessionId)

    // Client UIs poll this RPC eagerly on session creation, before any other RPC has caused
    // the server-side session to exist. A not-yet-created session trivially has zero
    // executions, so return an empty page rather than failing the UI with SESSION_NOT_FOUND.
    SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key) match {
      case None =>
      case Some(sessionHolder) =>
        if (request.hasClientObservedServerSideSessionId) {
          // Validate the previously observed server session id against the live holder.
          SparkConnectService.sessionManager.getIsolatedSession(
            key,
            Some(request.getClientObservedServerSideSessionId))
        }

        val statusStore = sessionHolder.session.sharedState.statusStore
        // The Connect server listener records, per Connect session, the set of SQL execution IDs
        // that ran under it. We use that to scope the response to this session's executions only;
        // sharedState.statusStore by itself is driver-wide and would leak other sessions'
        // queries.
        val connectStore = new SparkConnectServerAppStatusStore(
          sessionHolder.session.sparkContext.statusStore.store)

        val sessionExecIds: Seq[Long] = connectStore.getExecutionList
          .filter(_.sessionId == request.getSessionId)
          .flatMap(_.sqlExecId)
          .flatMap(id => Try(id.toLong).toOption)
          .distinct
          .sorted

        val offset = math.max(request.getOffset, 0)
        val rawLength = request.getLength
        val length = if (rawLength <= 0 || rawLength > MaxLength) MaxLength else rawLength

        val pageIds = sessionExecIds.slice(offset, offset + length)
        val executions = pageIds.flatMap(id => statusStore.execution(id))

        responseBuilder
          .setServerSideSessionId(sessionHolder.serverSessionId)
          .setTotalCount(sessionExecIds.length.toLong)

        executions.foreach { exec =>
          responseBuilder.addExecutions(buildSummary(exec))
        }
    }

    responseObserver.onNext(responseBuilder.build())
    responseObserver.onCompleted()
  }

  private def buildSummary(
      exec: SQLExecutionUIData): proto.ListSqlExecutionsResponse.SqlExecutionSummary = {
    val builder = proto.ListSqlExecutionsResponse.SqlExecutionSummary
      .newBuilder()
      .setExecutionId(exec.executionId)
      .setRootExecutionId(exec.rootExecutionId)
      .setDescription(Option(exec.description).getOrElse(""))
      .setStatus(mapStatus(exec))
      .setSubmissionTimeMs(exec.submissionTime)
      .setStageCount(exec.stages.size)

    exec.completionTime.foreach(t => builder.setCompletionTimeMs(t.getTime))
    exec.errorMessage.foreach(builder.setErrorMessage)
    exec.jobs.keys.toSeq.sorted.foreach(id => builder.addJobIds(id))
    Option(exec.queryId).foreach(id => builder.setQueryId(id.toString))
    Option(exec.details).filter(_.nonEmpty).foreach(builder.setDetails)

    builder.build()
  }

  private def mapStatus(
      exec: SQLExecutionUIData): proto.ListSqlExecutionsResponse.SqlExecutionStatus = {
    import proto.ListSqlExecutionsResponse.SqlExecutionStatus._
    exec.executionStatus match {
      case "RUNNING" => SQL_EXECUTION_STATUS_RUNNING
      case "COMPLETED" => SQL_EXECUTION_STATUS_COMPLETED
      case "FAILED" => SQL_EXECUTION_STATUS_FAILED
      case _ => SQL_EXECUTION_STATUS_UNSPECIFIED
    }
  }
}

object SparkConnectListSqlExecutionsHandler {
  // Server-side cap to keep list responses cheap. Clients that ask for more get this many.
  private[service] val MaxLength: Int = 1000
}
