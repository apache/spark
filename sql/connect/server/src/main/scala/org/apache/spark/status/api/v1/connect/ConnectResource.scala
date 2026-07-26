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

package org.apache.spark.status.api.v1.connect

import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType

import org.apache.spark.sql.connect.ui.{ExecutionInfo, SessionInfo, SparkConnectServerAppStatusStore}
import org.apache.spark.status.api.v1.{BadParameterException, BaseAppResource, NotFoundException}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ConnectResource extends BaseAppResource {

  @GET
  @Path("sessions")
  def sessionList(
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("-1") @QueryParam("length") length: Int): Seq[SessionData] = withUI { ui =>
    val store = new SparkConnectServerAppStatusStore(ui.store.store)
    val sessions =
      if (length <= 0) store.getSessionList else store.getSessionList(offset, length)
    sessions.map(prepareSessionData)
  }

  @GET
  @Path("sessions/{sessionId}")
  def session(@PathParam("sessionId") sessionId: String): SessionData = withUI { ui =>
    val store = new SparkConnectServerAppStatusStore(ui.store.store)
    store
      .getSession(sessionId)
      .map(prepareSessionData)
      .getOrElse(throw new NotFoundException("unknown session id: " + sessionId))
  }

  @GET
  @Path("operations")
  def operationList(
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("-1") @QueryParam("length") length: Int): Seq[ExecutionData] = withUI { ui =>
    val store = new SparkConnectServerAppStatusStore(ui.store.store)
    val operations =
      if (length <= 0) store.getExecutionList else store.getExecutionList(offset, length)
    operations.map(prepareExecutionData)
  }

  // The job tag is the unique natural key (userId, sessionId, operationId). It is taken as a query
  // parameter rather than a path segment because it embeds the raw user id, which may contain
  // characters such as '/' that are not safe in a path segment.
  @GET
  @Path("operations/detail")
  def operation(@QueryParam("jobTag") jobTag: String): ExecutionData = withUI { ui =>
    if (jobTag == null || jobTag.isEmpty) {
      throw new BadParameterException("jobTag is required.")
    }
    val store = new SparkConnectServerAppStatusStore(ui.store.store)
    store
      .getExecution(jobTag)
      .map(prepareExecutionData)
      .getOrElse(throw new NotFoundException("unknown jobTag: " + jobTag))
  }

  private def prepareSessionData(info: SessionInfo): SessionData = new SessionData(
    sessionId = info.sessionId,
    userId = info.userId,
    startTimestamp = info.startTimestamp,
    finishTimestamp = info.finishTimestamp,
    totalExecution = info.totalExecution,
    totalTime = info.totalTime)

  private def prepareExecutionData(info: ExecutionInfo): ExecutionData = new ExecutionData(
    jobTag = info.jobTag,
    operationId = info.operationId,
    sessionId = info.sessionId,
    userId = info.userId,
    statement = info.statement,
    state = info.state.toString,
    startTimestamp = info.startTimestamp,
    finishTimestamp = info.finishTimestamp,
    closeTimestamp = info.closeTimestamp,
    duration = info.totalTime(info.closeTimestamp),
    executionTime = info.totalTime(info.finishTimestamp),
    sparkSessionTags = info.sparkSessionTags.toSeq.sorted,
    jobIds = info.jobId.toSeq.sortBy(_.toInt),
    sqlExecIds = info.sqlExecId.toSeq.sortBy(_.toLong),
    detail = info.detail)
}
