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
package org.apache.spark.sql.connect

import java.util.UUID

import org.apache.spark.connect.proto
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteStatus, SessionHolder, SessionStatus, SparkConnectService}

object SparkConnectTestUtils {

  /** Creates a dummy session holder for use in tests. */
  def createDummySessionHolder(session: SparkSession): SessionHolder = {
    val ret =
      SessionHolder(
        userId = "testUser",
        sessionId = UUID.randomUUID().toString,
        session = session)
    SparkConnectService.sessionManager.putSessionForTesting(ret)
    if (session != null) {
      ret.initializeSession()
    }
    ret
  }

  /** Creates a dummy execute holder for use in tests. */
  def createDummyExecuteHolder(
      sessionHolder: SessionHolder,
      command: proto.Command): ExecuteHolder = {
    sessionHolder.eventManager.status_(SessionStatus.Started)
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(
        proto.Plan
          .newBuilder()
          .setCommand(command)
          .build())
      .setSessionId(sessionHolder.sessionId)
      .setUserContext(
        proto.UserContext
          .newBuilder()
          .setUserId(sessionHolder.userId)
          .build())
      .build()
    val executeHolder =
      SparkConnectService.executionManager.createExecuteHolder(request)
    executeHolder.eventsManager.status_(ExecuteStatus.Started)
    executeHolder
  }
}
