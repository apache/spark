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

import java.util.UUID

import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, Plan, UserContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class SparkConnectExecuteHolderSuite
    extends SparkFunSuite
    with MockitoSugar
    with SharedSparkSession {

  val DEFAULT_CLOCK = new ManualClock()
  val DEFAULT_USER_ID = "1"
  val DEFAULT_USER_NAME = "userName"
  val DEFAULT_SESSION_ID = UUID.randomUUID.toString
  val DEFAULT_QUERY_ID = UUID.randomUUID.toString
  val DEFAULT_CLIENT_TYPE = "clientType"

  test("SPARK-53339: ExecuteHolder should ignore interruption when execute status is Pending") {
    val executeHolder = setupExecuteHolder()
    executeHolder.sessionHolder.eventManager.postStarted()

    assert(executeHolder.eventsManager.status == ExecuteStatus.Pending)
    assert(!executeHolder.interrupt())

    executeHolder.eventsManager.postStarted()
    assert(executeHolder.eventsManager.status == ExecuteStatus.Started)
    assert(executeHolder.interrupt())
  }

  def setupExecuteHolder(): ExecuteHolder = {
    val relation = proto.Relation.newBuilder
      .setLimit(proto.Limit.newBuilder.setLimit(10))
      .build()

    val executePlanRequest = ExecutePlanRequest
      .newBuilder()
      .setPlan(Plan.newBuilder().setRoot(relation))
      .setUserContext(
        UserContext
          .newBuilder()
          .setUserId(DEFAULT_USER_ID)
          .setUserName(DEFAULT_USER_NAME))
      .setSessionId(DEFAULT_SESSION_ID)
      .setOperationId(DEFAULT_QUERY_ID)
      .setClientType(DEFAULT_CLIENT_TYPE)
      .build()

    val sessionHolder = SessionHolder(DEFAULT_USER_ID, DEFAULT_SESSION_ID, spark)
    val executeKey = ExecuteKey(executePlanRequest, sessionHolder)
    new ExecuteHolder(executeKey, executePlanRequest, sessionHolder)
  }
}
