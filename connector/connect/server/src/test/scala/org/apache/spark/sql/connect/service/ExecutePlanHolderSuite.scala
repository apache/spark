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

import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkFunSuite}
import org.apache.spark.connect.proto.ExecutePlanRequest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest

class ExecutePlanHolderSuite extends SparkFunSuite with MockitoSugar with SparkConnectPlanTest {
  val DEFAULT_USER_ID = "1"
  val DEFAULT_USER_NAME = "userName"
  val DEFAULT_SESSION_ID = "2"
  val DEFAULT_QUERY_ID = "3"
  val DEFAULT_CLIENT_TYPE = "clientType"
  val DEFAULT_JOB_GROUP_ID =
    s"User_${DEFAULT_USER_ID}_Session_${DEFAULT_SESSION_ID}_Request_${DEFAULT_QUERY_ID}"

  test("SPARK-43923: job group id matches pattern") {
    val mockSession = mock[SparkSession]
    val request = ExecutePlanRequest.newBuilder().build()
    val sessionHolder = SessionHolder(DEFAULT_USER_ID, DEFAULT_SESSION_ID, mockSession)
    val executePlanHolder = ExecutePlanHolder(DEFAULT_QUERY_ID, sessionHolder, request)
    assert(
      ExecutePlanHolder
        .getQueryOperationId(executePlanHolder.jobGroupId) == Some(DEFAULT_QUERY_ID))
  }

}
