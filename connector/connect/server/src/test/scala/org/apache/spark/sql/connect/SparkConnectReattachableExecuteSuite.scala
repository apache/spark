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
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectReattachableExecuteSuite extends SharedSparkSession {

  private val sessionId = UUID.randomUUID.toString()
  private val userContext = proto.UserContext
    .newBuilder()
    .setUserId("c1")
    .build()

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkConnectService.start(spark.sparkContext)
  }

  override def afterAll(): Unit = {
    SparkConnectService.stop()
  }

  test("client") {
    val client = SparkConnectClient.builder().build()
    val connect = new MockRemoteSession()
    val plan = proto.Plan
      .newBuilder()
      .setRoot(connect.sql("select * from range(1000)"))
      .build()
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setOperationId(UUID.randomUUID().toString)
      .setPlan(plan)
      .build()

    val iter = client.execute(plan)
    println(iter)
    println(iter.next())
    println(SparkConnectService.executionManager.listActiveExecutions)
    while (iter.hasNext) {
      println(iter.next())
    }
    println(SparkConnectService.executionManager.listActiveExecutions)
    // SparkConnectService.executionManager.periodicMaintenance(0)
    Thread.sleep(1000)
    println(SparkConnectService.executionManager.listActiveExecutions)
  }

  test("stub") {
    val conf = SparkConnectClient.Configuration()
    val channel = conf.createChannel()
    val bstub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)

    val connect = new MockRemoteSession()
    val plan = proto.Plan
      .newBuilder()
      .setRoot(connect.sql("select * from range(1000)"))
      .build()
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setUserContext(userContext)
      .setSessionId(sessionId)
      .setOperationId(UUID.randomUUID().toString)
      .setPlan(plan)
      .addRequestOptions(
        proto.ExecutePlanRequest.RequestOption
          .newBuilder()
          .setReattachOptions(proto.ReattachOptions.newBuilder().setReattachable(true).build())
          .build())
      .build()

    val iter = bstub.executePlan(request)
    iter.next()
    println(SparkConnectService.executionManager.listActiveExecutions)
  }
}
