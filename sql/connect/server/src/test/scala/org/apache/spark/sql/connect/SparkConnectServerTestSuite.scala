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

import org.scalatest.time.SpanSugar._

/**
 * Test suite showcasing the APIs provided by SparkConnectServerTest trait.
 *
 * This suite demonstrates:
 *   - Session and client helper methods (withSession, withClient, getServerSession)
 *   - Low-level stub helpers (withRawBlockingStub, withCustomBlockingStub)
 *   - Plan building helpers (buildPlan, buildExecutePlanRequest, etc.)
 *   - Assertion helpers for execution state
 */
class SparkConnectServerTestSuite extends SparkConnectServerTest {

  test("withSession: execute SQL and collect results") {
    withSession { session =>
      val df = session.sql("SELECT 1 as value")
      val result = df.collect()
      assert(result.length == 1)
      assert(result(0).getInt(0) == 1)
    }
  }

  test("withSession: with custom session and user IDs") {
    val customSessionId = java.util.UUID.randomUUID().toString
    val customUserId = "test-user"
    withSession(sessionId = customSessionId, userId = customUserId) { session =>
      val df = session.sql("SELECT 'hello' as greeting")
      val result = df.collect()
      assert(result.length == 1)
      assert(result(0).getString(0) == "hello")
    }
  }

  test("withSession: DataFrame operations") {
    withSession { session =>
      val df = session.range(10)
      assert(df.count() == 10)

      val sum = df.selectExpr("sum(id)").collect()(0).getLong(0)
      assert(sum == 45) // 0 + 1 + ... + 9 = 45
    }
  }

  test("withClient: execute plan and iterate results") {
    withClient { client =>
      val plan = buildPlan("SELECT 1 as x, 2 as y")
      val iter = client.execute(plan)
      var hasResults = false
      while (iter.hasNext) {
        iter.next()
        hasResults = true
      }
      assert(hasResults)
    }
  }

  test("withClient: with custom session and user IDs") {
    val customSessionId = java.util.UUID.randomUUID().toString
    val customUserId = "custom-user"
    withClient(sessionId = customSessionId, userId = customUserId) { client =>
      val plan = buildPlan("SELECT 42")
      val iter = client.execute(plan)
      while (iter.hasNext) iter.next()
    }
  }

  test("getServerSession: returns server-side classic session") {
    withSession { clientSession =>
      clientSession.sql("SELECT 1").collect()

      val serverSession = getServerSession(clientSession)

      assert(serverSession != null)
      assert(serverSession.sparkContext != null)
    }
  }

  test("getServerSession: client and server share configuration") {
    withSession { clientSession =>
      clientSession.sql("SET spark.sql.shuffle.partitions=17").collect()

      val serverSession = getServerSession(clientSession)
      assert(serverSession.conf.get("spark.sql.shuffle.partitions") == "17")
    }
  }

  test("getServerSession: register and use temporary view from server") {
    withSession { clientSession =>
      clientSession.sql("SELECT 1 as a, 2 as b").collect()

      val serverSession = getServerSession(clientSession)

      // Create a temp view on the server side
      import serverSession.implicits._
      val serverDf = Seq((100, "server"), (200, "side")).toDF("num", "source")
      serverDf.createOrReplaceTempView("server_view")

      // Access the view from the client
      val result = clientSession.sql("SELECT * FROM server_view ORDER BY num").collect()
      assert(result.length == 2)
      assert(result(0).getInt(0) == 100)
      assert(result(0).getString(1) == "server")
      assert(result(1).getInt(0) == 200)
      assert(result(1).getString(1) == "side")
    }
  }

  test("withRawBlockingStub: execute plan via raw gRPC stub") {
    withRawBlockingStub { stub =>
      val request = buildExecutePlanRequest(buildPlan("SELECT 'raw' as mode"))
      val iter = stub.executePlan(request)
      assert(iter.hasNext)
      while (iter.hasNext) iter.next()
    }
  }

  test("withCustomBlockingStub: execute plan via custom blocking stub") {
    withCustomBlockingStub() { stub =>
      val request = buildExecutePlanRequest(buildPlan("SELECT 'custom' as mode"))
      val iter = stub.executePlan(request)
      while (iter.hasNext) iter.next()
    }
  }

  test("buildPlan: creates plan from SQL query") {
    val plan = buildPlan("SELECT 1, 2, 3")
    assert(plan.hasRoot)
  }

  test("buildSqlCommandPlan: creates command plan") {
    val plan = buildSqlCommandPlan("SET spark.sql.adaptive.enabled=true")
    assert(plan.hasCommand)
    assert(plan.getCommand.hasSqlCommand)
  }

  test("buildLocalRelation: creates plan from local data") {
    val data = Seq((1, "a"), (2, "b"), (3, "c"))
    val plan = buildLocalRelation(data)
    assert(plan.hasRoot)
    assert(plan.getRoot.hasLocalRelation)
  }

  test("buildExecutePlanRequest: creates request with options") {
    val plan = buildPlan("SELECT 1")
    val request = buildExecutePlanRequest(plan)
    assert(request.hasPlan)
    assert(request.hasUserContext)
    assert(request.getSessionId == defaultSessionId)
  }

  test("buildExecutePlanRequest: with custom session and operation IDs") {
    val plan = buildPlan("SELECT 1")
    val customSessionId = "my-session"
    val customOperationId = "my-operation"
    val request =
      buildExecutePlanRequest(plan, sessionId = customSessionId, operationId = customOperationId)
    assert(request.getSessionId == customSessionId)
    assert(request.getOperationId == customOperationId)
  }

  test("runQuery: executes query string with timeout") {
    runQuery("SELECT * FROM range(100)", 30.seconds)
  }

  test("runQuery: executes plan with timeout and iter sleep") {
    val plan = buildPlan("SELECT * FROM range(10)")
    runQuery(plan, 30.seconds, iterSleep = 10)
  }

  test("assertNoActiveExecutions: verifies clean state") {
    assertNoActiveExecutions()
  }

  test("assertNoActiveRpcs: verifies no active RPCs") {
    assertNoActiveRpcs()
  }

  test("eventuallyGetExecutionHolder: retrieves active execution") {
    withRawBlockingStub { stub =>
      val request = buildExecutePlanRequest(buildPlan("SELECT * FROM range(1000000)"))
      val iter = stub.executePlan(request)
      iter.hasNext // trigger execution

      val holder = eventuallyGetExecutionHolder
      assert(holder != null)
      assert(holder.operationId == request.getOperationId)
    }
  }
}
