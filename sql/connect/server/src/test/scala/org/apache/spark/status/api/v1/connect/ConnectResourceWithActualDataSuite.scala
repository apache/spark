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

import java.io.IOException
import java.net.{HttpURLConnection, URI, URL}

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods

import org.apache.spark.SparkConf
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.connect.ui.ExecutionState
import org.apache.spark.util.Utils

/**
 * End-to-end test for the Spark Connect REST API: boots the real SparkConnectService with the UI
 * enabled, runs a real query through a Connect session so the listener populates the KVStore,
 * then exercises the `/api/v1/applications/{appId}/connect/...` endpoints over HTTP. Mirrors
 * SqlResourceWithActualMetricsSuite.
 */
class ConnectResourceWithActualDataSuite extends SparkConnectServerTest {

  private implicit val formats: Formats = DefaultFormats

  // Populated per JSON payload; only the fields we assert on are declared. json4s ignores the rest.
  private case class TestSessionData(sessionId: String, userId: String, totalExecution: Long)
  private case class TestExecutionData(
      jobTag: String,
      operationId: String,
      sessionId: String,
      userId: String,
      state: String)

  override def sparkConf: SparkConf = super.sparkConf.set("spark.ui.enabled", "true")

  private def baseUrl: String =
    spark.sparkContext.ui.get.webUrl +
      s"/api/v1/applications/${spark.sparkContext.applicationId}/connect"

  private def get(url: URL): (Int, Option[String]) = {
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    conn.connect()
    val code = conn.getResponseCode
    val body =
      try Option(conn.getInputStream).map(Utils.toString)
      catch { case _: IOException => Option(conn.getErrorStream).map(Utils.toString) }
    (code, body)
  }

  private def getOk(path: String): String = {
    val (code, body) = get(new URI(s"$baseUrl$path").toURL)
    assert(code == 200, s"expected 200 for $path but got $code, body=${body.orNull}")
    body.get
  }

  private def enc(s: String): String =
    java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8)

  test("Connect REST API exposes sessions and operations") {
    // Run a real query through a Connect session; this posts the session/operation listener
    // events that the SparkConnectServerListener writes into the KVStore.
    withSession { session =>
      assert(session.sql("select 1 + 1").collect().head.getInt(0) === 2)
    }
    // Listener events are posted asynchronously; drain the bus before reading the store.
    spark.sparkContext.listenerBus.waitUntilEmpty(eventuallyTimeout.toMillis)

    eventuallyWithTimeout {
      val sessions = JsonMethods.parse(getOk("/sessions")).extract[List[TestSessionData]]
      val session = sessions.find(_.sessionId == defaultSessionId)
      assert(session.isDefined, s"session $defaultSessionId not in $sessions")
      assert(session.get.userId === defaultUserId)
      assert(session.get.totalExecution >= 1L)

      val operations = JsonMethods.parse(getOk("/operations")).extract[List[TestExecutionData]]
      val op = operations.find(_.sessionId == defaultSessionId)
      assert(op.isDefined, s"no operation for session $defaultSessionId in $operations")
      assert(op.get.userId === defaultUserId)
      assert(ExecutionState.values.map(_.toString).contains(op.get.state))

      val one = JsonMethods.parse(getOk(s"/sessions/$defaultSessionId")).extract[TestSessionData]
      assert(one.sessionId === defaultSessionId)

      val oneOp = JsonMethods
        .parse(getOk(s"/operations/detail?jobTag=${enc(op.get.jobTag)}"))
        .extract[TestExecutionData]
      assert(oneOp.operationId === op.get.operationId)
      assert(oneOp.jobTag === op.get.jobTag)
    }
  }

  test("SPARK-57941: operation with a user id containing '/' is fetchable via the jobTag query") {
    // The job tag embeds the raw user id, so a '/' in it would break a path segment. The detail
    // endpoint takes the job tag as a query parameter, which handles such user ids.
    val userId = "tenant/alice"
    val sessionId = java.util.UUID.randomUUID.toString
    withSession(sessionId = sessionId, userId = userId) { session =>
      assert(session.sql("select 1 + 1").collect().head.getInt(0) === 2)
    }
    spark.sparkContext.listenerBus.waitUntilEmpty(eventuallyTimeout.toMillis)

    eventuallyWithTimeout {
      val operations = JsonMethods.parse(getOk("/operations")).extract[List[TestExecutionData]]
      val op = operations.find(o => o.sessionId == sessionId && o.userId == userId)
      assert(op.isDefined, s"no operation for user $userId in $operations")
      assert(op.get.jobTag.contains(userId))
      val oneOp = JsonMethods
        .parse(getOk(s"/operations/detail?jobTag=${enc(op.get.jobTag)}"))
        .extract[TestExecutionData]
      assert(oneOp.jobTag === op.get.jobTag)
      assert(oneOp.userId === userId)
    }
  }

  test("Connect REST API returns 404 for unknown ids") {
    assert(get(new URI(s"$baseUrl/sessions/does-not-exist").toURL)._1 === 404)
    assert(get(new URI(s"$baseUrl/operations/detail?jobTag=does-not-exist").toURL)._1 === 404)
  }

  test("SPARK-57941: operation detail without a jobTag returns 400") {
    assert(get(new URI(s"$baseUrl/operations/detail").toURL)._1 === 400)
    assert(get(new URI(s"$baseUrl/operations/detail?jobTag=").toURL)._1 === 400)
  }

  test("SPARK-57941: operations list honors offset and length") {
    withSession { session =>
      (1 to 3).foreach(i => assert(session.sql(s"select $i").collect().head.getInt(0) === i))
    }
    spark.sparkContext.listenerBus.waitUntilEmpty(eventuallyTimeout.toMillis)

    eventuallyWithTimeout {
      val all = JsonMethods.parse(getOk("/operations")).extract[List[TestExecutionData]]
      assert(all.size >= 3, s"expected at least 3 operations, got ${all.size}")

      // length bounds the page size...
      val firstTwo =
        JsonMethods.parse(getOk("/operations?length=2")).extract[List[TestExecutionData]]
      assert(firstTwo.size === 2)
      assert(firstTwo.map(_.jobTag) === all.take(2).map(_.jobTag))

      // ...and offset skips from the front, matching a slice of the full list.
      val skipOne = JsonMethods
        .parse(getOk("/operations?offset=1&length=2"))
        .extract[List[TestExecutionData]]
      assert(skipOne.map(_.jobTag) === all.slice(1, 3).map(_.jobTag))
    }
  }
}
