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

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

import org.apache.spark.SparkException
import org.apache.spark.sql.connect.{SparkConnectServerTest, SparkSession}
import org.apache.spark.sql.connect.config.Connect

/**
 * Records whether it was handed a call, so a test can tell whether it ran at all. Needs a
 * no-argument constructor to be loadable from `spark.connect.grpc.interceptor.classes`.
 */
class CallTrackingInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    CallTrackingInterceptor.ran = true
    next.startCall(call, headers)
  }
}

object CallTrackingInterceptor {
  // A flag, not a count: range(5).collect() issues one ExecutePlan plus a variable number of
  // fire-and-forget ReleaseExecute calls, so an exact count would be fragile.
  @volatile var ran: Boolean = false
}

class SparkConnectAuthSuite extends SparkConnectServerTest {
  private val tokenKey = "spark.connect.authenticate.token"
  private val markerKey = "spark.connect.test.marker"
  private val token = "deadbeef"

  override protected def sparkConf = {
    super.sparkConf
      .set(tokenKey, token)
      .set(markerKey, "visible")
  }

  override protected def extraServerConfs: Seq[(String, String)] = Seq(
    Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key -> classOf[CallTrackingInterceptor].getName)

  test("Test local authentication") {
    val session = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=$token")
      .create()
    try {
      session.range(5).collect()
    } finally {
      session.close()
    }

    val invalidSession = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=invalid")
      .create()
    try {
      val exception = intercept[SparkException] {
        invalidSession.range(5).collect()
      }
      assert(exception.getMessage.contains("Invalid authentication token"))
    } finally {
      invalidSession.close()
    }
  }

  test("Test the authentication token is not readable through the Config RPC") {
    val session = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=$token")
      .create()
    try {
      assert(session.conf.getOption(tokenKey).isEmpty)
      assert(session.conf.get(tokenKey, "absent") === "absent")
      intercept[NoSuchElementException](session.conf.get(tokenKey))
      assert(!session.conf.getAll.contains(tokenKey))

      // Server-side configurations that are not sensitive stay readable.
      assert(session.conf.get(markerKey) === "visible")
    } finally {
      session.close()
    }
  }

  test("an unauthenticated call is rejected before the configured interceptors run") {
    // The other tests in this suite made authenticated calls, so reset the flag first.
    CallTrackingInterceptor.ran = false

    val anonymous = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/")
      .create()
    try {
      val e = intercept[SparkException](anonymous.range(5).collect())
      assert(e.getMessage.contains("No authentication token provided"))
      assert(
        !CallTrackingInterceptor.ran,
        "a configured interceptor ran for a call that failed authentication")
    } finally {
      anonymous.close()
    }

    // Without this the assertion above would also hold if the interceptor were simply never
    // wired up, so prove it does run once the caller authenticates.
    val authenticated = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=$token")
      .create()
    try {
      assert(authenticated.range(5).collect().length === 5)
      assert(
        CallTrackingInterceptor.ran,
        "the configured interceptor never ran, so this test proves nothing")
    } finally {
      authenticated.close()
    }
  }
}
