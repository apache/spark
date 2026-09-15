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

import java.util.concurrent.atomic.AtomicInteger

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.connect.{SparkConnectServerTest, SparkSession}
import org.apache.spark.sql.connect.config.Connect

/**
 * Counts the calls it is handed, so a test can tell whether it ran at all. Needs a no-argument
 * constructor to be loadable from `spark.connect.grpc.interceptor.classes`.
 */
class CallCountingInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    CallCountingInterceptor.calls.incrementAndGet()
    next.startCall(call, headers)
  }
}

object CallCountingInterceptor {
  val calls = new AtomicInteger(0)
}

/**
 * Tests that authentication runs ahead of the other interceptors, rather than behind them.
 *
 * A `ServerBuilder` invokes interceptors in the reverse of the order they were added, so where
 * `PreSharedKeyAuthenticationInterceptor` is registered decides how much of the pipeline an
 * unauthenticated caller can drive before being turned away.
 */
class SparkConnectAuthInterceptorOrderSuite extends SparkConnectServerTest {

  private val token = "deadbeef"

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(Connect.CONNECT_AUTHENTICATE_TOKEN.key, token)

  override protected def extraServerConfs: Seq[(String, String)] = Seq(
    Connect.CONNECT_GRPC_INTERCEPTOR_CLASSES.key -> classOf[CallCountingInterceptor].getName)

  test("an unauthenticated call is rejected before the configured interceptors run") {
    CallCountingInterceptor.calls.set(0)

    val anonymous = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/")
      .create()
    val e = intercept[SparkException](anonymous.range(5).collect())
    assert(e.getMessage.contains("No authentication token provided"))
    assert(
      CallCountingInterceptor.calls.get() === 0,
      "a configured interceptor ran for a call that failed authentication")

    // Without this the assertion above would also hold if the interceptor were simply never
    // wired up, so prove it does run once the caller authenticates.
    val authenticated = SparkSession
      .builder()
      .remote(s"sc://localhost:${SparkConnectService.localPort}/;token=$token")
      .create()
    assert(authenticated.range(5).collect().length === 5)
    assert(
      CallCountingInterceptor.calls.get() > 0,
      "the configured interceptor never ran, so this suite proves nothing")
  }
}
