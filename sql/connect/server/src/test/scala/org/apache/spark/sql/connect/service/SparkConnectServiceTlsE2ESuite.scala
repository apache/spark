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

import java.io.File
import java.util.concurrent.TimeUnit

import io.grpc.ConnectivityState
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}

import org.apache.spark.sql.connect.SparkConnectServerTest

/**
 * End-to-end regression test that boots the real `SparkConnectService.start(sc)` with
 * `spark.ssl.connect.*` configured for mTLS and verifies a TLS-aware Netty client channel can
 * complete a handshake. Guards against future refactors of `startGRPCService()` that silently
 * drop the `sb.sslContext(_)` wiring -- unit tests of `buildConnectSslContext` alone would not
 * catch that regression.
 *
 * Kept minimal (one happy-path case) because `SharedSparkSession` boot is per-class.
 */
class SparkConnectServiceTlsE2ESuite extends SparkConnectServerTest {

  private def resourcePath(name: String): String = {
    val url = getClass.getClassLoader.getResource(s"connect-tls/$name")
    assert(url != null, s"missing test resource connect-tls/$name")
    new File(url.toURI).getAbsolutePath
  }

  override protected def extraServerConfs: Seq[(String, String)] = Seq(
    "spark.ssl.connect.enabled" -> "true",
    "spark.ssl.connect.certChain" -> resourcePath("server-cert.pem"),
    "spark.ssl.connect.privateKey" -> resourcePath("server-key.pem"),
    "spark.ssl.connect.needClientAuth" -> "true",
    "spark.ssl.connect.trustStore" -> resourcePath("truststore.jks"),
    "spark.ssl.connect.trustStorePassword" -> "changeit")

  test("SPARK-58622: real SparkConnectService applies configured mTLS end-to-end") {
    val sslCtx = GrpcSslContexts
      .forClient()
      .trustManager(new File(resourcePath("ca.pem")))
      .keyManager(
        new File(resourcePath("client-cert.pem")),
        new File(resourcePath("client-key.pem")))
      .build()
    val channel = NettyChannelBuilder
      .forAddress("localhost", serverPort)
      .overrideAuthority("localhost")
      .sslContext(sslCtx)
      .build()
    try {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L)
      var state = channel.getState(true)
      while (state != ConnectivityState.READY && state != ConnectivityState.TRANSIENT_FAILURE
        && System.nanoTime() < deadline) {
        Thread.sleep(50L)
        state = channel.getState(true)
      }
      assert(
        state == ConnectivityState.READY,
        s"expected READY after valid mTLS handshake against real SparkConnectService, got $state")
    } finally {
      channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS)
    }
  }
}
