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

import io.grpc.{ConnectivityState, ManagedChannel, Server}
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder, NettyServerBuilder}
import io.grpc.protobuf.services.ProtoReflectionService
import io.netty.handler.ssl.SslContext
import org.apache.logging.log4j.Level

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SparkFunSuite}

/**
 * Unit tests for `SparkConnectService.buildConnectSslContext`, which reads the
 * `spark.ssl.connect.*` namespace via `SSLOptions` and returns a Netty `SslContext` when TLS is
 * enabled. Also covers the mTLS branch (client-cert verification).
 */
class SparkConnectServiceTlsSuite extends SparkFunSuite {

  private def resourcePath(name: String): String = {
    val url = getClass.getClassLoader.getResource(s"connect-tls/$name")
    assert(url != null, s"missing test resource connect-tls/$name")
    new File(url.toURI).getAbsolutePath
  }

  private def enabledConf(extra: (String, String)*): SparkConf = {
    val conf = new SparkConf()
      .set("spark.ssl.connect.enabled", "true")
      .set("spark.ssl.connect.certChain", resourcePath("cert.pem"))
      .set("spark.ssl.connect.privateKey", resourcePath("key.pem"))
    extra.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  private def mtlsConf(extra: (String, String)*): SparkConf = {
    val conf = new SparkConf()
      .set("spark.ssl.connect.enabled", "true")
      .set("spark.ssl.connect.certChain", resourcePath("server-cert.pem"))
      .set("spark.ssl.connect.privateKey", resourcePath("server-key.pem"))
      .set("spark.ssl.connect.needClientAuth", "true")
      .set("spark.ssl.connect.trustStore", resourcePath("truststore.jks"))
      .set("spark.ssl.connect.trustStorePassword", "changeit")
    extra.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * Start a Netty gRPC server on an ephemeral port with only the reflection service bound, so the
   * handshake path is exercised end-to-end without dragging in SparkSession or the Connect
   * handlers. Returns (server, port).
   */
  private def startTlsServer(sslCtx: SslContext): (Server, Int) = {
    val server = NettyServerBuilder
      .forPort(0)
      .sslContext(sslCtx)
      .addService(ProtoReflectionService.newInstance())
      .build()
    server.start()
    (server, server.getPort)
  }

  /** Open a TLS client channel with an optional client cert. */
  private def openClientChannel(
      port: Int,
      trustPem: File,
      clientCertPem: Option[File],
      clientKeyPem: Option[File]): ManagedChannel = {
    val builder = GrpcSslContexts.forClient().trustManager(trustPem)
    (clientCertPem, clientKeyPem) match {
      case (Some(c), Some(k)) => builder.keyManager(c, k)
      case _ =>
    }
    NettyChannelBuilder
      .forAddress("localhost", port)
      .overrideAuthority("localhost")
      .sslContext(builder.build())
      .build()
  }

  /** Poll the channel state until it reaches READY or TRANSIENT_FAILURE. */
  private def awaitConnected(channel: ManagedChannel, timeoutMs: Long): ConnectivityState = {
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    var state = channel.getState(true)
    while (state != ConnectivityState.READY && state != ConnectivityState.TRANSIENT_FAILURE
      && System.nanoTime() < deadline) {
      val remainingMs = math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))
      channel.notifyWhenStateChanged(state, new Runnable { override def run(): Unit = () })
      Thread.sleep(math.min(50L, remainingMs))
      state = channel.getState(true)
    }
    state
  }

  test("disabled by default returns None") {
    val sm = new SecurityManager(new SparkConf())
    assert(SparkConnectService.buildConnectSslContext(sm).isEmpty)
  }

  test("does not inherit spark.ssl.enabled") {
    val conf = new SparkConf().set("spark.ssl.enabled", "true")
    val sm = new SecurityManager(conf)
    assert(SparkConnectService.buildConnectSslContext(sm).isEmpty)
  }

  test("enabled with PEM builds an SslContext") {
    val sm = new SecurityManager(enabledConf())
    val ctx = SparkConnectService.buildConnectSslContext(sm)
    assert(ctx.isDefined)
    assert(ctx.get.isServer)
  }

  test("enabled context wires into NettyServerBuilder without error") {
    val sm = new SecurityManager(enabledConf())
    val ctx = SparkConnectService.buildConnectSslContext(sm).get
    NettyServerBuilder.forPort(0).sslContext(ctx)
  }

  test("enabled without certChain fails fast") {
    val conf = new SparkConf()
      .set("spark.ssl.connect.enabled", "true")
      .set("spark.ssl.connect.privateKey", resourcePath("key.pem"))
    val e = intercept[SparkException] {
      SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
    }
    assert(e.getMessage.contains("spark.ssl.connect.certChain"))
  }

  test("enabled without privateKey fails fast") {
    val conf = new SparkConf()
      .set("spark.ssl.connect.enabled", "true")
      .set("spark.ssl.connect.certChain", resourcePath("cert.pem"))
    val e = intercept[SparkException] {
      SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
    }
    assert(e.getMessage.contains("spark.ssl.connect.privateKey"))
  }

  test("encrypted private key with privateKeyPassword builds an SslContext") {
    val conf = new SparkConf()
      .set("spark.ssl.connect.enabled", "true")
      .set("spark.ssl.connect.certChain", resourcePath("server-cert.pem"))
      .set("spark.ssl.connect.privateKey", resourcePath("server-key-encrypted.pem"))
      .set("spark.ssl.connect.privateKeyPassword", "changeit")
    val ctx = SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
    assert(ctx.exists(_.isServer))
  }

  test("openSslEnabled=true is rejected with a clear error") {
    val conf = enabledConf().set("spark.ssl.connect.openSslEnabled", "true")
    val e = intercept[SparkException] {
      SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
    }
    assert(e.getMessage.contains("spark.ssl.connect.openSslEnabled"))
    assert(e.getMessage.contains("not yet supported"))
  }

  test("mTLS: needClientAuth=true without trustStore fails fast") {
    val conf = enabledConf().set("spark.ssl.connect.needClientAuth", "true")
    val e = intercept[SparkException] {
      SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
    }
    assert(e.getMessage.contains("spark.ssl.connect.trustStore"))
    assert(e.getMessage.contains("needClientAuth"))
  }

  test("mTLS: corrupt trustStore fails fast naming the path") {
    withTempDir { dir =>
      val bogus = new File(dir, "bogus-truststore.jks")
      java.nio.file.Files.write(bogus.toPath, Array[Byte](0, 1, 2, 3, 4))
      val conf = enabledConf()
        .set("spark.ssl.connect.needClientAuth", "true")
        .set("spark.ssl.connect.trustStore", bogus.getAbsolutePath)
        .set("spark.ssl.connect.trustStorePassword", "changeit")
      val e = intercept[SparkException] {
        SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
      }
      assert(e.getMessage.contains(bogus.getAbsolutePath))
      assert(e.getMessage.contains("spark.ssl.connect.trustStore"))
    }
  }

  test("mTLS: trustStoreReloadingEnabled logs a warning and still loads statically") {
    val conf = mtlsConf("spark.ssl.connect.trustStoreReloadingEnabled" -> "true")
    val logs = new LogAppender()
    var ctx: Option[SslContext] = None
    withLogAppender(logs, level = Some(Level.WARN)) {
      ctx = SparkConnectService.buildConnectSslContext(new SecurityManager(conf))
    }
    assert(ctx.exists(_.isServer))
    assert(
      logs.loggingEvents.exists(_.getMessage.getFormattedMessage
        .contains("trustStoreReloadingEnabled=true is not yet supported")))
  }

  test("mTLS: trusted client cert handshake succeeds") {
    withMtlsChannel(
      clientCertPem = Some(new File(resourcePath("client-cert.pem"))),
      clientKeyPem = Some(new File(resourcePath("client-key.pem"))))(expected =
      ConnectivityState.READY)
  }

  test("mTLS: no client cert is rejected") {
    withMtlsChannel(clientCertPem = None, clientKeyPem = None)(expected =
      ConnectivityState.TRANSIENT_FAILURE)
  }

  test("mTLS: untrusted client cert is rejected") {
    withMtlsChannel(
      clientCertPem = Some(new File(resourcePath("untrusted-client-cert.pem"))),
      clientKeyPem = Some(new File(resourcePath("untrusted-client-key.pem"))))(expected =
      ConnectivityState.TRANSIENT_FAILURE)
  }

  private def withMtlsChannel(clientCertPem: Option[File], clientKeyPem: Option[File])(
      expected: ConnectivityState): Unit = {
    val sm = new SecurityManager(mtlsConf())
    val ctx = SparkConnectService.buildConnectSslContext(sm).get
    val (server, port) = startTlsServer(ctx)
    try {
      val channel = openClientChannel(
        port,
        trustPem = new File(resourcePath("ca.pem")),
        clientCertPem = clientCertPem,
        clientKeyPem = clientKeyPem)
      try {
        val state = awaitConnected(channel, timeoutMs = 10000L)
        assert(state == expected, s"expected $expected, got $state")
      } finally {
        channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS)
      }
    } finally {
      server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS)
    }
  }
}
