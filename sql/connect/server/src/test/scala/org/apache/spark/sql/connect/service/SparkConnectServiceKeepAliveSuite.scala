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
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkException
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.connect.client.{RetryPolicy, SparkConnectClient}
import org.apache.spark.sql.connect.config.Connect

/**
 * End-to-end test that the *real* `SparkConnectService.startGRPCService()` wiring actually
 * applies the configured gRPC keepalive settings (SPARK-58094).
 *
 * Unlike the tests in `SparkConnectClientSuite` (sql/connect/client/jvm), which hand-build their
 * own `NettyServerBuilder` that only mirrors the production server construction, this suite
 * starts the real `SparkConnectService` (via `SparkConnectServerTest`) with the keepalive confs
 * tuned to short test values, puts a freezable TCP relay in front of it, and confirms a blocked
 * client call fails with a keepalive-triggered `UNAVAILABLE` within a bounded time -- so a future
 * refactor of `startGRPCService()` that silently drops the keepalive wiring would be caught here,
 * not just in the client-side unit tests.
 */
class SparkConnectServiceKeepAliveSuite extends SparkConnectServerTest {

  // CONNECT_GRPC_KEEPALIVE_TIME/TIMEOUT are read via timeConf(TimeUnit.SECONDS), so anything
  // sub-second (e.g. "300ms") truncates to 0 whole seconds and gRPC rejects it -- use the
  // smallest whole-second value instead.
  private val keepAliveMs = 1000L

  override protected def extraServerConfs: Seq[(String, String)] = Seq(
    Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "true",
    Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "1s",
    Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "1s")

  test("SPARK-58094: real SparkConnectService applies configured keepalive end-to-end") {
    val serverSession =
      SparkConnectService
        .getOrCreateIsolatedSession(defaultUserId, defaultSessionId, None)
        .session
    serverSession.udf.register(
      "sleep",
      (ms: Int) => {
        Thread.sleep(ms.toLong)
        ms
      })

    val relay = new FreezableTcpRelay(serverPort)
    try {
      val client = SparkConnectClient
        .builder()
        .port(relay.port)
        .sessionId(defaultSessionId)
        .userId(defaultUserId)
        .enableReattachableExecute()
        .grpcKeepAliveEnabled(true)
        .grpcKeepAliveTimeMs(keepAliveMs)
        .grpcKeepAliveTimeoutMs(keepAliveMs)
        .retryPolicy(RetryPolicy(maxRetries = Some(0), canRetry = _ => false, name = "NoRetry"))
        .build()
      try {
        val operationId = UUID.randomUUID().toString
        val iter = client.execute(buildPlan("select sleep(30000) as s"), Some(operationId))

        // Wait for the query to genuinely start server-side before freezing, mirroring the
        // manual repro's timing lesson: freezing too early (before the connection is actually
        // established/in-flight) doesn't exercise the "dies while blocked" scenario.
        Eventually.eventually(timeout(eventuallyTimeout)) {
          assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
        }
        relay.freeze()

        // scalastyle:off awaitresult
        val ex = intercept[SparkException] {
          scala.concurrent.Await.result(
            scala.concurrent.Future {
              while (iter.hasNext) iter.next()
            }(scala.concurrent.ExecutionContext.global),
            FiniteDuration(15, TimeUnit.SECONDS))
        }
        // scalastyle:on awaitresult
        // A keepalive-triggered UNAVAILABLE carries no wrapped cause (same as
        // DEADLINE_EXCEEDED, see GrpcExceptionConverter.toThrowable), so the status
        // code/description is only in the message.
        assert(ex.getMessage.contains("UNAVAILABLE"))
        assert(ex.getMessage.contains("Keepalive failed"))
      } finally {
        client.shutdown()
      }
    } finally {
      relay.close()
    }
  }

  test(
    "SPARK-58094: keepalive does not disrupt a healthy long-lived connection through the " +
      "real server") {
    // Exercises the scenario flagged in review: a client whose ping cadence (11s) is faster
    // than the server's own keepAlive.time (20s), through the real startGRPCService() wiring.
    // With permitKeepAliveTime derived from the server's own keepAlive.time (the pre-review
    // behavior), this client's cadence would violate that coupled invariant; with today's fixed,
    // decoupled GRPC_KEEPALIVE_PERMIT_TIME_SECONDS floor (10s), it's comfortably tolerated.
    val clientKeepAliveMs = (SparkConnectService.GRPC_KEEPALIVE_PERMIT_TIME_SECONDS + 1) * 1000
    SparkConnectService.stop()
    withSparkEnvConfs(
      Connect.CONNECT_GRPC_BINDING_PORT.key -> serverPort.toString,
      Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "true",
      Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "20s",
      Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "5s") {
      SparkConnectService.start(spark.sparkContext)
    }
    try {
      val client = SparkConnectClient
        .builder()
        .port(serverPort)
        .sessionId(defaultSessionId)
        .userId(defaultUserId)
        .grpcKeepAliveEnabled(true)
        .grpcKeepAliveTimeMs(clientKeepAliveMs)
        .grpcKeepAliveTimeoutMs(clientKeepAliveMs)
        .retryPolicy(RetryPolicy(maxRetries = Some(0), canRetry = _ => false, name = "NoRetry"))
        .build()
      try {
        // Several real calls spaced out beyond the client's keepalive interval, so the client's
        // own periodic PINGs land against the real server wiring: if permitKeepAliveTime were
        // ever re-derived from the server's own keepAlive.time (reintroducing the coupling
        // flagged in review) or its wiring silently dropped, this is the exact shape of client
        // that would be affected.
        for (i <- 1 to 2) {
          val operationId = UUID.randomUUID().toString
          val iter = client.execute(buildPlan("select 1 as s"), Some(operationId))
          while (iter.hasNext) iter.next()
          Thread.sleep(clientKeepAliveMs + 2000)
        }
      } finally {
        client.shutdown()
      }
    } finally {
      SparkConnectService.stop()
      withSparkEnvConfs(
        Connect.CONNECT_GRPC_BINDING_PORT.key -> serverPort.toString,
        Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "true",
        Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "1s",
        Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "1s") {
        SparkConnectService.start(spark.sparkContext)
      }
    }
  }

  test(
    "SPARK-58094: server tolerates a default-on client's PINGs even with its own keepalive " +
      "detection disabled") {
    // Exercises the scenario flagged in review: permitKeepAliveTime/permitKeepAliveWithoutCalls
    // used to be set only inside `if (CONNECT_GRPC_KEEPALIVE_ENABLED)`, so disabling only the
    // server's own dead-client detection reverted permit-tolerance to grpc-netty's defaults
    // (permitKeepAliveTime=5min, permitKeepAliveWithoutCalls=false). With no outstanding calls,
    // grpc's KeepAliveEnforcer then requires 2 hours (its IMPLICIT_PERMIT_TIME_NANOS grace
    // period for permitWithoutCalls=false) between "acceptable" pings instead of the configured
    // permit floor, so a still-default-on client's periodic without-calls PINGs would each
    // strike, and the 3rd strike (io.grpc.internal.KeepAliveEnforcer.MAX_PING_STRIKES=2)
    // forcefully closes the connection (ForcefulCloseCommand) as too_many_pings -- even though
    // nothing was ever actually wrong with the connection. With the permit calls hoisted out of
    // the `if`, the enforcer instead uses the fixed 10s permit floor regardless of
    // CONNECT_GRPC_KEEPALIVE_ENABLED, comfortably tolerating an 11s client ping interval, so no
    // strikes accrue and the connection is never closed.
    //
    // A test loop that issues real RPC calls between pings cannot observe this: every server
    // write (response headers/data) resets the enforcer's strike counter
    // (NettyServerHandler$WriteMonitoringFrameWriter), so pings are only ever checked against a
    // truly idle connection -- hence one call to establish the transport, then real idle time
    // (no further calls) spanning several ping intervals, then a connection-count check.
    val clientKeepAliveMs = (SparkConnectService.GRPC_KEEPALIVE_PERMIT_TIME_SECONDS + 1) * 1000
    SparkConnectService.stop()
    withSparkEnvConfs(
      Connect.CONNECT_GRPC_BINDING_PORT.key -> serverPort.toString,
      Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "false",
      Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "1s",
      Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "1s") {
      SparkConnectService.start(spark.sparkContext)
    }
    val relay = new FreezableTcpRelay(serverPort)
    try {
      val client = SparkConnectClient
        .builder()
        .port(relay.port)
        .sessionId(defaultSessionId)
        .userId(defaultUserId)
        .grpcKeepAliveEnabled(true)
        .grpcKeepAliveTimeMs(clientKeepAliveMs)
        .grpcKeepAliveTimeoutMs(clientKeepAliveMs)
        .retryPolicy(RetryPolicy(maxRetries = Some(0), canRetry = _ => false, name = "NoRetry"))
        .build()
      try {
        def runOneCall(): Unit = {
          val operationId = UUID.randomUUID().toString
          val iter = client.execute(buildPlan("select 1 as s"), Some(operationId))
          while (iter.hasNext) iter.next()
        }

        // Establishes the transport; this write resets the enforcer's strike counter, so the
        // idle window below starts clean.
        runOneCall()
        val connectionsAfterFirstCall = relay.connectionCount

        // Stay genuinely idle (no calls) for several client ping intervals, so at least 3
        // without-calls PINGs land on the server with nothing resetting the strike counter in
        // between -- enough to trip too_many_pings if the permit calls were re-gated.
        Thread.sleep(clientKeepAliveMs * 4)

        runOneCall()
        assert(
          relay.connectionCount == connectionsAfterFirstCall,
          "server forcefully closed the connection under a default-on client's idle keepalive " +
            "PINGs (too_many_pings) even though its own keepAlive.enabled was false -- the " +
            "client had to open a new TCP connection to complete the second call")
      } finally {
        client.shutdown()
      }
    } finally {
      relay.close()
      SparkConnectService.stop()
      withSparkEnvConfs(
        Connect.CONNECT_GRPC_BINDING_PORT.key -> serverPort.toString,
        Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "true",
        Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "1s",
        Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "1s") {
        SparkConnectService.start(spark.sparkContext)
      }
    }
  }

  test(
    "SPARK-58094: disabling spark.connect.grpc.keepAlive.enabled reverts to the pre-fix hang") {
    // Restart the real service with keepalive fully disabled (not just given short/aggressive
    // timing) to prove the flag genuinely gates the fix rather than only tuning its timing.
    SparkConnectService.stop()
    withSparkEnvConfs(
      Connect.CONNECT_GRPC_BINDING_PORT.key -> serverPort.toString,
      Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "false",
      Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "1s",
      Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "1s") {
      SparkConnectService.start(spark.sparkContext)
    }
    try {
      val serverSession =
        SparkConnectService
          .getOrCreateIsolatedSession(defaultUserId, defaultSessionId, None)
          .session
      serverSession.udf.register(
        "sleep",
        (ms: Int) => {
          Thread.sleep(ms.toLong)
          ms
        })

      val relay = new FreezableTcpRelay(serverPort)
      try {
        val client = SparkConnectClient
          .builder()
          .port(relay.port)
          .sessionId(defaultSessionId)
          .userId(defaultUserId)
          .enableReattachableExecute()
          .grpcKeepAliveEnabled(false)
          .grpcKeepAliveTimeMs(keepAliveMs)
          .grpcKeepAliveTimeoutMs(keepAliveMs)
          .retryPolicy(RetryPolicy(maxRetries = Some(0), canRetry = _ => false, name = "NoRetry"))
          .build()
        try {
          val operationId = UUID.randomUUID().toString
          val iter = client.execute(buildPlan("select sleep(30000) as s"), Some(operationId))

          Eventually.eventually(timeout(eventuallyTimeout)) {
            assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
          }
          relay.freeze()

          // With keepalive disabled on both sides, the call must NOT resolve within a window
          // well past the 1s/1s that would have triggered detection if it were enabled --
          // confirming the flag genuinely disables the fix, matching the original bug's
          // "blocks forever, no exception" symptom, rather than the call just being slow.
          // scalastyle:off awaitresult
          val stillBlocked =
            try {
              scala.concurrent.Await.result(
                scala.concurrent.Future {
                  while (iter.hasNext) iter.next()
                }(scala.concurrent.ExecutionContext.global),
                FiniteDuration(5, TimeUnit.SECONDS))
              false
            } catch {
              case _: java.util.concurrent.TimeoutException => true
            }
          // scalastyle:on awaitresult
          assert(stillBlocked, "expected the call to remain hung with keepalive disabled")
        } finally {
          client.shutdown()
        }
      } finally {
        relay.close()
      }
    } finally {
      // Restore the enabled server for afterAll()/subsequent tests in this suite.
      SparkConnectService.stop()
      withSparkEnvConfs(
        Connect.CONNECT_GRPC_BINDING_PORT.key -> serverPort.toString,
        Connect.CONNECT_GRPC_KEEPALIVE_ENABLED.key -> "true",
        Connect.CONNECT_GRPC_KEEPALIVE_TIME.key -> "1s",
        Connect.CONNECT_GRPC_KEEPALIVE_TIMEOUT.key -> "1s") {
        SparkConnectService.start(spark.sparkContext)
      }
    }
  }
}

/**
 * A minimal bidirectional TCP relay that can be "frozen" mid-connection without closing either
 * side's socket -- simulating a NAT gateway/load balancer/corporate proxy that silently drops an
 * idle connection's mapping (no TCP RST/FIN sent to either endpoint). Mirrors the equivalent
 * helper in `SparkConnectClientSuite` (sql/connect/client/jvm); duplicated here rather than
 * shared across modules since it's a small, self-contained test utility.
 */
private class FreezableTcpRelay(targetPort: Int) {
  private val listener = new java.net.ServerSocket(0)
  @volatile private var frozen = false
  private val sockets = mutable.ListBuffer[java.net.Socket]()
  private val acceptedConnections = new java.util.concurrent.atomic.AtomicInteger(0)

  private val acceptThread = new Thread(() => {
    try {
      while (true) {
        val clientSocket = listener.accept()
        acceptedConnections.incrementAndGet()
        val serverSocket = new java.net.Socket("127.0.0.1", targetPort)
        sockets.synchronized {
          sockets += clientSocket
          sockets += serverSocket
        }
        pump(clientSocket, serverSocket)
        pump(serverSocket, clientSocket)
      }
    } catch {
      case _: java.io.IOException => // listener closed, relay shutting down
    }
  })
  acceptThread.setDaemon(true)
  acceptThread.start()

  def port: Int = listener.getLocalPort

  // Number of distinct client TCP connections accepted so far -- useful to detect a
  // client-transparent reconnect (e.g. after the server forcefully closes the connection)
  // that wouldn't otherwise surface as a visible failure to the application.
  def connectionCount: Int = acceptedConnections.get()

  // Once frozen, bytes read from either socket are dropped rather than forwarded: no data
  // (including keepalive PING/PONG frames) reaches the other side, but neither socket is
  // closed -- the same observable behavior as a middlebox silently black-holing the connection.
  def freeze(): Unit = frozen = true

  private def pump(src: java.net.Socket, dst: java.net.Socket): Unit = {
    val t = new Thread(() => {
      try {
        val in = src.getInputStream
        val out = dst.getOutputStream
        val buf = new Array[Byte](8192)
        var n = 0
        while ({ n = in.read(buf); n != -1 }) {
          if (!frozen) {
            out.write(buf, 0, n)
            out.flush()
          }
        }
      } catch {
        case _: java.io.IOException => // socket closed, nothing left to pump
      }
    })
    t.setDaemon(true)
    t.start()
  }

  def close(): Unit = {
    listener.close()
    sockets.synchronized(sockets.foreach { s =>
      try {
        s.close()
      } catch {
        case _: java.io.IOException =>
      }
    })
  }
}
