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

  private val acceptThread = new Thread(() => {
    try {
      while (true) {
        val clientSocket = listener.accept()
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
