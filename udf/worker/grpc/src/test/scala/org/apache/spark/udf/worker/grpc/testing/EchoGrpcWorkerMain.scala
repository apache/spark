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
package org.apache.spark.udf.worker.grpc.testing

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.EventLoopGroup
import io.netty.channel.unix.DomainSocketAddress

import org.apache.spark.udf.worker.grpc.{GrpcWorkerChannel, UnixDomainSocketTransport}

/**
 * Test-only worker binary. Spawned by integration tests via the standard
 * worker spec callable contract:
 *
 *   java -cp <classpath> org.apache.spark.udf.worker.grpc.testing.EchoGrpcWorkerMain
 *     --id <id> --connection <uds-path>
 *
 * Hosts an [[EchoWorkerService]] on the supplied Unix domain socket and
 * blocks until the JVM receives `SIGTERM` (then shuts down gracefully).
 *
 * The engine injects `--id` and `--connection` automatically, matching the
 * contract documented in `worker_spec.proto`.
 */
object EchoGrpcWorkerMain {

  // scalastyle:off println
  private def stderrln(s: String): Unit = System.err.println(s"[echo-worker] $s")

  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args)
    val socketPath = parsed.getOrElse("connection",
      throw new IllegalArgumentException("--connection <uds-path> is required"))
    val workerId = parsed.getOrElse("id", "<unknown>")

    val transport = UnixDomainSocketTransport.detect()
    val sockFile = Paths.get(socketPath)
    val cleanupStarted = new AtomicBoolean(false)
    var bossElg: EventLoopGroup = null
    var workerElg: EventLoopGroup = null
    var server: Server = null
    var shutdownHook: Thread = null

    def cleanup(): Unit = {
      if (!cleanupStarted.compareAndSet(false, true)) return
      try {
        if (server != null) {
          server.shutdown()
          if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow()
          }
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
        case NonFatal(e) => stderrln(s"failed to shut down gRPC server: ${e.getMessage}")
      } finally {
        try UnixDomainSocketTransport.shutdown(bossElg, 5000L) catch {
          case NonFatal(e) => stderrln(s"failed to shut down boss event loop: ${e.getMessage}")
        }
        try UnixDomainSocketTransport.shutdown(workerElg, 5000L) catch {
          case NonFatal(e) => stderrln(s"failed to shut down worker event loop: ${e.getMessage}")
        }
        // The dispatcher will also delete the socket file, but we delete it
        // here so a worker that ran without an engine cleans up after itself.
        try { Files.deleteIfExists(sockFile) } catch { case _: IOException => () }
      }
    }

    try {
      bossElg = transport.newEventLoopGroup()
      workerElg = transport.newEventLoopGroup()

      // Ensure the parent directory exists; the engine should already have
      // created it, but be defensive in case the test points at an unusual
      // path.
      val parent = sockFile.getParent
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent)
      }
      // The OS rejects bind() if the path already exists. Remove any stale file
      // from a previous run on a best-effort basis.
      try { Files.deleteIfExists(sockFile) } catch { case _: IOException => () }

      server = NettyServerBuilder
        .forAddress(new DomainSocketAddress(socketPath))
        .channelType(transport.serverChannelType)
        .bossEventLoopGroup(bossElg)
        .workerEventLoopGroup(workerElg)
        .maxInboundMessageSize(GrpcWorkerChannel.MAX_INBOUND_MESSAGE_SIZE)
        .addService(new EchoWorkerService)
        .build()

      // SIGTERM is the engine's graceful-stop signal per the worker spec.
      // SIGINT is added so manual testing (Ctrl-C) also shuts down cleanly.
      shutdownHook = new Thread(() => {
        stderrln(s"shutdown signal received (worker=$workerId)")
        cleanup()
      }, s"echo-worker-shutdown-$workerId")
      // This standalone test-worker main has no Spark on its classpath, so it
      // cannot use Spark's ShutdownHookManager and registers directly.
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(shutdownHook)
      // scalastyle:on runtimeaddshutdownhook

      server.start()
      stderrln(s"listening on $socketPath (transport=${transport.name})")
      // Park until the shutdown hook fires.
      server.awaitTermination()
    } catch {
      case NonFatal(e) =>
        stderrln(s"gRPC worker failed: ${e.getMessage}")
        throw e
    } finally {
      cleanup()
      if (shutdownHook != null) {
        try { Runtime.getRuntime.removeShutdownHook(shutdownHook) } catch {
          case _: IllegalStateException => () // JVM shutting down already
          case NonFatal(e) => stderrln(s"failed to remove shutdown hook: ${e.getMessage}")
        }
      }
    }
  }
  // scalastyle:on println

  /**
   * Parses `--key value` flags. The engine guarantees `--id` and
   * `--connection` are present; unknown flags are ignored so future
   * engine-side additions don't break older workers.
   */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    val out = scala.collection.mutable.Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      val a = args(i)
      if (a.startsWith("--") && i + 1 < args.length) {
        out(a.stripPrefix("--")) = args(i + 1)
        i += 2
      } else {
        i += 1
      }
    }
    out.toMap
  }
}
