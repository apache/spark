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
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
 * Test-only launcher that stands in for a sandbox or container launcher. It
 * receives the engine's `--id` and `--connection`, starts the real worker on a
 * different socket, and forwards each engine connection to it. This is the
 * shape a launcher has when the worker cannot bind the engine's socket itself:
 *
 *   java -cp <classpath> ...ForwardingWorkerMain <worker command...>
 *     --id <id> --connection <uds-path>
 *
 * The inner worker exits when this launcher exits, including by `SIGKILL`.
 * `SIGTERM` stops the inner worker with `SIGTERM` and waits for it.
 */
object ForwardingWorkerMain {

  def main(args: Array[String]): Unit = {
    val connection = args.indexOf("--connection")
    require(connection >= 0 && connection + 1 < args.length, "--connection is required")
    val socketPath = args(connection + 1)
    val innerPath = socketPath + ".inner"
    val workerCommand =
      args.take(connection) ++ args.drop(connection + 2) ++ Seq("--connection", innerPath)

    Files.deleteIfExists(Paths.get(innerPath))
    val worker = new ProcessBuilder(workerCommand.toSeq.asJava).inheritIO().start()
    // This standalone test-worker main has no Spark on its classpath, so it
    // cannot use Spark's ShutdownHookManager and registers directly.
    // scalastyle:off runtimeaddshutdownhook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      worker.destroy()
      if (!worker.waitFor(2, TimeUnit.SECONDS)) worker.destroyForcibly()
      Files.deleteIfExists(Paths.get(innerPath))
    }, "forwarding-worker-shutdown"))
    // scalastyle:on runtimeaddshutdownhook

    while (!Files.exists(Paths.get(innerPath))) {
      if (!worker.isAlive) sys.exit(worker.exitValue())
      Thread.sleep(20)
    }
    new Thread(() => sys.exit(worker.waitFor()), "forwarding-worker-exit").start()

    val server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    server.bind(UnixDomainSocketAddress.of(socketPath))
    while (true) {
      val client = server.accept()
      val inner = SocketChannel.open(UnixDomainSocketAddress.of(innerPath))
      pump(client, inner)
      pump(inner, client)
    }
  }

  private def pump(from: SocketChannel, to: SocketChannel): Unit = {
    val thread = new Thread(() => {
      val buffer = ByteBuffer.allocate(64 * 1024)
      try {
        while (from.read(buffer) >= 0) {
          buffer.flip()
          while (buffer.hasRemaining) to.write(buffer)
          buffer.clear()
        }
        to.shutdownOutput()
      } catch {
        case _: IOException => closeQuietly(from); closeQuietly(to)
      }
    }, "forwarding-worker-pump")
    thread.setDaemon(true)
    thread.start()
  }

  private def closeQuietly(channel: SocketChannel): Unit =
    try channel.close() catch { case NonFatal(_) => () }
}
