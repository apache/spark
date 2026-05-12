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

package org.apache.spark.network

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client._
import org.apache.spark.network.server._
import org.apache.spark.network.util._

/**
 * Benchmark for Spark's Netty transport layer.
 *
 * All suites measure performance through the actual Spark transport pipeline
 * (TransportServer + TransportClientFactory + TransportContext).
 *
 * Suite overview:
 *   1. RPC Latency            - driver-executor RPC overhead at different payload sizes
 *   2. Concurrent Throughput  - multi-executor pressure on the transport layer
 *   3. IOMode Comparison      - NIO vs native transport (AUTO selects EPOLL/KQUEUE)
 *   4. Server Thread Scaling  - validates MAX_DEFAULT_NETTY_THREADS=8 cap
 *   5. Multi-Connection       - numConnectionsPerPeer=1 vs 2 vs 4
 *   6. Async Write Pressure   - fire-and-forget RPCs to saturate the write path
 *   7. Large Block Transfer   - shuffle-like 16MB block transfers (in-memory payload)
 *   8. File-Backed Shuffle    - ChunkFetch from disk, NIO vs AUTO (EPOLL sendfile bypass detection)
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/NettyTransportBenchmark-results.txt".
 * }}}
 */
object NettyTransportBenchmark extends BenchmarkBase {

  private val SMALL_PAYLOAD = 1024              // 1 KB
  private val MEDIUM_PAYLOAD = 64 * 1024        // 64 KB
  private val LARGE_PAYLOAD = 1024 * 1024       // 1 MB
  private val XLARGE_PAYLOAD = 16 * 1024 * 1024 // 16 MB

  private val RPC_ITERS = 5000
  private val THROUGHPUT_ITERS = 20000

  // Fixed 8-byte ACK response, avoids heap allocation proportional to payload.
  private val ACK_BYTES = Array[Byte](0, 0, 0, 0, 0, 0, 0, 1)

  private def createEchoRpcHandler(): RpcHandler = new RpcHandler {
    override def receive(
        client: TransportClient,
        message: ByteBuffer,
        callback: RpcResponseCallback): Unit = {
      callback.onSuccess(ByteBuffer.wrap(ACK_BYTES))
    }
    override def getStreamManager: StreamManager = new OneForOneStreamManager()
  }

  private def createConf(
      ioMode: String,
      serverThreads: Int = 4,
      clientThreads: Int = 4,
      extraConf: Map[String, String] = Map.empty): TransportConf = {
    val configMap = (Map(
      "spark.shuffle.io.mode" -> ioMode,
      "spark.shuffle.io.serverThreads" -> serverThreads.toString,
      "spark.shuffle.io.clientThreads" -> clientThreads.toString
    ) ++ extraConf).asJava
    new TransportConf("shuffle", new MapConfigProvider(configMap))
  }

  /**
   * Sets up a TransportServer + TransportClientFactory, runs fn, then tears down.
   */
  private def withTransport(conf: TransportConf)(
      fn: (TransportClientFactory, Int) => Unit): Unit = {
    val context = new TransportContext(conf, createEchoRpcHandler())
    val server = context.createServer()
    val clientFactory = context.createClientFactory()
    try {
      fn(clientFactory, server.getPort)
    } finally {
      clientFactory.close()
      server.close()
      context.close()
    }
  }

  private def withTransport(ioMode: String)(
      fn: (TransportClientFactory, Int) => Unit): Unit = {
    withTransport(createConf(ioMode))(fn)
  }

  /**
   * Sends a synchronous RPC. Throws on failure to prevent silent result corruption.
   */
  private def sendRpcSync(
      client: TransportClient,
      payload: Array[Byte],
      timeoutMs: Long = 30000): Unit = {
    val latch = new CountDownLatch(1)
    @volatile var error: Throwable = null
    client.sendRpc(ByteBuffer.wrap(payload), new RpcResponseCallback {
      override def onSuccess(response: ByteBuffer): Unit = latch.countDown()
      override def onFailure(e: Throwable): Unit = {
        error = e
        latch.countDown()
      }
    })
    if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      throw new RuntimeException("RPC timed out")
    }
    if (error != null) {
      throw new RuntimeException("RPC failed", error)
    }
  }

  /**
   * Fires RPCs asynchronously and waits for all to complete.
   * Returns (successCount, failCount).
   */
  private def sendRpcsAsync(
      client: TransportClient,
      payload: Array[Byte],
      count: Int): (Long, Long) = {
    val successCount = new AtomicLong(0)
    val failCount = new AtomicLong(0)
    val latch = new CountDownLatch(count)
    var i = 0
    while (i < count) {
      client.sendRpc(ByteBuffer.wrap(payload), new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          successCount.incrementAndGet()
          latch.countDown()
        }
        override def onFailure(e: Throwable): Unit = {
          failCount.incrementAndGet()
          latch.countDown()
        }
      })
      i += 1
    }
    latch.await(120, TimeUnit.SECONDS)
    (successCount.get(), failCount.get())
  }

  /**
   * Creates N independent (non-pooled) connections to the server.
   * Uses createUnmanagedClient to bypass TransportClientFactory's connection pool,
   * which defaults to numConnectionsPerPeer=1 and would return the same client.
   */
  private def createClients(
      factory: TransportClientFactory,
      port: Int,
      count: Int): Seq[TransportClient] = {
    (0 until count).map(_ => factory.createUnmanagedClient("localhost", port))
  }

  // ==================== Benchmark Suites ====================

  /**
   * Suite 1: RPC Round-Trip Latency at different payload sizes.
   * Each size is a separate Benchmark so valuesPerIteration and Rate are accurate.
   */
  private def rpcLatencyBenchmark(): Unit = {
    val ioMode = detectIOMode()

    Seq(
      ("1 KB", SMALL_PAYLOAD, RPC_ITERS),
      ("64 KB", MEDIUM_PAYLOAD, RPC_ITERS),
      ("1 MB", LARGE_PAYLOAD, RPC_ITERS / 5),
      ("16 MB", XLARGE_PAYLOAD, RPC_ITERS / 50)
    ).foreach { case (sizeLabel, payloadSize, iters) =>
      runBenchmark(s"RPC Round-Trip Latency - $sizeLabel payload (IOMode=$ioMode)") {
        withTransport(ioMode) { (clientFactory, port) =>
          val client = clientFactory.createUnmanagedClient("localhost", port)
          val payload = new Array[Byte](payloadSize)

          val benchmark = new Benchmark(
            s"RPC Latency ($sizeLabel)",
            iters.toLong,
            minNumIters = 3,
            output = output)

          benchmark.addTimerCase(s"$sizeLabel payload", numIters = 5) { timer =>
            sendRpcSync(client, payload) // warm up

            timer.startTiming()
            var i = 0
            while (i < iters) {
              sendRpcSync(client, payload)
              i += 1
            }
            timer.stopTiming()
          }

          benchmark.run()
          client.close()
        }
      }
    }
  }

  /**
   * Suite 2: Concurrent RPC throughput with 1-16 independent connections.
   * Uses createUnmanagedClient so each "client" is a distinct TCP connection.
   */
  private def concurrentThroughputBenchmark(): Unit = {
    val ioMode = detectIOMode()

    runBenchmark(s"Concurrent RPC Throughput (IOMode=$ioMode)") {
      withTransport(ioMode) { (clientFactory, port) =>
        val payload = new Array[Byte](SMALL_PAYLOAD)
        val totalMessages = THROUGHPUT_ITERS.toLong

        val benchmark = new Benchmark(
          "Concurrent RPC Throughput",
          totalMessages,
          minNumIters = 3,
          output = output)

        Seq(1, 4, 8, 16).foreach { numThreads =>
          benchmark.addTimerCase(s"$numThreads client(s)", numIters = 3) { timer =>
            val messagesPerThread = (totalMessages / numThreads).toInt
            val latch = new CountDownLatch(numThreads)
            val clients = createClients(clientFactory, port, numThreads)

            clients.foreach(c => sendRpcSync(c, payload))

            timer.startTiming()

            clients.foreach { client =>
              val t = new Thread(() => {
                try {
                  var i = 0
                  while (i < messagesPerThread) {
                    sendRpcSync(client, payload)
                    i += 1
                  }
                } finally {
                  latch.countDown()
                }
              })
              t.setDaemon(true)
              t.start()
            }

            latch.await(120, TimeUnit.SECONDS)
            timer.stopTiming()
            clients.foreach(_.close())
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Suite 3: IOMode Comparison (NIO vs AUTO).
   * AUTO selects the best native transport via NettyUtils.createEventLoop
   * (EPOLL on Linux, KQUEUE on macOS, NIO fallback), so comparing NIO vs AUTO
   * shows the benefit of native transport without needing manual probing.
   * Uses concurrent load (8 clients) to amplify transport-level differences.
   */
  private def ioModeComparisonBenchmark(): Unit = {
    val payload = new Array[Byte](MEDIUM_PAYLOAD)
    val totalMessages = THROUGHPUT_ITERS.toLong
    val numClients = 8

    runBenchmark("IOMode Comparison (Concurrent Throughput)") {
      val benchmark = new Benchmark(
        "IOMode Comparison",
        totalMessages,
        minNumIters = 3,
        output = output)

      Seq("NIO", "AUTO").foreach { mode =>
        benchmark.addTimerCase(s"$mode ($numClients clients)", numIters = 3) { timer =>
          withTransport(mode) { (clientFactory, port) =>
            val messagesPerClient = (totalMessages / numClients).toInt
            val latch = new CountDownLatch(numClients)
            val clients = createClients(clientFactory, port, numClients)

            clients.foreach(c => sendRpcSync(c, payload))

            timer.startTiming()

            clients.foreach { client =>
              val t = new Thread(() => {
                try {
                  var i = 0
                  while (i < messagesPerClient) {
                    sendRpcSync(client, payload)
                    i += 1
                  }
                } finally {
                  latch.countDown()
                }
              })
              t.setDaemon(true)
              t.start()
            }

            latch.await(120, TimeUnit.SECONDS)
            timer.stopTiming()
            clients.foreach(_.close())
          }
        }
      }

      benchmark.run()
    }
  }

  /**
   * Suite 4: Server thread scaling (2-32 threads) under 16-client load.
   * Shows whether raising MAX_DEFAULT_NETTY_THREADS=8 helps.
   */
  private def serverThreadScalingBenchmark(): Unit = {
    val ioMode = detectIOMode()
    val numClients = 16
    val totalMessages = THROUGHPUT_ITERS.toLong
    val payload = new Array[Byte](SMALL_PAYLOAD)

    runBenchmark(s"Server Thread Scaling (IOMode=$ioMode, $numClients clients)") {
      val benchmark = new Benchmark(
        "Server Thread Scaling",
        totalMessages,
        minNumIters = 3,
        output = output)

      Seq(2, 4, 8, 16, 32).foreach { serverThreads =>
        benchmark.addTimerCase(s"$serverThreads server threads", numIters = 3) { timer =>
          val conf = createConf(ioMode, serverThreads = serverThreads)
          withTransport(conf) { (clientFactory, port) =>
            val messagesPerClient = (totalMessages / numClients).toInt
            val latch = new CountDownLatch(numClients)
            val clients = createClients(clientFactory, port, numClients)

            clients.foreach(c => sendRpcSync(c, payload))

            timer.startTiming()

            clients.foreach { client =>
              val t = new Thread(() => {
                try {
                  var i = 0
                  while (i < messagesPerClient) {
                    sendRpcSync(client, payload)
                    i += 1
                  }
                } finally {
                  latch.countDown()
                }
              })
              t.setDaemon(true)
              t.start()
            }

            latch.await(120, TimeUnit.SECONDS)
            timer.stopTiming()
            clients.foreach(_.close())
          }
        }
      }

      benchmark.run()
    }
  }

  /**
   * Suite 5: Multi-connection per peer (1 vs 2 vs 4 connections, 4 sender threads).
   * Uses createUnmanagedClient to ensure genuinely distinct TCP connections.
   */
  private def multiConnectionBenchmark(): Unit = {
    val ioMode = detectIOMode()
    val payload = new Array[Byte](LARGE_PAYLOAD) // 1MB
    val numSenderThreads = 4
    val totalMessages = THROUGHPUT_ITERS / 4

    runBenchmark(s"Multi-Connection Per Peer (IOMode=$ioMode, 1MB payload)") {
      withTransport(ioMode) { (clientFactory, port) =>
        val benchmark = new Benchmark(
          "Multi-Connection Throughput",
          totalMessages.toLong,
          minNumIters = 3,
          output = output)

        Seq(1, 2, 4).foreach { numConns =>
          benchmark.addTimerCase(
              s"$numConns conn(s), $numSenderThreads threads", numIters = 3) { timer =>
            val messagesPerThread = totalMessages / numSenderThreads
            val clients = createClients(clientFactory, port, numConns)
            clients.foreach(c => sendRpcSync(c, payload))

            val latch = new CountDownLatch(numSenderThreads)

            timer.startTiming()
            (0 until numSenderThreads).foreach { idx =>
              val client = clients(idx % numConns)
              val t = new Thread(() => {
                try {
                  var i = 0
                  while (i < messagesPerThread) {
                    sendRpcSync(client, payload)
                    i += 1
                  }
                } finally {
                  latch.countDown()
                }
              })
              t.setDaemon(true)
              t.start()
            }
            latch.await(120, TimeUnit.SECONDS)
            timer.stopTiming()
            clients.foreach(_.close())
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Suite 6: Async write pressure - fire-and-forget RPCs to saturate the write path.
   * Reports failure count to detect backpressure effects.
   */
  private def asyncWritePressureBenchmark(): Unit = {
    val ioMode = detectIOMode()
    val asyncBatchSize = 5000

    runBenchmark(s"Async Write Pressure (IOMode=$ioMode)") {
      withTransport(ioMode) { (clientFactory, port) =>
        val benchmark = new Benchmark(
          "Async Write Throughput",
          asyncBatchSize.toLong,
          minNumIters = 3,
          output = output)

        Seq(
          ("1 KB async burst", SMALL_PAYLOAD),
          ("64 KB async burst", MEDIUM_PAYLOAD),
          ("1 MB async burst", LARGE_PAYLOAD)
        ).foreach { case (name, payloadSize) =>
          val payload = new Array[Byte](payloadSize)

          benchmark.addTimerCase(name, numIters = 3) { timer =>
            val client = clientFactory.createUnmanagedClient("localhost", port)
            sendRpcSync(client, payload)

            timer.startTiming()
            val (success, fail) = sendRpcsAsync(client, payload, asyncBatchSize)
            timer.stopTiming()

            if (fail > 0) {
              // scalastyle:off println
              println(s"    $name: $success ok, $fail failed (backpressure)")
              // scalastyle:on println
            }
            client.close()
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Suite 7: Large block transfer (16MB), sequential and 4-thread parallel.
   */
  private def largeBlockTransferBenchmark(): Unit = {
    val ioMode = detectIOMode()
    val numBlocks = 100

    runBenchmark(s"Large Block Transfer Throughput (IOMode=$ioMode)") {
      withTransport(ioMode) { (clientFactory, port) =>
        val payload = new Array[Byte](XLARGE_PAYLOAD)

        val benchmark = new Benchmark(
          "16 MB Block Transfer",
          numBlocks.toLong,
          minNumIters = 3,
          output = output)

        benchmark.addTimerCase("Sequential sends", numIters = 3) { timer =>
          val client = clientFactory.createUnmanagedClient("localhost", port)
          sendRpcSync(client, payload)

          timer.startTiming()
          var i = 0
          while (i < numBlocks) {
            sendRpcSync(client, payload)
            i += 1
          }
          timer.stopTiming()
          client.close()
        }

        benchmark.addTimerCase("4-thread parallel sends", numIters = 3) { timer =>
          val numThreads = 4
          val blocksPerThread = numBlocks / numThreads
          val clients = createClients(clientFactory, port, numThreads)
          clients.foreach(c => sendRpcSync(c, payload))

          timer.startTiming()
          val latch = new CountDownLatch(numThreads)
          clients.foreach { c =>
            val t = new Thread(() => {
              try {
                var i = 0
                while (i < blocksPerThread) {
                  sendRpcSync(c, payload)
                  i += 1
                }
              } finally {
                latch.countDown()
              }
            })
            t.setDaemon(true)
            t.start()
          }
          latch.await(120, TimeUnit.SECONDS)
          timer.stopTiming()
          clients.foreach(_.close())
        }

        benchmark.run()
      }
    }
  }

  /**
   * Suite 8: File-Backed Shuffle Block Fetch via ChunkFetch.
   *
   * Writes temp files to disk, serves them as FileSegmentManagedBuffer via StreamManager,
   * and fetches them using client.fetchChunk(). This exercises the DefaultFileRegion
   * zero-copy sendfile/splice path.
   *
   * Compares NIO vs AUTO to verify that native transports (EPOLL/KQUEUE) use sendfile()
   * for file-backed transfers. AUTO should be equal to or faster than NIO.
   */
  private def fileBackedShuffleBenchmark(): Unit = {
    val numFiles = 100
    val fileSize = 16 * 1024 * 1024 // 16 MB per file

    // Create temp shuffle files
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), "spark-bench-shuffle")
    tmpDir.mkdirs()
    val files = (0 until numFiles).map { idx =>
      val f = new File(tmpDir, s"shuffle-$idx.data")
      f.deleteOnExit()
      val raf = new RandomAccessFile(f, "rw")
      try {
        val chunk = new Array[Byte](1024 * 1024)
        var written = 0
        while (written < fileSize) {
          val toWrite = math.min(chunk.length, fileSize - written)
          raf.write(chunk, 0, toWrite)
          written += toWrite
        }
      } finally {
        raf.close()
      }
      f
    }

    try {
      runBenchmark(s"File-Backed Shuffle Block Fetch (NIO vs AUTO, ${numFiles}x16MB)") {
        val benchmark = new Benchmark(
          "File-Backed Shuffle Fetch",
          numFiles.toLong,
          minNumIters = 3,
          output = output)

        Seq("NIO", "AUTO").foreach { mode =>
          benchmark.addTimerCase(s"$mode, sequential fetch", numIters = 3) { timer =>
            val conf = createConf(mode)
            val streamManager = createFileStreamManager(conf, files)
            val rpcHandler = createStreamRpcHandler(streamManager)
            val context = new TransportContext(conf, rpcHandler)
            val server = context.createServer()
            val clientFactory = context.createClientFactory()
            try {
              val client = clientFactory.createUnmanagedClient("localhost", server.getPort)
              fetchChunksSync(client, streamId = 0, chunkIndices = Seq(0))

              timer.startTiming()
              fetchChunksSync(client, streamId = 0, chunkIndices = 0 until numFiles)
              timer.stopTiming()
              client.close()
            } finally {
              clientFactory.close()
              server.close()
              context.close()
            }
          }

          benchmark.addTimerCase(s"$mode, parallel fetch (4 clients)", numIters = 3) { timer =>
            val conf = createConf(mode)
            val streamManager = createFileStreamManager(conf, files)
            val rpcHandler = createStreamRpcHandler(streamManager)
            val context = new TransportContext(conf, rpcHandler)
            val server = context.createServer()
            val clientFactory = context.createClientFactory()
            try {
              val port = server.getPort
              val numThreads = 4
              val chunksPerThread = numFiles / numThreads
              val clients = createClients(clientFactory, port, numThreads)
              clients.foreach(c => fetchChunksSync(c, streamId = 0, chunkIndices = Seq(0)))

              timer.startTiming()
              val latch = new CountDownLatch(numThreads)
              (0 until numThreads).foreach { threadIdx =>
                val client = clients(threadIdx)
                val startChunk = threadIdx * chunksPerThread
                val endChunk = startChunk + chunksPerThread
                val t = new Thread(() => {
                  try {
                    fetchChunksSync(
                      client, streamId = 0, chunkIndices = startChunk until endChunk)
                  } finally {
                    latch.countDown()
                  }
                })
                t.setDaemon(true)
                t.start()
              }
              latch.await(120, TimeUnit.SECONDS)
              timer.stopTiming()
              clients.foreach(_.close())
            } finally {
              clientFactory.close()
              server.close()
              context.close()
            }
          }
        }

        benchmark.run()
      }
    } finally {
      files.foreach(_.delete())
      tmpDir.delete()
    }
  }

  private def createFileStreamManager(
      conf: TransportConf, files: Seq[File]): StreamManager = {
    new StreamManager {
      override def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer = {
        new FileSegmentManagedBuffer(conf, files(chunkIndex), 0, files(chunkIndex).length())
      }
    }
  }

  private def createStreamRpcHandler(streamManager: StreamManager): RpcHandler = {
    new RpcHandler {
      override def receive(
          client: TransportClient,
          message: ByteBuffer,
          callback: RpcResponseCallback): Unit = {
        throw new UnsupportedOperationException()
      }
      override def getStreamManager: StreamManager = streamManager
    }
  }

  // ==================== Helpers ====================

  /** AUTO mode safely falls back to NIO if native .so is unavailable. */
  private def detectIOMode(): String = "AUTO"

  /**
   * Fetches chunks synchronously via ChunkFetchRequest and waits for all to complete.
   * The buffer is NOT retained or released here -- TransportResponseHandler releases
   * it after the callback returns.
   */
  private def fetchChunksSync(
      client: TransportClient,
      streamId: Long,
      chunkIndices: Seq[Int]): Unit = {
    val sem = new Semaphore(0)
    @volatile var error: Throwable = null
    val callback = new ChunkReceivedCallback {
      override def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = {
        sem.release()
      }
      override def onFailure(chunkIndex: Int, e: Throwable): Unit = {
        error = e
        sem.release()
      }
    }
    chunkIndices.foreach { idx =>
      client.fetchChunk(streamId, idx, callback)
    }
    if (!sem.tryAcquire(chunkIndices.size, 120, TimeUnit.SECONDS)) {
      throw new RuntimeException("ChunkFetch timed out")
    }
    if (error != null) {
      throw new RuntimeException("ChunkFetch failed", error)
    }
  }

  // ==================== Main ====================

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    rpcLatencyBenchmark()
    concurrentThroughputBenchmark()
    ioModeComparisonBenchmark()
    serverThreadScalingBenchmark()
    multiConnectionBenchmark()
    asyncWritePressureBenchmark()
    largeBlockTransferBenchmark()
    fileBackedShuffleBenchmark()
  }
}
