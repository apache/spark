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

package org.apache.spark.api.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream,
  EOFException, FilterOutputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ClosedByInterruptException, ServerSocketChannel, SocketChannel}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{LocalSparkContext, SparkContext, SparkEnv, SparkException,
  SparkFunSuite, TaskContext}
import org.apache.spark.util.DirectByteBufferOutputStream

class BasePythonRunnerSuite extends SparkFunSuite with LocalSparkContext {

  test("SPARK-58192: pipelined writer pool admits more concurrent writers than processors") {
    // A fractional spark.task.cpus admits more concurrent tasks -- each holding one writer
    // thread -- than the host has processors. Writers of a barrier stage block until every
    // task's worker makes progress, so a pool that queues writers beyond a processor-based
    // cap can deadlock; all writers must be able to start concurrently.
    val nWriters = Runtime.getRuntime.availableProcessors() + 2
    val started = new CountDownLatch(nWriters)
    val release = new CountDownLatch(1)
    val futures = (1 to nWriters).map { _ =>
      BasePythonRunner.pipelinedWriterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          started.countDown()
          release.await(1, TimeUnit.MINUTES)
        }
      })
    }
    try {
      assert(started.await(1, TimeUnit.MINUTES),
        s"expected all $nWriters writers to start concurrently")
    } finally {
      release.countDown()
      futures.foreach(_.get(1, TimeUnit.MINUTES))
    }
  }

  test("SPARK-58192: pyspark memory is split across the executor's concurrent task slots") {
    def workerMemoryMb(maxConcurrentTasks: Int): Option[Long] =
      BasePythonRunner.getWorkerMemoryMb(Some(4096L), maxConcurrentTasks)

    // One slot per concurrent task; each worker gets an equal share of the executor allocation.
    assert(workerMemoryMb(4) === Some(1024L))
    // A fractional spark.task.cpus (e.g. 4 cores / 0.5) admits more concurrent tasks than cores,
    // so each worker gets a smaller share and the aggregate stays within the allocation.
    assert(workerMemoryMb(8) === Some(512L))
    assert(workerMemoryMb(5) === Some(819L))
    // The concurrency is the limiting resource, not just cpu slots: a GPU that caps the executor
    // at a single concurrent task means the sole worker keeps the whole allocation, even though
    // the cpu-slot count (e.g. 64 cores / 0.1 = 640) is far higher.
    assert(workerMemoryMb(1) === Some(4096L))
    // Never split into less than one slot.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(4096L), 0) === Some(4096L))
    // No pyspark memory configured means no per-worker limit.
    assert(BasePythonRunner.getWorkerMemoryMb(None, 8) === None)
  }

  test("SPARK-58192: fail fast when the per-slot pyspark memory share rounds to zero") {
    // 512 MiB across 640 genuinely concurrent tasks rounds down to 0, which the worker would
    // treat as "no limit". Fail fast rather than silently dropping the configured cap.
    val e = intercept[SparkException] {
      BasePythonRunner.getWorkerMemoryMb(Some(512L), 640)
    }
    assert(e.getMessage.contains("spark.executor.pyspark.memory"))
    assert(e.getMessage.contains("640"))
    // A share of exactly 1 MiB is still enforceable and must not fail.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(640L), 640) === Some(1L))
    // An explicit spark.executor.pyspark.memory=0 means the limit is disabled (the worker
    // treats 0 as "no limit"), so it must pass through rather than fail fast, even when
    // concurrency is high enough that a positive budget would round to zero.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(0L), 640) === Some(0L))
    assert(BasePythonRunner.getWorkerMemoryMb(Some(0L), 1) === Some(0L))
    // The smallest positive budget still fails fast when it cannot give every slot at least
    // 1 MiB; only an explicit budget of 0 disables the limit.
    intercept[SparkException] {
      BasePythonRunner.getWorkerMemoryMb(Some(1L), 2)
    }
  }

  private val metricsReport =
    "{\"bootTimestampMs\":1250,\"initTimestampMs\":2500," +
      "\"finishTimestampMs\":3750,\"pythonExecutionDurationMs\":42," +
      "\"memoryBytesSpilled\":7,\"diskBytesSpilled\":9}"
  private val expectedMetrics = BasePythonRunner.WorkerMetrics(1250L, 2500L, 3750L, 42L, 7L, 9L)

  private def framedMetricsStream(json: String, declaredLength: Option[Int] = None,
      endMarkers: Boolean = true): DataInputStream = {
    val bytes = json.getBytes(StandardCharsets.UTF_8)
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputStream(buffer)
    output.writeInt(declaredLength.getOrElse(bytes.length))
    output.write(bytes)
    if (endMarkers) {
      output.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      output.writeInt(0) // No accumulator updates.
      output.writeInt(SpecialLengths.END_OF_STREAM)
    }
    new DataInputStream(new ByteArrayInputStream(buffer.toByteArray))
  }

  private def checkEndMarkers(stream: DataInputStream): Unit = {
    assert(stream.readInt() == SpecialLengths.END_OF_DATA_SECTION)
    assert(stream.readInt() == 0)
    assert(stream.readInt() == SpecialLengths.END_OF_STREAM)
    assert(stream.read() == -1)
  }

  test("SPARK-59773: read worker metrics and leave the following sections aligned") {
    val withExtra = metricsReport.dropRight(1) + ",\"futureMetric\":{\"value\":[1,2]}}"
    val stream = framedMetricsStream(withExtra)
    assert(BasePythonRunner.readWorkerMetrics(stream) == expectedMetrics)
    checkEndMarkers(stream)
  }

  test("SPARK-59773: worker metrics require all current fields") {
    val fields = Seq(
      "bootTimestampMs" -> "\"bootTimestampMs\":1250,",
      "initTimestampMs" -> "\"initTimestampMs\":2500,",
      "finishTimestampMs" -> "\"finishTimestampMs\":3750,",
      "pythonExecutionDurationMs" -> "\"pythonExecutionDurationMs\":42,",
      "memoryBytesSpilled" -> "\"memoryBytesSpilled\":7,",
      "diskBytesSpilled" -> ",\"diskBytesSpilled\":9")
    fields.foreach { case (name, field) =>
      val stream = framedMetricsStream(metricsReport.replace(field, ""))
      val error = intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      assert(error.getMessage.contains(name))
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics reject non-int64 values") {
    val invalidValues = Seq("null", "true", "\"42\"", "42.0", "[]", "{}",
      "9223372036854775808", "-9223372036854775809")
    val fields = Seq("pythonExecutionDurationMs" -> 42,
      "memoryBytesSpilled" -> 7, "diskBytesSpilled" -> 9)
    for {
      (name, originalValue) <- fields
      value <- invalidValues
    } {
      val field = "\"" + name + "\":"
      val json = metricsReport.replace(field + originalValue, field + value)
      val stream = framedMetricsStream(json)
      val error = intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      assert(error.getMessage.contains(name))
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics preserve int64 values exactly") {
    Seq(Long.MinValue, 9007199254740993L, Long.MaxValue).foreach { value =>
      val json = metricsReport.replace(
        "\"pythonExecutionDurationMs\":42", "\"pythonExecutionDurationMs\":" + value)
        .replace("\"memoryBytesSpilled\":7", "\"memoryBytesSpilled\":" + value)
        .replace("\"diskBytesSpilled\":9", "\"diskBytesSpilled\":" + value)
      val stream = framedMetricsStream(json)
      assert(BasePythonRunner.readWorkerMetrics(stream) ==
        expectedMetrics.copy(pythonExecutionDurationMs = value,
          memoryBytesSpilled = value, diskBytesSpilled = value))
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics reject malformed, duplicate, and non-object JSON") {
    val invalid = Seq("[]", "null", "42", metricsReport.dropRight(1), metricsReport + "{}",
      metricsReport.dropRight(1) + ",\"bootTimestampMs\":9}", "{\"future\":1,\"future\":2}")
    invalid.foreach { json =>
      val stream = framedMetricsStream(json)
      intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics reject invalid lengths and truncated frames") {
    Seq(-1, 0).foreach { length =>
      intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(framedMetricsStream(metricsReport, Some(length)))
      }
    }

    val truncated = framedMetricsStream(metricsReport,
      Some(metricsReport.getBytes(StandardCharsets.UTF_8).length + 1), endMarkers = false)
    intercept[EOFException] {
      BasePythonRunner.readWorkerMetrics(truncated)
    }
  }

  test("SPARK-59773: worker metrics accept a larger report with an additional field") {
    val json = metricsReport.dropRight(1) + ",\"future\":\"" + ("x" * (70 * 1024)) + "\"}"
    val stream = framedMetricsStream(json)
    assert(BasePythonRunner.readWorkerMetrics(stream) == expectedMetrics)
    checkEndMarkers(stream)
  }

  test("SPARK-60305: CloseShieldOutputStream forwards writes and does not close the " +
      "underlying stream") {
    val bufferStream = new DirectByteBufferOutputStream()
    val shielded = new CloseShieldOutputStream(bufferStream)
    shielded.write(Array[Byte](1, 2, 3), 1, 2)
    shielded.write(4)
    shielded.close()
    assert(bufferStream.size() === 3)
    val bytes = new Array[Byte](3)
    bufferStream.toByteBuffer.get(bytes)
    assert(bytes.toSeq === Seq[Byte](2, 3, 4))
    bufferStream.close()
  }

  test("SPARK-60305: interrupting the task thread inside a channel write keeps the input " +
      "buffer alive") {
    sc = new SparkContext("local", "test")
    val func = SimplePythonFunction(Seq.empty[Byte], new JHashMap[String, String](),
      new JArrayList[String](), "python3", "3", new JArrayList(), null)
    val inWrite = new CountDownLatch(1)
    val resume = new AtomicBoolean(false)
    @volatile var writeError: Throwable = null

    // Writes the way the Arrow inputs do: through `Channels.newChannel(dataOut)`, whose writes
    // the JVM closes, together with `dataOut`, when the writing thread is interrupted. The
    // write blocks until the test has interrupted the thread, then forwards to `dataOut`.
    class TestRunner extends BasePythonRunner[Int, Int](Seq(ChainedPythonFunctions(Seq(func))),
        PythonEvalType.NON_UDF, Array(Array(0)), None, Map.empty) {
      override protected def newWriter(env: SparkEnv, worker: PythonWorker,
          inputIterator: Iterator[Int], partitionIndex: Int, context: TaskContext): Writer = {
        new Writer(env, worker, inputIterator, partitionIndex, context) {
          override def open(dataOut: DataOutputStream): Unit = {}
          override protected def writeCommand(dataOut: DataOutputStream): Unit = {}
          override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
            val channel = Channels.newChannel(new FilterOutputStream(dataOut) {
              override def write(b: Array[Byte], off: Int, len: Int): Unit = {
                inWrite.countDown()
                while (!resume.get()) Thread.onSpinWait()
                try {
                  out.write(b, off, len)
                } catch {
                  case t: Throwable =>
                    writeError = t
                    throw t
                }
              }
            })
            channel.write(ByteBuffer.wrap(new Array[Byte](16)))
            false
          }
        }
      }

      override protected def newReaderIterator(stream: DataInputStream, writer: Writer,
          startTime: Long, env: SparkEnv, worker: PythonWorker,
          handle: Option[PythonWorkerHandle], releasedOrClosed: AtomicBoolean,
          context: TaskContext): Iterator[Int] = throw new UnsupportedOperationException()

      def newReaderInputStream(worker: PythonWorker): ReaderInputStream = new ReaderInputStream(
        worker, newWriter(SparkEnv.get, worker, Iterator.empty, 0, null), None, 0, false, null)
    }
    val runner = new TestRunner

    val server = ServerSocketChannel.open()
    server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val client = SocketChannel.open(server.getLocalAddress)
    val accepted = server.accept()
    client.configureBlocking(false)
    val worker = PythonWorker(client).refresh()
    try {
      val input = runner.newReaderInputStream(worker)
      @volatile var error: Throwable = null
      val taskThread = new Thread(() => {
        try {
          input.read(new Array[Byte](1))
        } catch {
          case t: Throwable => error = t
        }
      })
      taskThread.start()
      assert(inWrite.await(1, TimeUnit.MINUTES))
      // Like Task.kill(interruptThread = true). This closes the channel and `dataOut` on this
      // thread while the task thread is still inside the write.
      taskThread.interrupt()
      resume.set(true)
      taskThread.join(TimeUnit.MINUTES.toMillis(1))
      assert(!taskThread.isAlive)
      // The write into the buffer still succeeds: the buffer was not freed under the writer.
      // The interrupt then surfaces as the channel's ClosedByInterruptException.
      assert(writeError === null)
      assert(error.isInstanceOf[ClosedByInterruptException], error)
    } finally {
      worker.stop()
      accepted.close()
      server.close()
    }
  }
}
