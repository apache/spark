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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.{SparkException, SparkFunSuite}

class BasePythonRunnerSuite extends SparkFunSuite {

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
}
