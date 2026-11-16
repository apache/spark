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


package org.apache.spark.sql.execution.python

import java.util.Collections

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.{SparkFunSuite, TaskContext, TaskKilledException}
import org.apache.spark.api.python.{PythonFunction, SimplePythonFunction}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

/** Batches of [[InProcessArrowBatchExporter]] and their release, which need no Python. */
class InProcessArrowBatchExporterSuite extends SparkFunSuite {
  import InProcessEvaluatorTestUtils._

  // This session has no interpreter: registrations fail after the session recorded them.
  private var session: InProcessPythonRuntime.InterpreterSession = _
  private var allocatedBefore = 0L

  override def beforeEach(): Unit = {
    super.beforeEach()
    session = new InProcessPythonRuntime.InterpreterSession()
    allocatedBefore = ArrowUtils.rootAllocator.getAllocatedMemory
  }

  override def afterEach(): Unit = {
    try {
      assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocatedBefore)
      session.shutdown()
    } finally {
      super.afterEach()
    }
  }

  private val schema = new StructType().add("id", LongType).add("s", StringType)

  /** The calls into Python that an exporter made, by the metric each was charged to. */
  private class Calls(metrics: Map[String, SQLMetric]) {
    val charged = ArrayBuffer.empty[String]
    val run: InProcessArrowBatchExporter.PythonCall = (metric, call) => {
      charged += metrics.find(_._2 eq metric).get._1
      metric += call()
    }
  }

  private def exporter(
      metrics: Map[String, SQLMetric],
      functions: Seq[PythonFunction] = Seq.empty,
      readBackInput: Boolean = true,
      context: TaskContext = TaskContext.empty()): InProcessArrowBatchExporter = {
    val fields: Seq[Field] =
      functions.map(_ => ArrowUtils.toArrowField("result", LongType, true, "UTC"))
    new InProcessArrowBatchExporter(session, functions,
      functions.map(_ => Array(ArgumentMetadata(0, None))).toArray, fields,
      ArrowUtils.toArrowSchema(schema, "UTC", false), readBackInput,
      false, false, false, true, metrics, context)
  }

  private def function(): PythonFunction = SimplePythonFunction(
    Seq.empty, Collections.emptyMap[String, String](), Collections.emptyList[String](),
    "", "3.12", Collections.emptyList(), null)

  private def fill(exporter: InProcessArrowBatchExporter, ids: Range, value: String): Unit = {
    exporter.startBatch()
    assert(exporter.rowCount == 0)
    ids.foreach(i => exporter.write(InternalRow(i.toLong, UTF8String.fromString(value))))
    assert(exporter.rowCount == ids.size && exporter.sizeInBytes > 0)
  }

  test("a batch without UDFs reads its input back, and counts its rows and bytes") {
    val metrics = allMetrics()
    val calls = new Calls(metrics)
    val exporter = this.exporter(metrics)
    fill(exporter, 0 until 5, "a")
    val sent = exporter.sizeInBytes
    val rows = exporter.evaluate(calls.run).map(r => (r.getLong(0), r.getString(1))).toSeq
    assert(rows == (0 until 5).map(i => (i.toLong, "a")))
    assert(metrics("pythonDataSent").value == sent)
    assert(metrics("pythonNumRowsReceived").value == 5 && calls.charged.isEmpty)

    // The next batch replaces the previous one.
    fill(exporter, 5 until 8, "bc")
    assert(exporter.evaluate(calls.run).map(_.getLong(0)).toSeq == Seq(5L, 6L, 7L))
    assert(metrics("pythonNumRowsReceived").value == 8)
    exporter.close()
  }

  test("a batch that is not read back returns rows of the results only") {
    val metrics = allMetrics()
    val exporter = this.exporter(metrics, readBackInput = false)
    fill(exporter, 0 until 5, "a")
    val rows = exporter.evaluate(new Calls(metrics).run).toSeq
    assert(rows.size == 5 && rows.forall(_.numFields == 0))
    exporter.close()
  }

  test("closing releases a batch in any state, and is idempotent for batches") {
    val metrics = allMetrics()
    val unused = exporter(metrics)
    unused.close()
    unused.close()

    val filling = exporter(metrics)
    fill(filling, 0 until 100, "abc")
    assert(ArrowUtils.rootAllocator.getAllocatedMemory > allocatedBefore)
    filling.close()
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocatedBefore)
    filling.close()

    val evaluated = exporter(metrics)
    fill(evaluated, 0 until 100, "abc")
    evaluated.evaluate(new Calls(metrics).run)
    evaluated.close()
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocatedBefore)
  }

  test("a failed registration is released by close") {
    val metrics = allMetrics()
    val calls = new Calls(metrics)
    val exporter = this.exporter(metrics, Seq(function(), function()))
    fill(exporter, 0 until 5, "a")
    // The session has no interpreter, so the first registration fails once it is recorded.
    intercept[NullPointerException](exporter.evaluate(calls.run))
    assert(calls.charged == Seq("pythonInitTime"))
    assert(metrics("pythonDataSent").value == 0 && metrics("pythonNumRowsReceived").value == 0)
    exporter.close()
    // The session terminates only once every registration is released.
    session.shutdown()
    assert(session.isTerminated)
  }

  test("a call into Python that is refused stops the batch before anything is exported") {
    val metrics = allMetrics()
    val exporter = this.exporter(metrics, Seq(function()))
    fill(exporter, 0 until 5, "a")
    val e = intercept[NoSuchElementException] {
      exporter.evaluate((_, _) => throw new NoSuchElementException("closed"))
    }
    assert(e.getMessage == "closed" && metrics("pythonDataSent").value == 0)
    exporter.close()
    session.shutdown()
    assert(session.isTerminated)
  }

  test("cancellation is checked before each UDF is invoked") {
    val metrics = allMetrics()
    val context = TaskContext.empty()
    val calls = new Calls(metrics)
    val exporter = this.exporter(metrics, Seq(function()), context = context)
    fill(exporter, 0 until 5, "a")
    // Registration succeeds without Python, and the task is killed before the invocation.
    val run: InProcessArrowBatchExporter.PythonCall = (metric, _) => {
      calls.run(metric, () => 7L)
      context.markInterrupted("test cancellation")
    }
    intercept[TaskKilledException](exporter.evaluate(run))
    assert(calls.charged == Seq("pythonInitTime") && metrics("pythonInitTime").value == 7L)
    // The batch was finished and counted as sent, but no result was received.
    assert(metrics("pythonDataSent").value > 0 && metrics("pythonNumRowsReceived").value == 0)
    exporter.close()
  }

  test("flat vectors of a batch are pre-sized like the previous batch") {
    val metrics = allMetrics()
    val exporter = this.exporter(metrics)
    fill(exporter, 0 until 5000, "x" * 100)
    exporter.evaluate(new Calls(metrics).run)
    exporter.startBatch()
    // An empty batch already holds buffers for the rows and string bytes of the previous one.
    val presized = ArrowUtils.rootAllocator.getAllocatedMemory - allocatedBefore
    assert(presized >= 5000 * 8 + 5000 * 100)
    exporter.close()

    val fresh = this.exporter(metrics)
    fresh.startBatch()
    assert(ArrowUtils.rootAllocator.getAllocatedMemory - allocatedBefore < presized)
    fresh.close()
  }

  test("string data is pre-sized to the next power of two of the previous batch") {
    import InProcessArrowBatchExporter.roundedDensity
    // Arrow truncates count * density, so the result must not fall below the power of two.
    for ((count, bytes, expected) <- Seq(
        (10, 3000L, 4096L), (10000, 30000000L, 33554432L), (3, 1L, 1L), (7, 4096L, 4096L))) {
      assert((count * roundedDensity(count, bytes, Int.MaxValue)).toLong == expected)
    }
    // 32-bit offsets cap the data buffer.
    assert((10 * roundedDensity(10, Int.MaxValue.toLong - 1, Int.MaxValue)).toLong ==
      Int.MaxValue.toLong)
  }
}
