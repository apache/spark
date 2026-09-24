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

import java.io.File
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.node.IntNode

import org.apache.spark.{SparkException, SparkFunSuite}

class PythonWorkerMetricsDecoderSuite extends SparkFunSuite {
  import PythonWorkerMetricsDecoder.Metric

  private def metric(name: String, value: String, unit: String): String = {
    s"\"$name\":{\"value\":$value,\"unit\":\"$unit\"}"
  }

  private val boot = metric("bootTimestampMs", "1250", "timestampMillis")
  private val init = metric("initTimestampMs", "2500", "timestampMillis")
  private val finish = metric("finishTimestampMs", "3750", "timestampMillis")
  private val processing = metric("processingDurationMs", "42", "milliseconds")

  private val timingMetrics = Map(
    "bootTimestampMs" -> Metric(IntNode.valueOf(1250), "timestampMillis"),
    "initTimestampMs" -> Metric(IntNode.valueOf(2500), "timestampMillis"),
    "finishTimestampMs" -> Metric(IntNode.valueOf(3750), "timestampMillis"),
    "processingDurationMs" -> Metric(IntNode.valueOf(42), "milliseconds"))

  private def document(metrics: Seq[String] = Seq(boot, init, finish, processing)): String = {
    s"""{"kind":"spark.python.worker.metrics","version":1,"metrics":{${metrics.mkString(",")}}}"""
  }

  private def framedStream(json: String, declaredLength: Option[Int] = None,
      trailer: Boolean = true): DataInputStream = {
    val bytes = json.getBytes(StandardCharsets.UTF_8)
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputStream(buffer)
    output.writeInt(declaredLength.getOrElse(bytes.length))
    output.write(bytes)
    if (trailer) {
      output.writeLong(7L)
      output.writeLong(9L)
      output.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      output.writeInt(0) // No accumulator updates.
      output.writeInt(SpecialLengths.END_OF_STREAM)
    }
    new DataInputStream(new ByteArrayInputStream(buffer.toByteArray))
  }

  private def checkTrailer(stream: DataInputStream): Unit = {
    assert(stream.readLong() == 7L)
    assert(stream.readLong() == 9L)
    assert(stream.readInt() == SpecialLengths.END_OF_DATA_SECTION)
    assert(stream.readInt() == 0)
    assert(stream.readInt() == SpecialLengths.END_OF_STREAM)
    assert(stream.read() == -1)
  }

  test("additional metric survives decoding and leaves the trailer aligned") {
    val extra = metric("futureRatio", "0.75", "ratio")
    val stream = framedStream(document(Seq(boot, init, finish, processing, extra)))
    val decoded = PythonWorkerMetricsDecoder.read(stream)
    assert(decoded - "futureRatio" == timingMetrics)
    assert(decoded("futureRatio").value.isFloatingPointNumber)
    assert(decoded("futureRatio").value.doubleValue() == 0.75)
    assert(decoded("futureRatio").unit == "ratio")
    checkTrailer(stream)
  }

  test("Python writer bytes decode with the following trailer aligned") {
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env.getOrElse("SPARK_HOME", "."))
    val pythonPath = PythonUtils.mergePythonPaths(
      new File(sparkHome, "python").getAbsolutePath,
      new File(sparkHome, s"python/lib/${PythonUtils.PY4J_ZIP_NAME}").getAbsolutePath,
      sys.env.getOrElse("PYTHONPATH", ""))
    val script =
      """|import sys
         |from pyspark.serializers import SpecialLengths, write_int, write_long
         |from pyspark.worker import report_metrics
         |
         |metrics = {
         |    "bootTimestampMs": {"value": 1250, "unit": "timestampMillis"},
         |    "initTimestampMs": {"value": 2500, "unit": "timestampMillis"},
         |    "finishTimestampMs": {"value": 3750, "unit": "timestampMillis"},
         |    "processingDurationMs": {"value": 42, "unit": "milliseconds"},
         |    "futureRatio": {"value": 0.75, "unit": "ratio"},
         |}
         |report_metrics(sys.stdout.buffer, metrics)
         |write_long(7, sys.stdout.buffer)
         |write_long(9, sys.stdout.buffer)
         |write_int(SpecialLengths.END_OF_DATA_SECTION, sys.stdout.buffer)
         |write_int(0, sys.stdout.buffer)
         |write_int(SpecialLengths.END_OF_STREAM, sys.stdout.buffer)
         |""".stripMargin
    val process = new ProcessBuilder(PythonUtils.defaultPythonExec, "-c", script)
      .redirectError(ProcessBuilder.Redirect.INHERIT)
    process.environment().put("PYTHONPATH", pythonPath)
    process.environment().put("SPARK_PYTHON_RUNTIME", "PYTHON_WORKER")
    val child = process.start()
    val bytes = child.getInputStream.readAllBytes()
    assert(child.waitFor() == 0)

    val stream = new DataInputStream(new ByteArrayInputStream(bytes))
    assert(stream.readInt() == SpecialLengths.METRICS_DATA)
    val decoded = PythonWorkerMetricsDecoder.read(stream)
    assert(decoded - "futureRatio" == timingMetrics)
    assert(decoded("futureRatio").value.doubleValue() == 0.75)
    assert(decoded("futureRatio").unit == "ratio")
    checkTrailer(stream)
  }

  test("generic reports do not require timing fields") {
    val empty = framedStream(document(Seq.empty))
    assert(PythonWorkerMetricsDecoder.read(empty).isEmpty)
    checkTrailer(empty)

    val counter = framedStream(document(Seq(metric("rowsProcessed", "42", "count"))))
    assert(PythonWorkerMetricsDecoder.read(counter) ==
      Map("rowsProcessed" -> Metric(IntNode.valueOf(42), "count")))
    checkTrailer(counter)
  }

  test("additional JSON fields are ignored") {
    val counter = metric("rowsProcessed", "42", "count")
      .replace("\"value\":42", "\"value\":42,\"description\":\"future metadata\"")
    val json = document(Seq(counter)).replace("\"version\":1", "\"version\":1,\"future\":true")
    val stream = framedStream(json)
    assert(PythonWorkerMetricsDecoder.read(stream) ==
      Map("rowsProcessed" -> Metric(IntNode.valueOf(42), "count")))
    checkTrailer(stream)
  }

  test("integer values retain precision beyond the timing consumer's int64 range") {
    Seq(Long.MinValue.toString, "9007199254740993", Long.MaxValue.toString,
      "9223372036854775808", "-9223372036854775809").foreach { value =>
      val stream = framedStream(document(Seq(metric("counter", value, "count"))))
      val decoded = PythonWorkerMetricsDecoder.read(stream)("counter").value
      assert(decoded.isIntegralNumber)
      assert(decoded.bigIntegerValue().toString == value)
      checkTrailer(stream)
    }
  }

  test("decimal values retain precision without conversion to double") {
    val value = "0.12345678901234567890123456789"
    val stream = framedStream(document(Seq(metric("ratio", value, "ratio"))))
    val decoded = PythonWorkerMetricsDecoder.read(stream)("ratio").value
    assert(decoded.isFloatingPointNumber)
    assert(decoded.decimalValue().toPlainString == value)
    checkTrailer(stream)
  }

  test("JSON strings, booleans, nulls, arrays, and objects are preserved") {
    val stream = framedStream(document(Seq(
      metric("state", "\"ready\"", "state"),
      metric("enabled", "true", "flag"),
      metric("optional", "null", "sample"),
      metric("samples", "[1,0.75,null]", "sample"),
      metric("details", "{\"count\":42,\"label\":\"batch\"}", "sample"))))
    val decoded = PythonWorkerMetricsDecoder.read(stream)
    assert(decoded("state").value.textValue() == "ready")
    assert(decoded("enabled").value.isBoolean && decoded("enabled").value.booleanValue())
    assert(decoded("optional").value.isNull)
    assert(decoded("samples").value.isArray)
    assert(decoded("samples").value.get(1).doubleValue() == 0.75)
    assert(decoded("samples").value.get(2).isNull)
    assert(decoded("details").value.get("count").intValue() == 42)
    assert(decoded("details").value.get("label").textValue() == "batch")
    checkTrailer(stream)
  }

  test("the timing consumer validates its required fields") {
    val report = timingMetrics.updated("inputBytes", Metric(IntNode.valueOf(1234), "bytes"))
    assert(BasePythonRunner.workerTimingData(report) ==
      BasePythonRunner.WorkerTimingData(1250L, 2500L, 3750L, 42L))

    timingMetrics.foreach { case (name, metric) =>
      intercept[SparkException] {
        BasePythonRunner.workerTimingData(report - name)
      }
      intercept[SparkException] {
        BasePythonRunner.workerTimingData(report.updated(name, metric.copy(unit = "bytes")))
      }
    }
  }

  test("the timing consumer rejects unsuitable values after generic decoding") {
    Seq("null", "true", "\"42\"", "42.0", "[]", "{}",
      "9223372036854775808", "-9223372036854775809").foreach { value =>
      val stream = framedStream(document(Seq(
        boot, init, finish, metric("processingDurationMs", value, "milliseconds"))))
      val report = PythonWorkerMetricsDecoder.read(stream)
      intercept[SparkException] {
        BasePythonRunner.workerTimingData(report)
      }
      checkTrailer(stream)
    }
  }

  test("invalid v1 metrics fail after consuming the complete frame") {
    val missing = document(Seq(processing.replace("\"value\":42,", "")))
    val duplicate = document(Seq(boot, boot))
    val missingUnit = document(Seq(processing.replace(
      ",\"unit\":\"milliseconds\"", "")))
    val emptyUnit = document(Seq(metric("counter", "42", "")))
    val emptyName = document(Seq(metric("", "42", "count")))
    val nan = document(Seq(metric("ratio", "NaN", "ratio")))
    val infinity = document(Seq(metric("ratio", "Infinity", "ratio")))
    val wrongVersion = document().replace("\"version\":1", "\"version\":2")
    val missingKind = document().replace("\"kind\":\"spark.python.worker.metrics\",", "")
    val malformed = document().dropRight(1)
    val trailingJson = document() + "{}"

    Seq(missing, duplicate, missingUnit, emptyUnit, emptyName, nan, infinity,
      wrongVersion, missingKind, malformed, trailingJson).foreach { json =>
      val stream = framedStream(json)
      intercept[SparkException] {
        PythonWorkerMetricsDecoder.read(stream)
      }
      checkTrailer(stream)
    }
  }

  test("message kind and unit errors identify the failing field") {
    val wrongKind = document().replace("\"kind\":\"spark.python.worker.metrics\"",
      "\"kind\":\"other\"")
    val kindError = intercept[SparkException] {
      PythonWorkerMetricsDecoder.read(framedStream(wrongKind))
    }
    assert(kindError.getMessage.contains("invalid message kind"))

    val missingUnit = document(Seq(metric("futureMetric", "42", "")))
    val unitError = intercept[SparkException] {
      PythonWorkerMetricsDecoder.read(framedStream(missingUnit))
    }
    assert(unitError.getMessage.contains("unit for metric futureMetric"))
  }

  test("invalid length and truncated metrics frame fail") {
    Seq(-1, 0, PythonWorkerMetricsDecoder.maxPayloadLength + 1).foreach { length =>
      val invalidLength = framedStream(document(), Some(length))
      intercept[SparkException] {
        PythonWorkerMetricsDecoder.read(invalidLength)
      }
    }

    val truncated = framedStream(document(), Some(document().getBytes(StandardCharsets.UTF_8)
      .length + 1), trailer = false)
    intercept[EOFException] {
      PythonWorkerMetricsDecoder.read(truncated)
    }
  }
}
