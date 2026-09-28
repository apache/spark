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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, EOFException, File}
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkException, SparkFunSuite}

class PythonWorkerMetricsSuite extends SparkFunSuite {
  private val report =
    "{\"bootTimestampMs\":1250,\"initTimestampMs\":2500," +
      "\"finishTimestampMs\":3750,\"pythonExecutionDurationMs\":42," +
      "\"memoryBytesSpilled\":7,\"diskBytesSpilled\":9}"
  private val expected = BasePythonRunner.WorkerMetrics(1250L, 2500L, 3750L, 42L, 7L, 9L)

  private def framedStream(json: String, declaredLength: Option[Int] = None,
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

  test("reads metrics JSON and leaves the following sections aligned") {
    val withExtra = report.dropRight(1) + ",\"futureMetric\":{\"value\":[1,2]}}"
    val stream = framedStream(withExtra)
    val decoded = BasePythonRunner.readWorkerMetrics(stream)
    assert(decoded.get("futureMetric").get("value").get(1).intValue() == 2)
    assert(BasePythonRunner.validateWorkerMetrics(decoded) == expected)
    checkEndMarkers(stream)
  }

  test("decodes generic values without requiring current metric fields") {
    val json =
      """{"counter":9223372036854775808,"ratio":0.12345678901234567890123456789,
        |"samples":[true,null,"ready"]}""".stripMargin
    val stream = framedStream(json)
    val decoded = BasePythonRunner.readWorkerMetrics(stream)
    assert(decoded.get("counter").bigIntegerValue().toString == "9223372036854775808")
    assert(decoded.get("ratio").decimalValue().toPlainString == "0.12345678901234567890123456789")
    val samples = decoded.get("samples")
    assert(samples.get(0).isBoolean && samples.get(0).booleanValue())
    assert(samples.get(1).isNull)
    assert(samples.get(2).textValue() == "ready")
    val error = intercept[SparkException] {
      BasePythonRunner.validateWorkerMetrics(decoded)
    }
    assert(error.getMessage.contains("bootTimestampMs"))
    checkEndMarkers(stream)
  }

  test("reads the Python writer's bytes") {
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env.getOrElse("SPARK_HOME", "."))
    val pythonPath = PythonUtils.mergePythonPaths(
      new File(sparkHome, "python").getAbsolutePath,
      new File(sparkHome, s"python/lib/${PythonUtils.PY4J_ZIP_NAME}").getAbsolutePath,
      sys.env.getOrElse("PYTHONPATH", ""))
    val script =
      """|import sys
         |from pyspark.serializers import SpecialLengths, write_int
         |from pyspark.worker import report_metrics
         |
         |report_metrics(sys.stdout.buffer, 1.25, 2.5, 3.75, 42, 7, 9)
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
    val decoded = BasePythonRunner.readWorkerMetrics(stream)
    assert(BasePythonRunner.validateWorkerMetrics(decoded) == expected)
    checkEndMarkers(stream)
  }

  test("metric validation rejects non-int64 values after successful JSON decoding") {
    val invalidValues = Seq("null", "true", "\"42\"", "42.0", "[]", "{}",
      "9223372036854775808", "-9223372036854775809")
    val metricFields = Seq("pythonExecutionDurationMs" -> 42,
      "memoryBytesSpilled" -> 7, "diskBytesSpilled" -> 9)
    for {
      (name, originalValue) <- metricFields
      value <- invalidValues
    } {
      val field = "\"" + name + "\":"
      val json = report.replace(field + originalValue, field + value)
      val stream = framedStream(json)
      val decoded = BasePythonRunner.readWorkerMetrics(stream)
      val error = intercept[SparkException] {
        BasePythonRunner.validateWorkerMetrics(decoded)
      }
      assert(error.getMessage.contains(name))
      checkEndMarkers(stream)
    }
  }

  test("metric validation requires both spill fields") {
    val spillFields = Seq(
      "memoryBytesSpilled" -> "\"memoryBytesSpilled\":7,",
      "diskBytesSpilled" -> ",\"diskBytesSpilled\":9")
    spillFields.foreach { case (name, field) =>
      val stream = framedStream(report.replace(field, ""))
      val decoded = BasePythonRunner.readWorkerMetrics(stream)
      val error = intercept[SparkException] {
        BasePythonRunner.validateWorkerMetrics(decoded)
      }
      assert(error.getMessage.contains(name))
      checkEndMarkers(stream)
    }
  }

  test("metric validation preserves int64 values exactly") {
    Seq(Long.MinValue, 9007199254740993L, Long.MaxValue).foreach { value =>
      val json = report.replace(
        "\"pythonExecutionDurationMs\":42", "\"pythonExecutionDurationMs\":" + value)
        .replace("\"memoryBytesSpilled\":7", "\"memoryBytesSpilled\":" + value)
        .replace("\"diskBytesSpilled\":9", "\"diskBytesSpilled\":" + value)
      val stream = framedStream(json)
      val decoded = BasePythonRunner.readWorkerMetrics(stream)
      assert(BasePythonRunner.validateWorkerMetrics(decoded) ==
        expected.copy(pythonExecutionDurationMs = value,
          memoryBytesSpilled = value, diskBytesSpilled = value))
      checkEndMarkers(stream)
    }
  }

  test("rejects malformed, duplicate, and non-object JSON during decoding") {
    val invalid = Seq("[]", "null", "42", report.dropRight(1), report + "{}",
      report.dropRight(1) + ",\"bootTimestampMs\":9}", "{\"future\":1,\"future\":2}")
    invalid.foreach { json =>
      val stream = framedStream(json)
      intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      checkEndMarkers(stream)
    }
  }

  test("rejects invalid lengths and truncated frames") {
    Seq(-1, 0).foreach { length =>
      intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(framedStream(report, Some(length)))
      }
    }

    val truncated = framedStream(report,
      Some(report.getBytes(StandardCharsets.UTF_8).length + 1), endMarkers = false)
    intercept[EOFException] {
      BasePythonRunner.readWorkerMetrics(truncated)
    }
  }

  test("accepts a larger report with an additional field") {
    val json = report.dropRight(1) + ",\"future\":\"" + ("x" * (70 * 1024)) + "\"}"
    val stream = framedStream(json)
    val decoded = BasePythonRunner.readWorkerMetrics(stream)
    assert(decoded.get("future").textValue().length == 70 * 1024)
    assert(BasePythonRunner.validateWorkerMetrics(decoded) == expected)
    checkEndMarkers(stream)
  }
}
