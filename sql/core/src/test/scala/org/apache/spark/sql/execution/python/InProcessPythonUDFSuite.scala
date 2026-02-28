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

import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.SparkPlanTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Unit and integration tests for the in-process Python UDF framework.
 *
 * These tests verify:
 *   1. Query plan shape: ExtractInProcessPythonUDFs inserts InProcessEvalPython nodes
 *   2. Physical plan shape: InProcessEvalPython → InProcessArrowEvalExec
 *   3. Concurrency config validation: InProcessPythonChecks rejects bad configs
 *
 * Note: Tests that require an actual jep installation and CPython runtime are tagged
 * with the "inprocess" test tag and skipped in CI unless jep is present on the classpath.
 * Plan-shape tests run without jep because they only inspect the logical/physical plan.
 */
class InProcessPythonUDFSuite extends SparkPlanTest with SharedSparkSession {

  import testImplicits._

  // --- Helper: build a minimal InProcessPythonUDF expression (no real Python needed) ---
  // A trivially serialized no-op function — the plan tests only check structure, not execution.
  private val dummySerializedFunc: Array[Byte] = Array[Byte](0x80.toByte, 0x04.toByte)

  private def makeUDF(name: String, inputCol: Column, returnType: DataType): Column = {
    val expr = InProcessPythonUDF(
      name          = name,
      serializedFunc = dummySerializedFunc,
      children      = Seq(inputCol.expr),
      dataType      = returnType)
    new Column(expr)
  }

  // ---------------------------------------------------------------------------
  // Plan shape tests (no jep required)
  // ---------------------------------------------------------------------------

  test("ExtractInProcessPythonUDFs inserts InProcessEvalPython into logical plan") {
    val df = spark.range(10)
    val doubled = makeUDF("double", df("id"), LongType)
    val plan = df.select(doubled).queryExecution.optimizedPlan

    val evalNodes = plan.collect { case n: InProcessEvalPython => n }
    assert(evalNodes.size === 1,
      s"Expected 1 InProcessEvalPython node, got ${evalNodes.size}")
    assert(evalNodes.head.udfs.size === 1)
    assert(evalNodes.head.udfs.head.name === "double")
  }

  test("InProcessEvalPython is planned as InProcessArrowEvalExec") {
    val df = spark.range(10)
    val doubled = makeUDF("double", df("id"), LongType)
    val execPlan = df.select(doubled).queryExecution.executedPlan

    val execNodes = execPlan.collect { case n: InProcessArrowEvalExec => n }
    assert(execNodes.size === 1,
      s"Expected 1 InProcessArrowEvalExec node, got ${execNodes.size}")
  }

  test("multiple in-process UDFs on the same input are fused into one InProcessEvalPython") {
    val df = spark.range(10)
    val doubled = makeUDF("double", df("id"), LongType)
    val tripled = makeUDF("triple", df("id"), LongType)
    val plan = df.select(doubled, tripled).queryExecution.optimizedPlan

    val evalNodes = plan.collect { case n: InProcessEvalPython => n }
    assert(evalNodes.size === 1, "Expected one InProcessEvalPython for two UDFs on same input")
    assert(evalNodes.head.udfs.size === 2)
  }

  test("InProcessPythonChecks rejects config allowing multiple concurrent tasks") {
    val df = spark.range(10)
    val doubled = makeUDF("double", df("id"), LongType)

    withSQLConf(
      "spark.executor.cores" -> "4",
      "spark.task.cpus"      -> "1") {
      val ex = intercept[IllegalArgumentException] {
        df.select(doubled).queryExecution.optimizedPlan
      }
      assert(ex.getMessage.contains("4 concurrent tasks"))
      assert(ex.getMessage.contains("spark.executor.cores"))
    }
  }

  test("InProcessPythonChecks passes when executor.cores == task.cpus") {
    val df = spark.range(10)
    val doubled = makeUDF("double", df("id"), LongType)

    // Should not throw
    withSQLConf(
      "spark.executor.cores" -> "2",
      "spark.task.cpus"      -> "2") {
      val plan = df.select(doubled).queryExecution.optimizedPlan
      assert(plan.collect { case n: InProcessEvalPython => n }.size === 1)
    }
  }

  test("InProcessArrowBridge.arrowFormatString covers all Phase 1 types") {
    val cases = Seq(
      LongType    -> "l",
      IntegerType -> "i",
      DoubleType  -> "g",
      FloatType   -> "f",
      BooleanType -> "b",
      ShortType   -> "s",
      ByteType    -> "c")

    cases.foreach { case (dt, expected) =>
      val got = InProcessArrowBridge.arrowFormatString(dt)
      assert(got === expected, s"Expected $expected for $dt, got $got")
    }
  }

  test("InProcessArrowBridge.arrowFormatString rejects unsupported types") {
    val unsupported = Seq(StringType, BinaryType, ArrayType(LongType), MapType(StringType, LongType))
    unsupported.foreach { dt =>
      intercept[UnsupportedOperationException] {
        InProcessArrowBridge.arrowFormatString(dt)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Execution tests (require jep + CPython + PyArrow on the test classpath)
  // Run with: -Dinprocess.tests=true
  // ---------------------------------------------------------------------------

  private val runExecutionTests =
    sys.props.getOrElse("inprocess.tests", "false").toBoolean

  if (runExecutionTests) {
    test("end-to-end: in-process double(long) UDF returns correct results") {
      // Serialize a real Python UDF via cloudpickle (requires Python + cloudpickle)
      val serialized = serializePythonUDF(
        "import pyarrow.compute as pc\ndef double(x): return pc.multiply(x, 2)")

      val df = spark.range(1, 6)  // [1, 2, 3, 4, 5]
      val udf = InProcessPythonUDF("double", serialized, Seq(df("id").expr), LongType)
      val result = df.select(new Column(udf)).collect().map(_.getLong(0))
      assert(result.toSeq === Seq(2L, 4L, 6L, 8L, 10L))
    }

    test("end-to-end: in-process UDF with nulls preserves null positions") {
      val serialized = serializePythonUDF(
        "import pyarrow.compute as pc\ndef negate(x): return pc.negate(x)")

      val data = Seq(Some(1L), None, Some(3L))
      val df = spark.createDataset(data).toDF("v")
      val udf = InProcessPythonUDF("negate", serialized, Seq(df("v").expr), LongType)
      val result = df.select(new Column(udf)).collect()

      assert(result(0).getLong(0) === -1L)
      assert(result(1).isNullAt(0))
      assert(result(2).getLong(0) === -3L)
    }
  }

  /**
   * Serializes a Python function definition via cloudpickle by running a small Python script.
   * Requires Python + cloudpickle on the test machine's PATH.
   */
  private def serializePythonUDF(pythonCode: String): Array[Byte] = {
    import java.io.{File, FileOutputStream}
    import scala.sys.process._

    val script = s"""
      |import cloudpickle, sys
      |$pythonCode
      |func_name = [k for k in dir() if not k.startswith('_') and callable(eval(k))][0]
      |func = eval(func_name)
      |sys.stdout.buffer.write(cloudpickle.dumps(func))
      """.stripMargin

    val proc = Process(Seq("python3", "-c", script))
    val buf = new java.io.ByteArrayOutputStream()
    val exit = proc.#>(buf).run().exitValue()
    require(exit == 0, s"Failed to serialize Python UDF (exit code $exit)")
    buf.toByteArray
  }
}
