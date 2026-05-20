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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

/**
 * Unit tests for the ConvertToCatalyst optimizer rule, which rewrites
 * TranspiledPythonUDF nodes to their Catalyst equivalents.
 *
 * These tests exercise the rule directly via applyExpr rather than running the
 * full optimizer pipeline, which means no JVM/Python bridge is required.
 */
class ConvertToCatalystSuite extends PlanTest {

  private val attrA = $"a".long

  // A leaf PythonUDF that takes one column argument. func=null is intentional:
  // structural tests don't need an executable PythonFunction.
  private def makePyUDF(input: Expression = attrA): PythonUDF =
    PythonUDF("udf", null, LongType, Seq(input),
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)

  // A TranspiledPythonUDF wrapping pyUDF with a single Catalyst option.
  private def makeTPUDF(pyUDF: PythonUDF, catalystOpt: Expression): TranspiledPythonUDF =
    TranspiledPythonUDF("udf", pyUDF, List(catalystOpt))

  private val catalystExpr: Expression = Add(attrA, Literal(4L))

  // ---- helpers ----

  // Both ANSI and ATTEMPT_TRANSPILATION must be true for the transpile path to fire.
  private def transpileOn[T](block: => T): T =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true") { block }

  private def ansiOff[T](block: => T): T =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "true") { block }

  private def transpileOff[T](block: => T): T =
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ATTEMPT_TRANSPILATION_OF_PYTHON_UDFS.key -> "false") { block }

  // ---- tests ----

  test("transpiles when not nested (parent_is_udf = false)") {
    transpileOn {
      val tpudf = makeTPUDF(makePyUDF(), catalystExpr)
      val result = ConvertToCatalyst.applyExpr(tpudf, parent_is_udf = false)
      assert(!result.isInstanceOf[TranspiledPythonUDF])
      assert(!result.isInstanceOf[PythonUDF])
    }
  }

  test("guard prevents transpilation when parent_is_udf=true and inputs are plain PythonUDFs") {
    // PythonUDF -> TranspiledPythonUDF -> PythonUDF: the middle node should NOT be
    // transpiled when called from an outer UDF context, to preserve the batch pipeline.
    transpileOn {
      val innerPyUDF = makePyUDF(attrA)
      val outerPyUDF = makePyUDF(innerPyUDF)
      val outerTPUDF = makeTPUDF(outerPyUDF, Add(innerPyUDF, Literal(4L)))
      val result = ConvertToCatalyst.applyExpr(outerTPUDF, parent_is_udf = true)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("guard does not prevent transpilation when input to pythonUDFExpr is a TranspiledPythonUDF") {
    // When the input to a TPUDF is itself a TranspiledPythonUDF (has a Catalyst alternative),
    // hasOnlyPythonUDFInputs returns false so the outer TPUDF still transpiles.
    transpileOn {
      val innerPyUDF = makePyUDF(attrA)
      val innerTPUDF = makeTPUDF(innerPyUDF, catalystExpr)
      val outerPyUDF = makePyUDF(innerTPUDF)
      val outerTPUDF = makeTPUDF(outerPyUDF, Add(innerTPUDF, Literal(4L)))
      val result = ConvertToCatalyst.applyExpr(outerTPUDF, parent_is_udf = true)
      assert(!result.isInstanceOf[TranspiledPythonUDF])
      assert(!result.isInstanceOf[PythonUDF])
    }
  }

  test("hasOnlyPythonUDFInputs unit test") {
    val innerPyUDF = makePyUDF(attrA)
    val innerTPUDF = makeTPUDF(innerPyUDF, catalystExpr)

    // pythonUDFExpr's child is a plain PythonUDF -> true
    assert(makeTPUDF(makePyUDF(innerPyUDF), catalystExpr).hasOnlyPythonUDFInputs)
    // pythonUDFExpr's child is a TranspiledPythonUDF -> false
    assert(!makeTPUDF(makePyUDF(innerTPUDF), catalystExpr).hasOnlyPythonUDFInputs)
    // pythonUDFExpr's child is a plain column (leaf) -> false
    assert(!makeTPUDF(makePyUDF(attrA), catalystExpr).hasOnlyPythonUDFInputs)
    // zero-arg pythonUDFExpr -> false (nonEmpty guard)
    val zeroPyUDF = PythonUDF("udf", null, LongType, Seq.empty,
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)
    assert(!TranspiledPythonUDF("udf", zeroPyUDF, List(Literal(42L))).hasOnlyPythonUDFInputs)
  }

  test("falls back to PythonUDF when ANSI is disabled") {
    ansiOff {
      val tpudf = makeTPUDF(makePyUDF(), catalystExpr)
      val result = ConvertToCatalyst.applyExpr(tpudf, parent_is_udf = false)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("falls back to PythonUDF when transpilation is disabled") {
    transpileOff {
      val tpudf = makeTPUDF(makePyUDF(), catalystExpr)
      val result = ConvertToCatalyst.applyExpr(tpudf, parent_is_udf = false)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }

  test("falls back to PythonUDF when transpiledOptions is empty") {
    transpileOn {
      val pyUDF = makePyUDF()
      val tpudf = TranspiledPythonUDF("udf", pyUDF, List())
      val result = ConvertToCatalyst.applyExpr(tpudf, parent_is_udf = false)
      assert(result.isInstanceOf[PythonUDF])
      assert(!result.isInstanceOf[TranspiledPythonUDF])
    }
  }
}
