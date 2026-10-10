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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{
  Add, Alias, AttributeReference, Concat, Expression, Literal, PythonUDF, TranspiledPythonUDF}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, LongType}

/**
 * Unit tests for [[ResolveTranspiledPythonUDFOptions]], which prunes a
 * TranspiledPythonUDF's per-input-type options to those whose declared categories match the
 * resolved argument types. func=null in the leaf PythonUDF is intentional: these structural
 * tests don't execute Python.
 */
class ResolveTranspiledPythonUDFOptionsSuite extends PlanTest {

  private def pyUDF(children: Seq[Expression]): PythonUDF =
    PythonUDF("udf", null, LongType, children,
      PythonEvalType.SQL_BATCHED_UDF, udfDeterministic = true)

  // Runs the rule on a Project that wraps the node, and returns the (possibly pruned) node.
  private def prune(node: TranspiledPythonUDF, rel: LocalRelation): TranspiledPythonUDF = {
    val rewritten = ResolveTranspiledPythonUDFOptions(Project(Seq(Alias(node, "r")()), rel))
    rewritten.expressions.flatMap(_.collect { case t: TranspiledPythonUDF => t }).head
  }

  test("keeps the numeric option for numeric columns and drops the string one") {
    val a = $"a".long
    val b = $"b".long
    val numericOpt = Add(a, b)
    val stringOpt = Concat(Seq(a, b))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a, b)), List(numericOpt, stringOpt),
      List(List("numeric", "numeric"), List("string", "string")))
    val pruned = prune(node, LocalRelation(a, b))
    assert(pruned.transpiledOptions == List(numericOpt))
    assert(pruned.optionInputCategories.isEmpty)
  }

  test("keeps the string option for string columns and drops the numeric one") {
    val a = $"a".string
    val b = $"b".string
    val numericOpt = Add(a, b)
    val stringOpt = Concat(Seq(a, b))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a, b)), List(numericOpt, stringOpt),
      List(List("numeric", "numeric"), List("string", "string")))
    val pruned = prune(node, LocalRelation(a, b))
    assert(pruned.transpiledOptions == List(stringOpt))
    assert(pruned.optionInputCategories.isEmpty)
  }

  test("empties the options when no category set matches (falls back to Python UDF)") {
    val a = $"a".string
    val b = $"b".long
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a, b)),
      List(Add(a, b), Concat(Seq(a, b))),
      List(List("numeric", "numeric"), List("string", "string")))
    val pruned = prune(node, LocalRelation(a, b))
    assert(pruned.transpiledOptions.isEmpty)
    assert(pruned.optionInputCategories.isEmpty)
  }

  test("matches binary columns against neither category (string is StringType only)") {
    val a = $"a".binary
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)),
      List(Concat(Seq(a, a))), List(List("string")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions.isEmpty)
  }

  test("leaves options untouched when categories are empty (no restriction)") {
    val a = $"a".long
    val onlyOpt = Add(a, Literal(1L))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(onlyOpt), Nil)
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(onlyOpt))
  }

  test("keeps the integer option for integral columns and drops float and string") {
    val a = $"a".long
    val intOpt = Add(a, Literal(1L))
    val floatOpt = Add(a, Literal(1.0))
    val stringOpt = Concat(Seq(a, a))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)),
      List(intOpt, floatOpt, stringOpt),
      List(List("integer"), List("float"), List("string")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(intOpt))
    assert(pruned.optionInputCategories.isEmpty)
  }

  test("keeps the float option for fractional columns and drops integer and string") {
    val a = AttributeReference("a", DoubleType)()
    val intOpt = Add(a, Literal(1L))
    val floatOpt = Add(a, Literal(1.0))
    val stringOpt = Concat(Seq(a, a))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)),
      List(intOpt, floatOpt, stringOpt),
      List(List("integer"), List("float"), List("string")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(floatOpt))
    assert(pruned.optionInputCategories.isEmpty)
  }

  test("integer does not match float columns; float does not match integer columns") {
    val aLong = $"a".long
    val aDouble = AttributeReference("a", DoubleType)()
    val intOpt = Add(aLong, Literal(1L))
    val floatOpt = Add(aLong, Literal(1.0))
    // float column, integer-only option -> dropped
    val intOnlyForDouble = TranspiledPythonUDF("udf", pyUDF(Seq(aDouble)),
      List(intOpt), List(List("integer")))
    assert(prune(intOnlyForDouble, LocalRelation(aDouble)).transpiledOptions.isEmpty)
    // long column, float-only option -> dropped
    val floatOnlyForLong = TranspiledPythonUDF("udf", pyUDF(Seq(aLong)),
      List(floatOpt), List(List("float")))
    assert(prune(floatOnlyForLong, LocalRelation(aLong)).transpiledOptions.isEmpty)
  }

  test("neither integer nor float matches decimal columns") {
    val a = AttributeReference("a", DecimalType(10, 2))()
    val intOpt = Add(a, Literal(1L))
    val floatOpt = Add(a, Literal(1.0))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)),
      List(intOpt, floatOpt),
      List(List("integer"), List("float")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions.isEmpty)
  }

  test("float matches FloatType columns") {
    val a = AttributeReference("a", FloatType)()
    val floatOpt = Add(a, Literal(1.0f))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)),
      List(floatOpt), List(List("float")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(floatOpt))
  }
}
