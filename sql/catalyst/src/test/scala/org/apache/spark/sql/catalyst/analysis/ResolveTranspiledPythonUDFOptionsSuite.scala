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
  Add,
  Alias,
  AttributeReference,
  Concat,
  Expression,
  Literal,
  PythonUDF,
  TranspiledPythonUDF
}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StringType}

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

  test("keeps the map option for a map column and drops it for a non-map column") {
    // Mirrors `def f(x: dict, y: int): return y + 1` (SPARK-55219): the map param is
    // pinned "map" and the numeric one "numeric", so the option survives only when
    // the first argument is a MapType. The body reads only the numeric param.
    // AttributeReferences are built directly because the `$"a".map(...)` DSL helper
    // collides with the `map` method inherited from TreeNode.
    val m = AttributeReference("a", MapType(StringType, StringType))()
    val n = $"b".long
    val opt = Add(n, Literal(1L))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(m, n)), List(opt), List(List("map", "numeric")))
    val pruned = prune(node, LocalRelation(m, n))
    assert(pruned.transpiledOptions == List(opt))
    assert(pruned.optionInputCategories.isEmpty)

    val s = $"a".string
    val dropped =
      TranspiledPythonUDF("udf", pyUDF(Seq(s, n)), List(opt), List(List("map", "numeric")))
    assert(prune(dropped, LocalRelation(s, n)).transpiledOptions.isEmpty)
  }

  test("keeps the array option for an array column and drops it for a non-array column") {
    // Same shape as the map test, with the first param pinned "array" (SPARK-55219).
    val arr = AttributeReference("a", ArrayType(StringType))()
    val n = $"b".long
    val opt = Add(n, Literal(1L))
    val node =
      TranspiledPythonUDF("udf", pyUDF(Seq(arr, n)), List(opt), List(List("array", "numeric")))
    val pruned = prune(node, LocalRelation(arr, n))
    assert(pruned.transpiledOptions == List(opt))
    assert(pruned.optionInputCategories.isEmpty)

    val s = $"a".string
    val dropped =
      TranspiledPythonUDF("udf", pyUDF(Seq(s, n)), List(opt), List(List("array", "numeric")))
    assert(prune(dropped, LocalRelation(s, n)).transpiledOptions.isEmpty)
  }

  test("leaves options untouched when categories are empty (no restriction)") {
    val a = $"a".long
    val onlyOpt = Add(a, Literal(1L))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(onlyOpt), Nil)
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(onlyOpt))
  }
}
