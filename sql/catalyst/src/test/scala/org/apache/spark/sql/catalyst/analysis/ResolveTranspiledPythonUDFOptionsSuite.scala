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
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Concat, Expression, GreaterThan, Literal, Multiply, PythonUDF, Rand, TranspiledPythonUDF}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.LongType

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

  test("empties the options when an argument is non-deterministic") {
    // EXPECTED TO FAIL until the companion fix to this rule lands. Deliberately
    // red: it pins a real wrong-results bug rather than tolerating it silently.
    // Do not ignore or cancel it.
    //
    // The builder substitutes the argument expression at EVERY `_udf_param_N`
    // occurrence, so a non-deterministic argument becomes several independent
    // expressions with independent state. Since `and` / `or` and chained
    // comparisons evaluate later copies only on rows where earlier links held,
    // the copies desynchronize and stop describing the same value. The rule must
    // drop every option so ConvertToCatalyst falls back to the Python UDF, which
    // evaluates each argument exactly once per row.
    //
    // This must be checked here rather than in the builder: at call-construction
    // time `rand(7)` is still an `UnresolvedFunction` whose `deterministic`
    // derives from its `Literal` seed and so reports true.
    val a = $"a".double
    val arg = Multiply(Rand(Literal(7L)), Literal(100.0))
    assert(!arg.deterministic, "the fixture argument must be non-deterministic")
    val opt = GreaterThan(arg, Literal(50.0))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(arg)), List(opt), List(List("numeric")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions.isEmpty)
    assert(pruned.optionInputCategories.isEmpty)
  }

  test("keeps options when the argument is deterministic (control for the check above)") {
    // Same shape as the non-deterministic case, but with a plain column argument, so
    // the non-determinism guard must not fire and normal category matching applies.
    val a = $"a".double
    val opt = GreaterThan(a, Literal(50.0))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(opt), List(List("numeric")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(opt))
  }
}
