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
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Cast, Concat, Expression, Literal, PythonUDF, TranspiledPythonUDF}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{ByteType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

/**
 * Unit tests for [[ResolveTranspiledPythonUDFOptions]], which prunes a
 * TranspiledPythonUDF's per-input-type options to those whose declared categories match the
 * resolved argument types. func=null in the leaf PythonUDF is intentional: these structural
 * tests don't execute Python.
 */
class ResolveTranspiledPythonUDFOptionsSuite extends PlanTest with AnalysisTest {

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

  // "integral", "integral32" and "fractional" must each match strictly less than
  // "numeric" does (see ResolveTranspiledPythonUDFOptions). The option expressions
  // are stand-ins -- the rule only reads the declared categories. The column is
  // built inside the test body: `$"a"` goes through CatalystSqlParser and touching
  // SQLConf while the suite is still being constructed aborts it.
  private def narrowNode(column: AttributeReference, category: String):
      (Expression, TranspiledPythonUDF) = {
    val opt = Add(column, column)
    (opt, TranspiledPythonUDF("udf", pyUDF(Seq(column)), List(opt), List(List(category))))
  }

  Seq(
    // (category, column type, whether the option survives pruning)
    ("integral", LongType, true),
    ("integral", IntegerType, true),
    ("integral", DoubleType, false),
    ("integral", StringType, false),
    ("integral32", IntegerType, true),
    ("integral32", ShortType, true),
    ("integral32", ByteType, true),
    // A bigint above 2^53 loses precision on the cast to double, so int32-only.
    ("integral32", LongType, false),
    ("integral32", DoubleType, false),
    ("fractional", DoubleType, true),
    // Python has no single-precision float, so a float column's value arrives in the
    // UDF as a double and every step runs in double precision, while an expression
    // that stays in FloatType rounds to 24 bits per step (and overflows to Infinity
    // far earlier). No numeric category admits it -- see the pair below.
    ("fractional", FloatType, false),
    ("numeric", FloatType, false),
    ("numeric", DoubleType, true),
    ("fractional", LongType, false)
  ).foreach { case (category, dataType, survives) =>
    val verb = if (survives) "keeps" else "drops"
    test(s"$verb the $category option for a ${dataType.simpleString} column") {
      val column = AttributeReference("a", dataType)()
      val (opt, node) = narrowNode(column, category)
      val pruned = prune(node, LocalRelation(column))
      assert(pruned.transpiledOptions == (if (survives) List(opt) else Nil))
      assert(pruned.optionInputCategories.isEmpty)
    }
  }

  test("matches a decimal column against no numeric category (interpreted Python only)") {
    // Python receives decimal.Decimal for a decimal column, whose arithmetic and
    // precision semantics differ from Spark's, so every numeric category excludes it --
    // including "fractional", which DecimalType extends.
    val a = AttributeReference("a", DecimalType(10, 2))()
    Seq("numeric", "integral", "integral32", "fractional").foreach { category =>
      val (_, node) = narrowNode(a, category)
      assert(prune(node, LocalRelation(a)).transpiledOptions.isEmpty, category)
    }
  }

  test("picks the integral option over the fractional one for a bigint column") {
    // The transpiler emits one option per input-type variant for a body whose lowering
    // depends on integrality (e.g. `a // b`); exactly one must survive per column type.
    val a = $"a".long
    val integralOpt = Add(a, Literal(1L))
    val fractionalOpt = Add(a, Literal(2L))
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(integralOpt, fractionalOpt),
      List(List("integral"), List("fractional")))
    val pruned = prune(node, LocalRelation(a))
    assert(pruned.transpiledOptions == List(integralOpt))
  }

  Seq(IntegerType -> true, DoubleType -> false).foreach { case (countType, survives) =>
    val verb = if (survives) "keeps" else "drops"
    test(s"$verb a mixed string/integral option for a ${countType.simpleString} count") {
      // `"ab" * n` lowers to `repeat`, whose count Spark casts to int -- which would
      // silently truncate the 2.5 that Python rejects with a TypeError. The transpiler
      // therefore tags the count "integral", so a fractional count column drops the
      // option and the UDF falls back. Mixed category lists must be matched per
      // position, which nothing else in this suite covers.
      val s = AttributeReference("s", StringType)()
      val n = AttributeReference("n", countType)()
      val opt = Concat(Seq(s, s))
      val node = TranspiledPythonUDF("udf", pyUDF(Seq(s, n)), List(opt),
        List(List("string", "integral")))
      val pruned = prune(node, LocalRelation(s, n))
      assert(pruned.transpiledOptions == (if (survives) List(opt) else Nil))
    }
  }

  // SPARK-59090 probe (added on top of SPARK-55210): an option that matches its
  // category but can never resolve must be DROPPED by analysis so the call falls
  // back to interpreted Python, rather than failing the whole query at
  // CheckAnalysis. `cast(binary as bigint)` matches the "binary" category yet the
  // Cast is unresolved (binary is not castable to bigint), so this is exactly the
  // shape that needs the drop-unresolved rule. Without that rule (SPARK-59090 not
  // folded in), full analysis throws DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION
  // instead of dropping -- which is the failure this test pins.
  test("SPARK-59090: full analysis drops an unresolvable category-matching option") {
    val a = $"a".binary
    val neverResolves = Cast(a, LongType)
    assert(!neverResolves.resolved, "cast(binary,long) must be unresolved")
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(neverResolves),
      List(List("binary")))
    val plan = Project(Seq(Alias(node, "r")()), LocalRelation(a))
    val analyzed = getAnalyzer.executeAndCheck(plan, new QueryPlanningTracker)
    val out = analyzed.expressions.flatMap(_.collect { case t: TranspiledPythonUDF => t }).head
    assert(out.transpiledOptions.isEmpty,
      "an unresolvable option must be dropped so the call falls back to Python")
  }

  // Dropping an unresolved option resolves the UDF, but an enclosing UnresolvedAlias
  // (from `SELECT udf(a)`) was left untouched when the drop batch was Once: the
  // Resolution batch had already converged, so ResolveAliases never ran again.
  // CheckAnalysis then failed on the UnresolvedAlias instead of falling back.
  test("SPARK-59090: unaliased call with an unresolvable option still analyzes") {
    val a = $"a".binary
    val neverResolves = Cast(a, LongType)
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(neverResolves),
      List(List("binary")))
    // UnresolvedAlias, not Alias -- this is what `df.select(udf(col))` produces.
    val plan = Project(Seq(UnresolvedAlias(node, None)), LocalRelation(a))
    val analyzed = getAnalyzer.executeAndCheck(plan, new QueryPlanningTracker)
    // The UnresolvedAlias must be resolved to an Alias, not left unresolved.
    assert(analyzed.expressions.head.isInstanceOf[Alias],
      "an unaliased UDF whose option is dropped must still get an alias")
    val out = analyzed.expressions.flatMap(_.collect { case t: TranspiledPythonUDF => t }).head
    assert(out.transpiledOptions.isEmpty,
      "an unresolvable option must be dropped so the call falls back to Python")
  }

  // A reference to an aliased UDF result whose option is dropped: the inner Alias
  // becomes resolvable only after the drop, so the outer UnresolvedAttribute
  // needs ResolveReferences to run again too. (Uses a long literal so the Add's
  // operands match without a type-coercion re-run, which is a separate concern.)
  test("SPARK-59090: reference to an aliased call with an unresolvable option resolves") {
    val a = $"a".binary
    val neverResolves = Cast(a, LongType)
    val node = TranspiledPythonUDF("udf", pyUDF(Seq(a)), List(neverResolves),
      List(List("binary")))
    val inner = Project(Seq(Alias(node, "r")()), LocalRelation(a))
    val r = inner.output.head
    // `r + 1L` references the inner alias; unresolved until the inner UDF resolves.
    val outer = Project(Seq(Alias(Add(r, Literal(1L)), "r1")()), inner)
    val analyzed = getAnalyzer.executeAndCheck(outer, new QueryPlanningTracker)
    // The TranspiledPythonUDF is in the inner Project, not the outer one.
    val out = analyzed.children.head.expressions
      .flatMap(_.collect { case t: TranspiledPythonUDF => t }).head
    assert(out.transpiledOptions.isEmpty,
      "an unresolvable option must be dropped so the call falls back to Python")
  }
}
