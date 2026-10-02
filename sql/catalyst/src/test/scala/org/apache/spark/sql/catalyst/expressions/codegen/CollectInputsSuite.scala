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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/**
 * The policies of `CodegenContext.collectInputs`, one test per point where the places that move
 * generated code into a method answer differently; and the whole stage split built on it.
 */
class CollectInputsSuite extends SparkFunSuite with SQLHelper {

  private def evaluated(i: Int): ExprCode =
    ExprCode(EmptyBlock, JavaCode.isNullVariable(s"isNull_$i"),
      JavaCode.variable(s"value_$i", IntegerType))

  private def deferred(i: Int): ExprCode =
    ExprCode(code"int value_$i = row.getInt($i);", FalseLiteral,
      JavaCode.variable(s"value_$i", IntegerType))

  private def input(i: Int): BoundReference = BoundReference(i, IntegerType, nullable = true)

  private def names(inputs: CollectedInputs): Seq[String] = inputs.arguments.map(_.variableName)

  /** A context whose `INPUT_ROW` is `row`; a new one names `i`, so the default is set apart. */
  private def context(row: String = null): CodegenContext = {
    val ctx = new CodegenContext
    ctx.INPUT_ROW = row
    ctx
  }

  private val operator = InputPolicy.operatorMethod
  private val commonExpr = InputPolicy.commonExpressionMethod

  test("INPUT_ROW comes first wherever it is set") {
    val ctx = context("row")
    ctx.currentVars = Seq(evaluated(0))
    for (policy <- Seq(operator, commonExpr)) {
      val inputs = ctx.collectInputs(Seq(Add(input(0), Literal(1))), policy, Map.empty).get
      assert(names(inputs) == Seq("row", "value_0", "isNull_0"))
    }
  }

  test("an input the operator has not evaluated: hoisted for an operator's method, refused for " +
      "a common expression's") {
    val ctx = context()
    val notYet = deferred(1)
    ctx.currentVars = Seq(evaluated(0), notYet)
    val expr = Add(input(0), input(1))
    assert(ctx.collectInputs(Seq(expr), commonExpr, Map.empty).isEmpty)
    assert(notYet.code.nonEmpty, "a refusal leaves the input's code where it is")

    val inputs = ctx.collectInputs(Seq(expr), operator, Map.empty).get
    assert(names(inputs).toSet == Set("value_0", "isNull_0", "value_1"))
    assert(inputs.inputsToEvaluate.map(_.value.toString) == Seq("value_1"))
    assert(notYet.code == EmptyBlock, "the hoisted input is evaluated ahead of the method")
  }

  test("a subexpression state: its variables for an operator's method, refused for a common " +
      "expression's") {
    val ctx = context()
    ctx.currentVars = Seq(evaluated(0))
    val common = Multiply(input(0), Literal(3))
    val state = SubExprEliminationState(ExprCode(EmptyBlock,
      JavaCode.isNullVariable("subExprIsNull_0"), JavaCode.variable("subExpr_0", IntegerType)))
    val subExprs = Map(ExpressionEquals(common) -> state)
    val expr = Add(common, Literal(1))
    assert(ctx.collectInputs(Seq(expr), commonExpr, subExprs).isEmpty)
    val inputs = ctx.collectInputs(Seq(expr), operator, subExprs).get
    assert(names(inputs) == Seq("subExpr_0", "subExprIsNull_0"),
      "the walk stops at the state and takes nothing below it")
  }

  test("a With: a common expression's method follows its references, an operator's method its " +
      "children") {
    val ctx = context()
    ctx.currentVars = Seq(evaluated(0))
    // A definition no reference reaches is not generated, so a method computing the `With` reads
    // nothing of it; the walk over the children still reaches it.
    val unreached = With(Literal(1), Seq(CommonExpressionDef(input(0))))
    assert(names(ctx.collectInputs(Seq(unreached), commonExpr, Map.empty).get).isEmpty)
    assert(names(ctx.collectInputs(Seq(unreached), operator, Map.empty).get) ==
      Seq("value_0", "isNull_0"))

    val definition = CommonExpressionDef(input(0))
    val reached = With(Add(new CommonExpressionRef(definition), Literal(1)), Seq(definition))
    assert(names(ctx.collectInputs(Seq(reached), commonExpr, Map.empty).get) ==
      Seq("value_0", "isNull_0"))
  }

  test("a value no parameter can name: refused for a common expression's method, passed over " +
      "for an operator's") {
    val ctx = context()
    ctx.currentVars = Seq(ExprCode(EmptyBlock, JavaCode.isNullExpression("index_0 == -1"),
      JavaCode.variable("index_0", IntegerType)))
    val expr = Add(input(0), Literal(1))
    assert(ctx.collectInputs(Seq(expr), commonExpr, Map.empty).isEmpty)
    assert(names(ctx.collectInputs(Seq(expr), operator, Map.empty).get) == Seq("index_0"))
  }

  test("the parameter limit: checked for a common expression's method, left to the caller of an " +
      "operator's") {
    val ctx = context()
    // 128 longs take 256 slots, past the 255 a method has.
    ctx.currentVars = (0 until 128).map { i =>
      ExprCode(EmptyBlock, FalseLiteral, JavaCode.variable(s"long_$i", LongType))
    }
    val expr = (0 until 128).map(i => BoundReference(i, LongType, nullable = false): Expression)
      .reduce(Add(_, _))
    assert(ctx.collectInputs(Seq(expr), commonExpr, Map.empty).isEmpty)
    assert(ctx.collectInputs(Seq(expr), operator, Map.empty).get.arguments.length == 128)
  }

  test("an input outside currentVars is read from INPUT_ROW") {
    val ctx = context("row")
    val inputs = ctx.collectInputs(Seq(input(0)), operator, Map.empty).get
    assert(inputs.readsRow)
    ctx.currentVars = Seq(evaluated(0))
    assert(!ctx.collectInputs(Seq(input(0)), operator, Map.empty).get.readsRow)
  }

  test("a slot of a compacted mutable state array: a field for a whole stage split, refused for " +
      "a common expression's method, passed as it is for an operator's") {
    val ctx = context()
    // A state of a type that is not primitive is compacted into an array, and its name is a slot.
    val slot = ctx.addMutableState("UTF8String", "value")
    assert(slot.matches("\\w+\\[\\d+\\]"), slot)
    ctx.currentVars = Seq(ExprCode(EmptyBlock, FalseLiteral, JavaCode.variable(slot, StringType)))
    val expr = Length(BoundReference(0, StringType, nullable = false))
    assert(names(ctx.collectInputs(Seq(expr), InputPolicy.wholeStageSplit, Map.empty).get).isEmpty)
    assert(ctx.collectInputs(Seq(expr), commonExpr, Map.empty).isEmpty)
    assert(names(ctx.collectInputs(Seq(expr), operator, Map.empty).get) == Seq(slot))
  }

  test("the whole stage split leaves a block it cannot move inline, between the runs of calls") {
    // Under whole stage codegen (`currentVars` set), each piece is a block at this threshold; the
    // middle one reads an input not evaluated yet, so it stays where it is, and the calls before
    // and after it are two runs.
    withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1") {
      val ctx = context()
      ctx.currentVars = Seq(evaluated(0), deferred(1))
      val pieces = Seq(
        "int a = value_0;" -> Seq(input(0)),
        "int b = value_1;" -> Seq(input(1)),
        "int c = value_0;" -> Seq(input(0)))
      val code = ctx.splitExpressionsWithSources(pieces, "f")
      val calls = "f_\\d+_\\d+\\(value_0, isNull_0\\)".r.findAllIn(code).toSeq
      assert(calls.length == 2, code)
      val inline = code.indexOf("int b = value_1;")
      assert(code.indexOf(calls.head) < inline && inline < code.lastIndexOf(calls.last), code)
      assert(!code.contains("int a = value_0;") && !code.contains("int c = value_0;"),
        "the other blocks are in methods")
    }
  }

  test("a split call passing what its enclosing method does not take fails the audit") {
    withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1") {
      val ctx = context()
      ctx.currentVars = Seq(evaluated(0))
      val code = ctx.splitExpressionsWithSources(
        Seq("int a = value_0;" -> Seq(input(0)), "int b = value_0;" -> Seq(input(0))), "f")
      ctx.assertSplitCallsWithin(code, Seq("value_0", "isNull_0"), "m")
      val e = intercept[AssertionError] {
        ctx.assertSplitCallsWithin(code, Seq("value_0"), "m")
      }
      assert(e.getMessage.contains("passing isNull_0, which m does not take"), e.getMessage)
    }
  }
}
