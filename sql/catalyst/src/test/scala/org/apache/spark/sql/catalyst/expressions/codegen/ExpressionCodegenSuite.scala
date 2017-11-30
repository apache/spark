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
import org.apache.spark.sql.types.IntegerType

class ExpressionCodegenSuite extends SparkFunSuite {

  test("Returns eliminated subexpressions for expression") {
    val ctx = new CodegenContext()
    val subExpr = Add(Literal(1), Literal(2))
    val exprs = Seq(Add(subExpr, Literal(3)), Add(subExpr, Literal(4)))

    ctx.generateExpressions(exprs, doSubexpressionElimination = true)
    val subexpressions = ExpressionCodegen.getSubExprInChildren(ctx, exprs(0))
    assert(subexpressions.length == 1 && subexpressions(0) == subExpr)
  }

  test("Gets parameters for subexpressions") {
    val ctx = new CodegenContext()
    val subExprs = Seq(
      Add(Literal(1), AttributeReference("a", IntegerType, nullable = false)()), // non-nullable
      Add(Literal(2), AttributeReference("b", IntegerType, nullable = true)()))  // nullable

    ctx.subExprEliminationExprs.put(subExprs(0), SubExprEliminationState("false", "value1"))
    ctx.subExprEliminationExprs.put(subExprs(1), SubExprEliminationState("isNull2", "value2"))

    val params = ExpressionCodegen.getParamsForSubExprs(ctx, subExprs)
    assert(params.length == 3)
    assert(params(0) == Tuple2("value1", "int value1"))
    assert(params(1) == Tuple2("value2", "int value2"))
    assert(params(2) == Tuple2("isNull2", "boolean isNull2"))
  }

  test("Returns input variables for expression: current variables") {
    val ctx = new CodegenContext()
    val currentVars = Seq(
      ExprCode("", isNull = "false", value = "value1"),
      ExprCode("", isNull = "isNull2", value = "value2"),
      ExprCode("fake code;", isNull = "isNull3", value = "value3"))
    ctx.currentVars = currentVars
    ctx.INPUT_ROW = null

    val expr = If(Literal(false),
      Add(BoundReference(0, IntegerType, nullable = false),
          BoundReference(1, IntegerType, nullable = true)),
        BoundReference(2, IntegerType, nullable = true))

    val (inputAttrs, inputVars) = ExpressionCodegen.getInputVarsForChildren(ctx, expr)
    assert(inputAttrs.length == 2)
    assert(inputAttrs(0) == BoundReference(0, IntegerType, nullable = false))
    assert(inputAttrs(1) == BoundReference(1, IntegerType, nullable = true))

    assert(inputVars.length == 2)
    assert(inputVars(0) == currentVars(0))
    assert(inputVars(1) == currentVars(1))

    val params = ExpressionCodegen.prepareFunctionParams(ctx, inputAttrs, inputVars)
    assert(params.length == 3)
    assert(params(0) == Tuple2("value1", "int value1"))
    assert(params(1) == Tuple2("value2", "int value2"))
    assert(params(2) == Tuple2("isNull2", "boolean isNull2"))
  }

  test("Returns input variables for expression: deferred variables") {
    val ctx = new CodegenContext()

    // The referred column is not evaluated yet. But it depends on an evaluated column from
    // other operator.
    val currentVars = Seq(ExprCode("fake code;", isNull = "isNull1", value = "value1"))
    val fakeExpr = AttributeReference("a", IntegerType, nullable = true)()

    // currentVars(0) depends on this evaluated column.
    currentVars(0).inputVars += ExprInputVar(
      fakeExpr,
      ExprCode("", isNull = "isNull2", value = "value2"))
    ctx.currentVars = currentVars
    ctx.INPUT_ROW = null

    val expr = Add(Literal(1), BoundReference(0, IntegerType, nullable = false))
    val (inputAttrs, inputVars) = ExpressionCodegen.getInputVarsForChildren(ctx, expr)
    assert(inputAttrs.length == 1)
    assert(inputAttrs(0) == fakeExpr)

    val params = ExpressionCodegen.prepareFunctionParams(ctx, inputAttrs, inputVars)
    assert(params.length == 2)
    assert(params(0) == Tuple2("value2", "int value2"))
    assert(params(1) == Tuple2("isNull2", "boolean isNull2"))
  }

  test("Returns input rows for expression") {
    val ctx = new CodegenContext()
    ctx.currentVars = null
    ctx.INPUT_ROW = "i"

    val expr = Add(BoundReference(0, IntegerType, nullable = false),
      BoundReference(1, IntegerType, nullable = true))
    val inputRows = ExpressionCodegen.getInputRowsForChildren(ctx, expr)
    assert(inputRows.length == 1)
    assert(inputRows(0) == "i")
  }

  test("Returns input rows for expression: deferred expression") {
    val ctx = new CodegenContext()

    // The referred column is not evaluated yet. But it depends on an input row from
    // other operator.
    val currentVars = Seq(ExprCode("fake code;", isNull = "isNull1", value = "value1"))
    currentVars(0).inputRow = "inputadaptor_row1"
    ctx.currentVars = currentVars
    ctx.INPUT_ROW = null

    val expr = Add(Literal(1), BoundReference(0, IntegerType, nullable = false))
    val inputRows = ExpressionCodegen.getInputRowsForChildren(ctx, expr)
    assert(inputRows.length == 1)
    assert(inputRows(0) == "inputadaptor_row1")
  }
}
