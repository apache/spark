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

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Add, Literal}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class ExprValueSuite extends SparkFunSuite {

  test("TrueLiteral and FalseLiteral should be LiteralValue") {
    val trueLit = TrueLiteral
    val falseLit = FalseLiteral

    assert(trueLit.value == "true")
    assert(falseLit.value == "false")

    assert(trueLit.isPrimitive)
    assert(falseLit.isPrimitive)

    assert(trueLit === JavaCode.literal("true", BooleanType))
    assert(falseLit === JavaCode.literal("false", BooleanType))
  }

  test("add expr value codegen listener") {
    val context = new CodegenContext()
    val expr = Add(Literal(1), Literal(2))
    val listener = new TestExprValueListener()

    assert(ExprValue.currentListeners.get.size == 0)

    ExprValue.withExprValueCodegenListener(listener) { () =>
      assert(ExprValue.currentListeners.get.size == 1)
      val genCode = expr.genCode(context)
      // Materialize generated code
      s"${genCode.value} ${genCode.isNull}"
    }
    // 1 VariableValue + 3 LiteralValue (1, 2, false)
    assert(listener.exprValues.size == 4)
  }

  test("Listen to statement expression generation") {
    // This listener only listens statement expression.
    val listener = SimpleExprValueCodegenListener()

    val context = new CodegenContext()
    val expr = Add(Literal(1), Literal(2))

    assert(ExprValue.currentListeners.get.size == 0)

    ExprValue.withExprValueCodegenListener(listener) { () =>
      assert(ExprValue.currentListeners.get.size == 1)
      val genCode = expr.genCode(context)
      // Materialize generated code
      s"${genCode.value} ${genCode.isNull}"

      assert(listener.exprValues.size == 0)

      val statementValue = JavaCode.expression(s"${genCode.value} + 1", IntegerType)
      s"$statementValue"
    }
    assert(listener.exprValues.size == 1)
    assert(listener.exprValues.head.code == "(value_0 + 1)")
  }

  test("Use expr value handler to control expression generated code") {
    // Let all `SimpleExprValue` output special string instead of their code.
    def statementHanlder(exprValue: ExprValue): String = exprValue match {
      case _: SimpleExprValue => "%STATEMENT%"
      case _ => ExprValue.defaultExprValueToString(exprValue)
    }

    val context = new CodegenContext()

    val exprCode = ExprValue.withExprValueHandler(statementHanlder) { () =>
      val statementValue = JavaCode.expression(s"1 + 1", IntegerType)
      ExprCode(code = s"int result = $statementValue;",
        value = JavaCode.variable("result", IntegerType),
        isNull = FalseLiteral)
    }
    assert(exprCode.code == "int result = %STATEMENT%;")
  }
}

private class TestExprValueListener extends ExprValueCodegenListener {
  val exprValues: mutable.HashSet[ExprValue] = mutable.HashSet.empty[ExprValue]

  override def genExprValue(exprValue: ExprValue): Unit = {
    exprValues += exprValue
  }
}
