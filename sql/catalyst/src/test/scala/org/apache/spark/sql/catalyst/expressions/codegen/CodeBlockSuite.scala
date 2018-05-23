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
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class CodeBlockSuite extends SparkFunSuite {

  test("Block interpolates string and ExprValue inputs") {
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val stringLiteral = "false"
    val code = code"boolean $isNull = $stringLiteral;"
    assert(code.toString == "boolean expr1_isNull = false;")
  }

  test("Literals are folded into string code parts instead of block inputs") {
    val value = JavaCode.variable("expr1", IntegerType)
    val intLiteral = 1
    val code = code"int $value = $intLiteral;"
    assert(code.asInstanceOf[CodeBlock].blockInputs === Seq(value))
  }

  test("Block.stripMargin") {
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val value = JavaCode.variable("expr1", IntegerType)
    val code1 =
      code"""
           |boolean $isNull = false;
           |int $value = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin
    val expected =
      s"""
        |boolean expr1_isNull = false;
        |int expr1 = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin.trim
    assert(code1.toString == expected)

    val code2 =
      code"""
           >boolean $isNull = false;
           >int $value = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin('>')
    assert(code2.toString == expected)
  }

  test("Block can capture input expr values") {
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val value = JavaCode.variable("expr1", IntegerType)
    val code =
      code"""
           |boolean $isNull = false;
           |int $value = -1;
          """.stripMargin
    val exprValues = code.exprValues
    assert(exprValues.size == 2)
    assert(exprValues === Set(value, isNull))
  }

  test("concatenate blocks") {
    val isNull1 = JavaCode.isNullVariable("expr1_isNull")
    val value1 = JavaCode.variable("expr1", IntegerType)
    val isNull2 = JavaCode.isNullVariable("expr2_isNull")
    val value2 = JavaCode.variable("expr2", IntegerType)
    val literal = JavaCode.literal("100", IntegerType)

    val code =
      code"""
           |boolean $isNull1 = false;
           |int $value1 = -1;""".stripMargin +
      code"""
           |boolean $isNull2 = true;
           |int $value2 = $literal;""".stripMargin

    val expected =
      """
       |boolean expr1_isNull = false;
       |int expr1 = -1;
       |boolean expr2_isNull = true;
       |int expr2 = 100;""".stripMargin.trim

    assert(code.toString == expected)

    val exprValues = code.exprValues
    assert(exprValues.size == 5)
    assert(exprValues === Set(isNull1, value1, isNull2, value2, literal))
  }

  test("Throws exception when interpolating unexcepted object in code block") {
    val obj = Tuple2(1, 1)
    val e = intercept[IllegalArgumentException] {
      code"$obj"
    }
    assert(e.getMessage().contains(s"Can not interpolate ${obj.getClass.getName}"))
  }

  test("replace expr values in code block") {
    val expr = JavaCode.expression("1 + 1", IntegerType)
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val exprInFunc = JavaCode.variable("expr1", IntegerType)

    val code =
      code"""
           |callFunc(int $expr) {
           |  boolean $isNull = false;
           |  int $exprInFunc = $expr + 1;
           |}""".stripMargin

    val aliasedParam = JavaCode.variable("aliased", expr.javaType)
    val aliasedInputs = code.asInstanceOf[CodeBlock].blockInputs.map {
      case _: SimpleExprValue => aliasedParam
      case other => other
    }
    val aliasedCode = CodeBlock(code.asInstanceOf[CodeBlock].codeParts, aliasedInputs).stripMargin
    val expected =
      code"""
           |callFunc(int $aliasedParam) {
           |  boolean $isNull = false;
           |  int $exprInFunc = $aliasedParam + 1;
           |}""".stripMargin
    assert(aliasedCode.toString == expected.toString)
  }
}
