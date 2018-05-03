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

  test("Block can interpolate string and ExprValue inputs") {
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val code = code"boolean ${isNull} = ${JavaCode.defaultLiteral(BooleanType)};"
    assert(code.toString == "boolean expr1_isNull = false;")
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
        |int expr1 = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin
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
    assert(exprValues.length == 2)
    assert(exprValues(0).isInstanceOf[VariableValue] && exprValues(0).code == "expr1_isNull")
    assert(exprValues(1).isInstanceOf[VariableValue] && exprValues(1).code == "expr1")
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
       |int expr2 = 100;""".stripMargin

    assert(code.toString == expected)

    val exprValues = code.exprValues
    assert(exprValues.length == 5)
    assert(exprValues(0).isInstanceOf[VariableValue] && exprValues(0).code == "expr1_isNull")
    assert(exprValues(1).isInstanceOf[VariableValue] && exprValues(1).code == "expr1")
    assert(exprValues(2).isInstanceOf[VariableValue] && exprValues(2).code == "expr2_isNull")
    assert(exprValues(3).isInstanceOf[VariableValue] && exprValues(3).code == "expr2")
    assert(exprValues(4).isInstanceOf[LiteralValue] && exprValues(4).code == "100")
  }
}
