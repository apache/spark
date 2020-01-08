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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, IntegerType, MapType}

/**
 * A test suite for testing [[ExpressionEvalHelper]].
 *
 * Yes, we should write test cases for test harnesses, in case
 * they have behaviors that are easy to break.
 */
class ExpressionEvalHelperSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SPARK-16489 checkEvaluation should fail if expression reuses variable names") {
    val e = intercept[RuntimeException] { checkEvaluation(BadCodegenExpression(), 10) }
    assert(e.getMessage.contains("some_variable"))
  }

  test("SPARK-25388: checkEvaluation should fail if nullable in DataType is incorrect") {
    val e = intercept[RuntimeException] {
      checkEvaluation(MapIncorrectDataTypeExpression(), Map(3 -> 7, 6 -> null))
    }
    assert(e.getMessage.contains("and exprNullable was"))
  }
}

/**
 * An expression that generates bad code (variable name "some_variable" is not unique across
 * instances of the expression.
 */
case class BadCodegenExpression() extends LeafExpression {
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = 10
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.copy(code =
      code"""
        |int some_variable = 11;
        |int ${ev.value} = 10;
      """.stripMargin)
  }
  override def dataType: DataType = IntegerType
}

/**
 * An expression that returns a MapData with incorrect DataType whose valueContainsNull is false
 * while its value includes null
 */
case class MapIncorrectDataTypeExpression() extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = {
    val keys = new GenericArrayData(Array(3, 6))
    val values = new GenericArrayData(Array(7, null))
    new ArrayBasedMapData(keys, values)
  }
  // since values includes null, valueContainsNull must be true
  override def dataType: DataType = MapType(IntegerType, IntegerType, valueContainsNull = false)
}
