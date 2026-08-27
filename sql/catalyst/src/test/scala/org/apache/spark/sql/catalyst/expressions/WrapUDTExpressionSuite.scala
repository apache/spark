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

import org.apache.spark.{SPARK_DOC_ROOT, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, TestUDT}

class WrapUDTExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("WrapUDT should use target UDT with matching SQL type") {
    val udt = new TestUDT.MyDenseVectorUDT()
    val data = new GenericArrayData(Array[Any](1.0, 2.0))
    val wrapUDTExpression = WrapUDT(Literal.create(data, udt.sqlType), udt)

    assert(wrapUDTExpression.checkInputDataTypes().isSuccess)
    assert(wrapUDTExpression.dataType == udt)
    checkEvaluation(wrapUDTExpression, data)
  }

  test("WrapUDT should parse target UDT from foldable expression") {
    val udt = new TestUDT.MyDenseVectorUDT()
    val data = new GenericArrayData(Array[Any](1.0, 2.0))
    val json = udt.json
    val target = Concat(Seq(
      Literal.create(json.substring(0, 8), StringType),
      Literal.create(json.substring(8), StringType)))
    val wrapUDTExpression = new WrapUDT(Literal.create(data, udt.sqlType), target)

    assert(wrapUDTExpression.checkInputDataTypes().isSuccess)
    assert(wrapUDTExpression.dataType == udt)
    checkEvaluation(wrapUDTExpression, data)
  }

  test("WrapUDT target expression should be a UDT") {
    val target = Literal.create("int", StringType)
    checkError(
      exception = intercept[AnalysisException] {
        new WrapUDT(Literal.create(1, IntegerType), target)
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> toSQLExpr(target),
        "paramIndex" -> ordinalNumber(1),
        "requiredType" -> toSQLType("UserDefinedType"),
        "inputSql" -> toSQLExpr(target),
        "inputType" -> toSQLType(IntegerType)))
  }

  test("WrapUDT target expression should be foldable") {
    val target = AttributeReference("udt", StringType)()
    checkError(
      exception = intercept[AnalysisException] {
        new WrapUDT(Literal.create(1, IntegerType), target)
      },
      condition = "INVALID_SCHEMA.NON_STRING_LITERAL",
      parameters = Map("inputSchema" -> toSQLExpr(target)))
  }

  test("WrapUDT should reject wrong number of arguments through FunctionRegistry") {
    val expression = Literal.create(1, IntegerType)
    val target = Literal.create(new TestUDT.MyDenseVectorUDT().json, StringType)

    Seq(
      Seq(expression) -> "1",
      Seq(expression, target, Literal.create("extra", StringType)) -> "3").foreach {
      case (arguments, actualNum) =>
        checkError(
          exception = intercept[AnalysisException] {
            FunctionRegistry.internal.lookupFunction(FunctionIdentifier("wrap_udt"), arguments)
          },
          condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
          parameters = Map(
            "functionName" -> "`wrap_udt`",
            "expectedNum" -> "2",
            "actualNum" -> actualNum,
            "docroot" -> SPARK_DOC_ROOT))
    }
  }

  test("WrapUDT input type should match target UDT SQL type") {
    val b1 = Literal.create(false, BooleanType)
    val udt = new TestUDT.MyDenseVectorUDT()
    val wrapUDTExpression = WrapUDT(b1, udt)
    assert(wrapUDTExpression.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(udt.sqlType),
          "inputSql" -> "\"false\"",
          "inputType" -> "\"BOOLEAN\"")))
  }
}
