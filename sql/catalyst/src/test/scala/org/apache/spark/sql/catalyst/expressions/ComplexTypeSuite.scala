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
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class ComplexTypeSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("CreateStruct") {
    val row = InternalRow(1, 2, 3)
    val c1 = 'a.int.at(0).as("a")
    val c3 = 'c.int.at(2).as("c")
    checkEvaluation(CreateStruct(Seq(c1, c3)), InternalRow(1, 3), row)
  }

  test("complex type") {
    val row = create_row(
      "^Ba*n",                                // 0
      null.asInstanceOf[UTF8String],          // 1
      create_row("aa", "bb"),                 // 2
      Map("aa"->"bb"),                        // 3
      Seq("aa", "bb")                         // 4
    )

    val typeS = StructType(
      StructField("a", StringType, true) :: StructField("b", StringType, true) :: Nil
    )
    val typeMap = MapType(StringType, StringType)
    val typeArray = ArrayType(StringType)

    checkEvaluation(GetMapValue(BoundReference(3, typeMap, true),
      Literal("aa")), "bb", row)
    checkEvaluation(GetMapValue(Literal.create(null, typeMap), Literal("aa")), null, row)
    checkEvaluation(
      GetMapValue(Literal.create(null, typeMap), Literal.create(null, StringType)), null, row)
    checkEvaluation(GetMapValue(BoundReference(3, typeMap, true),
      Literal.create(null, StringType)), null, row)

    checkEvaluation(GetArrayItem(BoundReference(4, typeArray, true),
      Literal(1)), "bb", row)
    checkEvaluation(GetArrayItem(Literal.create(null, typeArray), Literal(1)), null, row)
    checkEvaluation(
      GetArrayItem(Literal.create(null, typeArray), Literal.create(null, IntegerType)), null, row)
    checkEvaluation(GetArrayItem(BoundReference(4, typeArray, true),
      Literal.create(null, IntegerType)), null, row)

    def getStructField(expr: Expression, fieldName: String): ExtractValue = {
      expr.dataType match {
        case StructType(fields) =>
          val field = fields.find(_.name == fieldName).get
          GetStructField(expr, field, fields.indexOf(field))
      }
    }

    def quickResolve(u: UnresolvedExtractValue): ExtractValue = {
      ExtractValue(u.child, u.extraction, _ == _)
    }

    checkEvaluation(getStructField(BoundReference(2, typeS, nullable = true), "a"), "aa", row)
    checkEvaluation(getStructField(Literal.create(null, typeS), "a"), null, row)

    val typeS_notNullable = StructType(
      StructField("a", StringType, nullable = false)
        :: StructField("b", StringType, nullable = false) :: Nil
    )

    assert(getStructField(BoundReference(2, typeS, nullable = true), "a").nullable === true)
    assert(getStructField(BoundReference(2, typeS_notNullable, nullable = false), "a").nullable
      === false)

    assert(getStructField(Literal.create(null, typeS), "a").nullable === true)
    assert(getStructField(Literal.create(null, typeS_notNullable), "a").nullable === true)

    checkEvaluation(quickResolve('c.map(typeMap).at(3).getItem("aa")), "bb", row)
    checkEvaluation(quickResolve('c.array(typeArray.elementType).at(4).getItem(1)), "bb", row)
    checkEvaluation(quickResolve('c.struct(typeS).at(2).getField("a")), "aa", row)
  }

  test("error message of ExtractValue") {
    val structType = StructType(StructField("a", StringType, true) :: Nil)
    val arrayStructType = ArrayType(structType)
    val arrayType = ArrayType(StringType)
    val otherType = StringType

    def checkErrorMessage(
      childDataType: DataType,
      fieldDataType: DataType,
      errorMesage: String): Unit = {
      val e = intercept[org.apache.spark.sql.AnalysisException] {
        ExtractValue(
          Literal.create(null, childDataType),
          Literal.create(null, fieldDataType),
          _ == _)
      }
      assert(e.getMessage().contains(errorMesage))
    }

    checkErrorMessage(structType, IntegerType, "Field name should be String Literal")
    checkErrorMessage(arrayStructType, BooleanType, "Field name should be String Literal")
    checkErrorMessage(arrayType, StringType, "Array index should be integral type")
    checkErrorMessage(otherType, StringType, "Can't extract value from")
  }
}
