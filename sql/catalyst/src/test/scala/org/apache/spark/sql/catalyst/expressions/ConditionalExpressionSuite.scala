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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types._

class ConditionalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("if") {
    val testcases = Seq[(java.lang.Boolean, Integer, Integer, Integer)](
      (true, 1, 2, 1),
      (false, 1, 2, 2),
      (null, 1, 2, 2),
      (true, null, 2, null),
      (false, 1, null, null),
      (null, null, 2, 2),
      (null, 1, null, null)
    )

    // dataType must match T.
    def testIf(convert: (Integer => Any), dataType: DataType): Unit = {
      for ((predicate, trueValue, falseValue, expected) <- testcases) {
        val trueValueConverted = if (trueValue == null) null else convert(trueValue)
        val falseValueConverted = if (falseValue == null) null else convert(falseValue)
        val expectedConverted = if (expected == null) null else convert(expected)

        checkEvaluation(
          If(Literal.create(predicate, BooleanType),
            Literal.create(trueValueConverted, dataType),
            Literal.create(falseValueConverted, dataType)),
          expectedConverted)
      }
    }

    testIf(_ == 1, BooleanType)
    testIf(_.toShort, ShortType)
    testIf(identity, IntegerType)
    testIf(_.toLong, LongType)

    testIf(_.toFloat, FloatType)
    testIf(_.toDouble, DoubleType)
    testIf(Decimal(_), DecimalType.USER_DEFAULT)

    testIf(identity, DateType)
    testIf(_.toLong, TimestampType)

    testIf(_.toString, StringType)

    DataTypeTestUtils.propertyCheckSupported.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(If, BooleanType, dt, dt)
    }
  }

  test("case when") {
    val row = create_row(null, false, true, "a", "b", "c")
    val c1 = 'a.boolean.at(0)
    val c2 = 'a.boolean.at(1)
    val c3 = 'a.boolean.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    val c6 = 'a.string.at(5)

    checkEvaluation(CaseWhen(Seq((c1, c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((c2, c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((c3, c4)), c6), "a", row)
    checkEvaluation(CaseWhen(Seq((Literal.create(null, BooleanType), c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((Literal.create(false, BooleanType), c4)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((Literal.create(true, BooleanType), c4)), c6), "a", row)

    checkEvaluation(CaseWhen(Seq((c3, c4), (c2, c5)), c6), "a", row)
    checkEvaluation(CaseWhen(Seq((c2, c4), (c3, c5)), c6), "b", row)
    checkEvaluation(CaseWhen(Seq((c1, c4), (c2, c5)), c6), "c", row)
    checkEvaluation(CaseWhen(Seq((c1, c4), (c2, c5))), null, row)

    assert(CaseWhen(Seq((c2, c4)), c6).nullable === true)
    assert(CaseWhen(Seq((c2, c4), (c3, c5)), c6).nullable === true)
    assert(CaseWhen(Seq((c2, c4), (c3, c5))).nullable === true)

    val c4_notNull = 'a.boolean.notNull.at(3)
    val c5_notNull = 'a.boolean.notNull.at(4)
    val c6_notNull = 'a.boolean.notNull.at(5)

    assert(CaseWhen(Seq((c2, c4_notNull)), c6_notNull).nullable === false)
    assert(CaseWhen(Seq((c2, c4)), c6_notNull).nullable === true)
    assert(CaseWhen(Seq((c2, c4_notNull))).nullable === true)
    assert(CaseWhen(Seq((c2, c4_notNull)), c6).nullable === true)

    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5_notNull)), c6_notNull).nullable === false)
    assert(CaseWhen(Seq((c2, c4), (c3, c5_notNull)), c6_notNull).nullable === true)
    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5)), c6_notNull).nullable === true)
    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5_notNull)), c6).nullable === true)

    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5_notNull))).nullable === true)
    assert(CaseWhen(Seq((c2, c4), (c3, c5_notNull))).nullable === true)
    assert(CaseWhen(Seq((c2, c4_notNull), (c3, c5))).nullable === true)
  }

  test("if/case when - null flags of non-primitive types") {
    val arrayWithNulls = Literal.create(Seq("a", null, "b"), ArrayType(StringType, true))
    val arrayWithoutNulls = Literal.create(Seq("c", "d"), ArrayType(StringType, false))
    val structWithNulls = Literal.create(
      create_row(null, null),
      StructType(Seq(StructField("a", IntegerType, true), StructField("b", StringType, true))))
    val structWithoutNulls = Literal.create(
      create_row(1, "a"),
      StructType(Seq(StructField("a", IntegerType, false), StructField("b", StringType, false))))
    val mapWithNulls = Literal.create(Map(1 -> null), MapType(IntegerType, StringType, true))
    val mapWithoutNulls = Literal.create(Map(1 -> "a"), MapType(IntegerType, StringType, false))

    val arrayIf1 = If(Literal.FalseLiteral, arrayWithNulls, arrayWithoutNulls)
    val arrayIf2 = If(Literal.FalseLiteral, arrayWithoutNulls, arrayWithNulls)
    val arrayIf3 = If(Literal.TrueLiteral, arrayWithNulls, arrayWithoutNulls)
    val arrayIf4 = If(Literal.TrueLiteral, arrayWithoutNulls, arrayWithNulls)
    val structIf1 = If(Literal.FalseLiteral, structWithNulls, structWithoutNulls)
    val structIf2 = If(Literal.FalseLiteral, structWithoutNulls, structWithNulls)
    val structIf3 = If(Literal.TrueLiteral, structWithNulls, structWithoutNulls)
    val structIf4 = If(Literal.TrueLiteral, structWithoutNulls, structWithNulls)
    val mapIf1 = If(Literal.FalseLiteral, mapWithNulls, mapWithoutNulls)
    val mapIf2 = If(Literal.FalseLiteral, mapWithoutNulls, mapWithNulls)
    val mapIf3 = If(Literal.TrueLiteral, mapWithNulls, mapWithoutNulls)
    val mapIf4 = If(Literal.TrueLiteral, mapWithoutNulls, mapWithNulls)

    val arrayCaseWhen1 = CaseWhen(Seq((Literal.FalseLiteral, arrayWithNulls)), arrayWithoutNulls)
    val arrayCaseWhen2 = CaseWhen(Seq((Literal.FalseLiteral, arrayWithoutNulls)), arrayWithNulls)
    val arrayCaseWhen3 = CaseWhen(Seq((Literal.TrueLiteral, arrayWithNulls)), arrayWithoutNulls)
    val arrayCaseWhen4 = CaseWhen(Seq((Literal.TrueLiteral, arrayWithoutNulls)), arrayWithNulls)
    val structCaseWhen1 = CaseWhen(Seq((Literal.FalseLiteral, structWithNulls)), structWithoutNulls)
    val structCaseWhen2 = CaseWhen(Seq((Literal.FalseLiteral, structWithoutNulls)), structWithNulls)
    val structCaseWhen3 = CaseWhen(Seq((Literal.TrueLiteral, structWithNulls)), structWithoutNulls)
    val structCaseWhen4 = CaseWhen(Seq((Literal.TrueLiteral, structWithoutNulls)), structWithNulls)
    val mapCaseWhen1 = CaseWhen(Seq((Literal.FalseLiteral, mapWithNulls)), mapWithoutNulls)
    val mapCaseWhen2 = CaseWhen(Seq((Literal.FalseLiteral, mapWithoutNulls)), mapWithNulls)
    val mapCaseWhen3 = CaseWhen(Seq((Literal.TrueLiteral, mapWithNulls)), mapWithoutNulls)
    val mapCaseWhen4 = CaseWhen(Seq((Literal.TrueLiteral, mapWithoutNulls)), mapWithNulls)

    def checkResult(expectedType: DataType, expectedValue: Any, result: Expression): Unit = {
      assert(expectedType == result.dataType)
      checkEvaluation(result, expectedValue)
    }

    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayIf1)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayIf2)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayIf3)
    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayIf4)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structIf1)
    checkResult(structWithNulls.dataType, structWithNulls.value, structIf2)
    checkResult(structWithNulls.dataType, structWithNulls.value, structIf3)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structIf4)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapIf1)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapIf2)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapIf3)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapIf4)

    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayCaseWhen1)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayCaseWhen2)
    checkResult(arrayWithNulls.dataType, arrayWithNulls.value, arrayCaseWhen3)
    checkResult(arrayWithNulls.dataType, arrayWithoutNulls.value, arrayCaseWhen4)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structCaseWhen1)
    checkResult(structWithNulls.dataType, structWithNulls.value, structCaseWhen2)
    checkResult(structWithNulls.dataType, structWithNulls.value, structCaseWhen3)
    checkResult(structWithNulls.dataType, structWithoutNulls.value, structCaseWhen4)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapCaseWhen1)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapCaseWhen2)
    checkResult(mapWithNulls.dataType, mapWithNulls.value, mapCaseWhen3)
    checkResult(mapWithNulls.dataType, mapWithoutNulls.value, mapCaseWhen4)
  }

  test("case key when") {
    val row = create_row(null, 1, 2, "a", "b", "c")
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    val c6 = 'a.string.at(5)

    val literalNull = Literal.create(null, IntegerType)
    val literalInt = Literal(1)
    val literalString = Literal("a")

    checkEvaluation(CaseKeyWhen(c1, Seq(c2, c4, c5)), "b", row)
    checkEvaluation(CaseKeyWhen(c1, Seq(c2, c4, literalNull, c5, c6)), "c", row)
    checkEvaluation(CaseKeyWhen(c2, Seq(literalInt, c4, c5)), "a", row)
    checkEvaluation(CaseKeyWhen(c2, Seq(c1, c4, c5)), "b", row)
    checkEvaluation(CaseKeyWhen(c4, Seq(literalString, c2, c3)), 1, row)
    checkEvaluation(CaseKeyWhen(c4, Seq(c6, c3, c5, c2, Literal(3))), 3, row)

    checkEvaluation(CaseKeyWhen(literalInt, Seq(c2, c4, c5)), "a", row)
    checkEvaluation(CaseKeyWhen(literalString, Seq(c5, c2, c4, c3)), 2, row)
    checkEvaluation(CaseKeyWhen(c6, Seq(c5, c2, c4, c3)), null, row)
    checkEvaluation(CaseKeyWhen(literalNull, Seq(c2, c5, c1, c6)), null, row)
  }

  test("case key whn - internal pattern matching expects a List while apply takes a Seq") {
    val indexedSeq = IndexedSeq(Literal(1), Literal(42), Literal(42), Literal(1))
    val caseKeyWhaen = CaseKeyWhen(Literal(12), indexedSeq)
    assert(caseKeyWhaen.branches ==
      IndexedSeq((Literal(12) === Literal(1), Literal(42)),
        (Literal(12) === Literal(42), Literal(1))))
  }

  test("SPARK-22705: case when should use less global variables") {
    val ctx = new CodegenContext()
    CaseWhen(Seq((Literal.create(false, BooleanType), Literal(1))), Literal(-1)).genCode(ctx)
    assert(ctx.inlinedMutableStates.size == 1)
  }

  test("field") {
    val str1 = Literal("花花世界")
    val str2 = Literal("a")
    val str3 = Literal("b")
    val str5 = Literal("999")
    val strNull = Literal.create(null, StringType)

    val bool1 = Literal(true)
    val bool2 = Literal(false)

    val int1 = Literal(1)
    val int2 = Literal(2)
    val int3 = Literal(3)
    val int4 = Literal(999)
    val intNull = Literal.create(null, IntegerType)

    val double1 = Literal(1.221)
    val double2 = Literal(1.222)
    val double3 = Literal(1.224)

    val timeStamp1 = Literal(new Timestamp(2016, 12, 27, 14, 22, 1, 1))
    val timeStamp2 = Literal(new Timestamp(1988, 6, 3, 1, 1, 1, 1))
    val timeStamp3 = Literal(new Timestamp(1990, 6, 5, 1, 1, 1, 1))

    val date1 = Literal(new Date(1949, 1, 1))
    val date2 = Literal(new Date(1979, 1, 1))
    val date3 = Literal(new Date(1989, 1, 1))

    checkEvaluation(Field(Seq(str1, str2, str3, str1)), 3)
    checkEvaluation(Field(Seq(str2, str2, str2, str1)), 1)
    checkEvaluation(Field(Seq(bool1, bool2, bool1, bool1)), 2)
    checkEvaluation(Field(Seq(int1, int2, int3, int1)), 3)
    checkEvaluation(Field(Seq(double2, double3, double1, double2)), 3)
    checkEvaluation(Field(Seq(timeStamp1, timeStamp2, timeStamp3, timeStamp1)), 3)
    checkEvaluation(Field(Seq(date1, date1, date2, date3)), 1)
    checkEvaluation(Field(Seq(int4, double3, str5, bool1, date1, timeStamp2, int4)), 6)
    checkEvaluation(Field(Seq(int4, double3, str5, bool1, date1, timeStamp2, int3)), 0)
    checkEvaluation(Field(Seq(int1, strNull, intNull, bool1, date1, timeStamp2, int3)), 0)
    checkEvaluation(Field(Seq(strNull, int1, str1, str2, str3)), 0)
  }
}
