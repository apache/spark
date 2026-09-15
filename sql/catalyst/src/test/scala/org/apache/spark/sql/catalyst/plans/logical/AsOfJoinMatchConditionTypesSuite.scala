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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin.MatchConditionTypes
import org.apache.spark.sql.types._

class AsOfJoinMatchConditionTypesSuite extends SparkFunSuite {

  test("scalar types widen via TypeCoercion") {
    assert(MatchConditionTypes.areOperandsCompatible(IntegerType, LongType))
    assert(!MatchConditionTypes.usesArrayOrderExpression(IntegerType, LongType))
    assert(!MatchConditionTypes.usesStructDecomposition(IntegerType, LongType))
  }

  test("scalar string and temporal types coerce like the comparison operator") {
    assert(MatchConditionTypes.areOperandsCompatible(StringType, TimestampType))
    assert(MatchConditionTypes.areOperandsCompatible(DateType, StringType))
    // Common type is the temporal type (string cast to it), so sort and comparison agree.
    assert(MatchConditionTypes.matchComparisonCommonType(DateType, StringType).contains(DateType))
    assert(
      MatchConditionTypes.matchComparisonCommonType(StringType, TimestampType)
        .contains(TimestampType))
  }

  test("scalar string and numeric types coerce like the comparison operator") {
    // Common type must be numeric, not string, so the buffer sorts by value.
    assert(MatchConditionTypes.areOperandsCompatible(IntegerType, StringType))
    assert(MatchConditionTypes.matchComparisonCommonType(IntegerType, StringType).nonEmpty)
    assert(
      !MatchConditionTypes.matchComparisonCommonType(IntegerType, StringType).contains(StringType))
  }

  test("scalar string vs interval is rejected, matching the comparison operator") {
    // No comparison common type exists, so reject it like `>=` rather than leave it uncoerced.
    val interval = DayTimeIntervalType()
    assert(!MatchConditionTypes.areOperandsCompatible(StringType, interval))
    assert(!MatchConditionTypes.areOperandsCompatible(YearMonthIntervalType(), StringType))
    assert(MatchConditionTypes.matchComparisonCommonType(StringType, interval).isEmpty)
  }

  test("struct fields keep the strict rule: string vs temporal field is rejected") {
    // Coercion is scoped to scalar operands, so a string vs temporal STRUCT field stays rejected.
    val leftStruct = StructType(StructField("f", DateType) :: Nil)
    val rightStruct = StructType(StructField("g", StringType) :: Nil)
    assert(!MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
  }

  test("array elements keep the strict rule: string vs numeric element is rejected") {
    val intArray = ArrayType(IntegerType)
    val stringArray = ArrayType(StringType)
    assert(!MatchConditionTypes.areOperandsCompatible(intArray, stringArray))
  }

  test("positional struct operands with different field names are compatible") {
    val leftStruct = StructType(
      StructField("a", IntegerType) ::
        StructField("b", LongType) ::
        Nil)
    val rightStruct = StructType(
      StructField("x", IntegerType) ::
        StructField("y", LongType) ::
        Nil)
    assert(MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
    assert(MatchConditionTypes.usesStructDecomposition(leftStruct, rightStruct))
    assert(!MatchConditionTypes.usesIdenticalStructSort(leftStruct, rightStruct))
  }

  test("nested struct operands are compatible when fields match positionally") {
    val innerLeft = StructType(StructField("k", IntegerType) :: Nil)
    val innerRight = StructType(StructField("z", IntegerType) :: Nil)
    val leftStruct = StructType(StructField("outer", innerLeft) :: Nil)
    val rightStruct = StructType(StructField("other", innerRight) :: Nil)
    assert(MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
    assert(MatchConditionTypes.usesStructDecomposition(leftStruct, rightStruct))
  }

  test("array operands require identical element types") {
    val leftArray = ArrayType(IntegerType)
    val rightArray = ArrayType(IntegerType)
    assert(MatchConditionTypes.areOperandsCompatible(leftArray, rightArray))
    assert(MatchConditionTypes.usesArrayOrderExpression(leftArray, rightArray))
  }

  test("array operands with different struct element field names are compatible") {
    val leftArray = ArrayType(
      StructType(
        StructField("x", IntegerType, nullable = false) ::
          StructField("y", IntegerType, nullable = false) ::
          Nil))
    val rightArray = ArrayType(
      StructType(
        StructField("p", IntegerType, nullable = false) ::
          StructField("q", IntegerType, nullable = false) ::
          Nil))
    assert(MatchConditionTypes.areOperandsCompatible(leftArray, rightArray))
    assert(MatchConditionTypes.usesArrayOrderExpression(leftArray, rightArray))
  }

  test("array operands with different element types are rejected") {
    val leftArray = ArrayType(IntegerType)
    val rightArray = ArrayType(StringType)
    assert(!MatchConditionTypes.areOperandsCompatible(leftArray, rightArray))
    assert(!MatchConditionTypes.usesArrayOrderExpression(leftArray, rightArray))
  }

  test("empty struct operands are invalid") {
    val emptyStruct = StructType(Nil)
    assert(!MatchConditionTypes.isValidOperandType(emptyStruct))
    assert(!MatchConditionTypes.areOperandsCompatible(emptyStruct, emptyStruct))
    assert(!MatchConditionTypes.usesStructDecomposition(emptyStruct, emptyStruct))
  }

  test("nested empty struct operands are invalid") {
    val nestedEmptyStruct = StructType(StructField("x", StructType(Nil)) :: Nil)
    assert(!MatchConditionTypes.isValidOperandType(nestedEmptyStruct))
    assert(!MatchConditionTypes.areOperandsCompatible(nestedEmptyStruct, nestedEmptyStruct))
  }

  test("identical struct schemas enable whole-struct sort") {
    val structType = StructType(
      StructField("ts", TimestampType) ::
        StructField("seq", IntegerType) ::
        Nil)
    assert(MatchConditionTypes.usesIdenticalStructSort(structType, structType))
    assert(MatchConditionTypes.usesStructDecomposition(structType, structType))
  }

  test("struct field count mismatch is not decomposable") {
    val leftStruct = StructType(StructField("a", IntegerType) :: Nil)
    val rightStruct = StructType(
      StructField("a", IntegerType) ::
        StructField("b", IntegerType) ::
        Nil)
    assert(!MatchConditionTypes.usesStructDecomposition(leftStruct, rightStruct))
    assert(!MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
  }
}
