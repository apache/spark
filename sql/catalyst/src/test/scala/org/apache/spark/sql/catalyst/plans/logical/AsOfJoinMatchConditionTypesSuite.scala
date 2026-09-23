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
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin.MatchConditionTypes
import org.apache.spark.sql.types._

class AsOfJoinMatchConditionTypesSuite extends SparkFunSuite {

  test("scalar types widen via TypeCoercion") {
    assert(MatchConditionTypes.areOperandsCompatible(IntegerType, LongType))
    assert(!MatchConditionTypes.usesArrayOrderExpression(IntegerType, LongType))
    assert(!MatchConditionTypes.usesStructDecomposition(IntegerType, LongType))
  }

  test("scalar string and temporal types coerce, matching the comparison operator") {
    assert(MatchConditionTypes.areOperandsCompatible(StringType, TimestampType))
    assert(MatchConditionTypes.areOperandsCompatible(DateType, StringType))
    // Common type is the temporal type (string is cast to it), so sort and comparison agree.
    assert(MatchConditionTypes.stringComparisonCommonType(DateType, StringType).contains(DateType))
    assert(
      MatchConditionTypes.stringComparisonCommonType(StringType, TimestampType)
        .contains(TimestampType))
    // TIME reaches the same coercion via the generic atomic fallback, not a temporal special case.
    assert(MatchConditionTypes.areOperandsCompatible(TimeType(), StringType))
    assert(
      MatchConditionTypes.stringComparisonCommonType(TimeType(), StringType).contains(TimeType()))
  }

  test("scalar string and numeric types coerce, matching the comparison operator") {
    // Common type must be numeric, not string, so the buffer sorts by value.
    assert(MatchConditionTypes.areOperandsCompatible(IntegerType, StringType))
    assert(MatchConditionTypes.stringComparisonCommonType(IntegerType, StringType).nonEmpty)
    assert(
      !MatchConditionTypes.stringComparisonCommonType(IntegerType, StringType).contains(StringType))
  }

  test("scalar string and boolean or binary coerce, matching the comparison operator") {
    // BOOLEAN/BINARY vs STRING have a comparison common type, so ASOF accepts them like `>=`.
    assert(MatchConditionTypes.areOperandsCompatible(BooleanType, StringType))
    assert(MatchConditionTypes.areOperandsCompatible(StringType, BinaryType))
    // Common type is the non-string type, so sort and comparison agree (not lexicographic).
    assert(
      MatchConditionTypes.stringComparisonCommonType(BooleanType, StringType).contains(BooleanType))
    assert(
      MatchConditionTypes.stringComparisonCommonType(StringType, BinaryType).contains(BinaryType))
  }

  test("scalar string vs interval is rejected, matching the comparison operator") {
    // No comparison common type exists, so reject it instead of leaving it uncoerced.
    val interval = DayTimeIntervalType()
    assert(!MatchConditionTypes.areOperandsCompatible(StringType, interval))
    assert(!MatchConditionTypes.areOperandsCompatible(YearMonthIntervalType(), StringType))
    assert(MatchConditionTypes.stringComparisonCommonType(StringType, interval).isEmpty)
  }

  test("struct fields keep the strict rule: string vs temporal field is rejected") {
    // Coercion is scoped to scalar operands, so a string vs temporal STRUCT field stays rejected.
    val leftStruct = StructType(StructField("f", DateType) :: Nil)
    val rightStruct = StructType(StructField("g", StringType) :: Nil)
    assert(!MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
    // TIME is a DatetimeType, so a string vs TIME field is rejected like DATE, though the
    // scalar TIME vs STRING pair above is accepted.
    val leftTimeStruct = StructType(StructField("f", TimeType()) :: Nil)
    val rightTimeStruct = StructType(StructField("g", StringType) :: Nil)
    assert(!MatchConditionTypes.areOperandsCompatible(leftTimeStruct, rightTimeStruct))
  }

  test("array elements keep the strict rule: string vs numeric element is rejected") {
    val intArray = ArrayType(IntegerType)
    val stringArray = ArrayType(StringType)
    assert(!MatchConditionTypes.areOperandsCompatible(intArray, stringArray))
  }

  test("orderable scalars with no common type are incompatible") {
    // Both are individually valid, orderable operands ...
    assert(MatchConditionTypes.isValidOperandType(TimestampType))
    assert(MatchConditionTypes.isValidOperandType(BooleanType))
    // ... but TIMESTAMP and BOOLEAN have no common wider type and are not a
    // string/temporal pair, so they are not comparable.
    assert(!MatchConditionTypes.areOperandsCompatible(TimestampType, BooleanType))
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

  test("map operands are invalid (not orderable)") {
    val mapType = MapType(StringType, IntegerType)
    assert(!MatchConditionTypes.isValidOperandType(mapType))
    assert(!MatchConditionTypes.areOperandsCompatible(mapType, mapType))
  }

  test("array operands with non-orderable elements are invalid") {
    val arrayOfMap = ArrayType(MapType(StringType, IntegerType))
    assert(!MatchConditionTypes.isValidOperandType(arrayOfMap))
    assert(!MatchConditionTypes.areOperandsCompatible(arrayOfMap, arrayOfMap))
    assert(!MatchConditionTypes.usesArrayOrderExpression(arrayOfMap, arrayOfMap))
  }

  test("struct operands with a non-orderable field are invalid") {
    val structWithMap = StructType(
      StructField("a", IntegerType) ::
        StructField("m", MapType(StringType, IntegerType)) ::
        Nil)
    assert(!MatchConditionTypes.isValidOperandType(structWithMap))
    assert(!MatchConditionTypes.areOperandsCompatible(structWithMap, structWithMap))
  }

  test("positional struct operands with incompatible field types are rejected") {
    val leftStruct = StructType(
      StructField("a", IntegerType) ::
        StructField("b", TimestampType) ::
        Nil)
    val rightStruct = StructType(
      StructField("x", IntegerType) ::
        StructField("y", BooleanType) ::
        Nil)
    // Same field count, so the operands are structurally decomposable ...
    assert(MatchConditionTypes.usesStructDecomposition(leftStruct, rightStruct))
    // ... but the second field pair (TIMESTAMP vs BOOLEAN) is not comparable.
    assert(!MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
  }

  test("array operands with empty struct elements are invalid") {
    // An array whose element contains an empty struct is invalid even though the array itself
    // is orderable (the ArrayType arm of containsEmptyStructType).
    val arrayOfEmptyStruct = ArrayType(StructType(Nil))
    // Pin orderability so the rejection is due to the empty struct, not non-orderability.
    assert(RowOrdering.isOrderable(arrayOfEmptyStruct))
    assert(!MatchConditionTypes.isValidOperandType(arrayOfEmptyStruct))
    assert(!MatchConditionTypes.areOperandsCompatible(arrayOfEmptyStruct, arrayOfEmptyStruct))
  }

  test("array operands with incompatible struct element field types are rejected") {
    val leftArray = ArrayType(
      StructType(
        StructField("a", IntegerType) ::
          StructField("b", TimestampType) ::
          Nil))
    val rightArray = ArrayType(
      StructType(
        StructField("x", IntegerType) ::
          StructField("y", BooleanType) ::
          Nil))
    // Each operand is individually a valid, orderable type ...
    assert(MatchConditionTypes.isValidOperandType(leftArray))
    assert(MatchConditionTypes.isValidOperandType(rightArray))
    // ... but the element structs' second field pair (TIMESTAMP vs BOOLEAN) is not comparable.
    assert(!MatchConditionTypes.areOperandsCompatible(leftArray, rightArray))
  }
}
