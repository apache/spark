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

  test("string and interval types are incompatible") {
    // SPARK-59528: STRING string-promotes to any atomic type, so findWiderTypeForTwo accepts STRING
    // vs INTERVAL, but the `>=` cannot coerce that pair (no common type) and would fail later with
    // BINARY_OP_DIFF_TYPES. Reject up front. Fails if the string/interval guard is dropped.
    assert(!MatchConditionTypes.areOperandsCompatible(StringType, YearMonthIntervalType()))
    assert(!MatchConditionTypes.areOperandsCompatible(DayTimeIntervalType(), StringType))
    // The guard is narrow: STRING still pairs with a numeric type, which the comparison promotes.
    assert(MatchConditionTypes.areOperandsCompatible(StringType, LongType))
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

  test("struct operands with different field names and coercible types are rejected") {
    // int vs bigint needs coercion, which aligns fields by name; different names have no common
    // type, so the `>=` cannot resolve. Fails if positional matching accepted coercible fields
    // (the accept-then-fail bug: validation passes, then analysis throws BINARY_OP_DIFF_TYPES).
    val leftStruct = StructType(StructField("a", IntegerType) :: Nil)
    val rightStruct = StructType(StructField("c", LongType) :: Nil)
    assert(!MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
  }

  test("struct operands with matching field names and coercible types are compatible") {
    // Same field name: the comparison widens field a from int to bigint by name.
    // Fails if the fix over-tightened and rejected same-named coercible structs.
    val leftStruct = StructType(StructField("a", IntegerType) :: Nil)
    val rightStruct = StructType(StructField("a", LongType) :: Nil)
    assert(MatchConditionTypes.areOperandsCompatible(leftStruct, rightStruct))
  }

  test("struct operands whose fields only string-promote or decimal-widen are rejected") {
    // Same field name, but a field pair the `>=` cannot widen: findTightestCommonType is None for
    // INT vs STRING (string promotion only) and for two different decimals (decimal widening only).
    // findWiderTypeForTwo would accept both by name, so the type check would pass and analysis then
    // throw BINARY_OP_DIFF_TYPES. Fails if the struct arm used findWiderTypeForTwo instead of the
    // tightest-common-type rule the array element path uses.
    val intStruct = StructType(StructField("a", IntegerType) :: Nil)
    val stringStruct = StructType(StructField("a", StringType) :: Nil)
    assert(!MatchConditionTypes.areOperandsCompatible(intStruct, stringStruct))

    val decimalStruct = StructType(StructField("a", DecimalType(10, 2)) :: Nil)
    val widerDecimalStruct = StructType(StructField("a", DecimalType(20, 5)) :: Nil)
    assert(!MatchConditionTypes.areOperandsCompatible(decimalStruct, widerDecimalStruct))
  }

  test("array operands with identical element types are compatible") {
    val leftArray = ArrayType(IntegerType)
    val rightArray = ArrayType(IntegerType)
    assert(MatchConditionTypes.areOperandsCompatible(leftArray, rightArray))
    assert(MatchConditionTypes.usesArrayOrderExpression(leftArray, rightArray))
  }

  test("array operands with coercible element types are compatible") {
    // SPARK-59528: elements widen via findTightestCommonType, like the array `>=` comparison.
    val intArray = ArrayType(IntegerType)
    val longArray = ArrayType(LongType)
    assert(MatchConditionTypes.areOperandsCompatible(intArray, longArray))
    assert(MatchConditionTypes.usesArrayOrderExpression(intArray, longArray))
    assert(MatchConditionTypes.areOperandsCompatible(longArray, intArray))
    assert(MatchConditionTypes.areOperandsCompatible(intArray, ArrayType(DoubleType)))
  }

  test("array operands whose elements only string-promote are rejected") {
    // SPARK-59528: INT vs STRING has no tightest common type, so the array `>=` cannot coerce it.
    val intArray = ArrayType(IntegerType)
    val stringArray = ArrayType(StringType)
    assert(!MatchConditionTypes.areOperandsCompatible(intArray, stringArray))
    assert(!MatchConditionTypes.usesArrayOrderExpression(intArray, stringArray))
  }

  test("nested array operands with coercible element types are compatible") {
    val leftArray = ArrayType(ArrayType(IntegerType))
    val rightArray = ArrayType(ArrayType(LongType))
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

  test("array operands with non-coercible element types are rejected") {
    // INT and BINARY are both orderable but have no common type.
    val leftArray = ArrayType(IntegerType)
    val rightArray = ArrayType(BinaryType)
    assert(!MatchConditionTypes.areOperandsCompatible(leftArray, rightArray))
    assert(!MatchConditionTypes.usesArrayOrderExpression(leftArray, rightArray))
  }

  test("array struct elements coerce only when field names match") {
    val leftArray = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    // Same field name, coercible field type: elements widen to struct<a:bigint>, so accepted.
    val sameNameArray = ArrayType(StructType(StructField("a", LongType) :: Nil))
    assert(MatchConditionTypes.areOperandsCompatible(leftArray, sameNameArray))
    assert(MatchConditionTypes.usesArrayOrderExpression(leftArray, sameNameArray))
    // Different field name, coercible field type: no tightest common type, so rejected.
    // Fails if the rule fell back to positional struct matching, which ignores field names.
    val diffNameArray = ArrayType(StructType(StructField("b", LongType) :: Nil))
    assert(!MatchConditionTypes.areOperandsCompatible(leftArray, diffNameArray))
    assert(!MatchConditionTypes.usesArrayOrderExpression(leftArray, diffNameArray))
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
