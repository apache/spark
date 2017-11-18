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

import scala.collection.immutable.HashSet

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExamplePointUDT
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._


class PredicateSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def booleanLogicTest(
    name: String,
    op: (Expression, Expression) => Expression,
    truthTable: Seq[(Any, Any, Any)]) {
    test(s"3VL $name") {
      truthTable.foreach {
        case (l, r, answer) =>
          val expr = op(NonFoldableLiteral.create(l, BooleanType),
            NonFoldableLiteral.create(r, BooleanType))
          checkEvaluation(expr, answer)
      }
    }
  }

  // scalastyle:off
  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   * I.e. in flat cpo "False -> Unknown -> True",
   *   OR is lowest upper bound,
   *   AND is greatest lower bound.
   * p       q       p OR q  p AND q  p = q
   * True    True    True    True     True
   * True    False   True    False    False
   * True    Unknown True    Unknown  Unknown
   * False   True    True    False    False
   * False   False   False   False    True
   * False   Unknown Unknown False    Unknown
   * Unknown True    True    Unknown  Unknown
   * Unknown False   Unknown False    Unknown
   * Unknown Unknown Unknown Unknown  Unknown
   *
   * p       NOT p
   * True    False
   * False   True
   * Unknown Unknown
   */
  // scalastyle:on

  test("3VL Not") {
    val notTrueTable =
      (true, false) ::
        (false, true) ::
        (null, null) :: Nil
    notTrueTable.foreach { case (v, answer) =>
      checkEvaluation(Not(NonFoldableLiteral.create(v, BooleanType)), answer)
    }
    checkConsistencyBetweenInterpretedAndCodegen(Not, BooleanType)
  }

  test("AND, OR, EqualTo, EqualNullSafe consistency check") {
    checkConsistencyBetweenInterpretedAndCodegen(And, BooleanType, BooleanType)
    checkConsistencyBetweenInterpretedAndCodegen(Or, BooleanType, BooleanType)
    DataTypeTestUtils.propertyCheckSupported.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(EqualTo, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(EqualNullSafe, dt, dt)
    }
  }

  booleanLogicTest("AND", And,
    (true, true, true) ::
      (true, false, false) ::
      (true, null, null) ::
      (false, true, false) ::
      (false, false, false) ::
      (false, null, false) ::
      (null, true, null) ::
      (null, false, false) ::
      (null, null, null) :: Nil)

  booleanLogicTest("OR", Or,
    (true, true, true) ::
      (true, false, true) ::
      (true, null, true) ::
      (false, true, true) ::
      (false, false, false) ::
      (false, null, null) ::
      (null, true, true) ::
      (null, false, null) ::
      (null, null, null) :: Nil)

  booleanLogicTest("=", EqualTo,
    (true, true, true) ::
      (true, false, false) ::
      (true, null, null) ::
      (false, true, false) ::
      (false, false, true) ::
      (false, null, null) ::
      (null, true, null) ::
      (null, false, null) ::
      (null, null, null) :: Nil)

  test("basic IN predicate test") {
    checkEvaluation(In(NonFoldableLiteral.create(null, IntegerType), Seq(Literal(1),
      Literal(2))), null)
    checkEvaluation(In(NonFoldableLiteral.create(null, IntegerType),
      Seq(NonFoldableLiteral.create(null, IntegerType))), null)
    checkEvaluation(In(NonFoldableLiteral.create(null, IntegerType), Seq.empty), null)
    checkEvaluation(In(Literal(1), Seq.empty), false)
    checkEvaluation(In(Literal(1), Seq(NonFoldableLiteral.create(null, IntegerType))), null)
    checkEvaluation(In(Literal(1), Seq(Literal(1), NonFoldableLiteral.create(null, IntegerType))),
      true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), NonFoldableLiteral.create(null, IntegerType))),
      null)
    checkEvaluation(In(Literal(1), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(3), Seq(Literal(1), Literal(2))), false)
    checkEvaluation(
      And(In(Literal(1), Seq(Literal(1), Literal(2))), In(Literal(2), Seq(Literal(1),
        Literal(2)))),
      true)

    val ns = NonFoldableLiteral.create(null, StringType)
    checkEvaluation(In(ns, Seq(Literal("1"), Literal("2"))), null)
    checkEvaluation(In(ns, Seq(ns)), null)
    checkEvaluation(In(Literal("a"), Seq(ns)), null)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("^Ba*n"), ns)), true)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^Ba*n"))), true)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^n"))), false)

  }

  test("IN with different types") {
    def testWithRandomDataGeneration(dataType: DataType, nullable: Boolean): Unit = {
      val maybeDataGen = RandomDataGenerator.forType(dataType, nullable = nullable)
      // Actually we won't pass in unsupported data types, this is a safety check.
      val dataGen = maybeDataGen.getOrElse(
        fail(s"Failed to create data generator for type $dataType"))
      val inputData = Seq.fill(10) {
        val value = dataGen.apply()
        def cleanData(value: Any) = value match {
          case d: Double if d.isNaN => 0.0d
          case f: Float if f.isNaN => 0.0f
          case _ => value
        }
        value match {
          case s: Seq[_] => s.map(cleanData(_))
          case m: Map[_, _] =>
            val pair = m.unzip
            val newKeys = pair._1.map(cleanData(_))
            val newValues = pair._2.map(cleanData(_))
            newKeys.zip(newValues).toMap
          case _ => cleanData(value)
        }
      }
      val input = inputData.map(NonFoldableLiteral.create(_, dataType))
      val expected = if (inputData(0) == null) {
        null
      } else if (inputData.slice(1, 10).contains(inputData(0))) {
        true
      } else if (inputData.slice(1, 10).contains(null)) {
        null
      } else {
        false
      }
      checkEvaluation(In(input(0), input.slice(1, 10)), expected)
    }

    val atomicTypes = DataTypeTestUtils.atomicTypes.filter { t =>
      RandomDataGenerator.forType(t).isDefined && !t.isInstanceOf[DecimalType]
    } ++ Seq(DecimalType.USER_DEFAULT)

    val atomicArrayTypes = atomicTypes.map(ArrayType(_, containsNull = true))

    // Basic types:
    for (
        dataType <- atomicTypes;
        nullable <- Seq(true, false)) {
      testWithRandomDataGeneration(dataType, nullable)
    }

    // Array types:
    for (
        arrayType <- atomicArrayTypes;
        nullable <- Seq(true, false)
        if RandomDataGenerator.forType(arrayType.elementType, arrayType.containsNull).isDefined) {
      testWithRandomDataGeneration(arrayType, nullable)
    }

    // Struct types:
    for (
        colOneType <- atomicTypes;
        colTwoType <- atomicTypes;
        nullable <- Seq(true, false)) {
      val structType = StructType(
        StructField("a", colOneType) :: StructField("b", colTwoType) :: Nil)
      testWithRandomDataGeneration(structType, nullable)
    }

    // Map types: not supported
    for (
        keyType <- atomicTypes;
        valueType <- atomicTypes;
        nullable <- Seq(true, false)) {
      val mapType = MapType(keyType, valueType)
      val e = intercept[Exception] {
        testWithRandomDataGeneration(mapType, nullable)
      }
      if (e.getMessage.contains("Code generation of")) {
        // If the `value` expression is null, `eval` will be short-circuited.
        // Codegen version evaluation will be run then.
        assert(e.getMessage.contains("cannot generate equality code for un-comparable type"))
      } else {
        assert(e.getMessage.contains("Exception evaluating"))
      }
    }
  }

  test("SPARK-22501: In should not generate codes beyond 64KB") {
    val N = 3000
    val sets = (1 to N).map(i => Literal(i.toDouble))
    checkEvaluation(In(Literal(1.0D), sets), true)
  }

  test("INSET") {
    val hS = HashSet[Any]() + 1 + 2
    val nS = HashSet[Any]() + 1 + 2 + null
    val one = Literal(1)
    val two = Literal(2)
    val three = Literal(3)
    val nl = Literal(null)
    checkEvaluation(InSet(one, hS), true)
    checkEvaluation(InSet(two, hS), true)
    checkEvaluation(InSet(two, nS), true)
    checkEvaluation(InSet(three, hS), false)
    checkEvaluation(InSet(three, nS), null)
    checkEvaluation(InSet(nl, hS), null)
    checkEvaluation(InSet(nl, nS), null)

    val primitiveTypes = Seq(IntegerType, FloatType, DoubleType, StringType, ByteType, ShortType,
      LongType, BinaryType, BooleanType, DecimalType.USER_DEFAULT, TimestampType)
    primitiveTypes.foreach { t =>
      val dataGen = RandomDataGenerator.forType(t, nullable = true).get
      val inputData = Seq.fill(10) {
        val value = dataGen.apply()
        value match {
          case d: Double if d.isNaN => 0.0d
          case f: Float if f.isNaN => 0.0f
          case _ => value
        }
      }
      val input = inputData.map(Literal(_))
      val expected = if (inputData(0) == null) {
        null
      } else if (inputData.slice(1, 10).contains(inputData(0))) {
        true
      } else if (inputData.slice(1, 10).contains(null)) {
        null
      } else {
        false
      }
      checkEvaluation(InSet(input(0), inputData.slice(1, 10).toSet), expected)
    }
  }

  private case class MyStruct(a: Long, b: String)
  private case class MyStruct2(a: MyStruct, b: Array[Int])
  private val udt = new ExamplePointUDT

  private val smallValues =
    Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1), Array(1.toByte), new Date(2000, 1, 1),
      new Timestamp(1), "a", 1f, 1d, 0f, 0d, false, Array(1L, 2L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(1L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 1))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 2.0)), udt))
  private val largeValues =
    Seq(2.toByte, 2.toShort, 2, 2L, Decimal(2), Array(2.toByte), new Date(2000, 1, 2),
      new Timestamp(2), "b", 2f, 2d, Float.NaN, Double.NaN, true, Array(2L, 1L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(2L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 2))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 3.0)), udt))

  private val equalValues1 =
    Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1), Array(1.toByte), new Date(2000, 1, 1),
      new Timestamp(1), "a", 1f, 1d, Float.NaN, Double.NaN, true, Array(1L, 2L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(1L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 1))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 2.0)), udt))
  private val equalValues2 =
    Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1), Array(1.toByte), new Date(2000, 1, 1),
      new Timestamp(1), "a", 1f, 1d, Float.NaN, Double.NaN, true, Array(1L, 2L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(1L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 1))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 2.0)), udt))

  test("BinaryComparison consistency check") {
    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(LessThan, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(LessThanOrEqual, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(GreaterThan, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(GreaterThanOrEqual, dt, dt)
    }
  }

  test("BinaryComparison: lessThan") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(LessThan(smallValues(i), largeValues(i)), true)
      checkEvaluation(LessThan(equalValues1(i), equalValues2(i)), false)
      checkEvaluation(LessThan(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: LessThanOrEqual") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(LessThanOrEqual(smallValues(i), largeValues(i)), true)
      checkEvaluation(LessThanOrEqual(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(LessThanOrEqual(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: GreaterThan") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(GreaterThan(smallValues(i), largeValues(i)), false)
      checkEvaluation(GreaterThan(equalValues1(i), equalValues2(i)), false)
      checkEvaluation(GreaterThan(largeValues(i), smallValues(i)), true)
    }
  }

  test("BinaryComparison: GreaterThanOrEqual") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(GreaterThanOrEqual(smallValues(i), largeValues(i)), false)
      checkEvaluation(GreaterThanOrEqual(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(GreaterThanOrEqual(largeValues(i), smallValues(i)), true)
    }
  }

  test("BinaryComparison: EqualTo") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(EqualTo(smallValues(i), largeValues(i)), false)
      checkEvaluation(EqualTo(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(EqualTo(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: EqualNullSafe") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(EqualNullSafe(smallValues(i), largeValues(i)), false)
      checkEvaluation(EqualNullSafe(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(EqualNullSafe(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: null test") {
    // Use -1 (default value for codegen) which can trigger some weird bugs, e.g. SPARK-14757
    val normalInt = Literal(-1)
    val nullInt = NonFoldableLiteral.create(null, IntegerType)
    val nullNullType = Literal.create(null, NullType)

    def nullTest(op: (Expression, Expression) => Expression): Unit = {
      checkEvaluation(op(normalInt, nullInt), null)
      checkEvaluation(op(nullInt, normalInt), null)
      checkEvaluation(op(nullInt, nullInt), null)
      checkEvaluation(op(nullNullType, nullNullType), null)
    }

    nullTest(LessThan)
    nullTest(LessThanOrEqual)
    nullTest(GreaterThan)
    nullTest(GreaterThanOrEqual)
    nullTest(EqualTo)

    checkEvaluation(EqualNullSafe(normalInt, nullInt), false)
    checkEvaluation(EqualNullSafe(nullInt, normalInt), false)
    checkEvaluation(EqualNullSafe(nullInt, nullInt), true)
    checkEvaluation(EqualNullSafe(nullNullType, nullNullType), true)
  }

  test("EqualTo on complex type") {
    val array = new GenericArrayData(Array(1, 2, 3))
    val struct = create_row("a", 1L, array)

    val arrayType = ArrayType(IntegerType)
    val structType = new StructType()
      .add("1", StringType)
      .add("2", LongType)
      .add("3", ArrayType(IntegerType))

    val projection = UnsafeProjection.create(
      new StructType().add("array", arrayType).add("struct", structType))

    val unsafeRow = projection(InternalRow(array, struct))

    val unsafeArray = unsafeRow.getArray(0)
    val unsafeStruct = unsafeRow.getStruct(1, 3)

    checkEvaluation(EqualTo(
      Literal.create(array, arrayType),
      Literal.create(unsafeArray, arrayType)), true)

    checkEvaluation(EqualTo(
      Literal.create(struct, structType),
      Literal.create(unsafeStruct, structType)), true)
  }

  test("EqualTo double/float infinity") {
    val infinity = Literal(Double.PositiveInfinity)
    checkEvaluation(EqualTo(infinity, infinity), true)
  }
}
