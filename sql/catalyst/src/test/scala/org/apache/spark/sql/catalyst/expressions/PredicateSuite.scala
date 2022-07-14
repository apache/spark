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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.encoders.ExamplePointUDT
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


class PredicateSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def booleanLogicTest(
    name: String,
    op: (Expression, Expression) => Expression,
    truthTable: Seq[(Any, Any, Any)]): Unit = {
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

  private def checkInAndInSet(in: In, expected: Any): Unit = {
    // expecting all in.list are Literal or NonFoldableLiteral.
    checkEvaluation(in, expected)
    checkEvaluation(InSet(in.value, HashSet() ++ in.list.map(_.eval())), expected)
  }

  test("basic IN/INSET predicate test") {
    checkInAndInSet(In(NonFoldableLiteral.create(null, IntegerType), Seq(Literal(1),
      Literal(2))), null)
    checkInAndInSet(In(NonFoldableLiteral.create(null, IntegerType),
      Seq(NonFoldableLiteral.create(null, IntegerType))), null)
    checkInAndInSet(In(NonFoldableLiteral.create(null, IntegerType), Seq.empty), null)
    checkInAndInSet(In(Literal(1), Seq.empty), false)
    checkInAndInSet(In(Literal(1), Seq(NonFoldableLiteral.create(null, IntegerType))), null)
    checkInAndInSet(In(Literal(1), Seq(Literal(1), NonFoldableLiteral.create(null, IntegerType))),
      true)
    checkInAndInSet(In(Literal(2), Seq(Literal(1), NonFoldableLiteral.create(null, IntegerType))),
      null)
    checkInAndInSet(In(Literal(1), Seq(Literal(1), Literal(2))), true)
    checkInAndInSet(In(Literal(2), Seq(Literal(1), Literal(2))), true)
    checkInAndInSet(In(Literal(3), Seq(Literal(1), Literal(2))), false)

    checkEvaluation(
      And(In(Literal(1), Seq(Literal(1), Literal(2))), In(Literal(2), Seq(Literal(1),
        Literal(2)))),
      true)
    checkEvaluation(
      And(InSet(Literal(1), HashSet(1, 2)), InSet(Literal(2), Set(1, 2))),
      true)

    val ns = NonFoldableLiteral.create(null, StringType)
    checkInAndInSet(In(ns, Seq(Literal("1"), Literal("2"))), null)
    checkInAndInSet(In(ns, Seq(ns)), null)
    checkInAndInSet(In(Literal("a"), Seq(ns)), null)
    checkInAndInSet(In(Literal("^Ba*n"), Seq(Literal("^Ba*n"), ns)), true)
    checkInAndInSet(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^Ba*n"))), true)
    checkInAndInSet(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^n"))), false)
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
      checkInAndInSet(In(input(0), input.slice(1, 10)), expected)
    }

    val atomicTypes = DataTypeTestUtils.atomicTypes.filter { t =>
      RandomDataGenerator.forType(t).isDefined &&
        !t.isInstanceOf[DecimalType] && !t.isInstanceOf[BinaryType]
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

    // In doesn't support map type and will fail the analyzer.
    val map = Literal.create(create_map(1 -> 1), MapType(IntegerType, IntegerType))
    In(map, Seq(map)).checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckFailure(msg) =>
        assert(msg.contains("function in does not support ordering on type map"))
      case _ => fail("In should not work on map type")
    }
  }

  test("switch statements in InSet for bytes, shorts, ints, dates") {
    val byteValues = Set[Any](1.toByte, 2.toByte, Byte.MinValue, Byte.MaxValue)
    val shortValues = Set[Any](-10.toShort, 20.toShort, Short.MinValue, Short.MaxValue)
    val intValues = Set[Any](20, -100, 30, Int.MinValue, Int.MaxValue)
    val dateValues = Set[Any](
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("2017-01-01")),
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("1950-01-02")))

    def check(presentValue: Expression, absentValue: Expression, values: Set[Any]): Unit = {
      require(presentValue.dataType == absentValue.dataType)

      val nullLiteral = Literal(null, presentValue.dataType)

      checkEvaluation(InSet(nullLiteral, values), expected = null)
      checkEvaluation(InSet(nullLiteral, values + null), expected = null)
      checkEvaluation(InSet(presentValue, values), expected = true)
      checkEvaluation(InSet(presentValue, values + null), expected = true)
      checkEvaluation(InSet(absentValue, values), expected = false)
      checkEvaluation(InSet(absentValue, values + null), expected = null)
    }

    def checkAllTypes(): Unit = {
      check(presentValue = Literal(2.toByte), absentValue = Literal(3.toByte), byteValues)
      check(presentValue = Literal(Byte.MinValue), absentValue = Literal(5.toByte), byteValues)
      check(presentValue = Literal(20.toShort), absentValue = Literal(-14.toShort), shortValues)
      check(presentValue = Literal(Short.MaxValue), absentValue = Literal(30.toShort), shortValues)
      check(presentValue = Literal(20), absentValue = Literal(-14), intValues)
      check(presentValue = Literal(Int.MinValue), absentValue = Literal(2), intValues)
      check(
        presentValue = Literal(Date.valueOf("2017-01-01")),
        absentValue = Literal(Date.valueOf("2017-01-02")),
        dateValues)
      check(
        presentValue = Literal(Date.valueOf("1950-01-02")),
        absentValue = Literal(Date.valueOf("2017-10-02")),
        dateValues)
    }

    withSQLConf(SQLConf.OPTIMIZER_INSET_SWITCH_THRESHOLD.key -> "0") {
      checkAllTypes()
    }
    withSQLConf(SQLConf.OPTIMIZER_INSET_SWITCH_THRESHOLD.key -> "20") {
      checkAllTypes()
    }
  }

  test("SPARK-22501: In should not generate codes beyond 64KB") {
    val N = 3000
    val sets = (1 to N).map(i => Literal(i.toDouble))
    checkEvaluation(In(Literal(1.0D), sets), true)
  }

  test("SPARK-22705: In should use less global variables") {
    val ctx = new CodegenContext()
    In(Literal(1.0D), Seq(Literal(1.0D), Literal(2.0D))).genCode(ctx)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("IN/INSET: binary") {
    val onetwo = Literal(Array(1.toByte, 2.toByte))
    val three = Literal(Array(3.toByte))
    val threefour = Literal(Array(3.toByte, 4.toByte))
    val nl = NonFoldableLiteral.create(null, onetwo.dataType)
    val hS = Seq(Literal(Array(1.toByte, 2.toByte)), Literal(Array(3.toByte)))
    val nS = Seq(Literal(Array(1.toByte, 2.toByte)), Literal(Array(3.toByte)),
      NonFoldableLiteral.create(null, onetwo.dataType))
    checkInAndInSet(In(onetwo, hS), true)
    checkInAndInSet(In(three, hS), true)
    checkInAndInSet(In(three, nS), true)
    checkInAndInSet(In(threefour, hS), false)
    checkInAndInSet(In(threefour, nS), null)
    checkInAndInSet(In(nl, hS), null)
    checkInAndInSet(In(nl, nS), null)
  }

  test("IN/INSET: struct") {
    val oneA = Literal.create((1, "a"))
    val twoB = Literal.create((2, "b"))
    val twoC = Literal.create((2, "c"))
    val nl = NonFoldableLiteral.create(null, oneA.dataType)
    val hS = Seq(Literal.create((1, "a")), Literal.create((2, "b")))
    val nS = Seq(Literal.create((1, "a")), Literal.create((2, "b")),
      NonFoldableLiteral.create(null, oneA.dataType))
    checkInAndInSet(In(oneA, hS), true)
    checkInAndInSet(In(twoB, hS), true)
    checkInAndInSet(In(twoB, nS), true)
    checkInAndInSet(In(twoC, hS), false)
    checkInAndInSet(In(twoC, nS), null)
    checkInAndInSet(In(nl, hS), null)
    checkInAndInSet(In(nl, nS), null)
  }

  test("IN/INSET: array") {
    val onetwo = Literal.create(Seq(1, 2))
    val three = Literal.create(Seq(3))
    val threefour = Literal.create(Seq(3, 4))
    val nl = NonFoldableLiteral.create(null, onetwo.dataType)
    val hS = Seq(Literal.create(Seq(1, 2)), Literal.create(Seq(3)))
    val nS = Seq(Literal.create(Seq(1, 2)), Literal.create(Seq(3)),
      NonFoldableLiteral.create(null, onetwo.dataType))
    checkInAndInSet(In(onetwo, hS), true)
    checkInAndInSet(In(three, hS), true)
    checkInAndInSet(In(three, nS), true)
    checkInAndInSet(In(threefour, hS), false)
    checkInAndInSet(In(threefour, nS), null)
    checkInAndInSet(In(nl, hS), null)
    checkInAndInSet(In(nl, nS), null)
  }

  private case class MyStruct(a: Long, b: String)
  private case class MyStruct2(a: MyStruct, b: Array[Int])
  private val udt = new ExamplePointUDT

  private val smallValues =
    Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1), Array(1.toByte), Date.valueOf("2000-01-01"),
      new Timestamp(1), "a", 1f, 1d, 0f, 0d, false, Array(1L, 2L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(1L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 1))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 2.0)), udt))
  private val largeValues =
    Seq(2.toByte, 2.toShort, 2, 2L, Decimal(2), Array(2.toByte), Date.valueOf("2000-01-02"),
      new Timestamp(2), "b", 2f, 2d, Float.NaN, Double.NaN, true, Array(2L, 1L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(2L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 2))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 3.0)), udt))

  private val equalValues1 =
    Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1), Array(1.toByte), Date.valueOf("2000-01-01"),
      new Timestamp(1), "a", 1f, 1d, Float.NaN, Double.NaN, true, Array(1L, 2L))
      .map(Literal(_)) ++ Seq(Literal.create(MyStruct(1L, "b")),
      Literal.create(MyStruct2(MyStruct(1L, "a"), Array(1, 1))),
      Literal.create(ArrayData.toArrayData(Array(1.0, 2.0)), udt))
  private val equalValues2 =
    Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1), Array(1.toByte), Date.valueOf("2000-01-01"),
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
    for (i <- smallValues.indices) {
      checkEvaluation(LessThan(smallValues(i), largeValues(i)), true)
      checkEvaluation(LessThan(equalValues1(i), equalValues2(i)), false)
      checkEvaluation(LessThan(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: LessThanOrEqual") {
    for (i <- smallValues.indices) {
      checkEvaluation(LessThanOrEqual(smallValues(i), largeValues(i)), true)
      checkEvaluation(LessThanOrEqual(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(LessThanOrEqual(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: GreaterThan") {
    for (i <- smallValues.indices) {
      checkEvaluation(GreaterThan(smallValues(i), largeValues(i)), false)
      checkEvaluation(GreaterThan(equalValues1(i), equalValues2(i)), false)
      checkEvaluation(GreaterThan(largeValues(i), smallValues(i)), true)
    }
  }

  test("BinaryComparison: GreaterThanOrEqual") {
    for (i <- smallValues.indices) {
      checkEvaluation(GreaterThanOrEqual(smallValues(i), largeValues(i)), false)
      checkEvaluation(GreaterThanOrEqual(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(GreaterThanOrEqual(largeValues(i), smallValues(i)), true)
    }
  }

  test("BinaryComparison: EqualTo") {
    for (i <- smallValues.indices) {
      checkEvaluation(EqualTo(smallValues(i), largeValues(i)), false)
      checkEvaluation(EqualTo(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(EqualTo(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: EqualNullSafe") {
    for (i <- smallValues.indices) {
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

  test("SPARK-22693: InSet should not use global variables") {
    val ctx = new CodegenContext
    InSet(Literal(1), Set(1, 2, 3, 4)).genCode(ctx)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("SPARK-24007: EqualNullSafe for FloatType and DoubleType might generate a wrong result") {
    checkEvaluation(EqualNullSafe(Literal(null, FloatType), Literal(-1.0f)), false)
    checkEvaluation(EqualNullSafe(Literal(-1.0f), Literal(null, FloatType)), false)
    checkEvaluation(EqualNullSafe(Literal(null, DoubleType), Literal(-1.0d)), false)
    checkEvaluation(EqualNullSafe(Literal(-1.0d), Literal(null, DoubleType)), false)
  }

  test("Interpreted Predicate should initialize nondeterministic expressions") {
    val interpreted = Predicate.create(LessThan(Rand(7), Literal(1.0)))
    interpreted.initialize(0)
    assert(interpreted.eval(new UnsafeRow()))
  }

  test("SPARK-24872: Replace taking the $symbol with $sqlOperator in BinaryOperator's" +
    " toString method") {
    val expression = CatalystSqlParser.parseExpression("id=1 or id=2").toString()
    val expected = "(('id = 1) OR ('id = 2))"
    assert(expression == expected)
  }

  test("isunknown and isnotunknown") {
    val row0 = create_row(null)

    checkEvaluation(IsUnknown(Literal.create(null, BooleanType)), true, row0)
    checkEvaluation(IsNotUnknown(Literal.create(null, BooleanType)), false, row0)
    IsUnknown(Literal.create(null, IntegerType)).checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckFailure(msg) =>
        assert(msg.contains("argument 1 requires boolean type"))
    }
  }

  test("SPARK-29100: InSet with empty input set") {
    val row = create_row(1)
    val inSet = InSet(BoundReference(0, IntegerType, true), Set.empty)
    checkEvaluation(inSet, false, row)
  }

  test("SPARK-32764: compare special double/float values") {
    checkEvaluation(EqualTo(Literal(Double.NaN), Literal(Double.NaN)), true)
    checkEvaluation(EqualTo(Literal(Double.NaN), Literal(Double.PositiveInfinity)), false)
    checkEvaluation(EqualTo(Literal(0.0D), Literal(-0.0D)), true)
    checkEvaluation(GreaterThan(Literal(Double.NaN), Literal(Double.PositiveInfinity)), true)
    checkEvaluation(GreaterThan(Literal(Double.NaN), Literal(Double.NaN)), false)
    checkEvaluation(GreaterThan(Literal(0.0D), Literal(-0.0D)), false)

    checkEvaluation(EqualTo(Literal(Float.NaN), Literal(Float.NaN)), true)
    checkEvaluation(EqualTo(Literal(Float.NaN), Literal(Float.PositiveInfinity)), false)
    checkEvaluation(EqualTo(Literal(0.0F), Literal(-0.0F)), true)
    checkEvaluation(GreaterThan(Literal(Float.NaN), Literal(Float.PositiveInfinity)), true)
    checkEvaluation(GreaterThan(Literal(Float.NaN), Literal(Float.NaN)), false)
    checkEvaluation(GreaterThan(Literal(0.0F), Literal(-0.0F)), false)
  }

  test("SPARK-32110: compare special double/float values in array") {
    def createUnsafeDoubleArray(d: Double): Literal = {
      Literal(UnsafeArrayData.fromPrimitiveArray(Array(d)), ArrayType(DoubleType))
    }
    def createSafeDoubleArray(d: Double): Literal = {
      Literal(new GenericArrayData(Array(d)), ArrayType(DoubleType))
    }
    def createUnsafeFloatArray(d: Double): Literal = {
      Literal(UnsafeArrayData.fromPrimitiveArray(Array(d.toFloat)), ArrayType(FloatType))
    }
    def createSafeFloatArray(d: Double): Literal = {
      Literal(new GenericArrayData(Array(d.toFloat)), ArrayType(FloatType))
    }
    def checkExpr(
        exprBuilder: (Expression, Expression) => Expression,
        left: Double,
        right: Double,
        expected: Any): Unit = {
      // test double
      checkEvaluation(
        exprBuilder(createUnsafeDoubleArray(left), createUnsafeDoubleArray(right)), expected)
      checkEvaluation(
        exprBuilder(createUnsafeDoubleArray(left), createSafeDoubleArray(right)), expected)
      checkEvaluation(
        exprBuilder(createSafeDoubleArray(left), createSafeDoubleArray(right)), expected)
      // test float
      checkEvaluation(
        exprBuilder(createUnsafeFloatArray(left), createUnsafeFloatArray(right)), expected)
      checkEvaluation(
        exprBuilder(createUnsafeFloatArray(left), createSafeFloatArray(right)), expected)
      checkEvaluation(
        exprBuilder(createSafeFloatArray(left), createSafeFloatArray(right)), expected)
    }

    checkExpr(EqualTo, Double.NaN, Double.NaN, true)
    checkExpr(EqualTo, Double.NaN, Double.PositiveInfinity, false)
    checkExpr(EqualTo, 0.0, -0.0, true)
    checkExpr(GreaterThan, Double.NaN, Double.PositiveInfinity, true)
    checkExpr(GreaterThan, Double.NaN, Double.NaN, false)
    checkExpr(GreaterThan, 0.0, -0.0, false)
  }

  test("SPARK-32110: compare special double/float values in struct") {
    def createUnsafeDoubleRow(d: Double): Literal = {
      val dt = new StructType().add("d", "double")
      val converter = UnsafeProjection.create(dt)
      val unsafeRow = converter.apply(InternalRow(d))
      Literal(unsafeRow, dt)
    }
    def createSafeDoubleRow(d: Double): Literal = {
      Literal(InternalRow(d), new StructType().add("d", "double"))
    }
    def createUnsafeFloatRow(d: Double): Literal = {
      val dt = new StructType().add("f", "float")
      val converter = UnsafeProjection.create(dt)
      val unsafeRow = converter.apply(InternalRow(d.toFloat))
      Literal(unsafeRow, dt)
    }
    def createSafeFloatRow(d: Double): Literal = {
      Literal(InternalRow(d.toFloat), new StructType().add("f", "float"))
    }
    def checkExpr(
        exprBuilder: (Expression, Expression) => Expression,
        left: Double,
        right: Double,
        expected: Any): Unit = {
      // test double
      checkEvaluation(
        exprBuilder(createUnsafeDoubleRow(left), createUnsafeDoubleRow(right)), expected)
      checkEvaluation(
        exprBuilder(createUnsafeDoubleRow(left), createSafeDoubleRow(right)), expected)
      checkEvaluation(
        exprBuilder(createSafeDoubleRow(left), createSafeDoubleRow(right)), expected)
      // test float
      checkEvaluation(
        exprBuilder(createUnsafeFloatRow(left), createUnsafeFloatRow(right)), expected)
      checkEvaluation(
        exprBuilder(createUnsafeFloatRow(left), createSafeFloatRow(right)), expected)
      checkEvaluation(
        exprBuilder(createSafeFloatRow(left), createSafeFloatRow(right)), expected)
    }

    checkExpr(EqualTo, Double.NaN, Double.NaN, true)
    checkExpr(EqualTo, Double.NaN, Double.PositiveInfinity, false)
    checkExpr(EqualTo, 0.0, -0.0, true)
    checkExpr(GreaterThan, Double.NaN, Double.PositiveInfinity, true)
    checkExpr(GreaterThan, Double.NaN, Double.NaN, false)
    checkExpr(GreaterThan, 0.0, -0.0, false)
  }

  test("SPARK-36792: InSet should handle Double.NaN and Float.NaN") {
    checkInAndInSet(In(Literal(Double.NaN), Seq(Literal(Double.NaN), Literal(2d))), true)
    checkInAndInSet(In(Literal.create(null, DoubleType),
      Seq(Literal(Double.NaN), Literal(2d), Literal.create(null, DoubleType))), null)
    checkInAndInSet(In(Literal.create(null, DoubleType),
      Seq(Literal(Double.NaN), Literal(2d))), null)
    checkInAndInSet(In(Literal(3d),
      Seq(Literal(Double.NaN), Literal(2d))), false)
    checkInAndInSet(In(Literal(3d),
      Seq(Literal(Double.NaN), Literal(2d), Literal.create(null, DoubleType))), null)
    checkInAndInSet(In(Literal(Double.NaN),
      Seq(Literal(Double.NaN), Literal(2d), Literal.create(null, DoubleType))), true)
  }
}
