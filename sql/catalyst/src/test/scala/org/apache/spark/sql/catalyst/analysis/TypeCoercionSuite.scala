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

package org.apache.spark.sql.catalyst.analysis

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{Division, FunctionArgumentConversion}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class TypeCoercionSuite extends PlanTest {

  test("eligible implicit type cast") {
    def shouldCast(from: DataType, to: AbstractDataType, expected: DataType): Unit = {
      val got = TypeCoercion.ImplicitTypeCasts.implicitCast(Literal.create(null, from), to)
      assert(got.map(_.dataType) == Option(expected),
        s"Failed to cast $from to $to")
    }

    shouldCast(NullType, NullType, NullType)
    shouldCast(NullType, IntegerType, IntegerType)
    shouldCast(NullType, DecimalType, DecimalType.SYSTEM_DEFAULT)

    shouldCast(ByteType, IntegerType, IntegerType)
    shouldCast(IntegerType, IntegerType, IntegerType)
    shouldCast(IntegerType, LongType, LongType)
    shouldCast(IntegerType, DecimalType, DecimalType(10, 0))
    shouldCast(LongType, IntegerType, IntegerType)
    shouldCast(LongType, DecimalType, DecimalType(20, 0))

    shouldCast(DateType, TimestampType, TimestampType)
    shouldCast(TimestampType, DateType, DateType)

    shouldCast(StringType, IntegerType, IntegerType)
    shouldCast(StringType, DateType, DateType)
    shouldCast(StringType, TimestampType, TimestampType)
    shouldCast(IntegerType, StringType, StringType)
    shouldCast(DateType, StringType, StringType)
    shouldCast(TimestampType, StringType, StringType)

    shouldCast(StringType, BinaryType, BinaryType)
    shouldCast(BinaryType, StringType, StringType)

    shouldCast(NullType, TypeCollection(StringType, BinaryType), StringType)

    shouldCast(StringType, TypeCollection(StringType, BinaryType), StringType)
    shouldCast(BinaryType, TypeCollection(StringType, BinaryType), BinaryType)
    shouldCast(StringType, TypeCollection(BinaryType, StringType), StringType)

    shouldCast(IntegerType, TypeCollection(IntegerType, BinaryType), IntegerType)
    shouldCast(IntegerType, TypeCollection(BinaryType, IntegerType), IntegerType)
    shouldCast(BinaryType, TypeCollection(BinaryType, IntegerType), BinaryType)
    shouldCast(BinaryType, TypeCollection(IntegerType, BinaryType), BinaryType)

    shouldCast(IntegerType, TypeCollection(StringType, BinaryType), StringType)
    shouldCast(IntegerType, TypeCollection(BinaryType, StringType), StringType)

    shouldCast(DecimalType.SYSTEM_DEFAULT,
      TypeCollection(IntegerType, DecimalType), DecimalType.SYSTEM_DEFAULT)
    shouldCast(DecimalType(10, 2), TypeCollection(IntegerType, DecimalType), DecimalType(10, 2))
    shouldCast(DecimalType(10, 2), TypeCollection(DecimalType, IntegerType), DecimalType(10, 2))
    shouldCast(IntegerType, TypeCollection(DecimalType(10, 2), StringType), DecimalType(10, 2))

    shouldCast(StringType, NumericType, DoubleType)
    shouldCast(StringType, TypeCollection(NumericType, BinaryType), DoubleType)

    // NumericType should not be changed when function accepts any of them.
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      DecimalType.SYSTEM_DEFAULT, DecimalType(10, 2)).foreach { tpe =>
      shouldCast(tpe, NumericType, tpe)
    }

    shouldCast(
      ArrayType(StringType, false),
      TypeCollection(ArrayType(StringType), StringType),
      ArrayType(StringType, false))

    shouldCast(
      ArrayType(StringType, true),
      TypeCollection(ArrayType(StringType), StringType),
      ArrayType(StringType, true))
  }

  test("ineligible implicit type cast") {
    def shouldNotCast(from: DataType, to: AbstractDataType): Unit = {
      val got = TypeCoercion.ImplicitTypeCasts.implicitCast(Literal.create(null, from), to)
      assert(got.isEmpty, s"Should not be able to cast $from to $to, but got $got")
    }

    shouldNotCast(IntegerType, DateType)
    shouldNotCast(IntegerType, TimestampType)
    shouldNotCast(LongType, DateType)
    shouldNotCast(LongType, TimestampType)
    shouldNotCast(DecimalType.SYSTEM_DEFAULT, DateType)
    shouldNotCast(DecimalType.SYSTEM_DEFAULT, TimestampType)

    shouldNotCast(IntegerType, TypeCollection(DateType, TimestampType))

    shouldNotCast(IntegerType, ArrayType)
    shouldNotCast(IntegerType, MapType)
    shouldNotCast(IntegerType, StructType)

    shouldNotCast(CalendarIntervalType, StringType)

    // Don't implicitly cast complex types to string.
    shouldNotCast(ArrayType(StringType), StringType)
    shouldNotCast(MapType(StringType, StringType), StringType)
    shouldNotCast(new StructType().add("a1", StringType), StringType)
    shouldNotCast(MapType(StringType, StringType), StringType)
  }

  test("tightest common bound for types") {
    def widenTest(t1: DataType, t2: DataType, tightestCommon: Option[DataType]) {
      var found = TypeCoercion.findTightestCommonTypeOfTwo(t1, t2)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest common type for $t1 and $t2, found $found")
      // Test both directions to make sure the widening is symmetric.
      found = TypeCoercion.findTightestCommonTypeOfTwo(t2, t1)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest common type for $t2 and $t1, found $found")
    }

    // Null
    widenTest(NullType, NullType, Some(NullType))

    // Boolean
    widenTest(NullType, BooleanType, Some(BooleanType))
    widenTest(BooleanType, BooleanType, Some(BooleanType))
    widenTest(IntegerType, BooleanType, None)
    widenTest(LongType, BooleanType, None)

    // Integral
    widenTest(NullType, ByteType, Some(ByteType))
    widenTest(NullType, IntegerType, Some(IntegerType))
    widenTest(NullType, LongType, Some(LongType))
    widenTest(ShortType, IntegerType, Some(IntegerType))
    widenTest(ShortType, LongType, Some(LongType))
    widenTest(IntegerType, LongType, Some(LongType))
    widenTest(LongType, LongType, Some(LongType))

    // Floating point
    widenTest(NullType, FloatType, Some(FloatType))
    widenTest(NullType, DoubleType, Some(DoubleType))
    widenTest(FloatType, DoubleType, Some(DoubleType))
    widenTest(FloatType, FloatType, Some(FloatType))
    widenTest(DoubleType, DoubleType, Some(DoubleType))

    // Integral mixed with floating point.
    widenTest(IntegerType, FloatType, Some(FloatType))
    widenTest(IntegerType, DoubleType, Some(DoubleType))
    widenTest(IntegerType, DoubleType, Some(DoubleType))
    widenTest(LongType, FloatType, Some(FloatType))
    widenTest(LongType, DoubleType, Some(DoubleType))

    // No up-casting for fixed-precision decimal (this is handled by arithmetic rules)
    widenTest(DecimalType(2, 1), DecimalType(3, 2), None)
    widenTest(DecimalType(2, 1), DoubleType, None)
    widenTest(DecimalType(2, 1), IntegerType, None)
    widenTest(DoubleType, DecimalType(2, 1), None)
    widenTest(IntegerType, DecimalType(2, 1), None)

    // StringType
    widenTest(NullType, StringType, Some(StringType))
    widenTest(StringType, StringType, Some(StringType))
    widenTest(IntegerType, StringType, None)
    widenTest(LongType, StringType, None)

    // TimestampType
    widenTest(NullType, TimestampType, Some(TimestampType))
    widenTest(TimestampType, TimestampType, Some(TimestampType))
    widenTest(IntegerType, TimestampType, None)
    widenTest(StringType, TimestampType, None)

    // ComplexType
    widenTest(NullType,
      MapType(IntegerType, StringType, false),
      Some(MapType(IntegerType, StringType, false)))
    widenTest(NullType, StructType(Seq()), Some(StructType(Seq())))
    widenTest(StringType, MapType(IntegerType, StringType, true), None)
    widenTest(ArrayType(IntegerType), StructType(Seq()), None)
  }

  private def ruleTest(rule: Rule[LogicalPlan], initial: Expression, transformed: Expression) {
    ruleTest(Seq(rule), initial, transformed)
  }

  private def ruleTest(
      rules: Seq[Rule[LogicalPlan]],
      initial: Expression,
      transformed: Expression): Unit = {
    val testRelation = LocalRelation(AttributeReference("a", IntegerType)())
    val analyzer = new RuleExecutor[LogicalPlan] {
      override val batches = Seq(Batch("Resolution", FixedPoint(3), rules: _*))
    }

    comparePlans(
      analyzer.execute(Project(Seq(Alias(initial, "a")()), testRelation)),
      Project(Seq(Alias(transformed, "a")()), testRelation))
  }

  test("cast NullType for expressions that implement ExpectsInputTypes") {
    import TypeCoercionSuite._

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      AnyTypeUnaryExpression(Literal.create(null, NullType)),
      AnyTypeUnaryExpression(Literal.create(null, NullType)))

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      NumericTypeUnaryExpression(Literal.create(null, NullType)),
      NumericTypeUnaryExpression(Literal.create(null, DoubleType)))
  }

  test("cast NullType for binary operators") {
    import TypeCoercionSuite._

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      AnyTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)),
      AnyTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)))

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      NumericTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)),
      NumericTypeBinaryOperator(Literal.create(null, DoubleType), Literal.create(null, DoubleType)))
  }

  test("coalesce casts") {
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      Coalesce(Literal(1.0)
        :: Literal(1)
        :: Literal.create(1.0, FloatType)
        :: Nil),
      Coalesce(Cast(Literal(1.0), DoubleType)
        :: Cast(Literal(1), DoubleType)
        :: Cast(Literal.create(1.0, FloatType), DoubleType)
        :: Nil))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      Coalesce(Literal(1L)
        :: Literal(1)
        :: Literal(new java.math.BigDecimal("1000000000000000000000"))
        :: Nil),
      Coalesce(Cast(Literal(1L), DecimalType(22, 0))
        :: Cast(Literal(1), DecimalType(22, 0))
        :: Cast(Literal(new java.math.BigDecimal("1000000000000000000000")), DecimalType(22, 0))
        :: Nil))
  }

  test("CreateArray casts") {
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal.create(1.0, FloatType)
        :: Nil),
      CreateArray(Cast(Literal(1.0), DoubleType)
        :: Cast(Literal(1), DoubleType)
        :: Cast(Literal.create(1.0, FloatType), DoubleType)
        :: Nil))

    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal("a")
        :: Nil),
      CreateArray(Cast(Literal(1.0), StringType)
        :: Cast(Literal(1), StringType)
        :: Cast(Literal("a"), StringType)
        :: Nil))
  }

  test("CreateMap casts") {
    // type coercion for map keys
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal.create(2.0, FloatType)
        :: Literal("b")
        :: Nil),
      CreateMap(Cast(Literal(1), FloatType)
        :: Literal("a")
        :: Cast(Literal.create(2.0, FloatType), FloatType)
        :: Literal("b")
        :: Nil))
    // type coercion for map values
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2)
        :: Literal(3.0)
        :: Nil),
      CreateMap(Literal(1)
        :: Cast(Literal("a"), StringType)
        :: Literal(2)
        :: Cast(Literal(3.0), StringType)
        :: Nil))
    // type coercion for both map keys and values
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2.0)
        :: Literal(3.0)
        :: Nil),
      CreateMap(Cast(Literal(1), DoubleType)
        :: Cast(Literal("a"), StringType)
        :: Cast(Literal(2.0), DoubleType)
        :: Cast(Literal(3.0), StringType)
        :: Nil))
  }

  test("greatest/least cast") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      ruleTest(TypeCoercion.FunctionArgumentConversion,
        operator(Literal(1.0)
          :: Literal(1)
          :: Literal.create(1.0, FloatType)
          :: Nil),
        operator(Cast(Literal(1.0), DoubleType)
          :: Cast(Literal(1), DoubleType)
          :: Cast(Literal.create(1.0, FloatType), DoubleType)
          :: Nil))
      ruleTest(TypeCoercion.FunctionArgumentConversion,
        operator(Literal(1L)
          :: Literal(1)
          :: Literal(new java.math.BigDecimal("1000000000000000000000"))
          :: Nil),
        operator(Cast(Literal(1L), DecimalType(22, 0))
          :: Cast(Literal(1), DecimalType(22, 0))
          :: Cast(Literal(new java.math.BigDecimal("1000000000000000000000")), DecimalType(22, 0))
          :: Nil))
    }
  }

  test("nanvl casts") {
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, FloatType), Literal.create(1.0, DoubleType)),
      NaNvl(Cast(Literal.create(1.0, FloatType), DoubleType), Literal.create(1.0, DoubleType)))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, FloatType)),
      NaNvl(Literal.create(1.0, DoubleType), Cast(Literal.create(1.0, FloatType), DoubleType)))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, DoubleType)),
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, DoubleType)))
  }

  test("type coercion for If") {
    val rule = TypeCoercion.IfCoercion

    ruleTest(rule,
      If(Literal(true), Literal(1), Literal(1L)),
      If(Literal(true), Cast(Literal(1), LongType), Literal(1L)))

    ruleTest(rule,
      If(Literal.create(null, NullType), Literal(1), Literal(1)),
      If(Literal.create(null, BooleanType), Literal(1), Literal(1)))

    ruleTest(rule,
      If(AssertTrue(Literal.create(true, BooleanType)), Literal(1), Literal(2)),
      If(Cast(AssertTrue(Literal.create(true, BooleanType)), BooleanType), Literal(1), Literal(2)))

    ruleTest(rule,
      If(AssertTrue(Literal.create(false, BooleanType)), Literal(1), Literal(2)),
      If(Cast(AssertTrue(Literal.create(false, BooleanType)), BooleanType), Literal(1), Literal(2)))
  }

  test("type coercion for CaseKeyWhen") {
    ruleTest(TypeCoercion.ImplicitTypeCasts,
      CaseKeyWhen(Literal(1.toShort), Seq(Literal(1), Literal("a"))),
      CaseKeyWhen(Cast(Literal(1.toShort), IntegerType), Seq(Literal(1), Literal("a")))
    )
    ruleTest(TypeCoercion.CaseWhenCoercion,
      CaseKeyWhen(Literal(true), Seq(Literal(1), Literal("a"))),
      CaseKeyWhen(Literal(true), Seq(Literal(1), Literal("a")))
    )
    ruleTest(TypeCoercion.CaseWhenCoercion,
      CaseWhen(Seq((Literal(true), Literal(1.2))), Literal.create(1, DecimalType(7, 2))),
      CaseWhen(Seq((Literal(true), Literal(1.2))),
        Cast(Literal.create(1, DecimalType(7, 2)), DoubleType))
    )
    ruleTest(TypeCoercion.CaseWhenCoercion,
      CaseWhen(Seq((Literal(true), Literal(100L))), Literal.create(1, DecimalType(7, 2))),
      CaseWhen(Seq((Literal(true), Cast(Literal(100L), DecimalType(22, 2)))),
        Cast(Literal.create(1, DecimalType(7, 2)), DecimalType(22, 2)))
    )
  }

  test("BooleanEquality type cast") {
    val be = TypeCoercion.BooleanEquality
    // Use something more than a literal to avoid triggering the simplification rules.
    val one = Add(Literal(Decimal(1)), Literal(Decimal(0)))

    ruleTest(be,
      EqualTo(Literal(true), one),
      EqualTo(Cast(Literal(true), one.dataType), one)
    )

    ruleTest(be,
      EqualTo(one, Literal(true)),
      EqualTo(one, Cast(Literal(true), one.dataType))
    )

    ruleTest(be,
      EqualNullSafe(Literal(true), one),
      EqualNullSafe(Cast(Literal(true), one.dataType), one)
    )

    ruleTest(be,
      EqualNullSafe(one, Literal(true)),
      EqualNullSafe(one, Cast(Literal(true), one.dataType))
    )
  }

  test("BooleanEquality simplification") {
    val be = TypeCoercion.BooleanEquality

    ruleTest(be,
      EqualTo(Literal(true), Literal(1)),
      Literal(true)
    )
    ruleTest(be,
      EqualTo(Literal(true), Literal(0)),
      Not(Literal(true))
    )
    ruleTest(be,
      EqualNullSafe(Literal(true), Literal(1)),
      And(IsNotNull(Literal(true)), Literal(true))
    )
    ruleTest(be,
      EqualNullSafe(Literal(true), Literal(0)),
      And(IsNotNull(Literal(true)), Not(Literal(true)))
    )

    ruleTest(be,
      EqualTo(Literal(true), Literal(1L)),
      Literal(true)
    )
    ruleTest(be,
      EqualTo(Literal(new java.math.BigDecimal(1)), Literal(true)),
      Literal(true)
    )
    ruleTest(be,
      EqualTo(Literal(BigDecimal(0)), Literal(true)),
      Not(Literal(true))
    )
    ruleTest(be,
      EqualTo(Literal(Decimal(1)), Literal(true)),
      Literal(true)
    )
    ruleTest(be,
      EqualTo(Literal.create(Decimal(1), DecimalType(8, 0)), Literal(true)),
      Literal(true)
    )
  }

  private def checkOutput(logical: LogicalPlan, expectTypes: Seq[DataType]): Unit = {
    logical.output.zip(expectTypes).foreach { case (attr, dt) =>
      assert(attr.dataType === dt)
    }
  }

  test("WidenSetOperationTypes for except and intersect") {
    val firstTable = LocalRelation(
      AttributeReference("i", IntegerType)(),
      AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("b", ByteType)(),
      AttributeReference("d", DoubleType)())
    val secondTable = LocalRelation(
      AttributeReference("s", StringType)(),
      AttributeReference("d", DecimalType(2, 1))(),
      AttributeReference("f", FloatType)(),
      AttributeReference("l", LongType)())

    val wt = TypeCoercion.WidenSetOperationTypes
    val expectedTypes = Seq(StringType, DecimalType.SYSTEM_DEFAULT, FloatType, DoubleType)

    val r1 = wt(Except(firstTable, secondTable)).asInstanceOf[Except]
    val r2 = wt(Intersect(firstTable, secondTable)).asInstanceOf[Intersect]
    checkOutput(r1.left, expectedTypes)
    checkOutput(r1.right, expectedTypes)
    checkOutput(r2.left, expectedTypes)
    checkOutput(r2.right, expectedTypes)

    // Check if a Project is added
    assert(r1.left.isInstanceOf[Project])
    assert(r1.right.isInstanceOf[Project])
    assert(r2.left.isInstanceOf[Project])
    assert(r2.right.isInstanceOf[Project])
  }

  test("WidenSetOperationTypes for union") {
    val firstTable = LocalRelation(
      AttributeReference("i", IntegerType)(),
      AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("b", ByteType)(),
      AttributeReference("d", DoubleType)())
    val secondTable = LocalRelation(
      AttributeReference("s", StringType)(),
      AttributeReference("d", DecimalType(2, 1))(),
      AttributeReference("f", FloatType)(),
      AttributeReference("l", LongType)())
    val thirdTable = LocalRelation(
      AttributeReference("m", StringType)(),
      AttributeReference("n", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("p", FloatType)(),
      AttributeReference("q", DoubleType)())
    val forthTable = LocalRelation(
      AttributeReference("m", StringType)(),
      AttributeReference("n", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("p", ByteType)(),
      AttributeReference("q", DoubleType)())

    val wt = TypeCoercion.WidenSetOperationTypes
    val expectedTypes = Seq(StringType, DecimalType.SYSTEM_DEFAULT, FloatType, DoubleType)

    val unionRelation = wt(
      Union(firstTable :: secondTable :: thirdTable :: forthTable :: Nil)).asInstanceOf[Union]
    assert(unionRelation.children.length == 4)
    checkOutput(unionRelation.children.head, expectedTypes)
    checkOutput(unionRelation.children(1), expectedTypes)
    checkOutput(unionRelation.children(2), expectedTypes)
    checkOutput(unionRelation.children(3), expectedTypes)

    assert(unionRelation.children.head.isInstanceOf[Project])
    assert(unionRelation.children(1).isInstanceOf[Project])
    assert(unionRelation.children(2).isInstanceOf[Project])
    assert(unionRelation.children(3).isInstanceOf[Project])
  }

  test("Transform Decimal precision/scale for union except and intersect") {
    def checkOutput(logical: LogicalPlan, expectTypes: Seq[DataType]): Unit = {
      logical.output.zip(expectTypes).foreach { case (attr, dt) =>
        assert(attr.dataType === dt)
      }
    }

    val dp = TypeCoercion.WidenSetOperationTypes

    val left1 = LocalRelation(
      AttributeReference("l", DecimalType(10, 8))())
    val right1 = LocalRelation(
      AttributeReference("r", DecimalType(5, 5))())
    val expectedType1 = Seq(DecimalType(10, 8))

    val r1 = dp(Union(left1, right1)).asInstanceOf[Union]
    val r2 = dp(Except(left1, right1)).asInstanceOf[Except]
    val r3 = dp(Intersect(left1, right1)).asInstanceOf[Intersect]

    checkOutput(r1.children.head, expectedType1)
    checkOutput(r1.children.last, expectedType1)
    checkOutput(r2.left, expectedType1)
    checkOutput(r2.right, expectedType1)
    checkOutput(r3.left, expectedType1)
    checkOutput(r3.right, expectedType1)

    val plan1 = LocalRelation(AttributeReference("l", DecimalType(10, 5))())

    val rightTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
    val expectedTypes = Seq(DecimalType(10, 5), DecimalType(10, 5), DecimalType(15, 5),
      DecimalType(25, 5), DoubleType, DoubleType)

    rightTypes.zip(expectedTypes).foreach { case (rType, expectedType) =>
      val plan2 = LocalRelation(
        AttributeReference("r", rType)())

      val r1 = dp(Union(plan1, plan2)).asInstanceOf[Union]
      val r2 = dp(Except(plan1, plan2)).asInstanceOf[Except]
      val r3 = dp(Intersect(plan1, plan2)).asInstanceOf[Intersect]

      checkOutput(r1.children.last, Seq(expectedType))
      checkOutput(r2.right, Seq(expectedType))
      checkOutput(r3.right, Seq(expectedType))

      val r4 = dp(Union(plan2, plan1)).asInstanceOf[Union]
      val r5 = dp(Except(plan2, plan1)).asInstanceOf[Except]
      val r6 = dp(Intersect(plan2, plan1)).asInstanceOf[Intersect]

      checkOutput(r4.children.last, Seq(expectedType))
      checkOutput(r5.left, Seq(expectedType))
      checkOutput(r6.left, Seq(expectedType))
    }
  }

  test("rule for date/timestamp operations") {
    val dateTimeOperations = TypeCoercion.DateTimeOperations
    val date = Literal(new java.sql.Date(0L))
    val timestamp = Literal(new Timestamp(0L))
    val interval = Literal(new CalendarInterval(0, 0))
    val str = Literal("2015-01-01")

    ruleTest(dateTimeOperations, Add(date, interval), Cast(TimeAdd(date, interval), DateType))
    ruleTest(dateTimeOperations, Add(interval, date), Cast(TimeAdd(date, interval), DateType))
    ruleTest(dateTimeOperations, Add(timestamp, interval),
      Cast(TimeAdd(timestamp, interval), TimestampType))
    ruleTest(dateTimeOperations, Add(interval, timestamp),
      Cast(TimeAdd(timestamp, interval), TimestampType))
    ruleTest(dateTimeOperations, Add(str, interval), Cast(TimeAdd(str, interval), StringType))
    ruleTest(dateTimeOperations, Add(interval, str), Cast(TimeAdd(str, interval), StringType))

    ruleTest(dateTimeOperations, Subtract(date, interval), Cast(TimeSub(date, interval), DateType))
    ruleTest(dateTimeOperations, Subtract(timestamp, interval),
      Cast(TimeSub(timestamp, interval), TimestampType))
    ruleTest(dateTimeOperations, Subtract(str, interval), Cast(TimeSub(str, interval), StringType))

    // interval operations should not be effected
    ruleTest(dateTimeOperations, Add(interval, interval), Add(interval, interval))
    ruleTest(dateTimeOperations, Subtract(interval, interval), Subtract(interval, interval))
  }

  /**
   * There are rules that need to not fire before child expressions get resolved.
   * We use this test to make sure those rules do not fire early.
   */
  test("make sure rules do not fire early") {
    // InConversion
    val inConversion = TypeCoercion.InConversion
    ruleTest(inConversion,
      In(UnresolvedAttribute("a"), Seq(Literal(1))),
      In(UnresolvedAttribute("a"), Seq(Literal(1)))
    )
    ruleTest(inConversion,
      In(Literal("test"), Seq(UnresolvedAttribute("a"), Literal(1))),
      In(Literal("test"), Seq(UnresolvedAttribute("a"), Literal(1)))
    )
    ruleTest(inConversion,
      In(Literal("a"), Seq(Literal(1), Literal("b"))),
      In(Cast(Literal("a"), StringType),
        Seq(Cast(Literal(1), StringType), Cast(Literal("b"), StringType)))
    )
  }

  test("SPARK-15776 Divide expression's dataType should be casted to Double or Decimal " +
    "in aggregation function like sum") {
    val rules = Seq(FunctionArgumentConversion, Division)
    // Casts Integer to Double
    ruleTest(rules, sum(Divide(4, 3)), sum(Divide(Cast(4, DoubleType), Cast(3, DoubleType))))
    // Left expression is Double, right expression is Int. Another rule ImplicitTypeCasts will
    // cast the right expression to Double.
    ruleTest(rules, sum(Divide(4.0, 3)), sum(Divide(4.0, 3)))
    // Left expression is Int, right expression is Double
    ruleTest(rules, sum(Divide(4, 3.0)), sum(Divide(Cast(4, DoubleType), Cast(3.0, DoubleType))))
    // Casts Float to Double
    ruleTest(
      rules,
      sum(Divide(4.0f, 3)),
      sum(Divide(Cast(4.0f, DoubleType), Cast(3, DoubleType))))
    // Left expression is Decimal, right expression is Int. Another rule DecimalPrecision will cast
    // the right expression to Decimal.
    ruleTest(rules, sum(Divide(Decimal(4.0), 3)), sum(Divide(Decimal(4.0), 3)))
  }
}


object TypeCoercionSuite {

  case class AnyTypeUnaryExpression(child: Expression)
    extends UnaryExpression with ExpectsInputTypes with Unevaluable {
    override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)
    override def dataType: DataType = NullType
  }

  case class NumericTypeUnaryExpression(child: Expression)
    extends UnaryExpression with ExpectsInputTypes with Unevaluable {
    override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)
    override def dataType: DataType = NullType
  }

  case class AnyTypeBinaryOperator(left: Expression, right: Expression)
    extends BinaryOperator with Unevaluable {
    override def dataType: DataType = NullType
    override def inputType: AbstractDataType = AnyDataType
    override def symbol: String = "anytype"
  }

  case class NumericTypeBinaryOperator(left: Expression, right: Expression)
    extends BinaryOperator with Unevaluable {
    override def dataType: DataType = NullType
    override def inputType: AbstractDataType = NumericType
    override def symbol: String = "numerictype"
  }
}
