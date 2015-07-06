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

import org.apache.spark.sql.catalyst.plans.PlanTest

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LocalRelation, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

class HiveTypeCoercionSuite extends PlanTest {

  test("eligible implicit type cast") {
    def shouldCast(from: DataType, to: AbstractDataType, expected: DataType): Unit = {
      val got = HiveTypeCoercion.ImplicitTypeCasts.implicitCast(Literal.create(null, from), to)
      assert(got.map(_.dataType) == Option(expected),
        s"Failed to cast $from to $to")
    }

    shouldCast(NullType, NullType, NullType)
    shouldCast(NullType, IntegerType, IntegerType)
    shouldCast(NullType, DecimalType, DecimalType.Unlimited)

    // TODO: write the entire implicit cast table out for test cases.
    shouldCast(ByteType, IntegerType, IntegerType)
    shouldCast(IntegerType, IntegerType, IntegerType)
    shouldCast(IntegerType, LongType, LongType)
    shouldCast(IntegerType, DecimalType, DecimalType.Unlimited)
    shouldCast(LongType, IntegerType, IntegerType)
    shouldCast(LongType, DecimalType, DecimalType.Unlimited)

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

    shouldCast(
      DecimalType.Unlimited, TypeCollection(IntegerType, DecimalType), DecimalType.Unlimited)
    shouldCast(DecimalType(10, 2), TypeCollection(IntegerType, DecimalType), DecimalType(10, 2))
    shouldCast(DecimalType(10, 2), TypeCollection(DecimalType, IntegerType), DecimalType(10, 2))
    shouldCast(IntegerType, TypeCollection(DecimalType(10, 2), StringType), DecimalType(10, 2))
  }

  test("ineligible implicit type cast") {
    def shouldNotCast(from: DataType, to: AbstractDataType): Unit = {
      val got = HiveTypeCoercion.ImplicitTypeCasts.implicitCast(Literal.create(null, from), to)
      assert(got.isEmpty, s"Should not be able to cast $from to $to, but got $got")
    }

    shouldNotCast(IntegerType, DateType)
    shouldNotCast(IntegerType, TimestampType)
    shouldNotCast(LongType, DateType)
    shouldNotCast(LongType, TimestampType)
    shouldNotCast(DecimalType.Unlimited, DateType)
    shouldNotCast(DecimalType.Unlimited, TimestampType)

    shouldNotCast(IntegerType, TypeCollection(DateType, TimestampType))

    shouldNotCast(IntegerType, ArrayType)
    shouldNotCast(IntegerType, MapType)
    shouldNotCast(IntegerType, StructType)
  }

  test("tightest common bound for types") {
    def widenTest(t1: DataType, t2: DataType, tightestCommon: Option[DataType]) {
      var found = HiveTypeCoercion.findTightestCommonTypeOfTwo(t1, t2)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest common type for $t1 and $t2, found $found")
      // Test both directions to make sure the widening is symmetric.
      found = HiveTypeCoercion.findTightestCommonTypeOfTwo(t2, t1)
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

    // Casting up to unlimited-precision decimal
    widenTest(IntegerType, DecimalType.Unlimited, Some(DecimalType.Unlimited))
    widenTest(DoubleType, DecimalType.Unlimited, Some(DecimalType.Unlimited))
    widenTest(DecimalType(3, 2), DecimalType.Unlimited, Some(DecimalType.Unlimited))
    widenTest(DecimalType.Unlimited, IntegerType, Some(DecimalType.Unlimited))
    widenTest(DecimalType.Unlimited, DoubleType, Some(DecimalType.Unlimited))
    widenTest(DecimalType.Unlimited, DecimalType(3, 2), Some(DecimalType.Unlimited))

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
    val testRelation = LocalRelation(AttributeReference("a", IntegerType)())
    comparePlans(
      rule(Project(Seq(Alias(initial, "a")()), testRelation)),
      Project(Seq(Alias(transformed, "a")()), testRelation))
  }

  test("coalesce casts") {
    ruleTest(HiveTypeCoercion.FunctionArgumentConversion,
      Coalesce(Literal(1.0)
        :: Literal(1)
        :: Literal.create(1.0, FloatType)
        :: Nil),
      Coalesce(Cast(Literal(1.0), DoubleType)
        :: Cast(Literal(1), DoubleType)
        :: Cast(Literal.create(1.0, FloatType), DoubleType)
        :: Nil))
    ruleTest(HiveTypeCoercion.FunctionArgumentConversion,
      Coalesce(Literal(1L)
        :: Literal(1)
        :: Literal(new java.math.BigDecimal("1000000000000000000000"))
        :: Nil),
      Coalesce(Cast(Literal(1L), DecimalType())
        :: Cast(Literal(1), DecimalType())
        :: Cast(Literal(new java.math.BigDecimal("1000000000000000000000")), DecimalType())
        :: Nil))
  }

  test("type coercion for If") {
    val rule = HiveTypeCoercion.IfCoercion
    ruleTest(rule,
      If(Literal(true), Literal(1), Literal(1L)),
      If(Literal(true), Cast(Literal(1), LongType), Literal(1L))
    )

    ruleTest(rule,
      If(Literal.create(null, NullType), Literal(1), Literal(1)),
      If(Literal.create(null, BooleanType), Literal(1), Literal(1))
    )
  }

  test("type coercion for CaseKeyWhen") {
    ruleTest(HiveTypeCoercion.CaseWhenCoercion,
      CaseKeyWhen(Literal(1.toShort), Seq(Literal(1), Literal("a"))),
      CaseKeyWhen(Cast(Literal(1.toShort), IntegerType), Seq(Literal(1), Literal("a")))
    )
    ruleTest(HiveTypeCoercion.CaseWhenCoercion,
      CaseKeyWhen(Literal(true), Seq(Literal(1), Literal("a"))),
      CaseKeyWhen(Literal(true), Seq(Literal(1), Literal("a")))
    )
  }

  test("type coercion simplification for equal to") {
    val be = HiveTypeCoercion.BooleanEquality

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
}
