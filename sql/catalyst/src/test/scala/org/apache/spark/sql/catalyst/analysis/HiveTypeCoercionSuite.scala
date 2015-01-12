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
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types._

class HiveTypeCoercionSuite extends PlanTest {

  test("tightest common bound for types") {
    def widenTest(t1: DataType, t2: DataType, tightestCommon: Option[DataType]) {
      var found = HiveTypeCoercion.findTightestCommonType(t1, t2)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest common type for $t1 and $t2, found $found")
      // Test both directions to make sure the widening is symmetric.
      found = HiveTypeCoercion.findTightestCommonType(t2, t1)
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
    widenTest(NullType, MapType(IntegerType, StringType, false),
      Some(MapType(IntegerType, StringType, false)))
    widenTest(NullType, StructType(Seq()), Some(StructType(Seq())))
    widenTest(StringType, MapType(IntegerType, StringType, true), None)
    widenTest(ArrayType(IntegerType), StructType(Seq()), None)
  }

  test("tightest bound for union types") {
    val widenTypes = new HiveTypeCoercion { }.WidenTypes
    def unionTypeTest(t1: DataType, t2: DataType, tightestCommon: Option[DataType]) {
      var found = widenTypes.findUnionType(t1, t2)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest union type for $t1 and $t2, found $found")
      // Test both directions to make sure the widening is symmetric.
      found = widenTypes.findUnionType(t2, t1)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest union type for $t2 and $t1, found $found")
    }
    // Null
    unionTypeTest(NullType, NullType, Some(NullType))

    // Boolean
    unionTypeTest(NullType, BooleanType, Some(BooleanType))
    unionTypeTest(BooleanType, BooleanType, Some(BooleanType))
    unionTypeTest(IntegerType, BooleanType, None)
    unionTypeTest(LongType, BooleanType, None)

    // Integral
    unionTypeTest(NullType, ByteType, Some(ByteType))
    unionTypeTest(NullType, IntegerType, Some(IntegerType))
    unionTypeTest(NullType, LongType, Some(LongType))
    unionTypeTest(ShortType, IntegerType, Some(IntegerType))
    unionTypeTest(ShortType, LongType, Some(LongType))
    unionTypeTest(IntegerType, LongType, Some(LongType))
    unionTypeTest(LongType, LongType, Some(LongType))

    // Floating point
    unionTypeTest(NullType, FloatType, Some(FloatType))
    unionTypeTest(NullType, DoubleType, Some(DoubleType))
    unionTypeTest(FloatType, DoubleType, Some(DoubleType))
    unionTypeTest(FloatType, FloatType, Some(FloatType))
    unionTypeTest(DoubleType, DoubleType, Some(DoubleType))

    // Integral mixed with floating point.
    unionTypeTest(IntegerType, FloatType, Some(FloatType))
    unionTypeTest(IntegerType, DoubleType, Some(DoubleType))
    unionTypeTest(IntegerType, DoubleType, Some(DoubleType))
    unionTypeTest(LongType, FloatType, Some(FloatType))
    unionTypeTest(LongType, DoubleType, Some(DoubleType))

    // Casting up to unlimited-precision decimal
    unionTypeTest(IntegerType, DecimalType.Unlimited, Some(DecimalType.Unlimited))
    unionTypeTest(DoubleType, DecimalType.Unlimited, Some(DecimalType.Unlimited))
    unionTypeTest(DecimalType(3, 2), DecimalType.Unlimited, Some(DecimalType.Unlimited))
    unionTypeTest(DecimalType.Unlimited, IntegerType, Some(DecimalType.Unlimited))
    unionTypeTest(DecimalType.Unlimited, DoubleType, Some(DecimalType.Unlimited))
    unionTypeTest(DecimalType.Unlimited, DecimalType(3, 2), Some(DecimalType.Unlimited))

    // Casting up for fixed-precision decimal
    unionTypeTest(DecimalType(2, 1), DecimalType(3, 2), Some(DecimalType(3, 2)))
    unionTypeTest(DecimalType(2, 1), DoubleType, Some(DoubleType))
    unionTypeTest(DecimalType(2, 1), IntegerType, Some(DecimalType(10, 1)))
    unionTypeTest(DoubleType, DecimalType(2, 1), Some(DoubleType))
    unionTypeTest(ShortType, DecimalType(2, 1), Some(DecimalType(5, 1)))

    // StringType
    unionTypeTest(NullType, StringType, Some(StringType))
    unionTypeTest(StringType, StringType, Some(StringType))
    unionTypeTest(IntegerType, StringType, Some(StringType))
    unionTypeTest(LongType, StringType, Some(StringType))

    // TimestampType
    unionTypeTest(NullType, TimestampType, Some(TimestampType))
    unionTypeTest(TimestampType, TimestampType, Some(TimestampType))
    unionTypeTest(IntegerType, TimestampType, None)
    unionTypeTest(StringType, TimestampType, Some(StringType))

    // ComplexType
    unionTypeTest(NullType, MapType(IntegerType, StringType, false),
      Some(MapType(IntegerType, StringType, false)))
    unionTypeTest(NullType, StructType(Seq()), Some(StructType(Seq())))
    unionTypeTest(StringType, MapType(IntegerType, StringType, true), Some(StringType))
    unionTypeTest(ArrayType(IntegerType), StructType(Seq()), None)
  }

  test("boolean casts") {
    val booleanCasts = new HiveTypeCoercion { }.BooleanCasts
    def ruleTest(initial: Expression, transformed: Expression) {
      val testRelation = LocalRelation(AttributeReference("a", IntegerType)())
      comparePlans(
        booleanCasts(Project(Seq(Alias(initial, "a")()), testRelation)),
        Project(Seq(Alias(transformed, "a")()), testRelation))
    }
    // Remove superflous boolean -> boolean casts.
    ruleTest(Cast(Literal(true), BooleanType), Literal(true))
    // Stringify boolean when casting to string.
    ruleTest(Cast(Literal(false), StringType), If(Literal(false), Literal("true"), Literal("false")))
  }

  test("coalesce casts") {
    val fac = new HiveTypeCoercion { }.FunctionArgumentConversion
    def ruleTest(initial: Expression, transformed: Expression) {
      val testRelation = LocalRelation(AttributeReference("a", IntegerType)())
      comparePlans(
        fac(Project(Seq(Alias(initial, "a")()), testRelation)),
        Project(Seq(Alias(transformed, "a")()), testRelation))
    }
    ruleTest(
      Coalesce(Literal(1.0)
        :: Literal(1)
        :: Literal.create(1.0, FloatType)
        :: Nil),
      Coalesce(Cast(Literal(1.0), DoubleType)
        :: Cast(Literal(1), DoubleType)
        :: Cast(Literal.create(1.0, FloatType), DoubleType)
        :: Nil))
    ruleTest(
      Coalesce(Literal(1L)
        :: Literal(1)
        :: Literal(new java.math.BigDecimal("1000000000000000000000"))
        :: Nil),
      Coalesce(Cast(Literal(1L), DecimalType())
        :: Cast(Literal(1), DecimalType())
        :: Cast(Literal(new java.math.BigDecimal("1000000000000000000000")), DecimalType())
        :: Nil))
  }
}
