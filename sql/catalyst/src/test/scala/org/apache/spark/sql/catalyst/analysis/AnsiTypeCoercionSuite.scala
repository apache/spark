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

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class AnsiTypeCoercionSuite extends AnalysisTest {
  import TypeCoercionSuite._

  // When Utils.isTesting is true, RuleIdCollection adds individual type coercion rules. Otherwise,
  // RuleIdCollection doesn't add them because they are called in a train inside
  // CombinedTypeCoercionRule.
  assert(Utils.isTesting, s"${IS_TESTING.key} is not set to true")

  // scalastyle:off line.size.limit
  // The following table shows all implicit data type conversions that are not visible to the user.
  // +----------------------+----------+-----------+-------------+----------+------------+------------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
  // | Source Type\CAST TO  | ByteType | ShortType | IntegerType | LongType | FloatType  | DoubleType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  | NullType | CalendarIntervalType |     DecimalType     | NumericType | IntegralType |
  // +----------------------+----------+-----------+-------------+----------+------------+------------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
  // | ByteType             | ByteType | ShortType | IntegerType | LongType | DoubleType | DoubleType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(3, 0)   | ByteType    | ByteType     |
  // | ShortType            | X        | ShortType | IntegerType | LongType | DoubleType | DoubleType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(5, 0)   | ShortType   | ShortType    |
  // | IntegerType          | X        | X         | IntegerType | LongType | DoubleType | DoubleType | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(10, 0)  | IntegerType | IntegerType  |
  // | LongType             | X        | X         | X           | LongType | DoubleType | DoubleType | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(20, 0)  | LongType    | LongType     |
  // | FloatType            | X        | X         | X           | X        | FloatType  | DoubleType | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(30, 15) | DoubleType  | X            |
  // | DoubleType           | X        | X         | X           | X        | X          | DoubleType | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(14, 7)  | FloatType   | X            |
  // | Dec(10, 2)           | X        | X         | X           | X        | DoubleType | DoubleType | Dec(10, 2) | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | DecimalType(10, 2)  | Dec(10, 2)  | X            |
  // | BinaryType           | X        | X         | X           | X        | X          | X          | X          | BinaryType | X           | X          | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | BooleanType          | X        | X         | X           | X        | X          | X          | X          | X          | BooleanType | X          | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | StringType           | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | DateType             | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | DateType | TimestampType | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | TimestampType        | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | X        | TimestampType | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | ArrayType            | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | X        | X             | ArrayType* | X        | X           | X        | X                    | X                   | X           | X            |
  // | MapType              | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | X        | X             | X          | MapType* | X           | X        | X                    | X                   | X           | X            |
  // | StructType           | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | X        | X             | X          | X        | StructType* | X        | X                    | X                   | X           | X            |
  // | NullType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType  | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  | NullType | CalendarIntervalType | DecimalType(38, 18) | DoubleType  | IntegerType  |
  // | CalendarIntervalType | X        | X         | X           | X        | X          | X          | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | CalendarIntervalType | X                   | X           | X            |
  // +----------------------+----------+-----------+-------------+----------+------------+------------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
  // Note: StructType* is castable when all the internal child types are castable according to the table.
  // Note: ArrayType* is castable when the element type is castable according to the table.
  // Note: MapType* is castable when both the key type and the value type are castable according to the table.
  // scalastyle:on line.size.limit

  private def shouldCast(from: DataType, to: AbstractDataType, expected: DataType): Unit = {
    // Check default value
    val castDefault = AnsiTypeCoercion.implicitCast(default(from), to)
    assert(DataType.equalsIgnoreCompatibleNullability(
      castDefault.map(_.dataType).getOrElse(null), expected),
      s"Failed to cast $from to $to")

    // Check null value
    val castNull = AnsiTypeCoercion.implicitCast(createNull(from), to)
    assert(DataType.equalsIgnoreCaseAndNullability(
      castNull.map(_.dataType).getOrElse(null), expected),
      s"Failed to cast $from to $to")
  }

  private def shouldNotCast(from: DataType, to: AbstractDataType): Unit = {
    // Check default value
    val castDefault = AnsiTypeCoercion.implicitCast(default(from), to)
    assert(castDefault.isEmpty, s"Should not be able to cast $from to $to, but got $castDefault")

    // Check null value
    val castNull = AnsiTypeCoercion.implicitCast(createNull(from), to)
    assert(castNull.isEmpty, s"Should not be able to cast $from to $to, but got $castNull")
  }

  private def shouldCastStringLiteral(to: AbstractDataType, expected: DataType): Unit = {
    val input = Literal("123")
    val castResult = AnsiTypeCoercion.implicitCast(input, to)
    assert(DataType.equalsIgnoreCaseAndNullability(
      castResult.map(_.dataType).getOrElse(null), expected),
      s"Failed to cast String literal to $to")
  }

  private def shouldNotCastStringLiteral(to: AbstractDataType): Unit = {
    val input = Literal("123")
    val castResult = AnsiTypeCoercion.implicitCast(input, to)
    assert(castResult.isEmpty, s"Should not be able to cast String literal to $to")
  }

  private def shouldNotCastStringInput(to: AbstractDataType): Unit = {
    val input = AttributeReference("s", StringType)()
    val castResult = AnsiTypeCoercion.implicitCast(input, to)
    assert(castResult.isEmpty, s"Should not be able to cast non-foldable String input to $to")
  }

  private def default(dataType: DataType): Expression = dataType match {
    case ArrayType(internalType: DataType, _) =>
      CreateArray(Seq(Literal.default(internalType)))
    case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
      CreateMap(Seq(Literal.default(keyDataType), Literal.default(valueDataType)))
    case _ => Literal.default(dataType)
  }

  private def createNull(dataType: DataType): Expression = dataType match {
    case ArrayType(internalType: DataType, _) =>
      CreateArray(Seq(Literal.create(null, internalType)))
    case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
      CreateMap(Seq(Literal.create(null, keyDataType), Literal.create(null, valueDataType)))
    case _ => Literal.create(null, dataType)
  }

  // Check whether the type `checkedType` can be cast to all the types in `castableTypes`,
  // but cannot be cast to the other types in `allTypes`.
  private def checkTypeCasting(checkedType: DataType, castableTypes: Seq[DataType]): Unit = {
    val nonCastableTypes = allTypes.filterNot(castableTypes.contains)

    castableTypes.foreach { tpe =>
      shouldCast(checkedType, tpe, tpe)
    }
    nonCastableTypes.foreach { tpe =>
      shouldNotCast(checkedType, tpe)
    }
  }

  private def checkWidenType(
      widenFunc: (DataType, DataType) => Option[DataType],
      t1: DataType,
      t2: DataType,
      expected: Option[DataType],
      isSymmetric: Boolean = true): Unit = {
    var found = widenFunc(t1, t2)
    assert(found == expected,
      s"Expected $expected as wider common type for $t1 and $t2, found $found")
    // Test both directions to make sure the widening is symmetric.
    if (isSymmetric) {
      found = widenFunc(t2, t1)
      assert(found == expected,
        s"Expected $expected as wider common type for $t2 and $t1, found $found")
    }
  }

  test("implicit type cast - ByteType") {
    val checkedType = ByteType
    checkTypeCasting(checkedType, castableTypes = numericTypes)
    shouldCast(checkedType, DecimalType, DecimalType.ByteDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - ShortType") {
    val checkedType = ShortType
    checkTypeCasting(checkedType, castableTypes = numericTypes.filterNot(_ == ByteType))
    shouldCast(checkedType, DecimalType, DecimalType.ShortDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - IntegerType") {
    val checkedType = IntegerType
    checkTypeCasting(checkedType, castableTypes =
      Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType.SYSTEM_DEFAULT))
    shouldCast(IntegerType, DecimalType, DecimalType.IntDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - LongType") {
    val checkedType = LongType
    checkTypeCasting(checkedType, castableTypes =
      Seq(LongType, FloatType, DoubleType, DecimalType.SYSTEM_DEFAULT))
    shouldCast(checkedType, DecimalType, DecimalType.LongDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - FloatType") {
    val checkedType = FloatType
    checkTypeCasting(checkedType, castableTypes = Seq(FloatType, DoubleType))
    shouldCast(checkedType, DecimalType, DecimalType.FloatDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - DoubleType") {
    val checkedType = DoubleType
    checkTypeCasting(checkedType, castableTypes = Seq(DoubleType))
    shouldCast(checkedType, DecimalType, DecimalType.DoubleDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - DecimalType(10, 2)") {
    val checkedType = DecimalType(10, 2)
    checkTypeCasting(checkedType, castableTypes = fractionalTypes)
    shouldCast(checkedType, DecimalType, checkedType)
    shouldCast(checkedType, NumericType, checkedType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - BinaryType") {
    val checkedType = BinaryType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - BooleanType") {
    val checkedType = BooleanType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
    shouldNotCast(checkedType, StringType)
  }

  test("implicit type cast - unfoldable StringType") {
    val nonCastableTypes = allTypes.filterNot(_ == StringType)
    nonCastableTypes.foreach { dt =>
      shouldNotCastStringInput(dt)
    }
    shouldNotCastStringInput(DecimalType)
    shouldNotCastStringInput(NumericType)
  }

  test("implicit type cast - foldable StringType") {
    atomicTypes.foreach { dt =>
      shouldCastStringLiteral(dt, dt)
    }
    allTypes.filterNot(atomicTypes.contains).foreach { dt =>
      shouldNotCastStringLiteral(dt)
    }
    shouldCastStringLiteral(DecimalType, DecimalType.defaultConcreteType)
    shouldCastStringLiteral(NumericType, DoubleType)
  }

  test("implicit type cast - DateType") {
    val checkedType = DateType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType, TimestampType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
    shouldNotCast(checkedType, StringType)
  }

  test("implicit type cast - TimestampType") {
    val checkedType = TimestampType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - unfoldable ArrayType(StringType)") {
    val input = AttributeReference("a", ArrayType(StringType))()
    val nonCastableTypes = allTypes.filterNot(_ == StringType)
    nonCastableTypes.map(ArrayType(_)).foreach { dt =>
      assert(AnsiTypeCoercion.implicitCast(input, dt).isEmpty)
    }
    assert(AnsiTypeCoercion.implicitCast(input, DecimalType).isEmpty)
    assert(AnsiTypeCoercion.implicitCast(input, NumericType).isEmpty)
  }

  test("implicit type cast - foldable arrayType(StringType)") {
    val input = Literal(Array("1"))
    assert(AnsiTypeCoercion.implicitCast(input, ArrayType(StringType)) == Some(input))
    (numericTypes ++ datetimeTypes ++ Seq(BinaryType)).foreach { dt =>
      assert(AnsiTypeCoercion.implicitCast(input, ArrayType(dt)) ==
        Some(Cast(input, ArrayType(dt))))
    }
  }

  test("implicit type cast between two Map types") {
    val sourceType = MapType(IntegerType, IntegerType, true)
    val castableTypes =
      Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType.SYSTEM_DEFAULT)
    val targetTypes = castableTypes.map { t =>
      MapType(t, sourceType.valueType, valueContainsNull = true)
    }
    val nonCastableTargetTypes = allTypes.filterNot(castableTypes.contains(_)).map {t =>
      MapType(t, sourceType.valueType, valueContainsNull = true)
    }

    // Tests that its possible to setup implicit casts between two map types when
    // source map's key type is integer and the target map's key type are either Byte, Short,
    // Long, Double, Float, Decimal(38, 18) or String.
    targetTypes.foreach { targetType =>
      shouldCast(sourceType, targetType, targetType)
    }

    // Tests that its not possible to setup implicit casts between two map types when
    // source map's key type is integer and the target map's key type are either Binary,
    // Boolean, Date, Timestamp, Array, Struct, CalendarIntervalType or NullType
    nonCastableTargetTypes.foreach { targetType =>
      shouldNotCast(sourceType, targetType)
    }

    // Tests that its not possible to cast from nullable map type to not nullable map type.
    val targetNotNullableTypes = allTypes.filterNot(_ == IntegerType).map { t =>
      MapType(t, sourceType.valueType, valueContainsNull = false)
    }
    val sourceMapExprWithValueNull =
      CreateMap(Seq(Literal.default(sourceType.keyType),
        Literal.create(null, sourceType.valueType)))
    targetNotNullableTypes.foreach { targetType =>
      val castDefault =
        AnsiTypeCoercion.implicitCast(sourceMapExprWithValueNull, targetType)
      assert(castDefault.isEmpty,
        s"Should not be able to cast $sourceType to $targetType, but got $castDefault")
    }
  }

  test("implicit type cast - StructType().add(\"a1\", StringType)") {
    val checkedType = new StructType().add("a1", StringType)
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - NullType") {
    val checkedType = NullType
    checkTypeCasting(checkedType, castableTypes = allTypes)
    shouldCast(checkedType, DecimalType, DecimalType.SYSTEM_DEFAULT)
    shouldCast(checkedType, NumericType, NumericType.defaultConcreteType)
    shouldCast(checkedType, IntegralType, IntegralType.defaultConcreteType)
  }

  test("implicit type cast - CalendarIntervalType") {
    val checkedType = CalendarIntervalType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("eligible implicit type cast - TypeCollection") {
    shouldCast(StringType, TypeCollection(StringType, BinaryType), StringType)
    shouldCast(BinaryType, TypeCollection(StringType, BinaryType), BinaryType)
    shouldCast(StringType, TypeCollection(BinaryType, StringType), StringType)

    shouldCast(IntegerType, TypeCollection(IntegerType, BinaryType), IntegerType)
    shouldCast(IntegerType, TypeCollection(BinaryType, IntegerType), IntegerType)
    shouldCast(BinaryType, TypeCollection(BinaryType, IntegerType), BinaryType)
    shouldCast(BinaryType, TypeCollection(IntegerType, BinaryType), BinaryType)

    shouldCast(DecimalType.SYSTEM_DEFAULT,
      TypeCollection(IntegerType, DecimalType), DecimalType.SYSTEM_DEFAULT)
    shouldCast(DecimalType(10, 2), TypeCollection(IntegerType, DecimalType), DecimalType(10, 2))
    shouldCast(DecimalType(10, 2), TypeCollection(DecimalType, IntegerType), DecimalType(10, 2))

    shouldCast(
      ArrayType(StringType, false),
      TypeCollection(ArrayType(StringType), StringType),
      ArrayType(StringType, false))

    shouldCast(
      ArrayType(StringType, true),
      TypeCollection(ArrayType(StringType), StringType),
      ArrayType(StringType, true))

    // When there are multiple convertible types in the `TypeCollection`, use the closest
    // convertible data type among convertible types.
    shouldCast(IntegerType, TypeCollection(BinaryType, FloatType, LongType), LongType)
    shouldCast(ShortType, TypeCollection(BinaryType, LongType, IntegerType), IntegerType)
    shouldCast(ShortType, TypeCollection(DateType, LongType, IntegerType, DoubleType), IntegerType)
    // If the result is Float type and Double type is also among the convertible target types,
    // use Double Type instead of Float type.
    shouldCast(LongType, TypeCollection(FloatType, DoubleType, StringType), DoubleType)
  }

  test("ineligible implicit type cast - TypeCollection") {
    shouldNotCast(IntegerType, TypeCollection(StringType, BinaryType))
    shouldNotCast(IntegerType, TypeCollection(BinaryType, StringType))
    shouldNotCast(IntegerType, TypeCollection(DateType, TimestampType))
    shouldNotCast(IntegerType, TypeCollection(DecimalType(10, 2), StringType))
    shouldNotCastStringInput(TypeCollection(NumericType, BinaryType))
    // When there are multiple convertible types in the `TypeCollection` and there is no such
    // a data type that can be implicit cast to all the other convertible types in the collection.
    Seq(TypeCollection(NumericType, BinaryType),
      TypeCollection(NumericType, DecimalType, BinaryType),
      TypeCollection(IntegerType, LongType, BooleanType),
      TypeCollection(DateType, TimestampType, BooleanType)).foreach { typeCollection =>
      shouldNotCastStringLiteral(typeCollection)
      shouldNotCast(NullType, typeCollection)
    }
  }

  test("tightest common bound for types") {
    def widenTest(t1: DataType, t2: DataType, expected: Option[DataType]): Unit =
      checkWidenType(AnsiTypeCoercion.findTightestCommonType, t1, t2, expected)

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
    widenTest(IntegerType, FloatType, Some(DoubleType))
    widenTest(IntegerType, DoubleType, Some(DoubleType))
    widenTest(IntegerType, DoubleType, Some(DoubleType))
    widenTest(LongType, FloatType, Some(DoubleType))
    widenTest(LongType, DoubleType, Some(DoubleType))

    widenTest(DecimalType(2, 1), DecimalType(3, 2), None)
    widenTest(DecimalType(2, 1), DoubleType, None)
    widenTest(DecimalType(2, 1), IntegerType, None)
    widenTest(DoubleType, DecimalType(2, 1), None)

    // StringType
    widenTest(NullType, StringType, Some(StringType))
    widenTest(StringType, StringType, Some(StringType))
    widenTest(IntegerType, StringType, None)
    widenTest(LongType, StringType, None)

    // TimestampType
    widenTest(NullType, TimestampType, Some(TimestampType))
    widenTest(TimestampType, TimestampType, Some(TimestampType))
    widenTest(DateType, TimestampType, Some(TimestampType))
    widenTest(IntegerType, TimestampType, None)
    widenTest(StringType, TimestampType, None)

    // ComplexType
    widenTest(NullType,
      MapType(IntegerType, StringType, false),
      Some(MapType(IntegerType, StringType, false)))
    widenTest(NullType, StructType(Seq()), Some(StructType(Seq())))
    widenTest(StringType, MapType(IntegerType, StringType, true), None)
    widenTest(ArrayType(IntegerType), StructType(Seq()), None)

    widenTest(
      StructType(Seq(StructField("a", IntegerType))),
      StructType(Seq(StructField("b", IntegerType))),
      None)
    widenTest(
      StructType(Seq(StructField("a", IntegerType, nullable = false))),
      StructType(Seq(StructField("a", DoubleType, nullable = false))),
      Some(StructType(Seq(StructField("a", DoubleType, nullable = false)))))

    widenTest(
      StructType(Seq(StructField("a", IntegerType, nullable = false))),
      StructType(Seq(StructField("a", IntegerType, nullable = false))),
      Some(StructType(Seq(StructField("a", IntegerType, nullable = false)))))
    widenTest(
      StructType(Seq(StructField("a", IntegerType, nullable = false))),
      StructType(Seq(StructField("a", IntegerType, nullable = true))),
      Some(StructType(Seq(StructField("a", IntegerType, nullable = true)))))
    widenTest(
      StructType(Seq(StructField("a", IntegerType, nullable = true))),
      StructType(Seq(StructField("a", IntegerType, nullable = false))),
      Some(StructType(Seq(StructField("a", IntegerType, nullable = true)))))
    widenTest(
      StructType(Seq(StructField("a", IntegerType, nullable = true))),
      StructType(Seq(StructField("a", IntegerType, nullable = true))),
      Some(StructType(Seq(StructField("a", IntegerType, nullable = true)))))

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      widenTest(
        StructType(Seq(StructField("a", IntegerType))),
        StructType(Seq(StructField("A", IntegerType))),
        None)
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkWidenType(
        AnsiTypeCoercion.findTightestCommonType,
        StructType(Seq(StructField("a", IntegerType), StructField("B", IntegerType))),
        StructType(Seq(StructField("A", IntegerType), StructField("b", IntegerType))),
        Some(StructType(Seq(StructField("a", IntegerType), StructField("B", IntegerType)))),
        isSymmetric = false)
    }

    widenTest(
      ArrayType(IntegerType, containsNull = true),
      ArrayType(IntegerType, containsNull = false),
      Some(ArrayType(IntegerType, containsNull = true)))

    widenTest(
      ArrayType(NullType, containsNull = true),
      ArrayType(IntegerType, containsNull = false),
      Some(ArrayType(IntegerType, containsNull = true)))

    widenTest(
      MapType(IntegerType, StringType, valueContainsNull = true),
      MapType(IntegerType, StringType, valueContainsNull = false),
      Some(MapType(IntegerType, StringType, valueContainsNull = true)))

    widenTest(
      MapType(NullType, NullType, true),
      MapType(IntegerType, StringType, false),
      Some(MapType(IntegerType, StringType, true)))

    widenTest(
      new StructType()
        .add("arr", ArrayType(IntegerType, containsNull = true), nullable = false),
      new StructType()
        .add("arr", ArrayType(IntegerType, containsNull = false), nullable = true),
      Some(new StructType()
        .add("arr", ArrayType(IntegerType, containsNull = true), nullable = true)))

    widenTest(
      new StructType()
        .add("null", NullType, nullable = true),
      new StructType()
        .add("null", IntegerType, nullable = false),
      Some(new StructType()
        .add("null", IntegerType, nullable = true)))

    widenTest(
      ArrayType(NullType, containsNull = false),
      ArrayType(IntegerType, containsNull = false),
      Some(ArrayType(IntegerType, containsNull = false)))

    widenTest(MapType(NullType, NullType, false),
      MapType(IntegerType, StringType, false),
      Some(MapType(IntegerType, StringType, false)))

    widenTest(
      new StructType()
        .add("null", NullType, nullable = false),
      new StructType()
        .add("null", IntegerType, nullable = false),
      Some(new StructType()
        .add("null", IntegerType, nullable = false)))
  }

  test("wider common type for decimal and array") {
    def widenTestWithoutStringPromotion(
        t1: DataType,
        t2: DataType,
        expected: Option[DataType],
        isSymmetric: Boolean = true): Unit = {
      checkWidenType(
        AnsiTypeCoercion.findWiderTypeWithoutStringPromotionForTwo, t1, t2, expected, isSymmetric)
    }

    widenTestWithoutStringPromotion(
      new StructType().add("num", IntegerType),
      new StructType().add("num", LongType).add("str", StringType),
      None)

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      widenTestWithoutStringPromotion(
        new StructType().add("a", IntegerType),
        new StructType().add("A", LongType),
        Some(new StructType().add("a", LongType)),
        isSymmetric = false)
    }

    // Without string promotion
    widenTestWithoutStringPromotion(IntegerType, StringType, None)
    widenTestWithoutStringPromotion(StringType, TimestampType, None)
    widenTestWithoutStringPromotion(ArrayType(LongType), ArrayType(StringType), None)
    widenTestWithoutStringPromotion(ArrayType(StringType), ArrayType(TimestampType), None)
    widenTestWithoutStringPromotion(
      MapType(LongType, IntegerType), MapType(StringType, IntegerType), None)
    widenTestWithoutStringPromotion(
      MapType(IntegerType, LongType), MapType(IntegerType, StringType), None)
    widenTestWithoutStringPromotion(
      MapType(StringType, IntegerType), MapType(TimestampType, IntegerType), None)
    widenTestWithoutStringPromotion(
      MapType(IntegerType, StringType), MapType(IntegerType, TimestampType), None)
    widenTestWithoutStringPromotion(
      new StructType().add("a", IntegerType),
      new StructType().add("a", StringType),
      None)
    widenTestWithoutStringPromotion(
      new StructType().add("a", StringType),
      new StructType().add("a", IntegerType),
      None)
  }

  private def ruleTest(rule: Rule[LogicalPlan],
      initial: Expression, transformed: Expression): Unit = {
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
    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      AnyTypeUnaryExpression(Literal.create(null, NullType)),
      AnyTypeUnaryExpression(Literal.create(null, NullType)))

    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      NumericTypeUnaryExpression(Literal.create(null, NullType)),
      NumericTypeUnaryExpression(Literal.create(null, DoubleType)))
  }

  test("cast NullType for binary operators") {
    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      AnyTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)),
      AnyTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)))

    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      NumericTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)),
      NumericTypeBinaryOperator(Literal.create(null, DoubleType), Literal.create(null, DoubleType)))
  }

  test("coalesce casts") {
    val rule = AnsiTypeCoercion.FunctionArgumentConversion

    val intLit = Literal(1)
    val longLit = Literal.create(1L)
    val doubleLit = Literal(1.0)
    val stringLit = Literal.create("c", StringType)
    val nullLit = Literal.create(null, NullType)
    val floatNullLit = Literal.create(null, FloatType)
    val floatLit = Literal.create(1.0f, FloatType)
    val doubleNullLit = Cast(floatNullLit, DoubleType)
    val timestampLit = Literal.create(Timestamp.valueOf("2017-04-12 00:00:00"), TimestampType)
    val decimalLit = Literal(new java.math.BigDecimal("1000000000000000000000"))
    val tsArrayLit = Literal(Array(new Timestamp(System.currentTimeMillis())))
    val strArrayLit = Literal(Array("c"))
    val intArrayLit = Literal(Array(1))

    ruleTest(rule,
      Coalesce(Seq(doubleLit, intLit, floatLit)),
      Coalesce(Seq(doubleLit, Cast(intLit, DoubleType), Cast(floatLit, DoubleType))))

    ruleTest(rule,
      Coalesce(Seq(longLit, intLit, decimalLit)),
      Coalesce(Seq(Cast(longLit, DecimalType(22, 0)),
        Cast(intLit, DecimalType(22, 0)), decimalLit)))

    ruleTest(rule,
      Coalesce(Seq(nullLit, intLit)),
      Coalesce(Seq(Cast(nullLit, IntegerType), intLit)))

    ruleTest(rule,
      Coalesce(Seq(timestampLit, stringLit)),
      Coalesce(Seq(timestampLit, stringLit)))

    ruleTest(rule,
      Coalesce(Seq(nullLit, floatNullLit, intLit)),
      Coalesce(Seq(Cast(nullLit, DoubleType), doubleNullLit, Cast(intLit, DoubleType))))

    ruleTest(rule,
      Coalesce(Seq(nullLit, intLit, decimalLit, doubleLit)),
      Coalesce(Seq(Cast(nullLit, DoubleType), Cast(intLit, DoubleType),
        Cast(decimalLit, DoubleType), doubleLit)))

    // There is no a common type among Float/Double/String
    ruleTest(rule,
      Coalesce(Seq(nullLit, floatNullLit, doubleLit, stringLit)),
      Coalesce(Seq(nullLit, floatNullLit, doubleLit, stringLit)))

    // There is no a common type among Timestamp/Int/String
    ruleTest(rule,
      Coalesce(Seq(timestampLit, intLit, stringLit)),
      Coalesce(Seq(timestampLit, intLit, stringLit)))

    ruleTest(rule,
      Coalesce(Seq(tsArrayLit, intArrayLit, strArrayLit)),
      Coalesce(Seq(tsArrayLit, intArrayLit, strArrayLit)))
  }

  test("CreateArray casts") {
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal.create(1.0f, FloatType)
        :: Nil),
      CreateArray(Literal(1.0)
        :: Cast(Literal(1), DoubleType)
        :: Cast(Literal.create(1.0f, FloatType), DoubleType)
        :: Nil))

    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal("a")
        :: Nil),
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal("a")
        :: Nil))

    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal.create(null, DecimalType(5, 3))
        :: Literal(1)
        :: Nil),
      CreateArray(Literal.create(null, DecimalType(5, 3)).cast(DecimalType(13, 3))
        :: Literal(1).cast(DecimalType(13, 3))
        :: Nil))

    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal.create(null, DecimalType(5, 3))
        :: Literal.create(null, DecimalType(22, 10))
        :: Literal.create(null, DecimalType(38, 38))
        :: Nil),
      CreateArray(Literal.create(null, DecimalType(5, 3)).cast(DecimalType(38, 38))
        :: Literal.create(null, DecimalType(22, 10)).cast(DecimalType(38, 38))
        :: Literal.create(null, DecimalType(38, 38))
        :: Nil))
  }

  test("CreateMap casts") {
    // type coercion for map keys
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal.create(2.0f, FloatType)
        :: Literal("b")
        :: Nil),
      CreateMap(Cast(Literal(1), DoubleType)
        :: Literal("a")
        :: Cast(Literal.create(2.0f, FloatType), DoubleType)
        :: Literal("b")
        :: Nil))
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal.create(null, DecimalType(5, 3))
        :: Literal("a")
        :: Literal.create(2.0f, FloatType)
        :: Literal("b")
        :: Nil),
      CreateMap(Literal.create(null, DecimalType(5, 3)).cast(DoubleType)
        :: Literal("a")
        :: Literal.create(2.0f, FloatType).cast(DoubleType)
        :: Literal("b")
        :: Nil))
    // type coercion for map values
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2)
        :: Literal(3.0)
        :: Nil),
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2)
        :: Literal(3.0)
        :: Nil))
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal.create(null, DecimalType(38, 0))
        :: Literal(2)
        :: Literal.create(null, DecimalType(38, 38))
        :: Nil),
      CreateMap(Literal(1)
        :: Literal.create(null, DecimalType(38, 0)).cast(DecimalType(38, 38))
        :: Literal(2)
        :: Literal.create(null, DecimalType(38, 38))
        :: Nil))
    // type coercion for both map keys and values
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2.0)
        :: Literal(3.0)
        :: Nil),
      CreateMap(Cast(Literal(1), DoubleType)
        :: Literal("a")
        :: Literal(2.0)
        :: Literal(3.0)
        :: Nil))
  }

  test("greatest/least cast") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
        operator(Literal(1.0)
          :: Literal(1)
          :: Literal.create(1.0f, FloatType)
          :: Nil),
        operator(Literal(1.0)
          :: Cast(Literal(1), DoubleType)
          :: Cast(Literal.create(1.0f, FloatType), DoubleType)
          :: Nil))
      ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
        operator(Literal(1L)
          :: Literal(1)
          :: Literal(new java.math.BigDecimal("1000000000000000000000"))
          :: Nil),
        operator(Cast(Literal(1L), DecimalType(22, 0))
          :: Cast(Literal(1), DecimalType(22, 0))
          :: Literal(new java.math.BigDecimal("1000000000000000000000"))
          :: Nil))
      ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
        operator(Literal(1.0)
          :: Literal.create(null, DecimalType(10, 5))
          :: Literal(1)
          :: Nil),
        operator(Literal(1.0)
          :: Literal.create(null, DecimalType(10, 5)).cast(DoubleType)
          :: Literal(1).cast(DoubleType)
          :: Nil))
      ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
        operator(Literal.create(null, DecimalType(15, 0))
          :: Literal.create(null, DecimalType(10, 5))
          :: Literal(1)
          :: Nil),
        operator(Literal.create(null, DecimalType(15, 0)).cast(DecimalType(20, 5))
          :: Literal.create(null, DecimalType(10, 5)).cast(DecimalType(20, 5))
          :: Literal(1).cast(DecimalType(20, 5))
          :: Nil))
      ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
        operator(Literal.create(2L, LongType)
          :: Literal(1)
          :: Literal.create(null, DecimalType(10, 5))
          :: Nil),
        operator(Literal.create(2L, LongType).cast(DecimalType(25, 5))
          :: Literal(1).cast(DecimalType(25, 5))
          :: Literal.create(null, DecimalType(10, 5)).cast(DecimalType(25, 5))
          :: Nil))
    }
  }

  test("nanvl casts") {
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0f, FloatType), Literal.create(1.0, DoubleType)),
      NaNvl(Cast(Literal.create(1.0f, FloatType), DoubleType), Literal.create(1.0, DoubleType)))
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0f, FloatType)),
      NaNvl(Literal.create(1.0, DoubleType), Cast(Literal.create(1.0f, FloatType), DoubleType)))
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, DoubleType)),
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, DoubleType)))
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0f, FloatType), Literal.create(null, NullType)),
      NaNvl(Literal.create(1.0f, FloatType), Cast(Literal.create(null, NullType), FloatType)))
    ruleTest(AnsiTypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(null, NullType)),
      NaNvl(Literal.create(1.0, DoubleType), Cast(Literal.create(null, NullType), DoubleType)))
  }

  test("type coercion for If") {
    val rule = AnsiTypeCoercion.IfCoercion
    val intLit = Literal(1)
    val doubleLit = Literal(1.0)
    val trueLit = Literal.create(true, BooleanType)
    val falseLit = Literal.create(false, BooleanType)
    val stringLit = Literal.create("c", StringType)
    val floatLit = Literal.create(1.0f, FloatType)
    val timestampLit = Literal.create(Timestamp.valueOf("2017-04-12 00:00:00"), TimestampType)
    val decimalLit = Literal(new java.math.BigDecimal("1000000000000000000000"))

    ruleTest(rule,
      If(Literal(true), Literal(1), Literal(1L)),
      If(Literal(true), Cast(Literal(1), LongType), Literal(1L)))

    ruleTest(rule,
      If(Literal.create(null, NullType), Literal(1), Literal(1)),
      If(Literal.create(null, BooleanType), Literal(1), Literal(1)))

    ruleTest(rule,
      If(AssertTrue(trueLit), Literal(1), Literal(2)),
      If(Cast(AssertTrue(trueLit), BooleanType), Literal(1), Literal(2)))

    ruleTest(rule,
      If(AssertTrue(falseLit), Literal(1), Literal(2)),
      If(Cast(AssertTrue(falseLit), BooleanType), Literal(1), Literal(2)))

    ruleTest(rule,
      If(trueLit, intLit, doubleLit),
      If(trueLit, Cast(intLit, DoubleType), doubleLit))

    ruleTest(rule,
      If(trueLit, floatLit, doubleLit),
      If(trueLit, Cast(floatLit, DoubleType), doubleLit))

    ruleTest(rule,
      If(trueLit, floatLit, decimalLit),
      If(trueLit, Cast(floatLit, DoubleType), Cast(decimalLit, DoubleType)))

    ruleTest(rule,
      If(falseLit, stringLit, doubleLit),
      If(falseLit, stringLit, doubleLit))

    ruleTest(rule,
      If(trueLit, timestampLit, stringLit),
      If(trueLit, timestampLit, stringLit))
  }

  test("type coercion for CaseKeyWhen") {
    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      CaseKeyWhen(Literal(1.toShort), Seq(Literal(1), Literal("a"))),
      CaseKeyWhen(Cast(Literal(1.toShort), IntegerType), Seq(Literal(1), Literal("a")))
    )
    ruleTest(AnsiTypeCoercion.CaseWhenCoercion,
      CaseKeyWhen(Literal(true), Seq(Literal(1), Literal("a"))),
      CaseKeyWhen(Literal(true), Seq(Literal(1), Literal("a")))
    )
    ruleTest(AnsiTypeCoercion.CaseWhenCoercion,
      CaseWhen(Seq((Literal(true), Literal(1.2))),
        Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2))),
      CaseWhen(Seq((Literal(true), Literal(1.2))),
        Cast(Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2)), DoubleType))
    )
    ruleTest(AnsiTypeCoercion.CaseWhenCoercion,
      CaseWhen(Seq((Literal(true), Literal(100L))),
        Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2))),
      CaseWhen(Seq((Literal(true), Cast(Literal(100L), DecimalType(22, 2)))),
        Cast(Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2)), DecimalType(22, 2)))
    )
  }

  test("type coercion for Stack") {
    val rule = AnsiTypeCoercion.StackCoercion

    ruleTest(rule,
      Stack(Seq(Literal(3), Literal(1), Literal(2), Literal(null))),
      Stack(Seq(Literal(3), Literal(1), Literal(2), Literal.create(null, IntegerType))))
    ruleTest(rule,
      Stack(Seq(Literal(3), Literal(1.0), Literal(null), Literal(3.0))),
      Stack(Seq(Literal(3), Literal(1.0), Literal.create(null, DoubleType), Literal(3.0))))
    ruleTest(rule,
      Stack(Seq(Literal(3), Literal(null), Literal("2"), Literal("3"))),
      Stack(Seq(Literal(3), Literal.create(null, StringType), Literal("2"), Literal("3"))))
    ruleTest(rule,
      Stack(Seq(Literal(3), Literal(null), Literal(null), Literal(null))),
      Stack(Seq(Literal(3), Literal(null), Literal(null), Literal(null))))

    ruleTest(rule,
      Stack(Seq(Literal(2),
        Literal(1), Literal("2"),
        Literal(null), Literal(null))),
      Stack(Seq(Literal(2),
        Literal(1), Literal("2"),
        Literal.create(null, IntegerType), Literal.create(null, StringType))))

    ruleTest(rule,
      Stack(Seq(Literal(2),
        Literal(1), Literal(null),
        Literal(null), Literal("2"))),
      Stack(Seq(Literal(2),
        Literal(1), Literal.create(null, StringType),
        Literal.create(null, IntegerType), Literal("2"))))

    ruleTest(rule,
      Stack(Seq(Literal(2),
        Literal(null), Literal(1),
        Literal("2"), Literal(null))),
      Stack(Seq(Literal(2),
        Literal.create(null, StringType), Literal(1),
        Literal("2"), Literal.create(null, IntegerType))))

    ruleTest(rule,
      Stack(Seq(Literal(2),
        Literal(null), Literal(null),
        Literal(1), Literal("2"))),
      Stack(Seq(Literal(2),
        Literal.create(null, IntegerType), Literal.create(null, StringType),
        Literal(1), Literal("2"))))

    ruleTest(rule,
      Stack(Seq(Subtract(Literal(3), Literal(1)),
        Literal(1), Literal("2"),
        Literal(null), Literal(null))),
      Stack(Seq(Subtract(Literal(3), Literal(1)),
        Literal(1), Literal("2"),
        Literal.create(null, IntegerType), Literal.create(null, StringType))))
  }

  test("type coercion for Concat") {
    val rule = AnsiTypeCoercion.ConcatCoercion

    ruleTest(rule,
      Concat(Seq(Literal("ab"), Literal("cde"))),
      Concat(Seq(Literal("ab"), Literal("cde"))))
    ruleTest(rule,
      Concat(Seq(Literal(null), Literal("abc"))),
      Concat(Seq(Cast(Literal(null), StringType), Literal("abc"))))
    ruleTest(rule,
      Concat(Seq(Literal(1), Literal("234"))),
      Concat(Seq(Literal(1), Literal("234"))))
    ruleTest(rule,
      Concat(Seq(Literal("1"), Literal("234".getBytes()))),
      Concat(Seq(Literal("1"), Literal("234".getBytes()))))
    ruleTest(rule,
      Concat(Seq(Literal(1L), Literal(2.toByte), Literal(0.1))),
      Concat(Seq(Literal(1L), Literal(2.toByte), Literal(0.1))))
    ruleTest(rule,
      Concat(Seq(Literal(true), Literal(0.1f), Literal(3.toShort))),
      Concat(Seq(Literal(true), Literal(0.1f), Literal(3.toShort))))
    ruleTest(rule,
      Concat(Seq(Literal(1L), Literal(0.1))),
      Concat(Seq(Literal(1L), Literal(0.1))))
    ruleTest(rule,
      Concat(Seq(Literal(Decimal(10)))),
      Concat(Seq(Literal(Decimal(10)))))
    ruleTest(rule,
      Concat(Seq(Literal(BigDecimal.valueOf(10)))),
      Concat(Seq(Literal(BigDecimal.valueOf(10)))))
    ruleTest(rule,
      Concat(Seq(Literal(java.math.BigDecimal.valueOf(10)))),
      Concat(Seq(Literal(java.math.BigDecimal.valueOf(10)))))
    ruleTest(rule,
      Concat(Seq(Literal(new java.sql.Date(0)), Literal(new Timestamp(0)))),
      Concat(Seq(Literal(new java.sql.Date(0)), Literal(new Timestamp(0)))))

    ruleTest(rule,
      Concat(Seq(Literal("123".getBytes), Literal("456".getBytes))),
      Concat(Seq(Literal("123".getBytes), Literal("456".getBytes))))
  }

  test("type coercion for Elt") {
    val rule = AnsiTypeCoercion.EltCoercion

    ruleTest(rule,
      Elt(Seq(Literal(1), Literal("ab"), Literal("cde"))),
      Elt(Seq(Literal(1), Literal("ab"), Literal("cde"))))
    ruleTest(rule,
      Elt(Seq(Literal(1.toShort), Literal("ab"), Literal("cde"))),
      Elt(Seq(Cast(Literal(1.toShort), IntegerType), Literal("ab"), Literal("cde"))))
    ruleTest(rule,
      Elt(Seq(Literal(2), Literal(null), Literal("abc"))),
      Elt(Seq(Literal(2), Cast(Literal(null), StringType), Literal("abc"))))
    ruleTest(rule,
      Elt(Seq(Literal(2), Literal(1), Literal("234"))),
      Elt(Seq(Literal(2), Literal(1), Literal("234"))))
    ruleTest(rule,
      Elt(Seq(Literal(3), Literal(1L), Literal(2.toByte), Literal(0.1))),
      Elt(Seq(Literal(3), Literal(1L), Literal(2.toByte), Literal(0.1))))
    ruleTest(rule,
      Elt(Seq(Literal(2), Literal(true), Literal(0.1f), Literal(3.toShort))),
      Elt(Seq(Literal(2), Literal(true), Literal(0.1f), Literal(3.toShort))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(1L), Literal(0.1))),
      Elt(Seq(Literal(1), Literal(1L), Literal(0.1))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(Decimal(10)))),
      Elt(Seq(Literal(1), Literal(Decimal(10)))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(BigDecimal.valueOf(10)))),
      Elt(Seq(Literal(1), Literal(BigDecimal.valueOf(10)))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(java.math.BigDecimal.valueOf(10)))),
      Elt(Seq(Literal(1), Literal(java.math.BigDecimal.valueOf(10)))))
    ruleTest(rule,
      Elt(Seq(Literal(2), Literal(new java.sql.Date(0)), Literal(new Timestamp(0)))),
      Elt(Seq(Literal(2), Literal(new java.sql.Date(0)), Literal(new Timestamp(0)))))

    ruleTest(rule,
      Elt(Seq(Literal(1), Literal("123".getBytes), Literal("456".getBytes))),
      Elt(Seq(Literal(1), Literal("123".getBytes), Literal("456".getBytes))))
  }

  private def checkOutput(logical: LogicalPlan, expectTypes: Seq[DataType]): Unit = {
    logical.output.zip(expectTypes).foreach { case (attr, dt) =>
      assert(attr.dataType === dt)
    }
  }

  private val timeZoneResolver = ResolveTimeZone

  private def widenSetOperationTypes(plan: LogicalPlan): LogicalPlan = {
    timeZoneResolver(AnsiTypeCoercion.WidenSetOperationTypes(plan))
  }

  test("WidenSetOperationTypes for except and intersect") {
    val firstTable = LocalRelation(
      AttributeReference("i", IntegerType)(),
      AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("b", ByteType)(),
      AttributeReference("d", DoubleType)())
    val secondTable = LocalRelation(
      AttributeReference("s", LongType)(),
      AttributeReference("d", DecimalType(2, 1))(),
      AttributeReference("f", FloatType)(),
      AttributeReference("l", LongType)())

    val expectedTypes = Seq(LongType, DecimalType.SYSTEM_DEFAULT, DoubleType, DoubleType)

    val r1 = widenSetOperationTypes(
      Except(firstTable, secondTable, isAll = false)).asInstanceOf[Except]
    val r2 = widenSetOperationTypes(
      Intersect(firstTable, secondTable, isAll = false)).asInstanceOf[Intersect]
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
      AttributeReference("i", DateType)(),
      AttributeReference("u", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("b", ByteType)(),
      AttributeReference("d", DoubleType)())
    val secondTable = LocalRelation(
      AttributeReference("s", DateType)(),
      AttributeReference("d", DecimalType(2, 1))(),
      AttributeReference("f", FloatType)(),
      AttributeReference("l", LongType)())
    val thirdTable = LocalRelation(
      AttributeReference("m", TimestampType)(),
      AttributeReference("n", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("p", FloatType)(),
      AttributeReference("q", DoubleType)())
    val forthTable = LocalRelation(
      AttributeReference("m", DateType)(),
      AttributeReference("n", DecimalType.SYSTEM_DEFAULT)(),
      AttributeReference("p", ByteType)(),
      AttributeReference("q", DoubleType)())

    val expectedTypes = Seq(TimestampType, DecimalType.SYSTEM_DEFAULT, DoubleType, DoubleType)

    val unionRelation = widenSetOperationTypes(
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

    val left1 = LocalRelation(
      AttributeReference("l", DecimalType(10, 8))())
    val right1 = LocalRelation(
      AttributeReference("r", DecimalType(5, 5))())
    val expectedType1 = Seq(DecimalType(10, 8))

    val r1 = widenSetOperationTypes(Union(left1, right1)).asInstanceOf[Union]
    val r2 = widenSetOperationTypes(
      Except(left1, right1, isAll = false)).asInstanceOf[Except]
    val r3 = widenSetOperationTypes(
      Intersect(left1, right1, isAll = false)).asInstanceOf[Intersect]

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

      val r1 = widenSetOperationTypes(Union(plan1, plan2)).asInstanceOf[Union]
      val r2 = widenSetOperationTypes(
        Except(plan1, plan2, isAll = false)).asInstanceOf[Except]
      val r3 = widenSetOperationTypes(
        Intersect(plan1, plan2, isAll = false)).asInstanceOf[Intersect]

      checkOutput(r1.children.last, Seq(expectedType))
      checkOutput(r2.right, Seq(expectedType))
      checkOutput(r3.right, Seq(expectedType))

      val r4 = widenSetOperationTypes(Union(plan2, plan1)).asInstanceOf[Union]
      val r5 = widenSetOperationTypes(
        Except(plan2, plan1, isAll = false)).asInstanceOf[Except]
      val r6 = widenSetOperationTypes(
        Intersect(plan2, plan1, isAll = false)).asInstanceOf[Intersect]

      checkOutput(r4.children.last, Seq(expectedType))
      checkOutput(r5.left, Seq(expectedType))
      checkOutput(r6.left, Seq(expectedType))
    }
  }

  test("SPARK-32638: corrects references when adding aliases in WidenSetOperationTypes") {
    val t1 = LocalRelation(AttributeReference("v", DecimalType(10, 0))())
    val t2 = LocalRelation(AttributeReference("v", DecimalType(11, 0))())
    val p1 = t1.select(t1.output.head).as("p1")
    val p2 = t2.select(t2.output.head).as("p2")
    val union = p1.union(p2)
    val wp1 = widenSetOperationTypes(union.select(p1.output.head, $"p2.v"))
    assert(wp1.isInstanceOf[Project])
    // The attribute `p1.output.head` should be replaced in the root `Project`.
    assert(wp1.expressions.forall(_.find(_ == p1.output.head).isEmpty))
    val wp2 = widenSetOperationTypes(Aggregate(Nil, sum(p1.output.head).as("v") :: Nil, union))
    assert(wp2.isInstanceOf[Aggregate])
    assert(wp2.missingInput.isEmpty)
  }

  /**
   * There are rules that need to not fire before child expressions get resolved.
   * We use this test to make sure those rules do not fire early.
   */
  test("make sure rules do not fire early") {
    // InConversion
    val inConversion = AnsiTypeCoercion.InConversion
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
      In(Literal("a"), Seq(Literal(1), Literal("b")))
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

  test("SPARK-17117 null type coercion in divide") {
    val rules = Seq(FunctionArgumentConversion, Division, ImplicitTypeCasts)
    val nullLit = Literal.create(null, NullType)
    ruleTest(rules, Divide(1L, nullLit), Divide(Cast(1L, DoubleType), Cast(nullLit, DoubleType)))
    ruleTest(rules, Divide(nullLit, 1L), Divide(Cast(nullLit, DoubleType), Cast(1L, DoubleType)))
  }

  test("cast WindowFrame boundaries to the type they operate upon") {
    // Can cast frame boundaries to order dataType.
    ruleTest(WindowFrameCoercion,
      windowSpec(
        Seq(UnresolvedAttribute("a")),
        Seq(SortOrder(Literal(1L), Ascending)),
        SpecifiedWindowFrame(RangeFrame, Literal(3), Literal(2147483648L))),
      windowSpec(
        Seq(UnresolvedAttribute("a")),
        Seq(SortOrder(Literal(1L), Ascending)),
        SpecifiedWindowFrame(RangeFrame, Cast(3, LongType), Literal(2147483648L)))
    )
    // Cannot cast frame boundaries to order dataType.
    ruleTest(WindowFrameCoercion,
      windowSpec(
        Seq(UnresolvedAttribute("a")),
        Seq(SortOrder(Literal.default(DateType), Ascending)),
        SpecifiedWindowFrame(RangeFrame, Literal(10.0), Literal(2147483648L))),
      windowSpec(
        Seq(UnresolvedAttribute("a")),
        Seq(SortOrder(Literal.default(DateType), Ascending)),
        SpecifiedWindowFrame(RangeFrame, Literal(10.0), Literal(2147483648L)))
    )
    // Should not cast SpecialFrameBoundary.
    ruleTest(WindowFrameCoercion,
      windowSpec(
        Seq(UnresolvedAttribute("a")),
        Seq(SortOrder(Literal(1L), Ascending)),
        SpecifiedWindowFrame(RangeFrame, CurrentRow, UnboundedFollowing)),
      windowSpec(
        Seq(UnresolvedAttribute("a")),
        Seq(SortOrder(Literal(1L), Ascending)),
        SpecifiedWindowFrame(RangeFrame, CurrentRow, UnboundedFollowing))
    )
  }

  test("SPARK-29000: skip to handle decimals in ImplicitTypeCasts") {
    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Literal(1)),
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Literal(1)))

    ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Cast(1, IntegerType)),
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Cast(1, IntegerType)))
  }

  test("SPARK-31468: null types should be casted to decimal types in ImplicitTypeCasts") {
    Seq(AnyTypeBinaryOperator(_, _), NumericTypeBinaryOperator(_, _)).foreach { binaryOp =>
      // binaryOp(decimal, null) case
      ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
        binaryOp(Literal.create(null, DecimalType.SYSTEM_DEFAULT),
          Literal.create(null, NullType)),
        binaryOp(Literal.create(null, DecimalType.SYSTEM_DEFAULT),
          Cast(Literal.create(null, NullType), DecimalType.SYSTEM_DEFAULT)))

      // binaryOp(null, decimal) case
      ruleTest(AnsiTypeCoercion.ImplicitTypeCasts,
        binaryOp(Literal.create(null, NullType),
          Literal.create(null, DecimalType.SYSTEM_DEFAULT)),
        binaryOp(Cast(Literal.create(null, NullType), DecimalType.SYSTEM_DEFAULT),
          Literal.create(null, DecimalType.SYSTEM_DEFAULT)))
    }
  }

  test("SPARK-31761: byte, short and int should be cast to long for IntegralDivide's datatype") {
    val rules = Seq(FunctionArgumentConversion, Division, ImplicitTypeCasts)
    // Casts Byte to Long
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2.toByte, 1.toByte),
      IntegralDivide(Cast(2.toByte, LongType), Cast(1.toByte, LongType)))
    // Casts Short to Long
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2.toShort, 1.toShort),
      IntegralDivide(Cast(2.toShort, LongType), Cast(1.toShort, LongType)))
    // Casts Integer to Long
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2, 1),
      IntegralDivide(Cast(2, LongType), Cast(1, LongType)))
    // should not be any change for Long data types
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2L, 1L), IntegralDivide(2L, 1L))
    // one of the operand is byte
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2L, 1.toByte),
      IntegralDivide(2L, Cast(1.toByte, LongType)))
    // one of the operand is short
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2.toShort, 1L),
      IntegralDivide(Cast(2.toShort, LongType), 1L))
    // one of the operand is int
    ruleTest(AnsiTypeCoercion.IntegralDivision, IntegralDivide(2, 1L),
      IntegralDivide(Cast(2, LongType), 1L))
  }

  test("Promote string literals") {
    val rule = AnsiTypeCoercion.PromoteStringLiterals
    val stringLiteral = Literal("123")
    val castStringLiteralAsInt = Cast(stringLiteral, IntegerType)
    val castStringLiteralAsDouble = Cast(stringLiteral, DoubleType)
    val castStringLiteralAsDate = Cast(stringLiteral, DateType)
    val castStringLiteralAsTimestamp = Cast(stringLiteral, TimestampType)
    ruleTest(rule,
      GreaterThan(stringLiteral, Literal(1)),
      GreaterThan(castStringLiteralAsInt, Literal(1)))
    ruleTest(rule,
      LessThan(Literal(true), stringLiteral),
      LessThan(Literal(true), Cast(stringLiteral, BooleanType)))
    ruleTest(rule,
      EqualTo(Literal(Array(1, 2)), stringLiteral),
      EqualTo(Literal(Array(1, 2)), stringLiteral))
    ruleTest(rule,
      GreaterThan(stringLiteral, Literal(0.5)),
      GreaterThan(castStringLiteralAsDouble, Literal(0.5)))

    val dateLiteral = Literal(java.sql.Date.valueOf("2021-01-01"))
    ruleTest(rule,
      EqualTo(stringLiteral, dateLiteral),
      EqualTo(castStringLiteralAsDate, dateLiteral))

    val timestampLiteral = Literal(Timestamp.valueOf("2021-01-01 00:00:00"))
    ruleTest(rule,
      EqualTo(stringLiteral, timestampLiteral),
      EqualTo(castStringLiteralAsTimestamp, timestampLiteral))

    ruleTest(rule, Add(stringLiteral, Literal(1)),
      Add(castStringLiteralAsInt, Literal(1)))
    ruleTest(rule, Divide(stringLiteral, Literal(1)),
      Divide(castStringLiteralAsInt, Literal(1)))

    ruleTest(rule,
      In(Literal(1), Seq(stringLiteral, Literal(2))),
      In(Literal(1), Seq(castStringLiteralAsInt, Literal(2))))
    ruleTest(rule,
      In(Literal(1.0), Seq(stringLiteral, Literal(2.2))),
      In(Literal(1.0), Seq(castStringLiteralAsDouble, Literal(2.2))))
    ruleTest(rule,
      In(dateLiteral, Seq(stringLiteral)),
      In(dateLiteral, Seq(castStringLiteralAsDate)))
    ruleTest(rule,
      In(timestampLiteral, Seq(stringLiteral)),
      In(timestampLiteral, Seq(castStringLiteralAsTimestamp)))
  }

  test("SPARK-35937: GetDateFieldOperations") {
    val ts = Literal(Timestamp.valueOf("2021-01-01 01:30:00"))
    Seq(
      DayOfYear, Year, YearOfWeek, Quarter, Month, DayOfMonth, DayOfWeek, WeekDay, WeekOfYear
    ).foreach { operation =>
      ruleTest(
        AnsiTypeCoercion.GetDateFieldOperations, operation(ts), operation(Cast(ts, DateType)))
    }
  }
}
