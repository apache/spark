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
import java.time.{Duration, LocalDateTime, Period}

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.catalyst.analysis.TypeCoercion._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

abstract class TypeCoercionSuiteBase extends AnalysisTest {
  import TypeCoercionSuite._

  // When Utils.isTesting is true, RuleIdCollection adds individual type coercion rules. Otherwise,
  // RuleIdCollection doesn't add them because they are called in a train inside
  // CombinedTypeCoercionRule.
  assert(Utils.isTesting, s"${IS_TESTING.key} is not set to true")

  protected def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression]

  protected def dateTimeOperationsRule: TypeCoercionRule

  protected def shouldCast(from: DataType, to: AbstractDataType, expected: DataType): Unit = {
    // Check default value
    val castDefault = implicitCast(default(from), to)
    assert(DataType.equalsIgnoreCompatibleNullability(
      castDefault.map(_.dataType).getOrElse(null), expected),
      s"Failed to cast $from to $to")

    // Check null value
    val castNull = implicitCast(createNull(from), to)
    assert(DataType.equalsIgnoreCaseAndNullability(
      castNull.map(_.dataType).getOrElse(null), expected),
      s"Failed to cast $from to $to")
  }

  protected def shouldNotCast(from: DataType, to: AbstractDataType): Unit = {
    // Check default value
    val castDefault = implicitCast(default(from), to)
    assert(castDefault.isEmpty, s"Should not be able to cast $from to $to, but got $castDefault")

    // Check null value
    val castNull = implicitCast(createNull(from), to)
    assert(castNull.isEmpty, s"Should not be able to cast $from to $to, but got $castNull")
  }

  protected def default(dataType: DataType): Expression = dataType match {
    case ArrayType(internalType: DataType, _) =>
      CreateArray(Seq(Literal.default(internalType)))
    case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
      CreateMap(Seq(Literal.default(keyDataType), Literal.default(valueDataType)))
    case _ => Literal.default(dataType)
  }

  protected def createNull(dataType: DataType): Expression = dataType match {
    case ArrayType(internalType: DataType, _) =>
      CreateArray(Seq(Literal.create(null, internalType)))
    case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
      CreateMap(Seq(Literal.create(null, keyDataType), Literal.create(null, valueDataType)))
    case _ => Literal.create(null, dataType)
  }

  // Check whether the type `checkedType` can be cast to all the types in `castableTypes`,
  // but cannot be cast to the other types in `allTypes`.
  protected def checkTypeCasting(checkedType: DataType, castableTypes: Seq[DataType]): Unit = {
    val nonCastableTypes = allTypes.filterNot(castableTypes.contains)

    castableTypes.foreach { tpe =>
      shouldCast(checkedType, tpe, tpe)
    }
    nonCastableTypes.foreach { tpe =>
      shouldNotCast(checkedType, tpe)
    }
  }

  protected def ruleTest(rule: Rule[LogicalPlan],
      initial: Expression, transformed: Expression): Unit = {
    ruleTest(Seq(rule), initial, transformed)
  }

  protected def ruleTest(
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

  test("implicit type cast - ByteType") {
    val checkedType = ByteType
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(checkedType, DecimalType, DecimalType.ByteDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - ShortType") {
    val checkedType = ShortType
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(checkedType, DecimalType, DecimalType.ShortDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - IntegerType") {
    val checkedType = IntegerType
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(IntegerType, DecimalType, DecimalType.IntDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - LongType") {
    val checkedType = LongType
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(checkedType, DecimalType, DecimalType.LongDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldCast(checkedType, IntegralType, checkedType)
  }

  test("implicit type cast - FloatType") {
    val checkedType = FloatType
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(checkedType, DecimalType, DecimalType.FloatDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - DoubleType") {
    val checkedType = DoubleType
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(checkedType, DecimalType, DecimalType.DoubleDecimal)
    shouldCast(checkedType, NumericType, checkedType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - DecimalType(10, 2)") {
    val checkedType = DecimalType(10, 2)
    checkTypeCasting(checkedType, castableTypes = numericTypes ++ Seq(StringType))
    shouldCast(checkedType, DecimalType, checkedType)
    shouldCast(checkedType, NumericType, checkedType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - BinaryType") {
    val checkedType = BinaryType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType, StringType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - BooleanType") {
    val checkedType = BooleanType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType, StringType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - DateType") {
    val checkedType = DateType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType, StringType, TimestampType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - TimestampType") {
    val checkedType = TimestampType
    checkTypeCasting(checkedType, castableTypes = Seq(checkedType, StringType, DateType))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast between two Map types") {
    val sourceType = MapType(IntegerType, IntegerType, true)
    val castableTypes = numericTypes ++ Seq(StringType).filter(!Cast.forceNullable(IntegerType, _))
    val targetTypes = numericTypes.filter(!Cast.forceNullable(IntegerType, _)).map { t =>
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
        TypeCoercion.implicitCast(sourceMapExprWithValueNull, targetType)
      assert(castDefault.isEmpty,
        s"Should not be able to cast $sourceType to $targetType, but got $castDefault")
    }
  }

  test("eligible implicit type cast - TypeCollection") {
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

    shouldCast(
      ArrayType(StringType, false),
      TypeCollection(ArrayType(StringType), StringType),
      ArrayType(StringType, false))

    shouldCast(
      ArrayType(StringType, true),
      TypeCollection(ArrayType(StringType), StringType),
      ArrayType(StringType, true))
  }

  test("ineligible implicit type cast - TypeCollection") {
    shouldNotCast(IntegerType, TypeCollection(DateType, TimestampType))
  }

  test("type coercion for Concat") {
    val rule = TypeCoercion.ConcatCoercion

    ruleTest(rule,
      Concat(Seq(Literal("ab"), Literal("cde"))),
      Concat(Seq(Literal("ab"), Literal("cde"))))
    ruleTest(rule,
      Concat(Seq(Literal(null), Literal("abc"))),
      Concat(Seq(Cast(Literal(null), StringType), Literal("abc"))))
    ruleTest(rule,
      Concat(Seq(Literal(1), Literal("234"))),
      Concat(Seq(Cast(Literal(1), StringType), Literal("234"))))
    ruleTest(rule,
      Concat(Seq(Literal("1"), Literal("234".getBytes()))),
      Concat(Seq(Literal("1"), Cast(Literal("234".getBytes()), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(1L), Literal(2.toByte), Literal(0.1))),
      Concat(Seq(Cast(Literal(1L), StringType), Cast(Literal(2.toByte), StringType),
        Cast(Literal(0.1), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(true), Literal(0.1f), Literal(3.toShort))),
      Concat(Seq(Cast(Literal(true), StringType), Cast(Literal(0.1f), StringType),
        Cast(Literal(3.toShort), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(1L), Literal(0.1))),
      Concat(Seq(Cast(Literal(1L), StringType), Cast(Literal(0.1), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(Decimal(10)))),
      Concat(Seq(Cast(Literal(Decimal(10)), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(BigDecimal.valueOf(10)))),
      Concat(Seq(Cast(Literal(BigDecimal.valueOf(10)), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(java.math.BigDecimal.valueOf(10)))),
      Concat(Seq(Cast(Literal(java.math.BigDecimal.valueOf(10)), StringType))))
    ruleTest(rule,
      Concat(Seq(Literal(new java.sql.Date(0)), Literal(new Timestamp(0)))),
      Concat(Seq(Cast(Literal(new java.sql.Date(0)), StringType),
        Cast(Literal(new Timestamp(0)), StringType))))

    withSQLConf(SQLConf.CONCAT_BINARY_AS_STRING.key -> "true") {
      ruleTest(rule,
        Concat(Seq(Literal("123".getBytes), Literal("456".getBytes))),
        Concat(Seq(Cast(Literal("123".getBytes), StringType),
          Cast(Literal("456".getBytes), StringType))))
    }

    withSQLConf(SQLConf.CONCAT_BINARY_AS_STRING.key -> "false") {
      ruleTest(rule,
        Concat(Seq(Literal("123".getBytes), Literal("456".getBytes))),
        Concat(Seq(Literal("123".getBytes), Literal("456".getBytes))))
    }
  }

  test("type coercion for Elt") {
    val rule = TypeCoercion.EltCoercion

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
      Elt(Seq(Literal(2), Cast(Literal(1), StringType), Literal("234"))))
    ruleTest(rule,
      Elt(Seq(Literal(3), Literal(1L), Literal(2.toByte), Literal(0.1))),
      Elt(Seq(Literal(3), Cast(Literal(1L), StringType), Cast(Literal(2.toByte), StringType),
        Cast(Literal(0.1), StringType))))
    ruleTest(rule,
      Elt(Seq(Literal(2), Literal(true), Literal(0.1f), Literal(3.toShort))),
      Elt(Seq(Literal(2), Cast(Literal(true), StringType), Cast(Literal(0.1f), StringType),
        Cast(Literal(3.toShort), StringType))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(1L), Literal(0.1))),
      Elt(Seq(Literal(1), Cast(Literal(1L), StringType), Cast(Literal(0.1), StringType))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(Decimal(10)))),
      Elt(Seq(Literal(1), Cast(Literal(Decimal(10)), StringType))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(BigDecimal.valueOf(10)))),
      Elt(Seq(Literal(1), Cast(Literal(BigDecimal.valueOf(10)), StringType))))
    ruleTest(rule,
      Elt(Seq(Literal(1), Literal(java.math.BigDecimal.valueOf(10)))),
      Elt(Seq(Literal(1), Cast(Literal(java.math.BigDecimal.valueOf(10)), StringType))))
    ruleTest(rule,
      Elt(Seq(Literal(2), Literal(new java.sql.Date(0)), Literal(new Timestamp(0)))),
      Elt(Seq(Literal(2), Cast(Literal(new java.sql.Date(0)), StringType),
        Cast(Literal(new Timestamp(0)), StringType))))

    withSQLConf(SQLConf.ELT_OUTPUT_AS_STRING.key -> "true") {
      ruleTest(rule,
        Elt(Seq(Literal(1), Literal("123".getBytes), Literal("456".getBytes))),
        Elt(Seq(Literal(1), Cast(Literal("123".getBytes), StringType),
          Cast(Literal("456".getBytes), StringType))))
    }

    withSQLConf(SQLConf.ELT_OUTPUT_AS_STRING.key -> "false") {
      ruleTest(rule,
        Elt(Seq(Literal(1), Literal("123".getBytes), Literal("456".getBytes))),
        Elt(Seq(Literal(1), Literal("123".getBytes), Literal("456".getBytes))))
    }
  }

  test("Datetime operations") {
    val rule = dateTimeOperationsRule
    val dateLiteral = Literal(java.sql.Date.valueOf("2021-01-01"))
    val timestampLiteral = Literal(Timestamp.valueOf("2021-01-01 00:00:00"))
    val timestampNTZLiteral = Literal(LocalDateTime.parse("2021-01-01T00:00:00"))
    val intLiteral = Literal(3)
    Seq(timestampLiteral, timestampNTZLiteral).foreach { tsLiteral =>
      ruleTest(rule,
        DateAdd(tsLiteral, intLiteral),
        DateAdd(Cast(tsLiteral, DateType), intLiteral))
      ruleTest(rule,
        DateSub(tsLiteral, intLiteral),
        DateSub(Cast(tsLiteral, DateType), intLiteral))
      ruleTest(rule,
        SubtractTimestamps(tsLiteral, dateLiteral),
        SubtractTimestamps(tsLiteral, Cast(dateLiteral, tsLiteral.dataType)))
      ruleTest(rule,
        SubtractTimestamps(dateLiteral, tsLiteral),
        SubtractTimestamps(Cast(dateLiteral, tsLiteral.dataType), tsLiteral))
    }

    ruleTest(rule,
      SubtractTimestamps(timestampLiteral, timestampNTZLiteral),
      SubtractTimestamps(Cast(timestampLiteral, TimestampNTZType), timestampNTZLiteral))
    ruleTest(rule,
      SubtractTimestamps(timestampNTZLiteral, timestampLiteral),
      SubtractTimestamps(timestampNTZLiteral, Cast(timestampLiteral, TimestampNTZType)))
  }

}

class TypeCoercionSuite extends TypeCoercionSuiteBase {
  import TypeCoercionSuite._

  // scalastyle:off line.size.limit
  // The following table shows all implicit data type conversions that are not visible to the user.
  // +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
  // | Source Type\CAST TO  | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  | NullType | CalendarIntervalType |     DecimalType     | NumericType | IntegralType |
  // +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
  // | ByteType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(3, 0)   | ByteType    | ByteType     |
  // | ShortType            | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(5, 0)   | ShortType   | ShortType    |
  // | IntegerType          | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(10, 0)  | IntegerType | IntegerType  |
  // | LongType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(20, 0)  | LongType    | LongType     |
  // | DoubleType           | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(30, 15) | DoubleType  | IntegerType  |
  // | FloatType            | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(14, 7)  | FloatType   | IntegerType  |
  // | Dec(10, 2)           | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | X          | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | DecimalType(10, 2)  | Dec(10, 2)  | IntegerType  |
  // | BinaryType           | X        | X         | X           | X        | X          | X         | X          | BinaryType | X           | StringType | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | BooleanType          | X        | X         | X           | X        | X          | X         | X          | X          | BooleanType | StringType | X        | X             | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | StringType           | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | X           | StringType | DateType | TimestampType | X          | X        | X           | X        | X                    | DecimalType(38, 18) | DoubleType  | X            |
  // | DateType             | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | DateType | TimestampType | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | TimestampType        | X        | X         | X           | X        | X          | X         | X          | X          | X           | StringType | DateType | TimestampType | X          | X        | X           | X        | X                    | X                   | X           | X            |
  // | ArrayType            | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | ArrayType* | X        | X           | X        | X                    | X                   | X           | X            |
  // | MapType              | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | X          | MapType* | X           | X        | X                    | X                   | X           | X            |
  // | StructType           | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | X          | X        | StructType* | X        | X                    | X                   | X           | X            |
  // | NullType             | ByteType | ShortType | IntegerType | LongType | DoubleType | FloatType | Dec(10, 2) | BinaryType | BooleanType | StringType | DateType | TimestampType | ArrayType  | MapType  | StructType  | NullType | CalendarIntervalType | DecimalType(38, 18) | DoubleType  | IntegerType  |
  // | CalendarIntervalType | X        | X         | X           | X        | X          | X         | X          | X          | X           | X          | X        | X             | X          | X        | X           | X        | CalendarIntervalType | X                   | X           | X            |
  // +----------------------+----------+-----------+-------------+----------+------------+-----------+------------+------------+-------------+------------+----------+---------------+------------+----------+-------------+----------+----------------------+---------------------+-------------+--------------+
  // Note: StructType* is castable when all the internal child types are castable according to the table.
  // Note: ArrayType* is castable when the element type is castable according to the table.
  // Note: MapType* is castable when both the key type and the value type are castable according to the table.
  // scalastyle:on line.size.limit
  override def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] =
    TypeCoercion.implicitCast(e, expectedType)

  override def dateTimeOperationsRule: TypeCoercionRule = TypeCoercion.DateTimeOperations

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

  test("implicit type cast - StringType") {
    val checkedType = StringType
    val nonCastableTypes =
      complexTypes ++ Seq(BooleanType, NullType, CalendarIntervalType)
    checkTypeCasting(checkedType, castableTypes = allTypes.filterNot(nonCastableTypes.contains))
    shouldCast(checkedType, DecimalType, DecimalType.SYSTEM_DEFAULT)
    shouldCast(checkedType, NumericType, NumericType.defaultConcreteType)
    shouldNotCast(checkedType, IntegralType)
  }

  test("implicit type cast - ArrayType(StringType)") {
    val checkedType = ArrayType(StringType)
    val nonCastableTypes =
      complexTypes ++ Seq(BooleanType, NullType, CalendarIntervalType)
    checkTypeCasting(checkedType,
      castableTypes = allTypes.filterNot(nonCastableTypes.contains).map(ArrayType(_)))
    nonCastableTypes.map(ArrayType(_)).foreach(shouldNotCast(checkedType, _))
    shouldNotCast(ArrayType(DoubleType, containsNull = false),
      ArrayType(LongType, containsNull = false))
    shouldNotCast(checkedType, DecimalType)
    shouldNotCast(checkedType, NumericType)
    shouldNotCast(checkedType, IntegralType)
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

  test("eligible implicit type cast - TypeCollection II") {
    shouldCast(StringType, TypeCollection(NumericType, BinaryType), DoubleType)
  }

  test("tightest common bound for types") {
    def widenTest(t1: DataType, t2: DataType, expected: Option[DataType]): Unit =
      checkWidenType(TypeCoercion.findTightestCommonType, t1, t2, expected)

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
        TypeCoercion.findTightestCommonType,
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
    def widenTestWithStringPromotion(
        t1: DataType,
        t2: DataType,
        expected: Option[DataType],
        isSymmetric: Boolean = true): Unit = {
      checkWidenType(TypeCoercion.findWiderTypeForTwo, t1, t2, expected, isSymmetric)
    }

    def widenTestWithoutStringPromotion(
        t1: DataType,
        t2: DataType,
        expected: Option[DataType],
        isSymmetric: Boolean = true): Unit = {
      checkWidenType(
        TypeCoercion.findWiderTypeWithoutStringPromotionForTwo, t1, t2, expected, isSymmetric)
    }

    // Decimal
    widenTestWithStringPromotion(
      DecimalType(2, 1), DecimalType(3, 2), Some(DecimalType(3, 2)))
    widenTestWithStringPromotion(
      DecimalType(2, 1), DoubleType, Some(DoubleType))
    widenTestWithStringPromotion(
      DecimalType(2, 1), IntegerType, Some(DecimalType(11, 1)))
    widenTestWithStringPromotion(
      DecimalType(2, 1), LongType, Some(DecimalType(21, 1)))

    // ArrayType
    widenTestWithStringPromotion(
      ArrayType(ShortType, containsNull = true),
      ArrayType(DoubleType, containsNull = false),
      Some(ArrayType(DoubleType, containsNull = true)))
    widenTestWithStringPromotion(
      ArrayType(TimestampType, containsNull = false),
      ArrayType(StringType, containsNull = true),
      Some(ArrayType(StringType, containsNull = true)))
    widenTestWithStringPromotion(
      ArrayType(ArrayType(IntegerType), containsNull = false),
      ArrayType(ArrayType(LongType), containsNull = false),
      Some(ArrayType(ArrayType(LongType), containsNull = false)))
    widenTestWithStringPromotion(
      ArrayType(MapType(IntegerType, FloatType), containsNull = false),
      ArrayType(MapType(LongType, DoubleType), containsNull = false),
      Some(ArrayType(MapType(LongType, DoubleType), containsNull = false)))
    widenTestWithStringPromotion(
      ArrayType(new StructType().add("num", ShortType), containsNull = false),
      ArrayType(new StructType().add("num", LongType), containsNull = false),
      Some(ArrayType(new StructType().add("num", LongType), containsNull = false)))
    widenTestWithStringPromotion(
      ArrayType(IntegerType, containsNull = false),
      ArrayType(DecimalType.IntDecimal, containsNull = false),
      Some(ArrayType(DecimalType.IntDecimal, containsNull = false)))
    widenTestWithStringPromotion(
      ArrayType(DecimalType(36, 0), containsNull = false),
      ArrayType(DecimalType(36, 35), containsNull = false),
      Some(ArrayType(DecimalType(38, 35), containsNull = true)))

    // MapType
    widenTestWithStringPromotion(
      MapType(ShortType, TimestampType, valueContainsNull = true),
      MapType(DoubleType, StringType, valueContainsNull = false),
      Some(MapType(DoubleType, StringType, valueContainsNull = true)))
    widenTestWithStringPromotion(
      MapType(IntegerType, ArrayType(TimestampType), valueContainsNull = false),
      MapType(LongType, ArrayType(StringType), valueContainsNull = true),
      Some(MapType(LongType, ArrayType(StringType), valueContainsNull = true)))
    widenTestWithStringPromotion(
      MapType(IntegerType, MapType(ShortType, TimestampType), valueContainsNull = false),
      MapType(LongType, MapType(DoubleType, StringType), valueContainsNull = false),
      Some(MapType(LongType, MapType(DoubleType, StringType), valueContainsNull = false)))
    widenTestWithStringPromotion(
      MapType(IntegerType, new StructType().add("num", ShortType), valueContainsNull = false),
      MapType(LongType, new StructType().add("num", LongType), valueContainsNull = false),
      Some(MapType(LongType, new StructType().add("num", LongType), valueContainsNull = false)))
    widenTestWithStringPromotion(
      MapType(StringType, IntegerType, valueContainsNull = false),
      MapType(StringType, DecimalType.IntDecimal, valueContainsNull = false),
      Some(MapType(StringType, DecimalType.IntDecimal, valueContainsNull = false)))
    widenTestWithStringPromotion(
      MapType(StringType, DecimalType(36, 0), valueContainsNull = false),
      MapType(StringType, DecimalType(36, 35), valueContainsNull = false),
      Some(MapType(StringType, DecimalType(38, 35), valueContainsNull = true)))
    widenTestWithStringPromotion(
      MapType(IntegerType, StringType, valueContainsNull = false),
      MapType(DecimalType.IntDecimal, StringType, valueContainsNull = false),
      Some(MapType(DecimalType.IntDecimal, StringType, valueContainsNull = false)))
    widenTestWithStringPromotion(
      MapType(DecimalType(36, 0), StringType, valueContainsNull = false),
      MapType(DecimalType(36, 35), StringType, valueContainsNull = false),
      None)

    // StructType
    widenTestWithStringPromotion(
      new StructType()
        .add("num", ShortType, nullable = true).add("ts", StringType, nullable = false),
      new StructType()
        .add("num", DoubleType, nullable = false).add("ts", TimestampType, nullable = true),
      Some(new StructType()
        .add("num", DoubleType, nullable = true).add("ts", StringType, nullable = true)))
    widenTestWithStringPromotion(
      new StructType()
        .add("arr", ArrayType(ShortType, containsNull = false), nullable = false),
      new StructType()
        .add("arr", ArrayType(DoubleType, containsNull = true), nullable = false),
      Some(new StructType()
        .add("arr", ArrayType(DoubleType, containsNull = true), nullable = false)))
    widenTestWithStringPromotion(
      new StructType()
        .add("map", MapType(ShortType, TimestampType, valueContainsNull = true), nullable = false),
      new StructType()
        .add("map", MapType(DoubleType, StringType, valueContainsNull = false), nullable = false),
      Some(new StructType()
        .add("map", MapType(DoubleType, StringType, valueContainsNull = true), nullable = false)))
    widenTestWithStringPromotion(
      new StructType().add("num", IntegerType, nullable = false),
      new StructType().add("num", DecimalType.IntDecimal, nullable = false),
      Some(new StructType().add("num", DecimalType.IntDecimal, nullable = false)))
    widenTestWithStringPromotion(
      new StructType().add("num", DecimalType(36, 0), nullable = false),
      new StructType().add("num", DecimalType(36, 35), nullable = false),
      Some(new StructType().add("num", DecimalType(38, 35), nullable = true)))

    widenTestWithStringPromotion(
      new StructType().add("num", IntegerType),
      new StructType().add("num", LongType).add("str", StringType),
      None)
    widenTestWithoutStringPromotion(
      new StructType().add("num", IntegerType),
      new StructType().add("num", LongType).add("str", StringType),
      None)
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      widenTestWithStringPromotion(
        new StructType().add("a", IntegerType),
        new StructType().add("A", LongType),
        None)
      widenTestWithoutStringPromotion(
        new StructType().add("a", IntegerType),
        new StructType().add("A", LongType),
        None)
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      widenTestWithStringPromotion(
        new StructType().add("a", IntegerType),
        new StructType().add("A", LongType),
        Some(new StructType().add("a", LongType)),
        isSymmetric = false)
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

    // String promotion
    widenTestWithStringPromotion(IntegerType, StringType, Some(StringType))
    widenTestWithStringPromotion(StringType, TimestampType, Some(StringType))
    widenTestWithStringPromotion(
      ArrayType(LongType), ArrayType(StringType), Some(ArrayType(StringType)))
    widenTestWithStringPromotion(
      ArrayType(StringType), ArrayType(TimestampType), Some(ArrayType(StringType)))
    widenTestWithStringPromotion(
      MapType(LongType, IntegerType),
      MapType(StringType, IntegerType),
      Some(MapType(StringType, IntegerType)))
    widenTestWithStringPromotion(
      MapType(IntegerType, LongType),
      MapType(IntegerType, StringType),
      Some(MapType(IntegerType, StringType)))
    widenTestWithStringPromotion(
      MapType(StringType, IntegerType),
      MapType(TimestampType, IntegerType),
      Some(MapType(StringType, IntegerType)))
    widenTestWithStringPromotion(
      MapType(IntegerType, StringType),
      MapType(IntegerType, TimestampType),
      Some(MapType(IntegerType, StringType)))
    widenTestWithStringPromotion(
      new StructType().add("a", IntegerType),
      new StructType().add("a", StringType),
      Some(new StructType().add("a", StringType)))
    widenTestWithStringPromotion(
      new StructType().add("a", StringType),
      new StructType().add("a", IntegerType),
      Some(new StructType().add("a", StringType)))
  }

  test("cast NullType for expressions that implement ExpectsInputTypes") {
    ruleTest(TypeCoercion.ImplicitTypeCasts,
      AnyTypeUnaryExpression(Literal.create(null, NullType)),
      AnyTypeUnaryExpression(Literal.create(null, NullType)))

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      NumericTypeUnaryExpression(Literal.create(null, NullType)),
      NumericTypeUnaryExpression(Literal.create(null, DoubleType)))
  }

  test("cast NullType for binary operators") {
    ruleTest(TypeCoercion.ImplicitTypeCasts,
      AnyTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)),
      AnyTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)))

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      NumericTypeBinaryOperator(Literal.create(null, NullType), Literal.create(null, NullType)),
      NumericTypeBinaryOperator(Literal.create(null, DoubleType), Literal.create(null, DoubleType)))
  }

  test("coalesce casts") {
    val rule = TypeCoercion.FunctionArgumentConversion

    val intLit = Literal(1)
    val longLit = Literal.create(1L)
    val doubleLit = Literal(1.0)
    val stringLit = Literal.create("c", StringType)
    val nullLit = Literal.create(null, NullType)
    val floatNullLit = Literal.create(null, FloatType)
    val floatLit = Literal.create(1.0f, FloatType)
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
      Coalesce(Seq(Cast(timestampLit, StringType), stringLit)))

    ruleTest(rule,
      Coalesce(Seq(nullLit, floatNullLit, intLit)),
      Coalesce(Seq(Cast(nullLit, FloatType), floatNullLit, Cast(intLit, FloatType))))

    ruleTest(rule,
      Coalesce(Seq(nullLit, intLit, decimalLit, doubleLit)),
      Coalesce(Seq(Cast(nullLit, DoubleType), Cast(intLit, DoubleType),
        Cast(decimalLit, DoubleType), doubleLit)))

    ruleTest(rule,
      Coalesce(Seq(nullLit, floatNullLit, doubleLit, stringLit)),
      Coalesce(Seq(Cast(nullLit, StringType), Cast(floatNullLit, StringType),
        Cast(doubleLit, StringType), stringLit)))

    ruleTest(rule,
      Coalesce(Seq(timestampLit, intLit, stringLit)),
      Coalesce(Seq(Cast(timestampLit, StringType), Cast(intLit, StringType), stringLit)))

    ruleTest(rule,
      Coalesce(Seq(tsArrayLit, intArrayLit, strArrayLit)),
      Coalesce(Seq(Cast(tsArrayLit, ArrayType(StringType)),
        Cast(intArrayLit, ArrayType(StringType)), strArrayLit)))
  }

  test("CreateArray casts") {
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal.create(1.0f, FloatType)
        :: Nil),
      CreateArray(Literal(1.0)
        :: Cast(Literal(1), DoubleType)
        :: Cast(Literal.create(1.0f, FloatType), DoubleType)
        :: Nil))

    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal(1.0)
        :: Literal(1)
        :: Literal("a")
        :: Nil),
      CreateArray(Cast(Literal(1.0), StringType)
        :: Cast(Literal(1), StringType)
        :: Literal("a")
        :: Nil))

    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateArray(Literal.create(null, DecimalType(5, 3))
        :: Literal(1)
        :: Nil),
      CreateArray(Literal.create(null, DecimalType(5, 3)).cast(DecimalType(13, 3))
        :: Literal(1).cast(DecimalType(13, 3))
        :: Nil))

    ruleTest(TypeCoercion.FunctionArgumentConversion,
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
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal.create(2.0f, FloatType)
        :: Literal("b")
        :: Nil),
      CreateMap(Cast(Literal(1), FloatType)
        :: Literal("a")
        :: Literal.create(2.0f, FloatType)
        :: Literal("b")
        :: Nil))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
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
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2)
        :: Literal(3.0)
        :: Nil),
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2)
        :: Cast(Literal(3.0), StringType)
        :: Nil))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
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
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      CreateMap(Literal(1)
        :: Literal("a")
        :: Literal(2.0)
        :: Literal(3.0)
        :: Nil),
      CreateMap(Cast(Literal(1), DoubleType)
        :: Literal("a")
        :: Literal(2.0)
        :: Cast(Literal(3.0), StringType)
        :: Nil))
  }

  test("greatest/least cast") {
    for (operator <- Seq[(Seq[Expression] => Expression)](Greatest, Least)) {
      ruleTest(TypeCoercion.FunctionArgumentConversion,
        operator(Literal(1.0)
          :: Literal(1)
          :: Literal.create(1.0f, FloatType)
          :: Nil),
        operator(Literal(1.0)
          :: Cast(Literal(1), DoubleType)
          :: Cast(Literal.create(1.0f, FloatType), DoubleType)
          :: Nil))
      ruleTest(TypeCoercion.FunctionArgumentConversion,
        operator(Literal(1L)
          :: Literal(1)
          :: Literal(new java.math.BigDecimal("1000000000000000000000"))
          :: Nil),
        operator(Cast(Literal(1L), DecimalType(22, 0))
          :: Cast(Literal(1), DecimalType(22, 0))
          :: Literal(new java.math.BigDecimal("1000000000000000000000"))
          :: Nil))
      ruleTest(TypeCoercion.FunctionArgumentConversion,
        operator(Literal(1.0)
          :: Literal.create(null, DecimalType(10, 5))
          :: Literal(1)
          :: Nil),
        operator(Literal(1.0)
          :: Literal.create(null, DecimalType(10, 5)).cast(DoubleType)
          :: Literal(1).cast(DoubleType)
          :: Nil))
      ruleTest(TypeCoercion.FunctionArgumentConversion,
        operator(Literal.create(null, DecimalType(15, 0))
          :: Literal.create(null, DecimalType(10, 5))
          :: Literal(1)
          :: Nil),
        operator(Literal.create(null, DecimalType(15, 0)).cast(DecimalType(20, 5))
          :: Literal.create(null, DecimalType(10, 5)).cast(DecimalType(20, 5))
          :: Literal(1).cast(DecimalType(20, 5))
          :: Nil))
      ruleTest(TypeCoercion.FunctionArgumentConversion,
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
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0f, FloatType), Literal.create(1.0, DoubleType)),
      NaNvl(Cast(Literal.create(1.0f, FloatType), DoubleType), Literal.create(1.0, DoubleType)))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0f, FloatType)),
      NaNvl(Literal.create(1.0, DoubleType), Cast(Literal.create(1.0f, FloatType), DoubleType)))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, DoubleType)),
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(1.0, DoubleType)))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0f, FloatType), Literal.create(null, NullType)),
      NaNvl(Literal.create(1.0f, FloatType), Cast(Literal.create(null, NullType), FloatType)))
    ruleTest(TypeCoercion.FunctionArgumentConversion,
      NaNvl(Literal.create(1.0, DoubleType), Literal.create(null, NullType)),
      NaNvl(Literal.create(1.0, DoubleType), Cast(Literal.create(null, NullType), DoubleType)))
  }

  test("type coercion for If") {
    val rule = TypeCoercion.IfCoercion
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
      If(falseLit, stringLit, Cast(doubleLit, StringType)))

    ruleTest(rule,
      If(trueLit, timestampLit, stringLit),
      If(trueLit, Cast(timestampLit, StringType), stringLit))
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
      CaseWhen(Seq((Literal(true), Literal(1.2))),
        Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2))),
      CaseWhen(Seq((Literal(true), Literal(1.2))),
        Cast(Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2)), DoubleType))
    )
    ruleTest(TypeCoercion.CaseWhenCoercion,
      CaseWhen(Seq((Literal(true), Literal(100L))),
        Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2))),
      CaseWhen(Seq((Literal(true), Cast(Literal(100L), DecimalType(22, 2)))),
        Cast(Literal.create(BigDecimal.valueOf(1), DecimalType(7, 2)), DecimalType(22, 2)))
    )
  }

  test("type coercion for Stack") {
    val rule = TypeCoercion.StackCoercion

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

  private val timeZoneResolver = ResolveTimeZone

  private def widenSetOperationTypes(plan: LogicalPlan): LogicalPlan = {
    timeZoneResolver(TypeCoercion.WidenSetOperationTypes(plan))
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

    val expectedTypes = Seq(StringType, DecimalType.SYSTEM_DEFAULT, FloatType, DoubleType)

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

    val expectedTypes = Seq(StringType, DecimalType.SYSTEM_DEFAULT, FloatType, DoubleType)

    val unionRelation = widenSetOperationTypes(
      Union(firstTable :: secondTable :: thirdTable :: forthTable :: Nil)).asInstanceOf[Union]
    assert(unionRelation.children.length == 4)
    checkOutput(unionRelation.children.head, expectedTypes)
    checkOutput(unionRelation.children(1), expectedTypes)
    checkOutput(unionRelation.children(2), expectedTypes)
    checkOutput(unionRelation.children(3), expectedTypes)

    assert(unionRelation.children.head.isInstanceOf[Project])
    assert(unionRelation.children(1).isInstanceOf[Project])
    // thirdTable has same datatypes as expected ones, so no need to add extra Project.
    assert(unionRelation.children(2).isInstanceOf[LocalRelation])
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

  test("SPARK-17117 null type coercion in divide") {
    val rules = Seq(FunctionArgumentConversion, Division, ImplicitTypeCasts)
    val nullLit = Literal.create(null, NullType)
    ruleTest(rules, Divide(1L, nullLit), Divide(Cast(1L, DoubleType), Cast(nullLit, DoubleType)))
    ruleTest(rules, Divide(nullLit, 1L), Divide(Cast(nullLit, DoubleType), Cast(1L, DoubleType)))
  }

  test("binary comparison with string promotion") {
    val rule = TypeCoercion.PromoteStrings
    ruleTest(rule,
      GreaterThan(Literal("123"), Literal(1)),
      GreaterThan(Cast(Literal("123"), IntegerType), Literal(1)))
    ruleTest(rule,
      LessThan(Literal(true), Literal("123")),
      LessThan(Literal(true), Cast(Literal("123"), BooleanType)))
    ruleTest(rule,
      EqualTo(Literal(Array(1, 2)), Literal("123")),
      EqualTo(Literal(Array(1, 2)), Literal("123")))
    ruleTest(rule,
      GreaterThan(Literal("1.5"), Literal(BigDecimal("0.5"))),
      GreaterThan(Cast(Literal("1.5"), DoubleType), Cast(Literal(BigDecimal("0.5")),
        DoubleType)))
    // Checks that dates/timestamps are not promoted to strings
    val date0301 = Literal(java.sql.Date.valueOf("2017-03-01"))
    val timestamp0301000000 = Literal(Timestamp.valueOf("2017-03-01 00:00:00"))
    val timestamp0301000001 = Literal(Timestamp.valueOf("2017-03-01 00:00:01"))
    // `Date` should be treated as timestamp at 00:00:00 See SPARK-23549
    ruleTest(rule, EqualTo(date0301, timestamp0301000000),
      EqualTo(Cast(date0301, TimestampType), timestamp0301000000))
    ruleTest(rule, LessThan(date0301, timestamp0301000001),
      LessThan(Cast(date0301, TimestampType), timestamp0301000001))
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
    ruleTest(TypeCoercion.ImplicitTypeCasts,
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Literal(1)),
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Literal(1)))

    ruleTest(TypeCoercion.ImplicitTypeCasts,
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Cast(1, IntegerType)),
      Multiply(CaseWhen(Seq((EqualTo(1, 2), Cast(1, DecimalType(34, 24)))),
        Cast(100, DecimalType(34, 24))), Cast(1, IntegerType)))
  }

  test("SPARK-31468: null types should be casted to decimal types in ImplicitTypeCasts") {
    Seq(AnyTypeBinaryOperator(_, _), NumericTypeBinaryOperator(_, _)).foreach { binaryOp =>
      // binaryOp(decimal, null) case
      ruleTest(TypeCoercion.ImplicitTypeCasts,
        binaryOp(Literal.create(null, DecimalType.SYSTEM_DEFAULT),
          Literal.create(null, NullType)),
        binaryOp(Literal.create(null, DecimalType.SYSTEM_DEFAULT),
          Cast(Literal.create(null, NullType), DecimalType.SYSTEM_DEFAULT)))

      // binaryOp(null, decimal) case
      ruleTest(TypeCoercion.ImplicitTypeCasts,
        binaryOp(Literal.create(null, NullType),
          Literal.create(null, DecimalType.SYSTEM_DEFAULT)),
        binaryOp(Cast(Literal.create(null, NullType), DecimalType.SYSTEM_DEFAULT),
          Literal.create(null, DecimalType.SYSTEM_DEFAULT)))
    }
  }

  test("SPARK-31761: byte, short and int should be cast to long for IntegralDivide's datatype") {
    val rules = Seq(FunctionArgumentConversion, Division, ImplicitTypeCasts)
    // Casts Byte to Long
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2.toByte, 1.toByte),
      IntegralDivide(Cast(2.toByte, LongType), Cast(1.toByte, LongType)))
    // Casts Short to Long
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2.toShort, 1.toShort),
      IntegralDivide(Cast(2.toShort, LongType), Cast(1.toShort, LongType)))
    // Casts Integer to Long
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2, 1),
      IntegralDivide(Cast(2, LongType), Cast(1, LongType)))
    // should not be any change for Long data types
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2L, 1L), IntegralDivide(2L, 1L))
    // one of the operand is byte
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2L, 1.toByte),
      IntegralDivide(2L, Cast(1.toByte, LongType)))
    // one of the operand is short
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2.toShort, 1L),
      IntegralDivide(Cast(2.toShort, LongType), 1L))
    // one of the operand is int
    ruleTest(TypeCoercion.IntegralDivision, IntegralDivide(2, 1L),
      IntegralDivide(Cast(2, LongType), 1L))
  }

  test("SPARK-36431: Support TypeCoercion of ANSI intervals with different fields") {
    DataTypeTestUtils.yearMonthIntervalTypes.foreach { ym1 =>
      DataTypeTestUtils.yearMonthIntervalTypes.foreach { ym2 =>
        val literal1 = Literal.create(Period.ofMonths(12), ym1)
        val literal2 = Literal.create(Period.ofMonths(12), ym2)
        val commonType = YearMonthIntervalType(
          ym1.startField.min(ym2.startField), ym1.endField.max(ym2.endField))
        if (commonType == ym1 && commonType == ym2) {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(literal1, literal2))
        } else if (commonType == ym1) {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(literal1, Cast(literal2, commonType)))
        } else if (commonType == ym2) {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(Cast(literal1, commonType), literal2))
        } else {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(Cast(literal1, commonType), Cast(literal2, commonType)))
        }
      }
    }

    DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt1 =>
      DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt2 =>
        val literal1 = Literal.create(Duration.ofSeconds(1111), dt1)
        val literal2 = Literal.create(Duration.ofSeconds(1111), dt2)
        val commonType = DayTimeIntervalType(
          dt1.startField.min(dt2.startField), dt1.endField.max(dt2.endField))
        if (commonType == dt1 && commonType == dt2) {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(literal1, literal2))
        } else if (commonType == dt1) {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(literal1, Cast(literal2, commonType)))
        } else if (commonType == dt2) {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(Cast(literal1, commonType), literal2))
        } else {
          ruleTest(TypeCoercion.ImplicitTypeCasts, EqualTo(literal1, literal2),
            EqualTo(Cast(literal1, commonType), Cast(literal2, commonType)))
        }
      }
    }
  }
}


object TypeCoercionSuite {

  val integralTypes: Seq[DataType] =
    Seq(ByteType, ShortType, IntegerType, LongType)
  val fractionalTypes: Seq[DataType] =
    Seq(DoubleType, FloatType, DecimalType.SYSTEM_DEFAULT, DecimalType(10, 2))
  val numericTypes: Seq[DataType] = integralTypes ++ fractionalTypes
  val datetimeTypes: Seq[DataType] = Seq(DateType, TimestampType)
  val atomicTypes: Seq[DataType] =
    numericTypes ++ datetimeTypes ++ Seq(BinaryType, BooleanType, StringType)
  val complexTypes: Seq[DataType] =
    Seq(ArrayType(IntegerType),
      ArrayType(StringType),
      MapType(StringType, StringType),
      new StructType().add("a1", StringType),
      new StructType().add("a1", StringType).add("a2", IntegerType))
  val allTypes: Seq[DataType] =
    atomicTypes ++ complexTypes ++ Seq(NullType, CalendarIntervalType)

  case class AnyTypeUnaryExpression(child: Expression)
    extends UnaryExpression with ExpectsInputTypes with Unevaluable {
    override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)
    override def dataType: DataType = NullType
    override protected def withNewChildInternal(newChild: Expression): AnyTypeUnaryExpression =
      copy(child = newChild)
  }

  case class NumericTypeUnaryExpression(child: Expression)
    extends UnaryExpression with ExpectsInputTypes with Unevaluable {
    override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)
    override def dataType: DataType = NullType
    override protected def withNewChildInternal(newChild: Expression): NumericTypeUnaryExpression =
      copy(child = newChild)
  }

  case class AnyTypeBinaryOperator(left: Expression, right: Expression)
    extends BinaryOperator with Unevaluable {
    override def dataType: DataType = NullType
    override def inputType: AbstractDataType = AnyDataType
    override def symbol: String = "anytype"
    override protected def withNewChildrenInternal(
        newLeft: Expression, newRight: Expression): AnyTypeBinaryOperator =
      copy(left = newLeft, right = newRight)
  }

  case class NumericTypeBinaryOperator(left: Expression, right: Expression)
    extends BinaryOperator with Unevaluable {
    override def dataType: DataType = NullType
    override def inputType: AbstractDataType = NumericType
    override def symbol: String = "numerictype"
    override protected def withNewChildrenInternal(
        newLeft: Expression, newRight: Expression): NumericTypeBinaryOperator =
      copy(left = newLeft, right = newRight)
  }
}
