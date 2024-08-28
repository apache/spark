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

import java.time.{ZoneId, ZoneOffset}
import java.util.Locale
import java.util.concurrent.TimeUnit._

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.types.{PhysicalFractionalType, PhysicalIntegralType, PhysicalNumericType}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.IntervalUtils.{dayTimeIntervalToByte, dayTimeIntervalToDecimal, dayTimeIntervalToInt, dayTimeIntervalToLong, dayTimeIntervalToShort, yearMonthIntervalToByte, yearMonthIntervalToInt, yearMonthIntervalToShort}
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}
import org.apache.spark.unsafe.types.UTF8String.{IntWrapper, LongWrapper}
import org.apache.spark.util.ArrayImplicits._

object Cast extends QueryErrorsBase {
  /**
   * As per section 6.13 "cast specification" in "Information technology — Database languages " +
   * "- SQL — Part 2: Foundation (SQL/Foundation)":
   * If the <cast operand> is a <value expression>, then the valid combinations of TD and SD
   * in a <cast specification> are given by the following table. "Y" indicates that the
   * combination is syntactically valid without restriction; "M" indicates that the combination
   * is valid subject to other Syntax Rules in this Sub- clause being satisfied; and "N" indicates
   * that the combination is not valid:
   * SD                   TD
   *     EN AN C D T TS YM DT BO UDT B RT CT RW
   * EN  Y  Y  Y N N  N  M  M  N   M N  M  N N
   * AN  Y  Y  Y N N  N  N  N  N   M N  M  N N
   * C   Y  Y  Y Y Y  Y  Y  Y  Y   M N  M  N N
   * D   N  N  Y Y N  Y  N  N  N   M N  M  N N
   * T   N  N  Y N Y  Y  N  N  N   M N  M  N N
   * TS  N  N  Y Y Y  Y  N  N  N   M N  M  N N
   * YM  M  N  Y N N  N  Y  N  N   M N  M  N N
   * DT  M  N  Y N N  N  N  Y  N   M N  M  N N
   * BO  N  N  Y N N  N  N  N  Y   M N  M  N N
   * UDT M  M  M M M  M  M  M  M   M M  M  M N
   * B   N  N  N N N  N  N  N  N   M Y  M  N N
   * RT  M  M  M M M  M  M  M  M   M M  M  N N
   * CT  N  N  N N N  N  N  N  N   M N  N  M N
   * RW  N  N  N N N  N  N  N  N   N N  N  N M
   *
   * Where:
   *   EN  = Exact Numeric
   *   AN  = Approximate Numeric
   *   C   = Character (Fixed- or Variable-Length, or Character Large Object)
   *   D   = Date
   *   T   = Time
   *   TS  = Timestamp
   *   YM  = Year-Month Interval
   *   DT  = Day-Time Interval
   *   BO  = Boolean
   *   UDT  = User-Defined Type
   *   B   = Binary (Fixed- or Variable-Length or Binary Large Object)
   *   RT  = Reference type
   *   CT  = Collection type
   *   RW  = Row type
   *
   * Spark's ANSI mode follows the syntax rules, except it specially allow the following
   * straightforward type conversions which are disallowed as per the SQL standard:
   *   - Numeric <=> Boolean
   *   - String <=> Binary
   */
  def canAnsiCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, _: StringType) => true

    case (_: StringType, _: BinaryType) => true

    case (_: StringType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

    case (_: StringType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (TimestampNTZType, TimestampType) => true
    case (_: NumericType, TimestampType) => true

    case (_: StringType, TimestampNTZType) => true
    case (DateType, TimestampNTZType) => true
    case (TimestampType, TimestampNTZType) => true

    case (_: StringType, _: CalendarIntervalType) => true
    case (_: StringType, _: AnsiIntervalType) => true

    case (_: AnsiIntervalType, _: IntegralType | _: DecimalType) => true
    case (_: IntegralType | _: DecimalType, _: AnsiIntervalType) => true

    case (_: DayTimeIntervalType, _: DayTimeIntervalType) => true
    case (_: YearMonthIntervalType, _: YearMonthIntervalType) => true

    case (_: StringType, DateType) => true
    case (TimestampType, DateType) => true
    case (TimestampNTZType, DateType) => true

    case (_: NumericType, _: NumericType) => true
    case (_: StringType, _: NumericType) => true
    case (BooleanType, _: NumericType) => true
    case (TimestampType, _: NumericType) => true

    case (VariantType, _) => variant.VariantGet.checkDataType(to)
    case (_, VariantType) => variant.VariantGet.checkDataType(from, allowStructsAndMaps = false)

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canAnsiCast(fromType, toType) && resolvableNullability(fn, tn)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      canAnsiCast(fromKey, toKey) && canAnsiCast(fromValue, toValue) &&
        resolvableNullability(fn, tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canAnsiCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(fromField.nullable, toField.nullable)
        }

    case (udt1: UserDefinedType[_], udt2: UserDefinedType[_]) if udt2.acceptsType(udt1) => true

    case _ => false
  }

  // If the target data type is a complex type which can't have Null values, we should guarantee
  // that the casting between the element types won't produce Null results.
  def canTryCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canCast(fromType, toType) &&
        resolvableNullability(fn || forceNullable(fromType, toType), tn)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      canCast(fromKey, toKey) &&
        (!forceNullable(fromKey, toKey)) &&
        canCast(fromValue, toValue) &&
        resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(
                fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                toField.nullable)
        }

    case _ =>
      Cast.canAnsiCast(from, to)
  }

  /**
   * A tag to identify if a CAST added by the table insertion resolver.
   */
  val BY_TABLE_INSERTION = TreeNodeTag[Unit]("by_table_insertion")

  /**
   * A tag to decide if a CAST is specified by user.
   */
  val USER_SPECIFIED_CAST = new TreeNodeTag[Unit]("user_specified_cast")

  /**
   * Returns true iff we can cast `from` type to `to` type.
   */
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, _: StringType) => true

    case (_: StringType, BinaryType) => true
    case (_: IntegralType, BinaryType) => true

    case (_: StringType, BooleanType) => true
    case (DateType, BooleanType) => true
    case (TimestampType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

    case (_: StringType, TimestampType) => true
    case (BooleanType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (_: NumericType, TimestampType) => true
    case (TimestampNTZType, TimestampType) => true

    case (_: StringType, TimestampNTZType) => true
    case (DateType, TimestampNTZType) => true
    case (TimestampType, TimestampNTZType) => true

    case (_: StringType, DateType) => true
    case (TimestampType, DateType) => true
    case (TimestampNTZType, DateType) => true

    case (_: StringType, CalendarIntervalType) => true
    case (_: StringType, _: DayTimeIntervalType) => true
    case (_: StringType, _: YearMonthIntervalType) => true
    case (_: IntegralType, DayTimeIntervalType(s, e)) if s == e => true
    case (_: IntegralType, YearMonthIntervalType(s, e)) if s == e => true

    case (_: DayTimeIntervalType, _: DayTimeIntervalType) => true
    case (_: YearMonthIntervalType, _: YearMonthIntervalType) => true
    case (_: AnsiIntervalType, _: IntegralType | _: DecimalType) => true
    case (_: IntegralType | _: DecimalType, _: AnsiIntervalType) => true

    case (_: StringType, _: NumericType) => true
    case (BooleanType, _: NumericType) => true
    case (DateType, _: NumericType) => true
    case (TimestampType, _: NumericType) => true
    case (_: NumericType, _: NumericType) => true

    case (VariantType, _) => variant.VariantGet.checkDataType(to)
    case (_, VariantType) => variant.VariantGet.checkDataType(from, allowStructsAndMaps = false)

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canCast(fromType, toType) &&
        resolvableNullability(fn || forceNullable(fromType, toType), tn)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      canCast(fromKey, toKey) &&
        (!forceNullable(fromKey, toKey)) &&
        canCast(fromValue, toValue) &&
        resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(
                fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                toField.nullable)
        }

    case (udt1: UserDefinedType[_], udt2: UserDefinedType[_]) if udt2.acceptsType(udt1) => true

    case _ => false
  }

  /**
   * Return true if we need to use the `timeZone` information casting `from` type to `to` type.
   * The patterns matched reflect the current implementation in the Cast node.
   * c.f. usage of `timeZone` in:
   * * Cast.castToString
   * * Cast.castToDate
   * * Cast.castToTimestamp
   */
  def needsTimeZone(from: DataType, to: DataType): Boolean = (from, to) match {
    case (VariantType, _) => true
    case (_: StringType, TimestampType) => true
    case (TimestampType, StringType) => true
    case (DateType, TimestampType) => true
    case (TimestampType, DateType) => true
    case (TimestampType, TimestampNTZType) => true
    case (TimestampNTZType, TimestampType) => true
    case (ArrayType(fromType, _), ArrayType(toType, _)) => needsTimeZone(fromType, toType)
    case (MapType(fromKey, fromValue, _), MapType(toKey, toValue, _)) =>
      needsTimeZone(fromKey, toKey) || needsTimeZone(fromValue, toValue)
    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).exists {
          case (fromField, toField) =>
            needsTimeZone(fromField.dataType, toField.dataType)
        }
    case _ => false
  }

  /**
   * Returns true iff we can safely up-cast the `from` type to `to` type without any truncating or
   * precision lose or possible runtime failures. For example, long -> int, string -> int are not
   * up-cast.
   */
  def canUpCast(from: DataType, to: DataType): Boolean = UpCastRule.canUpCast(from, to)

  /**
   * Returns true iff we can cast the `from` type to `to` type as per the ANSI SQL.
   * In practice, the behavior is mostly the same as PostgreSQL. It disallows certain unreasonable
   * type conversions such as converting `string` to `int` or `double` to `boolean`.
   */
  def canANSIStoreAssign(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if from == to => true
    case (NullType, _) => true
    case (_: NumericType, _: NumericType) => true
    case (_: AtomicType, _: StringType) => true
    case (_: CalendarIntervalType, _: StringType) => true
    case (_: DatetimeType, _: DatetimeType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      resolvableNullability(fn, tn) && canANSIStoreAssign(fromType, toType)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      resolvableNullability(fn, tn) && canANSIStoreAssign(fromKey, toKey) &&
        canANSIStoreAssign(fromValue, toValue)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (f1, f2) =>
            resolvableNullability(f1.nullable, f2.nullable) &&
              canANSIStoreAssign(f1.dataType, f2.dataType)
        }

    case _ => false
  }

  def canNullSafeCastToDecimal(from: DataType, to: DecimalType): Boolean = from match {
    case from: BooleanType if to.isWiderThan(DecimalType.BooleanDecimal) => true
    case from: NumericType if to.isWiderThan(from) => true
    case from: DecimalType =>
      // truncating or precision lose
      (to.precision - to.scale) > (from.precision - from.scale)
    case _ => false  // overflow
  }

  /**
   * Returns `true` if casting non-nullable values from `from` type to `to` type
   * may return null. Note that the caller side should take care of input nullability
   * first and only call this method if the input is not nullable.
   */
  def forceNullable(from: DataType, to: DataType): Boolean = (from, to) match {
    case (NullType, _) => false // empty array or map case
    case (_, _) if from == to => false
    case (VariantType, _) => true

    case (_: StringType, BinaryType | _: StringType) => false
    case (_: StringType, _) => true
    case (_, _: StringType) => false

    case (TimestampType, ByteType | ShortType | IntegerType) => true
    case (FloatType | DoubleType, TimestampType) => true
    case (TimestampType, DateType) => false
    case (_, DateType) => true
    case (DateType, TimestampType) => false
    case (DateType, _) => true
    case (_, CalendarIntervalType) => true

    case (_, to: DecimalType) if !canNullSafeCastToDecimal(from, to) => true
    case (_: FractionalType, _: IntegralType) => true  // NaN, infinity
    case _ => false
  }

  def resolvableNullability(from: Boolean, to: Boolean): Boolean = !from || to

  /**
   * We process literals such as 'Infinity', 'Inf', '-Infinity' and 'NaN' etc in case
   * insensitive manner to be compatible with other database systems such as PostgreSQL and DB2.
   */
  def processFloatingPointSpecialLiterals(v: String, isFloat: Boolean): Any = {
    v.trim.toLowerCase(Locale.ROOT) match {
      case "inf" | "+inf" | "infinity" | "+infinity" =>
        if (isFloat) Float.PositiveInfinity else Double.PositiveInfinity
      case "-inf" | "-infinity" =>
        if (isFloat) Float.NegativeInfinity else Double.NegativeInfinity
      case "nan" =>
        if (isFloat) Float.NaN else Double.NaN
      case _ => null
    }
  }

  def typeCheckFailureMessage(
      from: DataType,
      to: DataType,
      fallbackConf: Option[(String, String)]): DataTypeMismatch = {
    def withFunSuggest(names: String*): DataTypeMismatch = {
      DataTypeMismatch(
        errorSubClass = "CAST_WITH_FUNC_SUGGESTION",
        messageParameters = Map(
          "srcType" -> toSQLType(from),
          "targetType" -> toSQLType(to),
          "functionNames" -> names.map(toSQLId).mkString("/")))
    }
    (from, to) match {
      case (_: NumericType, TimestampType) =>
        withFunSuggest("TIMESTAMP_SECONDS", "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS")

      case (TimestampType, _: NumericType) =>
        withFunSuggest("UNIX_SECONDS", "UNIX_MILLIS", "UNIX_MICROS")

      case (_: NumericType, DateType) =>
        withFunSuggest("DATE_FROM_UNIX_DATE")

      case (DateType, _: NumericType) =>
        withFunSuggest("UNIX_DATE")

      case _ if fallbackConf.isDefined && Cast.canCast(from, to) =>
        DataTypeMismatch(
          errorSubClass = "CAST_WITH_CONF_SUGGESTION",
          messageParameters = Map(
            "srcType" -> toSQLType(from),
            "targetType" -> toSQLType(to),
            "config" -> toSQLConf(fallbackConf.get._1),
            "configVal" -> toSQLValue(fallbackConf.get._2, StringType)))

      case _ =>
        DataTypeMismatch(
          errorSubClass = "CAST_WITHOUT_SUGGESTION",
          messageParameters = Map(
            "srcType" -> toSQLType(from),
            "targetType" -> toSQLType(to)))
    }
  }

  // The function arguments are: `input`, `result` and `resultIsNull`. We don't need `inputIsNull`
  // in parameter list, because the returned code will be put in null safe evaluation region.
  type CastFunction = (ExprValue, ExprValue, ExprValue) => Block

  def apply(
      child: Expression,
      dataType: DataType,
      ansiEnabled: Boolean): Cast =
    Cast(child, dataType, None, EvalMode.fromBoolean(ansiEnabled))

  def apply(
      child: Expression,
      dataType: DataType,
      timeZoneId: Option[String],
      ansiEnabled: Boolean): Cast =
    Cast(child, dataType, timeZoneId, EvalMode.fromBoolean(ansiEnabled))
}

/**
 * Cast the child expression to the target data type.
 *
 * When cast from/to timezone related types, we need timeZoneId, which will be resolved with
 * session local timezone by an analyzer [[ResolveTimeZone]].
 */
@ExpressionDescription(
  usage = "_FUNC_(expr AS type) - Casts the value `expr` to the target data type `type`." +
          " `expr` :: `type` alternative casting syntax is also supported.",
  examples = """
    Examples:
      > SELECT _FUNC_('10' as int);
       10
      > SELECT '10' :: int;
       10
  """,
  since = "1.0.0",
  group = "conversion_funcs")
case class Cast(
    child: Expression,
    dataType: DataType,
    timeZoneId: Option[String] = None,
    evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get))
  extends UnaryExpression
  with TimeZoneAwareExpression
  with ToStringBase
  with NullIntolerant
  with SupportQueryContext
  with QueryErrorsBase {

  def this(child: Expression, dataType: DataType, timeZoneId: Option[String]) =
    this(child, dataType, timeZoneId, evalMode = EvalMode.fromSQLConf(SQLConf.get))

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def withNewChildInternal(newChild: Expression): Cast = copy(child = newChild)

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(CAST)

  def ansiEnabled: Boolean = {
    evalMode == EvalMode.ANSI || (evalMode == EvalMode.TRY && !canUseLegacyCastForTryCast)
  }

  // Whether this expression is used for `try_cast()`.
  def isTryCast: Boolean = {
    evalMode == EvalMode.TRY
  }

  private def typeCheckFailureInCast: DataTypeMismatch = evalMode match {
    case EvalMode.ANSI =>
      if (getTagValue(Cast.BY_TABLE_INSERTION).isDefined) {
        Cast.typeCheckFailureMessage(child.dataType, dataType,
          Some(SQLConf.STORE_ASSIGNMENT_POLICY.key ->
            SQLConf.StoreAssignmentPolicy.LEGACY.toString))
      } else {
        Cast.typeCheckFailureMessage(child.dataType, dataType,
          Some(SQLConf.ANSI_ENABLED.key -> "false"))
      }
    case EvalMode.TRY =>
      Cast.typeCheckFailureMessage(child.dataType, dataType, None)
    case _ =>
      DataTypeMismatch(
        errorSubClass = "CAST_WITHOUT_SUGGESTION",
        messageParameters = Map(
          "srcType" -> toSQLType(child.dataType),
          "targetType" -> toSQLType(dataType)))
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val canCast = evalMode match {
      case EvalMode.LEGACY => Cast.canCast(child.dataType, dataType)
      case EvalMode.ANSI => Cast.canAnsiCast(child.dataType, dataType)
      case EvalMode.TRY => Cast.canTryCast(child.dataType, dataType)
      case other => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3232",
        messageParameters = Map("other" -> other.toString))
    }
    if (canCast) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      typeCheckFailureInCast
    }
  }

  override def nullable: Boolean = if (!isTryCast) {
    child.nullable || Cast.forceNullable(child.dataType, dataType)
  } else {
    (child.dataType, dataType) match {
      case (_: StringType, BinaryType) => child.nullable
      // TODO: Implement a more accurate method for checking whether a decimal value can be cast
      //       as integral types without overflow. Currently, the cast can overflow even if
      //       "Cast.canUpCast" method returns true.
      case (_: DecimalType, _: IntegralType) => true
      case _ => child.nullable || !Cast.canUpCast(child.dataType, dataType)
    }
  }

  override def initQueryContext(): Option[QueryContext] = if (ansiEnabled) {
    Some(origin.context)
  } else {
    None
  }

  // When this cast involves TimeZone, it's only resolved if the timeZoneId is set;
  // Otherwise behave like Expression.resolved.
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && (!needsTimeZone || timeZoneId.isDefined)

  override lazy val canonicalized: Expression = {
    val basic = withNewChildren(Seq(child.canonicalized)).asInstanceOf[Cast]
    if (timeZoneId.isDefined && !needsTimeZone) {
      basic.withTimeZone(null)
    } else {
      basic
    }
  }

  def needsTimeZone: Boolean = Cast.needsTimeZone(child.dataType, dataType)

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline protected[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  private val legacyCastToStr = SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)

  protected val (leftBracket, rightBracket) = if (legacyCastToStr) ("[", "]") else ("{", "}")

  override protected def nullString: String = if (legacyCastToStr) "" else "null"

  // In ANSI mode, Spark always use plain string representation on casting Decimal values
  // as strings. Otherwise, the casting is using `BigDecimal.toString` which may use scientific
  // notation if an exponent is needed.
  override protected def useDecimalPlainString: Boolean = ansiEnabled

  // The class name of `DateTimeUtils`
  protected def dateTimeUtilsCls: String = DateTimeUtils.getClass.getName.stripSuffix("$")

  // BinaryConverter
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case _: StringType => buildCast[UTF8String](_, _.getBytes)
    case ByteType => buildCast[Byte](_, NumberConverter.toBinary)
    case ShortType => buildCast[Short](_, NumberConverter.toBinary)
    case IntegerType => buildCast[Int](_, NumberConverter.toBinary)
    case LongType => buildCast[Long](_, NumberConverter.toBinary)
  }

  // UDFToBoolean
  private[this] def castToBoolean(from: DataType): Any => Any = from match {
    case _: StringType =>
      buildCast[UTF8String](_, s => {
        if (StringUtils.isTrueString(s)) {
          true
        } else if (StringUtils.isFalseString(s)) {
          false
        } else {
          if (ansiEnabled) {
            throw QueryExecutionErrors.invalidInputSyntaxForBooleanError(s, getContextOrNull())
          } else {
            null
          }
        }
      })
    case TimestampType =>
      buildCast[Long](_, t => t != 0)
    case DateType =>
      // Hive would return null when cast from date to boolean
      buildCast[Int](_, d => null)
    case LongType =>
      buildCast[Long](_, _ != 0)
    case IntegerType =>
      buildCast[Int](_, _ != 0)
    case ShortType =>
      buildCast[Short](_, _ != 0)
    case ByteType =>
      buildCast[Byte](_, _ != 0)
    case DecimalType() =>
      buildCast[Decimal](_, !_.isZero)
    case DoubleType =>
      buildCast[Double](_, _ != 0)
    case FloatType =>
      buildCast[Float](_, _ != 0)
  }

  // TimestampConverter
  private[this] def castToTimestamp(from: DataType): Any => Any = from match {
    case _: StringType =>
      buildCast[UTF8String](_, utfs => {
        if (ansiEnabled) {
          DateTimeUtils.stringToTimestampAnsi(utfs, zoneId, getContextOrNull())
        } else {
          DateTimeUtils.stringToTimestamp(utfs, zoneId).orNull
        }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case LongType =>
      buildCast[Long](_, l => longToTimestamp(l))
    case IntegerType =>
      buildCast[Int](_, i => longToTimestamp(i.toLong))
    case ShortType =>
      buildCast[Short](_, s => longToTimestamp(s.toLong))
    case ByteType =>
      buildCast[Byte](_, b => longToTimestamp(b.toLong))
    case DateType =>
      buildCast[Int](_, d => daysToMicros(d, zoneId))
    case TimestampNTZType =>
      buildCast[Long](_, ts => convertTz(ts, zoneId, ZoneOffset.UTC))
    // TimestampWritable.decimalToTimestamp
    case DecimalType() =>
      buildCast[Decimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      if (ansiEnabled) {
        buildCast[Double](_, d => doubleToTimestampAnsi(d, getContextOrNull()))
      } else {
        buildCast[Double](_, d => doubleToTimestamp(d))
      }
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      if (ansiEnabled) {
        buildCast[Float](_, f => doubleToTimestampAnsi(f.toDouble, getContextOrNull()))
      } else {
        buildCast[Float](_, f => doubleToTimestamp(f.toDouble))
      }
  }

  private[this] def castToTimestampNTZ(from: DataType): Any => Any = from match {
    case _: StringType =>
      buildCast[UTF8String](_, utfs => {
        if (ansiEnabled) {
          DateTimeUtils.stringToTimestampWithoutTimeZoneAnsi(utfs, getContextOrNull())
        } else {
          DateTimeUtils.stringToTimestampWithoutTimeZone(utfs).orNull
        }
      })
    case DateType =>
      buildCast[Int](_, d => daysToMicros(d, ZoneOffset.UTC))
    case TimestampType =>
      buildCast[Long](_, ts => convertTz(ts, ZoneOffset.UTC, zoneId))
  }

  private[this] def decimalToTimestamp(d: Decimal): Long = {
    (d.toBigDecimal * MICROS_PER_SECOND).longValue
  }
  private[this] def doubleToTimestamp(d: Double): Any = {
    if (d.isNaN || d.isInfinite) null else (d * MICROS_PER_SECOND).toLong
  }

  // converting seconds to us
  private[this] def longToTimestamp(t: Long): Long = SECONDS.toMicros(t)
  // converting us to seconds
  private[this] def timestampToLong(ts: Long): Long = {
    Math.floorDiv(ts, MICROS_PER_SECOND)
  }
  // converting us to seconds in double
  private[this] def timestampToDouble(ts: Long): Double = {
    ts / MICROS_PER_SECOND.toDouble
  }

  // DateConverter
  private[this] def castToDate(from: DataType): Any => Any = from match {
    case _: StringType =>
      if (ansiEnabled) {
        buildCast[UTF8String](_, s => DateTimeUtils.stringToDateAnsi(s, getContextOrNull()))
      } else {
        buildCast[UTF8String](_, s => DateTimeUtils.stringToDate(s).orNull)
      }
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Long](_, t => microsToDays(t, zoneId))
    case TimestampNTZType =>
      buildCast[Long](_, t => microsToDays(t, ZoneOffset.UTC))
  }

  // IntervalConverter
  private[this] def castToInterval(from: DataType): Any => Any = from match {
    case _: StringType =>
      buildCast[UTF8String](_, s => IntervalUtils.safeStringToInterval(s))
  }

  private[this] def castToDayTimeInterval(
      from: DataType,
      it: DayTimeIntervalType): Any => Any = from match {
    case _: StringType => buildCast[UTF8String](_, s =>
      IntervalUtils.castStringToDTInterval(s, it.startField, it.endField))
    case _: DayTimeIntervalType => buildCast[Long](_, s =>
      IntervalUtils.durationToMicros(IntervalUtils.microsToDuration(s), it.endField))
    case x: IntegralType =>
      if (x == LongType) {
        b => IntervalUtils.longToDayTimeInterval(
          PhysicalIntegralType.integral(x).toLong(b), it.startField, it.endField)
      } else {
        b => IntervalUtils.intToDayTimeInterval(
          PhysicalIntegralType.integral(x).toInt(b), it.startField, it.endField)
      }
    case DecimalType.Fixed(p, s) =>
      buildCast[Decimal](_, d =>
        IntervalUtils.decimalToDayTimeInterval(d, p, s, it.startField, it.endField))
  }

  private[this] def castToYearMonthInterval(
      from: DataType,
      it: YearMonthIntervalType): Any => Any = from match {
    case _: StringType => buildCast[UTF8String](_, s =>
      IntervalUtils.castStringToYMInterval(s, it.startField, it.endField))
    case _: YearMonthIntervalType => buildCast[Int](_, s =>
      IntervalUtils.periodToMonths(IntervalUtils.monthsToPeriod(s), it.endField))
    case x: IntegralType =>
      if (x == LongType) {
        b => IntervalUtils.longToYearMonthInterval(
          PhysicalIntegralType.integral(x).toLong(b), it.startField, it.endField)
      } else {
        b => IntervalUtils.intToYearMonthInterval(
          PhysicalIntegralType.integral(x).toInt(b), it.startField, it.endField)
      }
    case DecimalType.Fixed(p, s) =>
      buildCast[Decimal](_, d =>
        IntervalUtils.decimalToYearMonthInterval(d, p, s, it.startField, it.endField))
  }

  // LongConverter
  private[this] def castToLong(from: DataType): Any => Any = from match {
    case _: StringType if ansiEnabled =>
      buildCast[UTF8String](_, v => UTF8StringUtils.toLongExact(v, getContextOrNull()))
    case _: StringType =>
      val result = new LongWrapper()
      buildCast[UTF8String](_, s => if (s.toLong(result)) result.value else null)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t))
    case x: NumericType if ansiEnabled =>
      val exactNumeric = PhysicalNumericType.exactNumeric(x)
      b => exactNumeric.toLong(b)
    case x: NumericType =>
      val numeric = PhysicalNumericType.numeric(x)
      b => numeric.toLong(b)
    case x: DayTimeIntervalType =>
      buildCast[Long](_, i => dayTimeIntervalToLong(i, x.startField, x.endField))
    case x: YearMonthIntervalType =>
      buildCast[Int](_, i => yearMonthIntervalToInt(i, x.startField, x.endField).toLong)
  }

  private def errorOrNull(t: Any, from: DataType, to: DataType) = {
    if (ansiEnabled) {
      throw QueryExecutionErrors.castingCauseOverflowError(t, from, to)
    } else {
      null
    }
  }

  // IntConverter
  private[this] def castToInt(from: DataType): Any => Any = from match {
    case _: StringType if ansiEnabled =>
      buildCast[UTF8String](_, v => UTF8StringUtils.toIntExact(v, getContextOrNull()))
    case _: StringType =>
      val result = new IntWrapper()
      buildCast[UTF8String](_, s => if (s.toInt(result)) result.value else null)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => {
        val longValue = timestampToLong(t)
        if (longValue == longValue.toInt) {
          longValue.toInt
        } else {
          errorOrNull(t, from, IntegerType)
        }
      })
    case x: NumericType if ansiEnabled =>
      val exactNumeric = PhysicalNumericType.exactNumeric(x)
      b => exactNumeric.toInt(b)
    case x: NumericType =>
      val numeric = PhysicalNumericType.numeric(x)
      b => numeric.toInt(b)
    case x: DayTimeIntervalType =>
      buildCast[Long](_, i => dayTimeIntervalToInt(i, x.startField, x.endField))
    case x: YearMonthIntervalType =>
      buildCast[Int](_, i => yearMonthIntervalToInt(i, x.startField, x.endField))
  }

  // ShortConverter
  private[this] def castToShort(from: DataType): Any => Any = from match {
    case _: StringType if ansiEnabled =>
      buildCast[UTF8String](_, v => UTF8StringUtils.toShortExact(v, getContextOrNull()))
    case _: StringType =>
      val result = new IntWrapper()
      buildCast[UTF8String](_, s => if (s.toShort(result)) {
        result.value.toShort
      } else {
        null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toShort else 0.toShort)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => {
        val longValue = timestampToLong(t)
        if (longValue == longValue.toShort) {
          longValue.toShort
        } else {
          errorOrNull(t, from, ShortType)
        }
      })
    case x: NumericType if ansiEnabled =>
      val exactNumeric = PhysicalNumericType.exactNumeric(x)
      b =>
        val intValue = try {
          exactNumeric.toInt(b)
        } catch {
          case _: ArithmeticException =>
            throw QueryExecutionErrors.castingCauseOverflowError(b, from, ShortType)
        }
        if (intValue == intValue.toShort) {
          intValue.toShort
        } else {
          throw QueryExecutionErrors.castingCauseOverflowError(b, from, ShortType)
        }
    case x: NumericType =>
      val numeric = PhysicalNumericType.numeric(x)
      b => numeric.toInt(b).toShort
    case x: DayTimeIntervalType =>
      buildCast[Long](_, i => dayTimeIntervalToShort(i, x.startField, x.endField))
    case x: YearMonthIntervalType =>
      buildCast[Int](_, i => yearMonthIntervalToShort(i, x.startField, x.endField))
  }

  // ByteConverter
  private[this] def castToByte(from: DataType): Any => Any = from match {
    case _: StringType if ansiEnabled =>
      buildCast[UTF8String](_, v => UTF8StringUtils.toByteExact(v, getContextOrNull()))
    case _: StringType =>
      val result = new IntWrapper()
      buildCast[UTF8String](_, s => if (s.toByte(result)) {
        result.value.toByte
      } else {
        null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toByte else 0.toByte)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => {
        val longValue = timestampToLong(t)
        if (longValue == longValue.toByte) {
          longValue.toByte
        } else {
          errorOrNull(t, from, ByteType)
        }
      })
    case x: NumericType if ansiEnabled =>
      val exactNumeric = PhysicalNumericType.exactNumeric(x)
      b =>
        val intValue = try {
          exactNumeric.toInt(b)
        } catch {
          case _: ArithmeticException =>
            throw QueryExecutionErrors.castingCauseOverflowError(b, from, ByteType)
        }
        if (intValue == intValue.toByte) {
          intValue.toByte
        } else {
          throw QueryExecutionErrors.castingCauseOverflowError(b, from, ByteType)
        }
    case x: NumericType =>
      val numeric = PhysicalNumericType.numeric(x)
      b => numeric.toInt(b).toByte
    case x: DayTimeIntervalType =>
      buildCast[Long](_, i => dayTimeIntervalToByte(i, x.startField, x.endField))
    case x: YearMonthIntervalType =>
      buildCast[Int](_, i => yearMonthIntervalToByte(i, x.startField, x.endField))
  }

  /**
   * Change the precision / scale in a given decimal to those set in `decimalType` (if any),
   * modifying `value` in-place and returning it if successful. If an overflow occurs, it
   * either returns null or throws an exception according to the value set for
   * `spark.sql.ansi.enabled`.
   *
   * NOTE: this modifies `value` in-place, so don't call it on external data.
   */
  private[this] def changePrecision(value: Decimal, decimalType: DecimalType): Decimal = {
    changePrecision(value, decimalType, !ansiEnabled)
  }

  private[this] def changePrecision(
      value: Decimal,
      decimalType: DecimalType,
      nullOnOverflow: Boolean): Decimal = {
    if (value.changePrecision(decimalType.precision, decimalType.scale)) {
      value
    } else {
      if (nullOnOverflow) {
        null
      } else {
        throw QueryExecutionErrors.cannotChangeDecimalPrecisionError(
          value, decimalType.precision, decimalType.scale, getContextOrNull())
      }
    }
  }

  /**
   * Create new `Decimal` with precision and scale given in `decimalType` (if any).
   * If overflow occurs, if `spark.sql.ansi.enabled` is false, null is returned;
   * otherwise, an `ArithmeticException` is thrown.
   */
  private[this] def toPrecision(
      value: Decimal,
      decimalType: DecimalType,
      context: QueryContext): Decimal =
    value.toPrecision(
      decimalType.precision, decimalType.scale, Decimal.ROUND_HALF_UP, !ansiEnabled, context)


  private[this] def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case _: StringType if !ansiEnabled =>
      buildCast[UTF8String](_, s => {
        val d = Decimal.fromString(s)
        if (d == null) null else changePrecision(d, target)
      })
    case _: StringType if ansiEnabled =>
      buildCast[UTF8String](_,
        s => changePrecision(Decimal.fromStringANSI(s, target, getContextOrNull()), target))
    case BooleanType =>
      buildCast[Boolean](_,
        b => toPrecision(if (b) Decimal.ONE else Decimal.ZERO, target, getContextOrNull()))
    case DateType =>
      buildCast[Int](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Long](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
    case dt: DecimalType =>
      b => toPrecision(b.asInstanceOf[Decimal], target, getContextOrNull())
    case t: IntegralType =>
      b => changePrecision(Decimal(PhysicalIntegralType.integral(t).toLong(b)), target)
    case x: FractionalType =>
      val fractional = PhysicalFractionalType.fractional(x)
      b => try {
        changePrecision(Decimal(fractional.toDouble(b)), target)
      } catch {
        case _: NumberFormatException => null
      }
    case x: DayTimeIntervalType =>
      buildCast[Long](_, dt =>
        changePrecision(
          value = dayTimeIntervalToDecimal(dt, x.endField),
          decimalType = target,
          nullOnOverflow = false))
    case x: YearMonthIntervalType =>
      buildCast[Int](_, ym =>
        changePrecision(
          value = Decimal(yearMonthIntervalToInt(ym, x.startField, x.endField)),
          decimalType = target,
          nullOnOverflow = false))
  }

  // DoubleConverter
  private[this] def castToDouble(from: DataType): Any => Any = from match {
    case _: StringType =>
      buildCast[UTF8String](_, s => {
        val doubleStr = s.toString
        try doubleStr.toDouble catch {
          case _: NumberFormatException =>
            val d = Cast.processFloatingPointSpecialLiterals(doubleStr, false)
            if(ansiEnabled && d == null) {
              throw QueryExecutionErrors.invalidInputInCastToNumberError(
                DoubleType, s, getContextOrNull())
            } else {
              d
            }
        }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t))
    case x: NumericType =>
      val numeric = PhysicalNumericType.numeric(x)
      b => numeric.toDouble(b)
  }

  // FloatConverter
  private[this] def castToFloat(from: DataType): Any => Any = from match {
    case _: StringType =>
      buildCast[UTF8String](_, s => {
        val floatStr = s.toString
        try floatStr.toFloat catch {
          case _: NumberFormatException =>
            val f = Cast.processFloatingPointSpecialLiterals(floatStr, true)
            if (ansiEnabled && f == null) {
              throw QueryExecutionErrors.invalidInputInCastToNumberError(
                FloatType, s, getContextOrNull())
            } else {
              f
            }
        }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t).toFloat)
    case x: NumericType =>
      val numeric = PhysicalNumericType.numeric(x)
      b => numeric.toFloat(b)
  }

  private[this] def castArray(fromType: DataType, toType: DataType): Any => Any = {
    val elementCast = cast(fromType, toType)
    // TODO: Could be faster?
    buildCast[ArrayData](_, array => {
      val values = new Array[Any](array.numElements())
      array.foreach(fromType, (i, e) => {
        if (e == null) {
          values(i) = null
        } else {
          values(i) = elementCast(e)
        }
      })
      new GenericArrayData(values)
    })
  }

  private[this] def castMap(from: MapType, to: MapType): Any => Any = {
    val keyCast = castArray(from.keyType, to.keyType)
    val valueCast = castArray(from.valueType, to.valueType)
    buildCast[MapData](_, map => {
      val keys = keyCast(map.keyArray()).asInstanceOf[ArrayData]
      val values = valueCast(map.valueArray()).asInstanceOf[ArrayData]
      new ArrayBasedMapData(keys, values)
    })
  }

  private[this] def castStruct(from: StructType, to: StructType): Any => Any = {
    val castFuncs: Array[(Any) => Any] = from.fields.zip(to.fields).map {
      case (fromField, toField) => cast(fromField.dataType, toField.dataType)
    }
    // TODO: Could be faster?
    buildCast[InternalRow](_, row => {
      val newRow = new GenericInternalRow(from.fields.length)
      var i = 0
      while (i < row.numFields) {
        newRow.update(i,
          if (row.isNullAt(i)) null else castFuncs(i)(row.get(i, from.apply(i).dataType)))
        i += 1
      }
      newRow
    })
  }

  private def castInternal(from: DataType, to: DataType): Any => Any = {
    // If the cast does not change the structure, then we don't really need to cast anything.
    // We can return what the children return. Same thing should happen in the codegen path.
    if (DataType.equalsStructurally(from, to)) {
      identity
    } else if (from == NullType) {
      // According to `canCast`, NullType can be casted to any type.
      // For primitive types, we don't reach here because the guard of `nullSafeEval`.
      // But for nested types like struct, we might reach here for nested null type field.
      // We won't call the returned function actually, but returns a placeholder.
      _ => throw QueryExecutionErrors.cannotCastFromNullTypeError(to)
    } else if (from.isInstanceOf[VariantType]) {
      buildCast[VariantVal](_, v => {
        variant.VariantGet.cast(v, to, evalMode != EvalMode.TRY, timeZoneId, zoneId)
      })
    } else {
      to match {
        case dt if dt == from => identity[Any]
        case VariantType => input => variant.VariantExpressionEvalUtils.castToVariant(input, from)
        case _: StringType => castToString(from)
        case BinaryType => castToBinary(from)
        case DateType => castToDate(from)
        case decimal: DecimalType => castToDecimal(from, decimal)
        case TimestampType => castToTimestamp(from)
        case TimestampNTZType => castToTimestampNTZ(from)
        case CalendarIntervalType => castToInterval(from)
        case it: DayTimeIntervalType => castToDayTimeInterval(from, it)
        case it: YearMonthIntervalType => castToYearMonthInterval(from, it)
        case BooleanType => castToBoolean(from)
        case ByteType => castToByte(from)
        case ShortType => castToShort(from)
        case IntegerType => castToInt(from)
        case FloatType => castToFloat(from)
        case LongType => castToLong(from)
        case DoubleType => castToDouble(from)
        case array: ArrayType =>
          castArray(from.asInstanceOf[ArrayType].elementType, array.elementType)
        case map: MapType => castMap(from.asInstanceOf[MapType], map)
        case struct: StructType => castStruct(from.asInstanceOf[StructType], struct)
        case udt: UserDefinedType[_] if udt.acceptsType(from) =>
          identity[Any]
        case _: UserDefinedType[_] =>
          throw QueryExecutionErrors.cannotCastError(from, to)
      }
    }
  }

  private def cast(from: DataType, to: DataType): Any => Any = {
    if (!isTryCast || canUseLegacyCastForTryCast) {
      castInternal(from, to)
    } else {
      (input: Any) =>
        try {
          castInternal(from, to)(input)
        } catch {
          case _: Exception =>
            null
        }
    }
  }

  // Whether Spark SQL can evaluation the try_cast as the legacy cast, so that no `try...catch`
  // is needed and the performance can be faster.
  private lazy val canUseLegacyCastForTryCast: Boolean = {
    if (!child.resolved) {
      false
    } else {
      (child.dataType, dataType) match {
        case (_: StringType, _: FractionalType) => true
        case (_: StringType, _: DatetimeType) => true
        case _ => false
      }
    }
  }

  protected[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

  protected override def nullSafeEval(input: Any): Any = cast(input)

  override def genCode(ctx: CodegenContext): ExprCode = {
    // If the cast does not change the structure, then we don't really need to cast anything.
    // We can return what the children return. Same thing should happen in the interpreted path.
    if (DataType.equalsStructurally(child.dataType, dataType)) {
      child.genCode(ctx)
    } else {
      super.genCode(ctx)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val nullSafeCast = nullSafeCastFunction(child.dataType, dataType, ctx)

    ev.copy(code = eval.code +
      castCode(ctx, eval.value, eval.isNull, ev.value, ev.isNull, dataType, nullSafeCast))
  }

  private[this] def nullSafeCastFunction(
      from: DataType,
      to: DataType,
      ctx: CodegenContext): Cast.CastFunction = to match {

    case _ if from == NullType => (c, evPrim, evNull) => code"$evNull = true;"
    case _ if to == from => (c, evPrim, evNull) => code"$evPrim = $c;"
    case _ if from.isInstanceOf[VariantType] => (c, evPrim, evNull) =>
      val tmp = ctx.freshVariable("tmp", classOf[Object])
      val dataTypeArg = ctx.addReferenceObj("dataType", to)
      val zoneStrArg = ctx.addReferenceObj("zoneStr", timeZoneId)
      val zoneIdArg = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
      val failOnError = evalMode != EvalMode.TRY
      val cls = classOf[variant.VariantGet].getName
      code"""
        Object $tmp = $cls.cast($c, $dataTypeArg, $failOnError, $zoneStrArg, $zoneIdArg);
        if ($tmp == null) {
          $evNull = true;
        } else {
          $evPrim = (${CodeGenerator.boxedType(to)})$tmp;
        }
      """
    case VariantType =>
      val cls = variant.VariantExpressionEvalUtils.getClass.getName.stripSuffix("$")
      val fromArg = ctx.addReferenceObj("from", from)
      (c, evPrim, evNull) => code"$evPrim = $cls.castToVariant($c, $fromArg);"
    case _: StringType => (c, evPrim, _) => castToStringCode(from, ctx).apply(c, evPrim)
    case BinaryType => castToBinaryCode(from)
    case DateType => castToDateCode(from, ctx)
    case decimal: DecimalType => castToDecimalCode(from, decimal, ctx)
    case TimestampType => castToTimestampCode(from, ctx)
    case TimestampNTZType => castToTimestampNTZCode(from, ctx)
    case CalendarIntervalType => castToIntervalCode(from)
    case it: DayTimeIntervalType => castToDayTimeIntervalCode(from, it)
    case it: YearMonthIntervalType => castToYearMonthIntervalCode(from, it)
    case BooleanType => castToBooleanCode(from, ctx)
    case ByteType => castToByteCode(from, ctx)
    case ShortType => castToShortCode(from, ctx)
    case IntegerType => castToIntCode(from, ctx)
    case FloatType => castToFloatCode(from, ctx)
    case LongType => castToLongCode(from, ctx)
    case DoubleType => castToDoubleCode(from, ctx)

    case array: ArrayType =>
      castArrayCode(from.asInstanceOf[ArrayType].elementType, array.elementType, ctx)
    case map: MapType => castMapCode(from.asInstanceOf[MapType], map, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
    case udt: UserDefinedType[_] if udt.acceptsType(from) =>
      (c, evPrim, evNull) => code"$evPrim = $c;"
    case _: UserDefinedType[_] =>
      throw QueryExecutionErrors.cannotCastError(from, to)
  }

  // Since we need to cast input expressions recursively inside ComplexTypes, such as Map's
  // Key and Value, Struct's field, we need to name out all the variable names involved in a cast.
  protected[this] def castCode(
      ctx: CodegenContext,
      input: ExprValue,
      inputIsNull: ExprValue,
      result: ExprValue,
      resultIsNull: ExprValue,
      resultType: DataType,
      cast: Cast.CastFunction): Block = {
    val javaType = JavaCode.javaType(resultType)
    val castCodeWithTryCatchIfNeeded = if (!isTryCast || canUseLegacyCastForTryCast) {
      s"${cast(input, result, resultIsNull)}"
    } else {
      s"""
         |try {
         |  ${cast(input, result, resultIsNull)}
         |} catch (Exception e) {
         |  $resultIsNull = true;
         |}
         |""".stripMargin
    }
    code"""
      boolean $resultIsNull = $inputIsNull;
      $javaType $result = ${CodeGenerator.defaultValue(resultType)};
      if (!$inputIsNull) {
        $castCodeWithTryCatchIfNeeded
      }
    """
  }

  private[this] def castToBinaryCode(from: DataType): Cast.CastFunction = from match {
    case _: StringType =>
      (c, evPrim, evNull) =>
        code"$evPrim = $c.getBytes();"
    case _: IntegralType =>
      (c, evPrim, evNull) =>
        code"$evPrim = ${NumberConverter.getClass.getName.stripSuffix("$")}.toBinary($c);"
  }

  private[this] def castToDateCode(
      from: DataType,
      ctx: CodegenContext): Cast.CastFunction = {
    from match {
      case _: StringType =>
        val intOpt = ctx.freshVariable("intOpt", classOf[Option[Integer]])
        (c, evPrim, evNull) =>
          if (ansiEnabled) {
            val errorContext = getContextOrNullCode(ctx)
            code"""
              $evPrim = $dateTimeUtilsCls.stringToDateAnsi($c, $errorContext);
            """
          } else {
            code"""
              scala.Option<Integer> $intOpt =
                org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToDate($c);
              if ($intOpt.isDefined()) {
                $evPrim = ((Integer) $intOpt.get()).intValue();
              } else {
                $evNull = true;
              }
            """
          }

      case TimestampType =>
        val zidClass = classOf[ZoneId]
        val zid = JavaCode.global(ctx.addReferenceObj("zoneId", zoneId, zidClass.getName), zidClass)
        (c, evPrim, evNull) =>
          code"""$evPrim =
            org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToDays($c, $zid);"""
      case TimestampNTZType =>
        (c, evPrim, evNull) =>
          code"$evPrim = $dateTimeUtilsCls.microsToDays($c, java.time.ZoneOffset.UTC);"
      case _ =>
        (c, evPrim, evNull) => code"$evNull = true;"
    }
  }

  private[this] def changePrecision(
      d: ExprValue,
      decimalType: DecimalType,
      evPrim: ExprValue,
      evNull: ExprValue,
      canNullSafeCast: Boolean,
      ctx: CodegenContext,
      nullOnOverflow: Boolean): Block = {
    if (canNullSafeCast) {
      code"""
         |$d.changePrecision(${decimalType.precision}, ${decimalType.scale});
         |$evPrim = $d;
       """.stripMargin
    } else {
      val errorContextCode = getContextOrNullCode(ctx, !nullOnOverflow)
      val overflowCode = if (nullOnOverflow) {
        s"$evNull = true;"
      } else {
        s"""
           |throw QueryExecutionErrors.cannotChangeDecimalPrecisionError(
           |  $d, ${decimalType.precision}, ${decimalType.scale}, $errorContextCode);
         """.stripMargin
      }
      code"""
         |if ($d.changePrecision(${decimalType.precision}, ${decimalType.scale})) {
         |  $evPrim = $d;
         |} else {
         |  $overflowCode
         |}
       """.stripMargin
    }
  }

  private[this] def changePrecision(
      d: ExprValue,
      decimalType: DecimalType,
      evPrim: ExprValue,
      evNull: ExprValue,
      canNullSafeCast: Boolean,
      ctx: CodegenContext): Block = {
    changePrecision(d, decimalType, evPrim, evNull, canNullSafeCast, ctx, !ansiEnabled)
  }

  private[this] def castToDecimalCode(
      from: DataType,
      target: DecimalType,
      ctx: CodegenContext): Cast.CastFunction = {
    val tmp = ctx.freshVariable("tmpDecimal", classOf[Decimal])
    val canNullSafeCast = Cast.canNullSafeCastToDecimal(from, target)
    from match {
      case _: StringType if !ansiEnabled =>
        (c, evPrim, evNull) =>
          code"""
              Decimal $tmp = Decimal.fromString($c);
              if ($tmp == null) {
                $evNull = true;
              } else {
                ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
              }
          """
      case _: StringType if ansiEnabled =>
        val errorContext = getContextOrNullCode(ctx)
        val toType = ctx.addReferenceObj("toType", target)
        (c, evPrim, evNull) =>
          code"""
              Decimal $tmp = Decimal.fromStringANSI($c, $toType, $errorContext);
              ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
          """
      case BooleanType =>
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = $c ? Decimal.apply(1) : Decimal.apply(0);
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
          """
      case DateType =>
        // date can't cast to decimal in Hive
        (c, evPrim, evNull) => code"$evNull = true;"
      case TimestampType =>
        // Note that we lose precision here.
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = Decimal.apply(
              scala.math.BigDecimal.valueOf(${timestampToDoubleCode(c)}));
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
          """
      case DecimalType() =>
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = $c.clone();
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
          """
      case x: IntegralType =>
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = Decimal.apply((long) $c);
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
          """
      case x: FractionalType =>
        // All other numeric types can be represented precisely as Doubles
        (c, evPrim, evNull) =>
          code"""
            try {
              Decimal $tmp = Decimal.apply(scala.math.BigDecimal.valueOf((double) $c));
              ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """
      case x: DayTimeIntervalType =>
        (c, evPrim, evNull) =>
          val u = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
          code"""
            Decimal $tmp = $u.dayTimeIntervalToDecimal($c, (byte)${x.endField});
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx, false)}
          """
      case x: YearMonthIntervalType =>
        (c, evPrim, evNull) =>
          val u = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
          val tmpYm = ctx.freshVariable("tmpYm", classOf[Int])
          code"""
            int $tmpYm = $u.yearMonthIntervalToInt($c, (byte)${x.startField}, (byte)${x.endField});
            Decimal $tmp = Decimal.apply($tmpYm);
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast, ctx, false)}
          """
    }
  }

  private[this] def castToTimestampCode(
      from: DataType,
      ctx: CodegenContext): Cast.CastFunction = from match {
    case _: StringType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      val longOpt = ctx.freshVariable("longOpt", classOf[Option[Long]])
      (c, evPrim, evNull) =>
        if (ansiEnabled) {
          val errorContext = getContextOrNullCode(ctx)
          code"""
            $evPrim = $dateTimeUtilsCls.stringToTimestampAnsi($c, $zid, $errorContext);
           """
        } else {
          code"""
            scala.Option<Long> $longOpt =
              org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToTimestamp($c, $zid);
            if ($longOpt.isDefined()) {
              $evPrim = ((Long) $longOpt.get()).longValue();
            } else {
              $evNull = true;
            }
           """
        }
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1L : 0L;"
    case _: IntegralType =>
      (c, evPrim, evNull) => code"$evPrim = ${longToTimeStampCode(c)};"
    case DateType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      (c, evPrim, evNull) =>
        code"""$evPrim =
          org.apache.spark.sql.catalyst.util.DateTimeUtils.daysToMicros($c, $zid);"""
    case TimestampNTZType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      (c, evPrim, evNull) =>
        code"$evPrim = $dateTimeUtilsCls.convertTz($c, $zid, java.time.ZoneOffset.UTC);"
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = ${decimalToTimestampCode(c)};"
    case DoubleType =>
      (c, evPrim, evNull) =>
        if (ansiEnabled) {
          val errorContext = getContextOrNullCode(ctx)
          code"$evPrim = $dateTimeUtilsCls.doubleToTimestampAnsi($c, $errorContext);"
        } else {
          code"""
            if (Double.isNaN($c) || Double.isInfinite($c)) {
              $evNull = true;
            } else {
              $evPrim = (long)($c * $MICROS_PER_SECOND);
            }
          """
        }
    case FloatType =>
      (c, evPrim, evNull) =>
        if (ansiEnabled) {
          val errorContext = getContextOrNullCode(ctx)
          code"$evPrim = $dateTimeUtilsCls.doubleToTimestampAnsi((double)$c, $errorContext);"
        } else {
          code"""
            if (Float.isNaN($c) || Float.isInfinite($c)) {
              $evNull = true;
            } else {
              $evPrim = (long)((double)$c * $MICROS_PER_SECOND);
            }
          """
        }
  }

  private[this] def castToTimestampNTZCode(
      from: DataType,
      ctx: CodegenContext): Cast.CastFunction = from match {
    case _: StringType =>
      val longOpt = ctx.freshVariable("longOpt", classOf[Option[Long]])
      (c, evPrim, evNull) =>
        if (ansiEnabled) {
          val errorContext = getContextOrNullCode(ctx)
          code"""
            $evPrim = $dateTimeUtilsCls.stringToTimestampWithoutTimeZoneAnsi($c, $errorContext);
           """
        } else {
          code"""
            scala.Option<Long> $longOpt = $dateTimeUtilsCls.stringToTimestampWithoutTimeZone($c);
            if ($longOpt.isDefined()) {
              $evPrim = ((Long) $longOpt.get()).longValue();
            } else {
              $evNull = true;
            }
           """
        }
    case DateType =>
      (c, evPrim, evNull) =>
        code"$evPrim = $dateTimeUtilsCls.daysToMicros($c, java.time.ZoneOffset.UTC);"
    case TimestampType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      (c, evPrim, evNull) =>
        code"$evPrim = $dateTimeUtilsCls.convertTz($c, java.time.ZoneOffset.UTC, $zid);"
  }

  private[this] def castToIntervalCode(from: DataType): Cast.CastFunction = from match {
    case _: StringType =>
      val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, evNull) =>
        code"""$evPrim = $util.safeStringToInterval($c);
           if(${evPrim} == null) {
             ${evNull} = true;
           }
         """.stripMargin

  }

  private[this] def castToDayTimeIntervalCode(
      from: DataType,
      it: DayTimeIntervalType): Cast.CastFunction = from match {
    case _: StringType =>
      val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, _) =>
        code"""
          $evPrim = $util.castStringToDTInterval($c, (byte)${it.startField}, (byte)${it.endField});
        """
    case _: DayTimeIntervalType =>
      val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, _) =>
        code"""
          $evPrim = $util.durationToMicros($util.microsToDuration($c), (byte)${it.endField});
        """
    case x: IntegralType =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      if (x == LongType) {
        (c, evPrim, _) =>
          code"""
            $evPrim = $iu.longToDayTimeInterval($c, (byte)${it.startField}, (byte)${it.endField});
          """
      } else {
        (c, evPrim, _) =>
          code"""
            $evPrim = $iu.intToDayTimeInterval($c, (byte)${it.startField}, (byte)${it.endField});
          """
      }
    case DecimalType.Fixed(p, s) =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, _) =>
        code"""
          $evPrim = $iu.decimalToDayTimeInterval(
            $c, $p, $s, (byte)${it.startField}, (byte)${it.endField});
        """
  }

  private[this] def castToYearMonthIntervalCode(
      from: DataType,
      it: YearMonthIntervalType): Cast.CastFunction = from match {
    case _: StringType =>
      val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, _) =>
        code"""
          $evPrim = $util.castStringToYMInterval($c, (byte)${it.startField}, (byte)${it.endField});
        """
    case _: YearMonthIntervalType =>
      val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, _) =>
        code"""
          $evPrim = $util.periodToMonths($util.monthsToPeriod($c), (byte)${it.endField});
        """
    case x: IntegralType =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      if (x == LongType) {
        (c, evPrim, _) =>
          code"""
            $evPrim = $iu.longToYearMonthInterval($c, (byte)${it.startField}, (byte)${it.endField});
          """
      } else {
        (c, evPrim, _) =>
          code"""
            $evPrim = $iu.intToYearMonthInterval($c, (byte)${it.startField}, (byte)${it.endField});
          """
      }
    case DecimalType.Fixed(p, s) =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      (c, evPrim, _) =>
        code"""
          $evPrim = $iu.decimalToYearMonthInterval(
            $c, $p, $s, (byte)${it.startField}, (byte)${it.endField});
        """
  }

  private[this] def decimalToTimestampCode(d: ExprValue): Block = {
    val block = inline"new java.math.BigDecimal($MICROS_PER_SECOND)"
    code"($d.toBigDecimal().bigDecimal().multiply($block)).longValue()"
  }
  private[this] def longToTimeStampCode(l: ExprValue): Block =
    code"java.util.concurrent.TimeUnit.SECONDS.toMicros($l)"
  private[this] def timestampToLongCode(ts: ExprValue): Block =
    code"java.lang.Math.floorDiv($ts, $MICROS_PER_SECOND)"
  private[this] def timestampToDoubleCode(ts: ExprValue): Block =
    code"$ts / (double)$MICROS_PER_SECOND"

  private[this] def castToBooleanCode(
      from: DataType,
      ctx: CodegenContext): Cast.CastFunction = from match {
    case _: StringType =>
      val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
      (c, evPrim, evNull) =>
        val castFailureCode = if (ansiEnabled) {
          val errorContext = getContextOrNullCode(ctx)
          s"throw QueryExecutionErrors.invalidInputSyntaxForBooleanError($c, $errorContext);"
        } else {
          s"$evNull = true;"
        }
        code"""
          if ($stringUtils.isTrueString($c)) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c)) {
            $evPrim = false;
          } else {
            $castFailureCode
          }
        """
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = $c != 0;"
    case DateType =>
      // Hive would return null when cast from date to boolean
      (c, evPrim, evNull) => code"$evNull = true;"
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = !$c.isZero();"
    case n: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = $c != 0;"
  }

  private[this] def castTimestampToIntegralTypeCode(
      ctx: CodegenContext,
      integralType: String,
      from: DataType,
      to: DataType): Cast.CastFunction = {

    val longValue = ctx.freshName("longValue")
    val fromDt = ctx.addReferenceObj("from", from, from.getClass.getName)
    val toDt = ctx.addReferenceObj("to", to, to.getClass.getName)

    (c, evPrim, evNull) =>
      val overflow = if (ansiEnabled) {
        code"""throw QueryExecutionErrors.castingCauseOverflowError($c, $fromDt, $toDt);"""
      } else {
        code"$evNull = true;"
      }

      code"""
          long $longValue = ${timestampToLongCode(c)};
          if ($longValue == ($integralType) $longValue) {
            $evPrim = ($integralType) $longValue;
          } else {
            $overflow
          }
        """
  }

  private[this] def castDayTimeIntervalToIntegralTypeCode(
      startField: Byte,
      endField: Byte,
      integralType: String): Cast.CastFunction = {
    val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
    (c, evPrim, _) =>
      code"""
        $evPrim = $util.dayTimeIntervalTo$integralType($c, (byte)$startField, (byte)$endField);
      """
  }

  private[this] def castYearMonthIntervalToIntegralTypeCode(
      startField: Byte,
      endField: Byte,
      integralType: String): Cast.CastFunction = {
    val util = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
    (c, evPrim, _) =>
      code"""
        $evPrim = $util.yearMonthIntervalTo$integralType($c, (byte)$startField, (byte)$endField);
      """
  }

  private[this] def castDecimalToIntegralTypeCode(integralType: String): Cast.CastFunction = {
    if (ansiEnabled) {
      (c, evPrim, _) => code"$evPrim = $c.roundTo${integralType.capitalize}();"
    } else {
      (c, evPrim, _) => code"$evPrim = $c.to${integralType.capitalize}();"
    }
  }

  private[this] def castIntegralTypeToIntegralTypeExactCode(
      ctx: CodegenContext,
      integralType: String,
      from: DataType,
      to: DataType): Cast.CastFunction = {
    assert(ansiEnabled)
    val fromDt = ctx.addReferenceObj("from", from, from.getClass.getName)
    val toDt = ctx.addReferenceObj("to", to, to.getClass.getName)
    (c, evPrim, _) =>
      code"""
        if ($c == ($integralType) $c) {
          $evPrim = ($integralType) $c;
        } else {
          throw QueryExecutionErrors.castingCauseOverflowError($c, $fromDt, $toDt);
        }
      """
  }


  private[this] def lowerAndUpperBound(integralType: String): (String, String) = {
    val (min, max, typeIndicator) = integralType.toLowerCase(Locale.ROOT) match {
      case "long" => (Long.MinValue, Long.MaxValue, "L")
      case "int" => (Int.MinValue, Int.MaxValue, "")
      case "short" => (Short.MinValue, Short.MaxValue, "")
      case "byte" => (Byte.MinValue, Byte.MaxValue, "")
    }
    (min.toString + typeIndicator, max.toString + typeIndicator)
  }

  private[this] def castFractionToIntegralTypeCode(
      ctx: CodegenContext,
      integralType: String,
      from: DataType,
      to: DataType): Cast.CastFunction = {
    assert(ansiEnabled)
    val (min, max) = lowerAndUpperBound(integralType)
    val mathClass = classOf[Math].getName
    val fromDt = ctx.addReferenceObj("from", from, from.getClass.getName)
    val toDt = ctx.addReferenceObj("to", to, to.getClass.getName)
    // When casting floating values to integral types, Spark uses the method `Numeric.toInt`
    // Or `Numeric.toLong` directly. For positive floating values, it is equivalent to `Math.floor`;
    // for negative floating values, it is equivalent to `Math.ceil`.
    // So, we can use the condition `Math.floor(x) <= upperBound && Math.ceil(x) >= lowerBound`
    // to check if the floating value x is in the range of an integral type after rounding.
    (c, evPrim, _) =>
      code"""
        if ($mathClass.floor($c) <= $max && $mathClass.ceil($c) >= $min) {
          $evPrim = ($integralType) $c;
        } else {
          throw QueryExecutionErrors.castingCauseOverflowError($c, $fromDt, $toDt);
        }
      """
  }

  private[this] def castToByteCode(from: DataType, ctx: CodegenContext): Cast.CastFunction =
    from match {
    case _: StringType if ansiEnabled =>
      val stringUtils = UTF8StringUtils.getClass.getCanonicalName.stripSuffix("$")
      val errorContext = getContextOrNullCode(ctx)
      (c, evPrim, evNull) => code"$evPrim = $stringUtils.toByteExact($c, $errorContext);"
    case _: StringType =>
      val wrapper = ctx.freshVariable("intWrapper", classOf[UTF8String.IntWrapper])
      (c, evPrim, evNull) =>
        code"""
          UTF8String.IntWrapper $wrapper = new UTF8String.IntWrapper();
          if ($c.toByte($wrapper)) {
            $evPrim = (byte) $wrapper.value;
          } else {
            $evNull = true;
          }
          $wrapper = null;
        """
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? (byte) 1 : (byte) 0;"
    case DateType =>
      (c, evPrim, evNull) => code"$evNull = true;"
    case TimestampType => castTimestampToIntegralTypeCode(ctx, "byte", from, ByteType)
    case DecimalType() => castDecimalToIntegralTypeCode("byte")
    case ShortType | IntegerType | LongType if ansiEnabled =>
      castIntegralTypeToIntegralTypeExactCode(ctx, "byte", from, ByteType)
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode(ctx, "byte", from, ByteType)
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (byte) $c;"
    case x: DayTimeIntervalType =>
      castDayTimeIntervalToIntegralTypeCode(x.startField, x.endField, "Byte")
    case x: YearMonthIntervalType =>
      castYearMonthIntervalToIntegralTypeCode(x.startField, x.endField, "Byte")
  }

  private[this] def castToShortCode(
      from: DataType,
      ctx: CodegenContext): Cast.CastFunction = from match {
    case _: StringType if ansiEnabled =>
      val stringUtils = UTF8StringUtils.getClass.getCanonicalName.stripSuffix("$")
      val errorContext = getContextOrNullCode(ctx)
      (c, evPrim, evNull) => code"$evPrim = $stringUtils.toShortExact($c, $errorContext);"
    case _: StringType =>
      val wrapper = ctx.freshVariable("intWrapper", classOf[UTF8String.IntWrapper])
      (c, evPrim, evNull) =>
        code"""
          UTF8String.IntWrapper $wrapper = new UTF8String.IntWrapper();
          if ($c.toShort($wrapper)) {
            $evPrim = (short) $wrapper.value;
          } else {
            $evNull = true;
          }
          $wrapper = null;
        """
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? (short) 1 : (short) 0;"
    case DateType =>
      (c, evPrim, evNull) => code"$evNull = true;"
    case TimestampType => castTimestampToIntegralTypeCode(ctx, "short", from, ShortType)
    case DecimalType() => castDecimalToIntegralTypeCode("short")
    case IntegerType | LongType if ansiEnabled =>
      castIntegralTypeToIntegralTypeExactCode(ctx, "short", from, ShortType)
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode(ctx, "short", from, ShortType)
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (short) $c;"
    case x: DayTimeIntervalType =>
      castDayTimeIntervalToIntegralTypeCode(x.startField, x.endField, "Short")
    case x: YearMonthIntervalType =>
      castYearMonthIntervalToIntegralTypeCode(x.startField, x.endField, "Short")
  }

  private[this] def castToIntCode(from: DataType, ctx: CodegenContext): Cast.CastFunction =
    from match {
    case _: StringType if ansiEnabled =>
      val stringUtils = UTF8StringUtils.getClass.getCanonicalName.stripSuffix("$")
      val errorContext = getContextOrNullCode(ctx)
      (c, evPrim, evNull) => code"$evPrim = $stringUtils.toIntExact($c, $errorContext);"
    case _: StringType =>
      val wrapper = ctx.freshVariable("intWrapper", classOf[UTF8String.IntWrapper])
      (c, evPrim, evNull) =>
        code"""
          UTF8String.IntWrapper $wrapper = new UTF8String.IntWrapper();
          if ($c.toInt($wrapper)) {
            $evPrim = $wrapper.value;
          } else {
            $evNull = true;
          }
          $wrapper = null;
        """
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1 : 0;"
    case DateType =>
      (c, evPrim, evNull) => code"$evNull = true;"
    case TimestampType => castTimestampToIntegralTypeCode(ctx, "int", from, IntegerType)
    case DecimalType() => castDecimalToIntegralTypeCode("int")
    case LongType if ansiEnabled =>
      castIntegralTypeToIntegralTypeExactCode(ctx, "int", from, IntegerType)
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode(ctx, "int", from, IntegerType)
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (int) $c;"
    case x: DayTimeIntervalType =>
      castDayTimeIntervalToIntegralTypeCode(x.startField, x.endField, "Int")
    case x: YearMonthIntervalType =>
      castYearMonthIntervalToIntegralTypeCode(x.startField, x.endField, "Int")
  }

  private[this] def castToLongCode(from: DataType, ctx: CodegenContext): Cast.CastFunction =
    from match {
    case _: StringType if ansiEnabled =>
      val stringUtils = UTF8StringUtils.getClass.getCanonicalName.stripSuffix("$")
      val errorContext = getContextOrNullCode(ctx)
      (c, evPrim, evNull) => code"$evPrim = $stringUtils.toLongExact($c, $errorContext);"
    case _: StringType =>
      val wrapper = ctx.freshVariable("longWrapper", classOf[UTF8String.LongWrapper])
      (c, evPrim, evNull) =>
        code"""
          UTF8String.LongWrapper $wrapper = new UTF8String.LongWrapper();
          if ($c.toLong($wrapper)) {
            $evPrim = $wrapper.value;
          } else {
            $evNull = true;
          }
          $wrapper = null;
        """
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1L : 0L;"
    case DateType =>
      (c, evPrim, evNull) => code"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = (long) ${timestampToLongCode(c)};"
    case DecimalType() => castDecimalToIntegralTypeCode("long")
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode(ctx, "long", from, LongType)
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (long) $c;"
    case x: DayTimeIntervalType =>
      castDayTimeIntervalToIntegralTypeCode(x.startField, x.endField, "Long")
    case x: YearMonthIntervalType =>
      castYearMonthIntervalToIntegralTypeCode(x.startField, x.endField, "Int")
  }

  private[this] def castToFloatCode(from: DataType, ctx: CodegenContext): Cast.CastFunction = {
    from match {
      case _: StringType =>
        val floatStr = ctx.freshVariable("floatStr", StringType)
        (c, evPrim, evNull) =>
          val handleNull = if (ansiEnabled) {
            val errorContext = getContextOrNullCode(ctx)
            "throw QueryExecutionErrors.invalidInputInCastToNumberError(" +
              s"org.apache.spark.sql.types.FloatType$$.MODULE$$,$c, $errorContext);"
          } else {
            s"$evNull = true;"
          }
          code"""
          final String $floatStr = $c.toString();
          try {
            $evPrim = Float.valueOf($floatStr);
          } catch (java.lang.NumberFormatException e) {
            final Float f = (Float) Cast.processFloatingPointSpecialLiterals($floatStr, true);
            if (f == null) {
              $handleNull
            } else {
              $evPrim = f.floatValue();
            }
          }
        """
      case BooleanType =>
        (c, evPrim, evNull) => code"$evPrim = $c ? 1.0f : 0.0f;"
      case DateType =>
        (c, evPrim, evNull) => code"$evNull = true;"
      case TimestampType =>
        (c, evPrim, evNull) => code"$evPrim = (float) (${timestampToDoubleCode(c)});"
      case DecimalType() =>
        (c, evPrim, evNull) => code"$evPrim = $c.toFloat();"
      case x: NumericType =>
        (c, evPrim, evNull) => code"$evPrim = (float) $c;"
    }
  }

  private[this] def castToDoubleCode(from: DataType, ctx: CodegenContext): Cast.CastFunction = {
    from match {
      case _: StringType =>
        val doubleStr = ctx.freshVariable("doubleStr", StringType)
        (c, evPrim, evNull) =>
          val handleNull = if (ansiEnabled) {
            val errorContext = getContextOrNullCode(ctx)
            "throw QueryExecutionErrors.invalidInputInCastToNumberError(" +
              s"org.apache.spark.sql.types.DoubleType$$.MODULE$$, $c, $errorContext);"
          } else {
            s"$evNull = true;"
          }
          code"""
          final String $doubleStr = $c.toString();
          try {
            $evPrim = Double.valueOf($doubleStr);
          } catch (java.lang.NumberFormatException e) {
            final Double d = (Double) Cast.processFloatingPointSpecialLiterals($doubleStr, false);
            if (d == null) {
              $handleNull
            } else {
              $evPrim = d.doubleValue();
            }
          }
        """
      case BooleanType =>
        (c, evPrim, evNull) => code"$evPrim = $c ? 1.0d : 0.0d;"
      case DateType =>
        (c, evPrim, evNull) => code"$evNull = true;"
      case TimestampType =>
        (c, evPrim, evNull) => code"$evPrim = ${timestampToDoubleCode(c)};"
      case DecimalType() =>
        (c, evPrim, evNull) => code"$evPrim = $c.toDouble();"
      case x: NumericType =>
        (c, evPrim, evNull) => code"$evPrim = (double) $c;"
    }
  }

  private[this] def castArrayCode(
      fromType: DataType, toType: DataType, ctx: CodegenContext): Cast.CastFunction = {
    val elementCast = nullSafeCastFunction(fromType, toType, ctx)
    val arrayClass = JavaCode.javaType(classOf[GenericArrayData])
    val fromElementNull = ctx.freshVariable("feNull", BooleanType)
    val fromElementPrim = ctx.freshVariable("fePrim", fromType)
    val toElementNull = ctx.freshVariable("teNull", BooleanType)
    val toElementPrim = ctx.freshVariable("tePrim", toType)
    val size = ctx.freshVariable("n", IntegerType)
    val j = ctx.freshVariable("j", IntegerType)
    val values = ctx.freshVariable("values", classOf[Array[Object]])
    val javaType = JavaCode.javaType(fromType)

    (c, evPrim, evNull) =>
      code"""
        final int $size = $c.numElements();
        final Object[] $values = new Object[$size];
        for (int $j = 0; $j < $size; $j ++) {
          if ($c.isNullAt($j)) {
            $values[$j] = null;
          } else {
            boolean $fromElementNull = false;
            $javaType $fromElementPrim =
              ${CodeGenerator.getValue(c, fromType, j)};
            ${castCode(ctx, fromElementPrim,
              fromElementNull, toElementPrim, toElementNull, toType, elementCast)}
            if ($toElementNull) {
              $values[$j] = null;
            } else {
              $values[$j] = $toElementPrim;
            }
          }
        }
        $evPrim = new $arrayClass($values);
      """
  }

  private[this] def castMapCode(
      from: MapType,
      to: MapType,
      ctx: CodegenContext): Cast.CastFunction = {
    val keysCast = castArrayCode(from.keyType, to.keyType, ctx)
    val valuesCast = castArrayCode(from.valueType, to.valueType, ctx)

    val mapClass = JavaCode.javaType(classOf[ArrayBasedMapData])

    val keys = ctx.freshVariable("keys", ArrayType(from.keyType))
    val convertedKeys = ctx.freshVariable("convertedKeys", ArrayType(to.keyType))
    val convertedKeysNull = ctx.freshVariable("convertedKeysNull", BooleanType)

    val values = ctx.freshVariable("values", ArrayType(from.valueType))
    val convertedValues = ctx.freshVariable("convertedValues", ArrayType(to.valueType))
    val convertedValuesNull = ctx.freshVariable("convertedValuesNull", BooleanType)

    (c, evPrim, evNull) =>
      code"""
        final ArrayData $keys = $c.keyArray();
        final ArrayData $values = $c.valueArray();
        ${castCode(ctx, keys, FalseLiteral,
          convertedKeys, convertedKeysNull, ArrayType(to.keyType), keysCast)}
        ${castCode(ctx, values, FalseLiteral,
          convertedValues, convertedValuesNull, ArrayType(to.valueType), valuesCast)}

        $evPrim = new $mapClass($convertedKeys, $convertedValues);
      """
  }

  private[this] def castStructCode(
      from: StructType, to: StructType, ctx: CodegenContext): Cast.CastFunction = {

    val fieldsCasts = from.fields.zip(to.fields).map {
      case (fromField, toField) => nullSafeCastFunction(fromField.dataType, toField.dataType, ctx)
    }
    val tmpResult = ctx.freshVariable("tmpResult", classOf[GenericInternalRow])
    val rowClass = JavaCode.javaType(classOf[GenericInternalRow])
    val tmpInput = ctx.freshVariable("tmpInput", classOf[InternalRow])

    val fieldsEvalCode = fieldsCasts.zipWithIndex.map { case (cast, i) =>
      val fromFieldPrim = ctx.freshVariable("ffp", from.fields(i).dataType)
      val fromFieldNull = ctx.freshVariable("ffn", BooleanType)
      val toFieldPrim = ctx.freshVariable("tfp", to.fields(i).dataType)
      val toFieldNull = ctx.freshVariable("tfn", BooleanType)
      val fromType = JavaCode.javaType(from.fields(i).dataType)
      val setColumn = CodeGenerator.setColumn(tmpResult, to.fields(i).dataType, i, toFieldPrim)
      code"""
        boolean $fromFieldNull = $tmpInput.isNullAt($i);
        if ($fromFieldNull) {
          $tmpResult.setNullAt($i);
        } else {
          $fromType $fromFieldPrim =
            ${CodeGenerator.getValue(tmpInput, from.fields(i).dataType, i.toString)};
          ${castCode(ctx, fromFieldPrim,
            fromFieldNull, toFieldPrim, toFieldNull, to.fields(i).dataType, cast)}
          if ($toFieldNull) {
            $tmpResult.setNullAt($i);
          } else {
            $setColumn;
          }
        }
       """
    }
    val fieldsEvalCodes = ctx.splitExpressions(
      expressions = fieldsEvalCode.map(_.code).toImmutableArraySeq,
      funcName = "castStruct",
      arguments = ("InternalRow", tmpInput.code) :: (rowClass.code, tmpResult.code) :: Nil)

    (input, result, resultIsNull) =>
      code"""
        final $rowClass $tmpResult = new $rowClass(${fieldsCasts.length});
        final InternalRow $tmpInput = $input;
        $fieldsEvalCodes
        $result = $tmpResult;
      """
  }

  override def prettyName: String = if (!isTryCast) {
    "cast"
  } else {
    "try_cast"
  }

  override def toString: String = {
    s"$prettyName($child as ${dataType.simpleString})"
  }

  override def sql: String = dataType match {
    // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL, this
    // type of casting can only be introduced by the analyzer, and can be omitted when converting
    // back to SQL query string.
    case _: ArrayType | _: MapType | _: StructType => child.sql
    case _ => s"${prettyName.toUpperCase(Locale.ROOT)}(${child.sql} AS ${dataType.sql})"
  }
}

/**
 * Cast the child expression to the target data type, but will throw error if the cast might
 * truncate, e.g. long -> int, timestamp -> data.
 *
 * Note: `target` is `AbstractDataType`, so that we can put `object DecimalType`, which means
 * we accept `DecimalType` with any valid precision/scale.
 */
case class UpCast(child: Expression, target: AbstractDataType, walkedTypePath: Seq[String] = Nil)
  extends UnaryExpression with Unevaluable {
  override lazy val resolved = false

  final override val nodePatterns: Seq[TreePattern] = Seq(UP_CAST)

  def dataType: DataType = target match {
    case DecimalType => DecimalType.SYSTEM_DEFAULT
    case _ => target.asInstanceOf[DataType]
  }

  override protected def withNewChildInternal(newChild: Expression): UpCast = copy(child = newChild)
}

/**
 * Casting a numeric value as another numeric type in store assignment. It can capture the
 * arithmetic errors and show proper error messages to users.
 */
case class CheckOverflowInTableInsert(child: Expression, columnName: String)
    extends UnaryExpression {

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }

  private def getCast: Option[Cast] = child match {
    case c: Cast =>
      Some(c)
    case ExpressionProxy(c: Cast, _, _) =>
      Some(c)
    case _ => None
  }

  override def eval(input: InternalRow): Any = try {
    child.eval(input)
  } catch {
    case e: SparkArithmeticException =>
      getCast match {
        case Some(cast) =>
          throw QueryExecutionErrors.castingCauseOverflowErrorInTableInsert(
            cast.child.dataType,
            cast.dataType,
            columnName)
        case None => throw e
      }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    getCast match {
      case Some(child) => doGenCodeWithBetterErrorMsg(ctx, ev, child)
      case None => child.genCode(ctx)
    }
  }

  def doGenCodeWithBetterErrorMsg(ctx: CodegenContext, ev: ExprCode, child: Cast): ExprCode = {
    val childGen = child.genCode(ctx)
    val exceptionClass = classOf[SparkArithmeticException].getCanonicalName
    val fromDt =
      ctx.addReferenceObj("from", child.child.dataType, child.child.dataType.getClass.getName)
    val toDt = ctx.addReferenceObj("to", child.dataType, child.dataType.getClass.getName)
    val col = ctx.addReferenceObj("colName", columnName, "java.lang.String")
    // scalastyle:off line.size.limit
    ev.copy(code = code"""
      boolean ${ev.isNull} = true;
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      try {
        ${childGen.code}
        ${ev.isNull} = ${childGen.isNull};
        ${ev.value} = ${childGen.value};
      } catch ($exceptionClass e) {
        throw QueryExecutionErrors.castingCauseOverflowErrorInTableInsert($fromDt, $toDt, $col);
      }"""
    )
    // scalastyle:on line.size.limit
  }

  override def dataType: DataType = child.dataType

  override def sql: String = child.sql

  override def toString: String = child.toString
}
