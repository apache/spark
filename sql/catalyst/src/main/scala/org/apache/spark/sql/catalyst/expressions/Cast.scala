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

import java.math.{BigDecimal => JavaBigDecimal}
import java.time.ZoneId
import java.util.concurrent.TimeUnit._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{InternalRow, WalkedTypePath}
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.unsafe.types.UTF8String.{IntWrapper, LongWrapper}

object Cast {

  /**
   * Returns true iff we can cast `from` type to `to` type.
   */
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, StringType) => true

    case (StringType, BinaryType) => true
    case (_: IntegralType, BinaryType) => true

    case (StringType, BooleanType) => true
    case (DateType, BooleanType) => true
    case (TimestampType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

    case (StringType, TimestampType) => true
    case (BooleanType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (_: NumericType, TimestampType) => true

    case (StringType, DateType) => true
    case (TimestampType, DateType) => true

    case (StringType, CalendarIntervalType) => true

    case (StringType, _: NumericType) => true
    case (BooleanType, _: NumericType) => true
    case (DateType, _: NumericType) => true
    case (TimestampType, _: NumericType) => true
    case (_: NumericType, _: NumericType) => true

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

    case (udt1: UserDefinedType[_], udt2: UserDefinedType[_]) if udt1.userClass == udt2.userClass =>
      true

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
    case (StringType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (TimestampType, StringType) => true
    case (TimestampType, DateType) => true
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
  def canUpCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case _ if from == to => true
    case (from: NumericType, to: DecimalType) if to.isWiderThan(from) => true
    case (from: DecimalType, to: NumericType) if from.isTighterThan(to) => true
    case (f, t) if legalNumericPrecedence(f, t) => true
    case (DateType, TimestampType) => true
    case (_, StringType) => true

    // Spark supports casting between long and timestamp, please see `longToTimestamp` and
    // `timestampToLong` for details.
    case (TimestampType, LongType) => true
    case (LongType, TimestampType) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      resolvableNullability(fn, tn) && canUpCast(fromType, toType)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      resolvableNullability(fn, tn) && canUpCast(fromKey, toKey) && canUpCast(fromValue, toValue)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (f1, f2) =>
            resolvableNullability(f1.nullable, f2.nullable) && canUpCast(f1.dataType, f2.dataType)
        }

    case _ => false
  }

  private def legalNumericPrecedence(from: DataType, to: DataType): Boolean = {
    val fromPrecedence = TypeCoercion.numericPrecedence.indexOf(from)
    val toPrecedence = TypeCoercion.numericPrecedence.indexOf(to)
    fromPrecedence >= 0 && fromPrecedence < toPrecedence
  }

  def canNullSafeCastToDecimal(from: DataType, to: DecimalType): Boolean = from match {
    case from: BooleanType if to.isWiderThan(DecimalType.BooleanDecimal) => true
    case from: NumericType if to.isWiderThan(from) => true
    case from: DecimalType =>
      // truncating or precision lose
      (to.precision - to.scale) > (from.precision - from.scale)
    case _ => false  // overflow
  }

  def forceNullable(from: DataType, to: DataType): Boolean = (from, to) match {
    case (NullType, _) => true
    case (_, _) if from == to => false

    case (StringType, BinaryType) => false
    case (StringType, _) => true
    case (_, StringType) => false

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
}

/**
 * Cast the child expression to the target data type.
 *
 * When cast from/to timezone related types, we need timeZoneId, which will be resolved with
 * session local timezone by an analyzer [[ResolveTimeZone]].
 */
@ExpressionDescription(
  usage = "_FUNC_(expr AS type) - Casts the value `expr` to the target data type `type`.",
  examples = """
    Examples:
      > SELECT _FUNC_('10' as int);
       10
  """)
case class Cast(child: Expression, dataType: DataType, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  def this(child: Expression, dataType: DataType) = this(child, dataType, None)

  override def toString: String = s"cast($child as ${dataType.simpleString})"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast ${child.dataType.catalogString} to ${dataType.catalogString}")
    }
  }

  override def nullable: Boolean = Cast.forceNullable(child.dataType, dataType) || child.nullable

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  // When this cast involves TimeZone, it's only resolved if the timeZoneId is set;
  // Otherwise behave like Expression.resolved.
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && (!needsTimeZone || timeZoneId.isDefined)

  private[this] def needsTimeZone: Boolean = Cast.needsTimeZone(child.dataType, dataType)

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  private lazy val dateFormatter = DateFormatter()
  private lazy val timestampFormatter = TimestampFormatter.getFractionFormatter(zoneId)

  // UDFToString
  private[this] def castToString(from: DataType): Any => Any = from match {
    case BinaryType => buildCast[Array[Byte]](_, UTF8String.fromBytes)
    case DateType => buildCast[Int](_, d => UTF8String.fromString(dateFormatter.format(d)))
    case TimestampType => buildCast[Long](_,
      t => UTF8String.fromString(DateTimeUtils.timestampToString(timestampFormatter, t)))
    case ArrayType(et, _) =>
      buildCast[ArrayData](_, array => {
        val builder = new UTF8StringBuilder
        builder.append("[")
        if (array.numElements > 0) {
          val toUTF8String = castToString(et)
          if (!array.isNullAt(0)) {
            builder.append(toUTF8String(array.get(0, et)).asInstanceOf[UTF8String])
          }
          var i = 1
          while (i < array.numElements) {
            builder.append(",")
            if (!array.isNullAt(i)) {
              builder.append(" ")
              builder.append(toUTF8String(array.get(i, et)).asInstanceOf[UTF8String])
            }
            i += 1
          }
        }
        builder.append("]")
        builder.build()
      })
    case MapType(kt, vt, _) =>
      buildCast[MapData](_, map => {
        val builder = new UTF8StringBuilder
        builder.append("[")
        if (map.numElements > 0) {
          val keyArray = map.keyArray()
          val valueArray = map.valueArray()
          val keyToUTF8String = castToString(kt)
          val valueToUTF8String = castToString(vt)
          builder.append(keyToUTF8String(keyArray.get(0, kt)).asInstanceOf[UTF8String])
          builder.append(" ->")
          if (!valueArray.isNullAt(0)) {
            builder.append(" ")
            builder.append(valueToUTF8String(valueArray.get(0, vt)).asInstanceOf[UTF8String])
          }
          var i = 1
          while (i < map.numElements) {
            builder.append(", ")
            builder.append(keyToUTF8String(keyArray.get(i, kt)).asInstanceOf[UTF8String])
            builder.append(" ->")
            if (!valueArray.isNullAt(i)) {
              builder.append(" ")
              builder.append(valueToUTF8String(valueArray.get(i, vt))
                .asInstanceOf[UTF8String])
            }
            i += 1
          }
        }
        builder.append("]")
        builder.build()
      })
    case StructType(fields) =>
      buildCast[InternalRow](_, row => {
        val builder = new UTF8StringBuilder
        builder.append("[")
        if (row.numFields > 0) {
          val st = fields.map(_.dataType)
          val toUTF8StringFuncs = st.map(castToString)
          if (!row.isNullAt(0)) {
            builder.append(toUTF8StringFuncs(0)(row.get(0, st(0))).asInstanceOf[UTF8String])
          }
          var i = 1
          while (i < row.numFields) {
            builder.append(",")
            if (!row.isNullAt(i)) {
              builder.append(" ")
              builder.append(toUTF8StringFuncs(i)(row.get(i, st(i))).asInstanceOf[UTF8String])
            }
            i += 1
          }
        }
        builder.append("]")
        builder.build()
      })
    case pudt: PythonUserDefinedType => castToString(pudt.sqlType)
    case udt: UserDefinedType[_] =>
      buildCast[Any](_, o => UTF8String.fromString(udt.deserialize(o).toString))
    case _ => buildCast[Any](_, o => UTF8String.fromString(o.toString))
  }

  // BinaryConverter
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case StringType => buildCast[UTF8String](_, _.getBytes)
    case ByteType => buildCast[Byte](_, NumberConverter.toBinary)
    case ShortType => buildCast[Short](_, NumberConverter.toBinary)
    case IntegerType => buildCast[Int](_, NumberConverter.toBinary)
    case LongType => buildCast[Long](_, NumberConverter.toBinary)
  }

  // UDFToBoolean
  private[this] def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => {
        if (StringUtils.isTrueString(s)) {
          true
        } else if (StringUtils.isFalseString(s)) {
          false
        } else {
          null
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
    case StringType =>
      buildCast[UTF8String](_, utfs => DateTimeUtils.stringToTimestamp(utfs, zoneId).orNull)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0)
    case LongType =>
      buildCast[Long](_, l => longToTimestamp(l))
    case IntegerType =>
      buildCast[Int](_, i => longToTimestamp(i.toLong))
    case ShortType =>
      buildCast[Short](_, s => longToTimestamp(s.toLong))
    case ByteType =>
      buildCast[Byte](_, b => longToTimestamp(b.toLong))
    case DateType =>
      buildCast[Int](_, d => epochDaysToMicros(d, zoneId))
    // TimestampWritable.decimalToTimestamp
    case DecimalType() =>
      buildCast[Decimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      buildCast[Double](_, d => doubleToTimestamp(d))
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      buildCast[Float](_, f => doubleToTimestamp(f.toDouble))
  }

  private[this] def decimalToTimestamp(d: Decimal): Long = {
    (d.toBigDecimal * MICROS_PER_SECOND).longValue()
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
    case StringType =>
      buildCast[UTF8String](_, s => DateTimeUtils.stringToDate(s).orNull)
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Long](_, t => microsToEpochDays(t, zoneId))
  }

  // IntervalConverter
  private[this] def castToInterval(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => CalendarInterval.fromString(s.toString))
  }

  // LongConverter
  private[this] def castToLong(from: DataType): Any => Any = from match {
    case StringType =>
      val result = new LongWrapper()
      buildCast[UTF8String](_, s => if (s.toLong(result)) result.value else null)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  // IntConverter
  private[this] def castToInt(from: DataType): Any => Any = from match {
    case StringType =>
      val result = new IntWrapper()
      buildCast[UTF8String](_, s => if (s.toInt(result)) result.value else null)
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToLong(t).toInt)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  // ShortConverter
  private[this] def castToShort(from: DataType): Any => Any = from match {
    case StringType =>
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
      buildCast[Long](_, t => timestampToLong(t).toShort)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  // ByteConverter
  private[this] def castToByte(from: DataType): Any => Any = from match {
    case StringType =>
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
      buildCast[Long](_, t => timestampToLong(t).toByte)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toByte
  }

  /**
   * Change the precision / scale in a given decimal to those set in `decimalType` (if any),
   * returning null if it overflows or modifying `value` in-place and returning it if successful.
   *
   * NOTE: this modifies `value` in-place, so don't call it on external data.
   */
  private[this] def changePrecision(value: Decimal, decimalType: DecimalType): Decimal = {
    if (value.changePrecision(decimalType.precision, decimalType.scale)) value else null
  }

  /**
   * Create new `Decimal` with precision and scale given in `decimalType` (if any),
   * returning null if it overflows or creating a new `value` and returning it if successful.
   */
  private[this] def toPrecision(value: Decimal, decimalType: DecimalType): Decimal =
    value.toPrecision(decimalType.precision, decimalType.scale)


  private[this] def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try {
        changePrecision(Decimal(new JavaBigDecimal(s.toString)), target)
      } catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => toPrecision(if (b) Decimal.ONE else Decimal.ZERO, target))
    case DateType =>
      buildCast[Int](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Long](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
    case dt: DecimalType =>
      b => toPrecision(b.asInstanceOf[Decimal], target)
    case t: IntegralType =>
      b => changePrecision(Decimal(t.integral.asInstanceOf[Integral[Any]].toLong(b)), target)
    case x: FractionalType =>
      b => try {
        changePrecision(Decimal(x.fractional.asInstanceOf[Fractional[Any]].toDouble(b)), target)
      } catch {
        case _: NumberFormatException => null
      }
  }

  // DoubleConverter
  private[this] def castToDouble(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toDouble catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  // FloatConverter
  private[this] def castToFloat(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toFloat catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Long](_, t => timestampToDouble(t).toFloat)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
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

  private[this] def cast(from: DataType, to: DataType): Any => Any = {
    // If the cast does not change the structure, then we don't really need to cast anything.
    // We can return what the children return. Same thing should happen in the codegen path.
    if (DataType.equalsStructurally(from, to)) {
      identity
    } else if (from == NullType) {
      // According to `canCast`, NullType can be casted to any type.
      // For primitive types, we don't reach here because the guard of `nullSafeEval`.
      // But for nested types like struct, we might reach here for nested null type field.
      // We won't call the returned function actually, but returns a placeholder.
      _ => throw new SparkException(s"should not directly cast from NullType to $to.")
    } else {
      to match {
        case dt if dt == from => identity[Any]
        case StringType => castToString(from)
        case BinaryType => castToBinary(from)
        case DateType => castToDate(from)
        case decimal: DecimalType => castToDecimal(from, decimal)
        case TimestampType => castToTimestamp(from)
        case CalendarIntervalType => castToInterval(from)
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
        case udt: UserDefinedType[_]
          if udt.userClass == from.asInstanceOf[UserDefinedType[_]].userClass =>
          identity[Any]
        case _: UserDefinedType[_] =>
          throw new SparkException(s"Cannot cast $from to $to.")
      }
    }
  }

  private[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

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

  // The function arguments are: `input`, `result` and `resultIsNull`. We don't need `inputIsNull`
  // in parameter list, because the returned code will be put in null safe evaluation region.
  private[this] type CastFunction = (ExprValue, ExprValue, ExprValue) => Block

  private[this] def nullSafeCastFunction(
      from: DataType,
      to: DataType,
      ctx: CodegenContext): CastFunction = to match {

    case _ if from == NullType => (c, evPrim, evNull) => code"$evNull = true;"
    case _ if to == from => (c, evPrim, evNull) => code"$evPrim = $c;"
    case StringType => castToStringCode(from, ctx)
    case BinaryType => castToBinaryCode(from)
    case DateType => castToDateCode(from, ctx)
    case decimal: DecimalType => castToDecimalCode(from, decimal, ctx)
    case TimestampType => castToTimestampCode(from, ctx)
    case CalendarIntervalType => castToIntervalCode(from)
    case BooleanType => castToBooleanCode(from)
    case ByteType => castToByteCode(from, ctx)
    case ShortType => castToShortCode(from, ctx)
    case IntegerType => castToIntCode(from, ctx)
    case FloatType => castToFloatCode(from)
    case LongType => castToLongCode(from, ctx)
    case DoubleType => castToDoubleCode(from)

    case array: ArrayType =>
      castArrayCode(from.asInstanceOf[ArrayType].elementType, array.elementType, ctx)
    case map: MapType => castMapCode(from.asInstanceOf[MapType], map, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
    case udt: UserDefinedType[_]
      if udt.userClass == from.asInstanceOf[UserDefinedType[_]].userClass =>
      (c, evPrim, evNull) => code"$evPrim = $c;"
    case _: UserDefinedType[_] =>
      throw new SparkException(s"Cannot cast $from to $to.")
  }

  // Since we need to cast input expressions recursively inside ComplexTypes, such as Map's
  // Key and Value, Struct's field, we need to name out all the variable names involved in a cast.
  private[this] def castCode(ctx: CodegenContext, input: ExprValue, inputIsNull: ExprValue,
    result: ExprValue, resultIsNull: ExprValue, resultType: DataType, cast: CastFunction): Block = {
    val javaType = JavaCode.javaType(resultType)
    code"""
      boolean $resultIsNull = $inputIsNull;
      $javaType $result = ${CodeGenerator.defaultValue(resultType)};
      if (!$inputIsNull) {
        ${cast(input, result, resultIsNull)}
      }
    """
  }

  private def writeArrayToStringBuilder(
      et: DataType,
      array: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {
    val elementToStringCode = castToStringCode(et, ctx)
    val funcName = ctx.freshName("elementToString")
    val element = JavaCode.variable("element", et)
    val elementStr = JavaCode.variable("elementStr", StringType)
    val elementToStringFunc = inline"${ctx.addNewFunction(funcName,
      s"""
         |private UTF8String $funcName(${CodeGenerator.javaType(et)} $element) {
         |  UTF8String $elementStr = null;
         |  ${elementToStringCode(element, elementStr, null /* resultIsNull won't be used */)}
         |  return elementStr;
         |}
       """.stripMargin)}"

    val loopIndex = ctx.freshVariable("loopIndex", IntegerType)
    code"""
       |$buffer.append("[");
       |if ($array.numElements() > 0) {
       |  if (!$array.isNullAt(0)) {
       |    $buffer.append($elementToStringFunc(${CodeGenerator.getValue(array, et, "0")}));
       |  }
       |  for (int $loopIndex = 1; $loopIndex < $array.numElements(); $loopIndex++) {
       |    $buffer.append(",");
       |    if (!$array.isNullAt($loopIndex)) {
       |      $buffer.append(" ");
       |      $buffer.append($elementToStringFunc(${CodeGenerator.getValue(array, et, loopIndex)}));
       |    }
       |  }
       |}
       |$buffer.append("]");
     """.stripMargin
  }

  private def writeMapToStringBuilder(
      kt: DataType,
      vt: DataType,
      map: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {

    def dataToStringFunc(func: String, dataType: DataType) = {
      val funcName = ctx.freshName(func)
      val dataToStringCode = castToStringCode(dataType, ctx)
      val data = JavaCode.variable("data", dataType)
      val dataStr = JavaCode.variable("dataStr", StringType)
      val functionCall = ctx.addNewFunction(funcName,
        s"""
           |private UTF8String $funcName(${CodeGenerator.javaType(dataType)} $data) {
           |  UTF8String $dataStr = null;
           |  ${dataToStringCode(data, dataStr, null /* resultIsNull won't be used */)}
           |  return dataStr;
           |}
         """.stripMargin)
      inline"$functionCall"
    }

    val keyToStringFunc = dataToStringFunc("keyToString", kt)
    val valueToStringFunc = dataToStringFunc("valueToString", vt)
    val loopIndex = ctx.freshVariable("loopIndex", IntegerType)
    val mapKeyArray = JavaCode.expression(s"$map.keyArray()", classOf[ArrayData])
    val mapValueArray = JavaCode.expression(s"$map.valueArray()", classOf[ArrayData])
    val getMapFirstKey = CodeGenerator.getValue(mapKeyArray, kt, JavaCode.literal("0", IntegerType))
    val getMapFirstValue = CodeGenerator.getValue(mapValueArray, vt,
      JavaCode.literal("0", IntegerType))
    val getMapKeyArray = CodeGenerator.getValue(mapKeyArray, kt, loopIndex)
    val getMapValueArray = CodeGenerator.getValue(mapValueArray, vt, loopIndex)
    code"""
       |$buffer.append("[");
       |if ($map.numElements() > 0) {
       |  $buffer.append($keyToStringFunc($getMapFirstKey));
       |  $buffer.append(" ->");
       |  if (!$map.valueArray().isNullAt(0)) {
       |    $buffer.append(" ");
       |    $buffer.append($valueToStringFunc($getMapFirstValue));
       |  }
       |  for (int $loopIndex = 1; $loopIndex < $map.numElements(); $loopIndex++) {
       |    $buffer.append(", ");
       |    $buffer.append($keyToStringFunc($getMapKeyArray));
       |    $buffer.append(" ->");
       |    if (!$map.valueArray().isNullAt($loopIndex)) {
       |      $buffer.append(" ");
       |      $buffer.append($valueToStringFunc($getMapValueArray));
       |    }
       |  }
       |}
       |$buffer.append("]");
     """.stripMargin
  }

  private def writeStructToStringBuilder(
      st: Seq[DataType],
      row: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {
    val structToStringCode = st.zipWithIndex.map { case (ft, i) =>
      val fieldToStringCode = castToStringCode(ft, ctx)
      val field = ctx.freshVariable("field", ft)
      val fieldStr = ctx.freshVariable("fieldStr", StringType)
      val javaType = JavaCode.javaType(ft)
      code"""
         |${if (i != 0) code"""$buffer.append(",");""" else EmptyBlock}
         |if (!$row.isNullAt($i)) {
         |  ${if (i != 0) code"""$buffer.append(" ");""" else EmptyBlock}
         |
         |  // Append $i field into the string buffer
         |  $javaType $field = ${CodeGenerator.getValue(row, ft, s"$i")};
         |  UTF8String $fieldStr = null;
         |  ${fieldToStringCode(field, fieldStr, null /* resultIsNull won't be used */)}
         |  $buffer.append($fieldStr);
         |}
       """.stripMargin
    }

    val writeStructCode = ctx.splitExpressions(
      expressions = structToStringCode.map(_.code),
      funcName = "fieldToString",
      arguments = ("InternalRow", row.code) ::
        (classOf[UTF8StringBuilder].getName, buffer.code) :: Nil)

    code"""
       |$buffer.append("[");
       |$writeStructCode
       |$buffer.append("]");
     """.stripMargin
  }

  private[this] def castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
      case BinaryType =>
        (c, evPrim, evNull) => code"$evPrim = UTF8String.fromBytes($c);"
      case DateType =>
        val df = JavaCode.global(
          ctx.addReferenceObj("dateFormatter", dateFormatter),
          dateFormatter.getClass)
        (c, evPrim, evNull) => code"""$evPrim = UTF8String.fromString(${df}.format($c));"""
      case TimestampType =>
        val tf = JavaCode.global(
          ctx.addReferenceObj("timestampFormatter", timestampFormatter),
          timestampFormatter.getClass)
        (c, evPrim, evNull) => code"""$evPrim = UTF8String.fromString(
          org.apache.spark.sql.catalyst.util.DateTimeUtils.timestampToString($tf, $c));"""
      case ArrayType(et, _) =>
        (c, evPrim, evNull) => {
          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
          val writeArrayElemCode = writeArrayToStringBuilder(et, c, buffer, ctx)
          code"""
             |$bufferClass $buffer = new $bufferClass();
             |$writeArrayElemCode;
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case MapType(kt, vt, _) =>
        (c, evPrim, evNull) => {
          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
          val writeMapElemCode = writeMapToStringBuilder(kt, vt, c, buffer, ctx)
          code"""
             |$bufferClass $buffer = new $bufferClass();
             |$writeMapElemCode;
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case StructType(fields) =>
        (c, evPrim, evNull) => {
          val row = ctx.freshVariable("row", classOf[InternalRow])
          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
          val writeStructCode = writeStructToStringBuilder(fields.map(_.dataType), row, buffer, ctx)
          code"""
             |InternalRow $row = $c;
             |$bufferClass $buffer = new $bufferClass();
             |$writeStructCode
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case pudt: PythonUserDefinedType => castToStringCode(pudt.sqlType, ctx)
      case udt: UserDefinedType[_] =>
        val udtRef = JavaCode.global(ctx.addReferenceObj("udt", udt), udt.sqlType)
        (c, evPrim, evNull) => {
          code"$evPrim = UTF8String.fromString($udtRef.deserialize($c).toString());"
        }
      case _ =>
        (c, evPrim, evNull) => code"$evPrim = UTF8String.fromString(String.valueOf($c));"
    }
  }

  private[this] def castToBinaryCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        code"$evPrim = $c.getBytes();"
    case _: IntegralType =>
      (c, evPrim, evNull) =>
        code"$evPrim = ${NumberConverter.getClass.getName.stripSuffix("$")}.toBinary($c);"
  }

  private[this] def castToDateCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val intOpt = ctx.freshVariable("intOpt", classOf[Option[Integer]])
      (c, evPrim, evNull) => code"""
        scala.Option<Integer> $intOpt =
          org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToDate($c);
        if ($intOpt.isDefined()) {
          $evPrim = ((Integer) $intOpt.get()).intValue();
        } else {
          $evNull = true;
        }
       """
    case TimestampType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      (c, evPrim, evNull) =>
        code"""$evPrim =
          org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToEpochDays($c, $zid);"""
    case _ =>
      (c, evPrim, evNull) => code"$evNull = true;"
  }

  private[this] def changePrecision(d: ExprValue, decimalType: DecimalType,
      evPrim: ExprValue, evNull: ExprValue, canNullSafeCast: Boolean): Block = {
    if (canNullSafeCast) {
      code"""
         |$d.changePrecision(${decimalType.precision}, ${decimalType.scale});
         |$evPrim = $d;
       """.stripMargin
    } else {
      code"""
         |if ($d.changePrecision(${decimalType.precision}, ${decimalType.scale})) {
         |  $evPrim = $d;
         |} else {
         |  $evNull = true;
         |}
       """.stripMargin
    }
  }

  private[this] def castToDecimalCode(
      from: DataType,
      target: DecimalType,
      ctx: CodegenContext): CastFunction = {
    val tmp = ctx.freshVariable("tmpDecimal", classOf[Decimal])
    val canNullSafeCast = Cast.canNullSafeCastToDecimal(from, target)
    from match {
      case StringType =>
        (c, evPrim, evNull) =>
          code"""
            try {
              Decimal $tmp = Decimal.apply(new java.math.BigDecimal($c.toString()));
              ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """
      case BooleanType =>
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = $c ? Decimal.apply(1) : Decimal.apply(0);
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
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
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
          """
      case DecimalType() =>
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = $c.clone();
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
          """
      case x: IntegralType =>
        (c, evPrim, evNull) =>
          code"""
            Decimal $tmp = Decimal.apply((long) $c);
            ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
          """
      case x: FractionalType =>
        // All other numeric types can be represented precisely as Doubles
        (c, evPrim, evNull) =>
          code"""
            try {
              Decimal $tmp = Decimal.apply(scala.math.BigDecimal.valueOf((double) $c));
              ${changePrecision(tmp, target, evPrim, evNull, canNullSafeCast)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """
    }
  }

  private[this] def castToTimestampCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      val zoneIdClass = classOf[ZoneId]
      val zid = JavaCode.global(
        ctx.addReferenceObj("zoneId", zoneId, zoneIdClass.getName),
        zoneIdClass)
      val longOpt = ctx.freshVariable("longOpt", classOf[Option[Long]])
      (c, evPrim, evNull) =>
        code"""
          scala.Option<Long> $longOpt =
            org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToTimestamp($c, $zid);
          if ($longOpt.isDefined()) {
            $evPrim = ((Long) $longOpt.get()).longValue();
          } else {
            $evNull = true;
          }
         """
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
          org.apache.spark.sql.catalyst.util.DateTimeUtils.epochDaysToMicros($c, $zid);"""
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = ${decimalToTimestampCode(c)};"
    case DoubleType =>
      (c, evPrim, evNull) =>
        code"""
          if (Double.isNaN($c) || Double.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * $MICROS_PER_SECOND);
          }
        """
    case FloatType =>
      (c, evPrim, evNull) =>
        code"""
          if (Float.isNaN($c) || Float.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * $MICROS_PER_SECOND);
          }
        """
  }

  private[this] def castToIntervalCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        code"""$evPrim = CalendarInterval.fromString($c.toString());
           if(${evPrim} == null) {
             ${evNull} = true;
           }
         """.stripMargin

  }

  private[this] def decimalToTimestampCode(d: ExprValue): Block = {
    val block = inline"new java.math.BigDecimal($MICROS_PER_SECOND)"
    code"($d.toBigDecimal().bigDecimal().multiply($block)).longValue()"
  }
  private[this] def longToTimeStampCode(l: ExprValue): Block = code"$l * (long)$MICROS_PER_SECOND"
  private[this] def timestampToIntegerCode(ts: ExprValue): Block =
    code"java.lang.Math.floorDiv($ts, $MICROS_PER_SECOND)"
  private[this] def timestampToDoubleCode(ts: ExprValue): Block =
    code"$ts / (double)$MICROS_PER_SECOND"

  private[this] def castToBooleanCode(from: DataType): CastFunction = from match {
    case StringType =>
      val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
      (c, evPrim, evNull) =>
        code"""
          if ($stringUtils.isTrueString($c)) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c)) {
            $evPrim = false;
          } else {
            $evNull = true;
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

  private[this] def castToByteCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
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
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = (byte) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = $c.toByte();"
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (byte) $c;"
  }

  private[this] def castToShortCode(
      from: DataType,
      ctx: CodegenContext): CastFunction = from match {
    case StringType =>
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
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = (short) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = $c.toShort();"
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (short) $c;"
  }

  private[this] def castToIntCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
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
    case TimestampType =>
      (c, evPrim, evNull) => code"$evPrim = (int) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = $c.toInt();"
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (int) $c;"
  }

  private[this] def castToLongCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
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
      (c, evPrim, evNull) => code"$evPrim = (long) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => code"$evPrim = $c.toLong();"
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (long) $c;"
  }

  private[this] def castToFloatCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        code"""
          try {
            $evPrim = Float.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
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

  private[this] def castToDoubleCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        code"""
          try {
            $evPrim = Double.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
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

  private[this] def castArrayCode(
      fromType: DataType, toType: DataType, ctx: CodegenContext): CastFunction = {
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

  private[this] def castMapCode(from: MapType, to: MapType, ctx: CodegenContext): CastFunction = {
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
      from: StructType, to: StructType, ctx: CodegenContext): CastFunction = {

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
      expressions = fieldsEvalCode.map(_.code),
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

  override def sql: String = dataType match {
    // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL, this
    // type of casting can only be introduced by the analyzer, and can be omitted when converting
    // back to SQL query string.
    case _: ArrayType | _: MapType | _: StructType => child.sql
    case _ => s"CAST(${child.sql} AS ${dataType.sql})"
  }
}

/**
 * Cast the child expression to the target data type, but will throw error if the cast might
 * truncate, e.g. long -> int, timestamp -> data.
 */
case class UpCast(child: Expression, dataType: DataType, walkedTypePath: Seq[String] = Nil)
  extends UnaryExpression with Unevaluable {
  override lazy val resolved = false
}
