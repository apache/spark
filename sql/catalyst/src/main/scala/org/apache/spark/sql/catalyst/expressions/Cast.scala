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
import java.text.{DateFormat, SimpleDateFormat}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.decimal.Decimal

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression with Logging {
  override def foldable = child.foldable

  override def nullable = (child.dataType, dataType) match {
    case (StringType, _: NumericType) => true
    case (StringType, TimestampType)  => true
    case (StringType, DateType)       => true
    case (_: NumericType, DateType)   => true
    case (BooleanType, DateType)      => true
    case (DateType, _: NumericType)   => true
    case (DateType, BooleanType)      => true
    case (_, DecimalType.Fixed(_, _)) => true  // TODO: not all upcasts here can really give null
    case _                            => child.nullable
  }

  override def toString = s"CAST($child, $dataType)"

  type EvaluatedType = Any

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  // UDFToString
  private[this] def castToString: Any => Any = child.dataType match {
    case BinaryType => buildCast[Array[Byte]](_, new String(_, "UTF-8"))
    case DateType => buildCast[Date](_, dateToString)
    case TimestampType => buildCast[Timestamp](_, timestampToString)
    case _ => buildCast[Any](_, _.toString)
  }

  // BinaryConverter
  private[this] def castToBinary: Any => Any = child.dataType match {
    case StringType => buildCast[String](_, _.getBytes("UTF-8"))
  }

  // UDFToBoolean
  private[this] def castToBoolean: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, _.length() != 0)
    case TimestampType =>
      buildCast[Timestamp](_, t => t.getTime() != 0 || t.getNanos() != 0)
    case DateType =>
      // Hive would return null when cast from date to boolean
      buildCast[Date](_, d => null)
    case LongType =>
      buildCast[Long](_, _ != 0)
    case IntegerType =>
      buildCast[Int](_, _ != 0)
    case ShortType =>
      buildCast[Short](_, _ != 0)
    case ByteType =>
      buildCast[Byte](_, _ != 0)
    case DecimalType() =>
      buildCast[Decimal](_, _ != 0)
    case DoubleType =>
      buildCast[Double](_, _ != 0)
    case FloatType =>
      buildCast[Float](_, _ != 0)
  }

  // TimestampConverter
  private[this] def castToTimestamp: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => {
        // Throw away extra if more than 9 decimal places
        val periodIdx = s.indexOf(".")
        var n = s
        if (periodIdx != -1 && n.length() - periodIdx > 9) {
          n = n.substring(0, periodIdx + 10)
        }
        try Timestamp.valueOf(n) catch { case _: java.lang.IllegalArgumentException => null }
      })
    case BooleanType =>
      buildCast[Boolean](_, b => new Timestamp((if (b) 1 else 0)))
    case LongType =>
      buildCast[Long](_, l => new Timestamp(l))
    case IntegerType =>
      buildCast[Int](_, i => new Timestamp(i))
    case ShortType =>
      buildCast[Short](_, s => new Timestamp(s))
    case ByteType =>
      buildCast[Byte](_, b => new Timestamp(b))
    case DateType =>
      buildCast[Date](_, d => new Timestamp(d.getTime))
    // TimestampWritable.decimalToTimestamp
    case DecimalType() =>
      buildCast[Decimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      buildCast[Double](_, d => decimalToTimestamp(Decimal(d)))
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      buildCast[Float](_, f => decimalToTimestamp(Decimal(f)))
  }

  private[this]  def decimalToTimestamp(d: Decimal) = {
    val seconds = Math.floor(d.toDouble).toLong
    val bd = (d.toBigDecimal - seconds) * 1000000000
    val nanos = bd.intValue()

    val millis = seconds * 1000
    val t = new Timestamp(millis)

    // remaining fractional portion as nanos
    t.setNanos(nanos)
    t
  }

  // Timestamp to long, converting milliseconds to seconds
  private[this] def timestampToLong(ts: Timestamp) = Math.floor(ts.getTime / 1000.0).toLong

  private[this] def timestampToDouble(ts: Timestamp) = {
    // First part is the seconds since the beginning of time, followed by nanosecs.
    Math.floor(ts.getTime / 1000.0).toLong + ts.getNanos.toDouble / 1000000000
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention
  private[this] def timestampToString(ts: Timestamp): String = {
    val timestampString = ts.toString
    val formatted = Cast.threadLocalTimestampFormat.get.format(ts)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention
  private[this] def timestampToDateString(ts: Timestamp): String = {
    Cast.threadLocalDateFormat.get.format(ts)
  }

  // DateConverter
  private[this] def castToDate: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s =>
        try Date.valueOf(s) catch { case _: java.lang.IllegalArgumentException => null }
      )
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Timestamp](_, t => new Date(Math.floor(t.getTime / 1000.0).toLong * 1000))
    // Hive throws this exception as a Semantic Exception
    // It is never possible to compare result when hive return with exception, so we can return null
    // NULL is more reasonable here, since the query itself obeys the grammar.
    case _ => _ => null
  }

  // Date cannot be cast to long, according to hive
  private[this] def dateToLong(d: Date) = null

  // Date cannot be cast to double, according to hive
  private[this] def dateToDouble(d: Date) = null

  // Converts Date to string according to Hive DateWritable convention
  private[this] def dateToString(d: Date): String = {
    Cast.threadLocalDateFormat.get.format(d)
  }

  // LongConverter
  private[this] def castToLong: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toLong catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case DateType =>
      buildCast[Date](_, d => dateToLong(d))
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t))
    case DecimalType() =>
      buildCast[Decimal](_, _.toLong)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  // IntConverter
  private[this] def castToInt: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toInt catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case DateType =>
      buildCast[Date](_, d => dateToLong(d))
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toInt)
    case DecimalType() =>
      buildCast[Decimal](_, _.toInt)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  // ShortConverter
  private[this] def castToShort: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toShort catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toShort else 0.toShort)
    case DateType =>
      buildCast[Date](_, d => dateToLong(d))
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toShort)
    case DecimalType() =>
      buildCast[Decimal](_, _.toShort)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  // ByteConverter
  private[this] def castToByte: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toByte catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toByte else 0.toByte)
    case DateType =>
      buildCast[Date](_, d => dateToLong(d))
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toByte)
    case DecimalType() =>
      buildCast[Decimal](_, _.toByte)
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
    decimalType match {
      case DecimalType.Unlimited =>
        value
      case DecimalType.Fixed(precision, scale) =>
        if (value.changePrecision(precision, scale)) value else null
    }
  }

  private[this] def castToDecimal(target: DecimalType): Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try changePrecision(Decimal(s.toDouble), target) catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => changePrecision(if (b) Decimal(1) else Decimal(0), target))
    case DateType =>
      buildCast[Date](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Timestamp](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
    case DecimalType() =>
      b => changePrecision(b.asInstanceOf[Decimal].clone(), target)
    case LongType =>
      b => changePrecision(Decimal(b.asInstanceOf[Long]), target)
    case x: NumericType =>  // All other numeric types can be represented precisely as Doubles
      b => changePrecision(Decimal(x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)), target)
  }

  // DoubleConverter
  private[this] def castToDouble: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toDouble catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case DateType =>
      buildCast[Date](_, d => dateToDouble(d))
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToDouble(t))
    case DecimalType() =>
      buildCast[Decimal](_, _.toDouble)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  // FloatConverter
  private[this] def castToFloat: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toFloat catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case DateType =>
      buildCast[Date](_, d => dateToDouble(d))
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToDouble(t).toFloat)
    case DecimalType() =>
      buildCast[Decimal](_, _.toFloat)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  private[this] lazy val cast: Any => Any = dataType match {
    case dt if dt == child.dataType => identity[Any]
    case StringType    => castToString
    case BinaryType    => castToBinary
    case DateType      => castToDate
    case decimal: DecimalType => castToDecimal(decimal)
    case TimestampType => castToTimestamp
    case BooleanType   => castToBoolean
    case ByteType      => castToByte
    case ShortType     => castToShort
    case IntegerType   => castToInt
    case FloatType     => castToFloat
    case LongType      => castToLong
    case DoubleType    => castToDouble
  }

  override def eval(input: Row): Any = {
    val evaluated = child.eval(input)
    if (evaluated == null) null else cast(evaluated)
  }
}

object Cast {
  // `SimpleDateFormat` is not thread-safe.
  private[sql] val threadLocalDateFormat = new ThreadLocal[DateFormat] {
    override def initialValue() = {
      new SimpleDateFormat("yyyy-MM-dd")
    }
  }

  // `SimpleDateFormat` is not thread-safe.
  private[sql] val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue() = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }
}
