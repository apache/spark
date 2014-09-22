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

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import org.apache.spark.sql.catalyst.types._

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression {
  override def foldable = child.foldable

  override def nullable = (child.dataType, dataType) match {
    case (StringType, _: NumericType) => true
    case (StringType, TimestampType)  => true
    case _                            => child.nullable
  }

  override def toString = s"CAST($child, $dataType)"

  type EvaluatedType = Any

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  // UDFToString
  private[this] def castToString: Any => Any = child.dataType match {
    case BinaryType => buildCast[Array[Byte]](_, new String(_, "UTF-8"))
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
      buildCast[Timestamp](_, b => b.getTime() != 0 || b.getNanos() != 0)
    case LongType =>
      buildCast[Long](_, _ != 0)
    case IntegerType =>
      buildCast[Int](_, _ != 0)
    case ShortType =>
      buildCast[Short](_, _ != 0)
    case ByteType =>
      buildCast[Byte](_, _ != 0)
    case DecimalType =>
      buildCast[BigDecimal](_, _ != 0)
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
      buildCast[Boolean](_, b => new Timestamp((if (b) 1 else 0) * 1000))
    case LongType =>
      buildCast[Long](_, l => new Timestamp(l * 1000))
    case IntegerType =>
      buildCast[Int](_, i => new Timestamp(i * 1000))
    case ShortType =>
      buildCast[Short](_, s => new Timestamp(s * 1000))
    case ByteType =>
      buildCast[Byte](_, b => new Timestamp(b * 1000))
    // TimestampWritable.decimalToTimestamp
    case DecimalType =>
      buildCast[BigDecimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      buildCast[Double](_, d => decimalToTimestamp(d))
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      buildCast[Float](_, f => decimalToTimestamp(f))
  }

  private[this]  def decimalToTimestamp(d: BigDecimal) = {
    val seconds = d.longValue()
    val bd = (d - seconds) * 1000000000
    val nanos = bd.intValue()

    // Convert to millis
    val millis = seconds * 1000
    val t = new Timestamp(millis)

    // remaining fractional portion as nanos
    t.setNanos(nanos)
    t
  }

  // Timestamp to long, converting milliseconds to seconds
  private[this] def timestampToLong(ts: Timestamp) = ts.getTime / 1000

  private[this] def timestampToDouble(ts: Timestamp) = {
    // First part is the seconds since the beginning of time, followed by nanosecs.
    ts.getTime / 1000 + ts.getNanos.toDouble / 1000000000
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention
  private[this] def timestampToString(ts: Timestamp): String = {
    val timestampString = ts.toString
    val formatted = Cast.threadLocalDateFormat.get.format(ts)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }

  private[this] def castToLong: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toLong catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t))
    case DecimalType =>
      buildCast[BigDecimal](_, _.toLong)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  private[this] def castToInt: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toInt catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toInt)
    case DecimalType =>
      buildCast[BigDecimal](_, _.toInt)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  private[this] def castToShort: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toShort catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toShort else 0.toShort)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toShort)
    case DecimalType =>
      buildCast[BigDecimal](_, _.toShort)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  private[this] def castToByte: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toByte catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toByte else 0.toByte)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toByte)
    case DecimalType =>
      buildCast[BigDecimal](_, _.toByte)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toByte
  }

  private[this] def castToDecimal: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try BigDecimal(s.toDouble) catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) BigDecimal(1) else BigDecimal(0))
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Timestamp](_, t => BigDecimal(timestampToDouble(t)))
    case x: NumericType =>
      b => BigDecimal(x.numeric.asInstanceOf[Numeric[Any]].toDouble(b))
  }

  private[this] def castToDouble: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toDouble catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToDouble(t))
    case DecimalType =>
      buildCast[BigDecimal](_, _.toDouble)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  private[this] def castToFloat: Any => Any = child.dataType match {
    case StringType =>
      buildCast[String](_, s => try s.toFloat catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToDouble(t).toFloat)
    case DecimalType =>
      buildCast[BigDecimal](_, _.toFloat)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  private[this] lazy val cast: Any => Any = dataType match {
    case dt if dt == child.dataType => identity[Any]
    case StringType => castToString
    case BinaryType => castToBinary
    case DecimalType => castToDecimal
    case TimestampType => castToTimestamp
    case BooleanType => castToBoolean
    case ByteType => castToByte
    case ShortType => castToShort
    case IntegerType => castToInt
    case FloatType => castToFloat
    case LongType => castToLong
    case DoubleType => castToDouble
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
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }
}
