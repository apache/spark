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

import org.apache.spark.sql.catalyst.types._

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression {
  override def foldable = child.foldable
  def nullable = (child.dataType, dataType) match {
    case (StringType, _: NumericType) => true
    case (StringType, TimestampType)  => true
    case _                            => child.nullable
  }
  override def toString = s"CAST($child, $dataType)"

  type EvaluatedType = Any

  def nullOrCast[T](a: Any, func: T => Any): Any = if(a == null) {
    null
  } else {
    func(a.asInstanceOf[T])
  }

  // UDFToString
  def castToString: Any => Any = child.dataType match {
    case BinaryType => nullOrCast[Array[Byte]](_, new String(_, "UTF-8"))
    case _ => nullOrCast[Any](_, _.toString)
  }

  // BinaryConverter
  def castToBinary: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, _.getBytes("UTF-8"))
  }

  // UDFToBoolean
  def castToBoolean: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, _.length() != 0)
    case TimestampType => nullOrCast[Timestamp](_, b => {(b.getTime() != 0 || b.getNanos() != 0)})
    case LongType => nullOrCast[Long](_, _ != 0)
    case IntegerType => nullOrCast[Int](_, _ != 0)
    case ShortType => nullOrCast[Short](_, _ != 0)
    case ByteType => nullOrCast[Byte](_, _ != 0)
    case DecimalType => nullOrCast[BigDecimal](_, _ != 0)
    case DoubleType => nullOrCast[Double](_, _ != 0)
    case FloatType => nullOrCast[Float](_, _ != 0)
  }

  // TimestampConverter
  def castToTimestamp: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => {
      // Throw away extra if more than 9 decimal places
      val periodIdx = s.indexOf(".");
      var n = s
      if (periodIdx != -1) {
        if (n.length() - periodIdx > 9) {
          n = n.substring(0, periodIdx + 10)
        }
      }
      try Timestamp.valueOf(n) catch { case _: java.lang.IllegalArgumentException => null}
    })
    case BooleanType => nullOrCast[Boolean](_, b => new Timestamp((if(b) 1 else 0) * 1000))
    case LongType => nullOrCast[Long](_, l => new Timestamp(l * 1000))
    case IntegerType => nullOrCast[Int](_, i => new Timestamp(i * 1000))
    case ShortType => nullOrCast[Short](_, s => new Timestamp(s * 1000))
    case ByteType => nullOrCast[Byte](_, b => new Timestamp(b * 1000))
    // TimestampWritable.decimalToTimestamp
    case DecimalType => nullOrCast[BigDecimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType => nullOrCast[Double](_, d => decimalToTimestamp(d))
    // TimestampWritable.floatToTimestamp
    case FloatType => nullOrCast[Float](_, f => decimalToTimestamp(f))
  }

  private def decimalToTimestamp(d: BigDecimal) = {
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
  private def timestampToLong(ts: Timestamp) = ts.getTime / 1000

  private def timestampToDouble(ts: Timestamp) = {
    // First part is the seconds since the beginning of time, followed by nanosecs.
    ts.getTime / 1000 + ts.getNanos.toDouble / 1000000000
  }

  def castToLong: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try s.toLong catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) 1L else 0L)
    case TimestampType => nullOrCast[Timestamp](_, t => timestampToLong(t))
    case DecimalType => nullOrCast[BigDecimal](_, _.toLong)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  def castToInt: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try s.toInt catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp](_, t => timestampToLong(t).toInt)
    case DecimalType => nullOrCast[BigDecimal](_, _.toInt)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  def castToShort: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try s.toShort catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) 1.toShort else 0.toShort)
    case TimestampType => nullOrCast[Timestamp](_, t => timestampToLong(t).toShort)
    case DecimalType => nullOrCast[BigDecimal](_, _.toShort)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  def castToByte: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try s.toByte catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) 1.toByte else 0.toByte)
    case TimestampType => nullOrCast[Timestamp](_, t => timestampToLong(t).toByte)
    case DecimalType => nullOrCast[BigDecimal](_, _.toByte)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toByte
  }

  def castToDecimal: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try BigDecimal(s.toDouble) catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) BigDecimal(1) else BigDecimal(0))
    case TimestampType =>
      // Note that we lose precision here.
      nullOrCast[Timestamp](_, t => BigDecimal(timestampToDouble(t)))
    case x: NumericType => b => BigDecimal(x.numeric.asInstanceOf[Numeric[Any]].toDouble(b))
  }

  def castToDouble: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try s.toDouble catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) 1d else 0d)
    case TimestampType => nullOrCast[Timestamp](_, t => timestampToDouble(t))
    case DecimalType => nullOrCast[BigDecimal](_, _.toDouble)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  def castToFloat: Any => Any = child.dataType match {
    case StringType => nullOrCast[String](_, s => try s.toFloat catch {
      case _: NumberFormatException => null
    })
    case BooleanType => nullOrCast[Boolean](_, b => if(b) 1f else 0f)
    case TimestampType => nullOrCast[Timestamp](_, t => timestampToDouble(t).toFloat)
    case DecimalType => nullOrCast[BigDecimal](_, _.toFloat)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  private lazy val cast: Any => Any = dataType match {
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
    if (evaluated == null) {
      null
    } else {
      cast(evaluated)
    }
  }
}
