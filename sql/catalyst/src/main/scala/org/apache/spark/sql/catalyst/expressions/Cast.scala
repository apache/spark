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
import java.lang.{NumberFormatException => NFE} 

import org.apache.spark.sql.catalyst.types._

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression {
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"CAST($child, $dataType)"

  type EvaluatedType = Any
  
  def nullOrCast[T, B](a: Any, func: T => B): B = if(a == null) {
    null.asInstanceOf[B]
  } else {
    func(a.asInstanceOf[T])
  }
  
  // UDFToString
  def castToString: Any => String = child.dataType match {
    case BinaryType => nullOrCast[Array[Byte], String](_, new String(_, "UTF-8"))
    case _ => nullOrCast[Any, String](_, _.toString)
  }
  
  // BinaryConverter
  def castToBinary: Any => Array[Byte] = child.dataType match {
    case StringType => nullOrCast[String, Array[Byte]](_, _.getBytes("UTF-8"))
  }

  // UDFToBoolean
  def castToBoolean: Any => Boolean = child.dataType match {
    case StringType => nullOrCast[String, Boolean](_, _.length() != 0)
    case TimestampType => nullOrCast[Timestamp, Boolean](_, b => {(b.getTime() != 0 || b.getNanos() != 0)})
    case LongType => nullOrCast[Long, Boolean](_, _ != 0)
    case IntegerType => nullOrCast[Int, Boolean](_, _ != 0)
    case ShortType => nullOrCast[Short, Boolean](_, _ != 0)
    case ByteType => nullOrCast[Byte, Boolean](_, _ != 0)
    case DecimalType => nullOrCast[BigDecimal, Boolean](_, _ != 0)
    case DoubleType => nullOrCast[Double, Boolean](_, _ != 0)
    case FloatType => nullOrCast[Float, Boolean](_, _ != 0)
  }
  
  // TimestampConverter
  def castToTimestamp: Any => Timestamp = child.dataType match {
    case StringType => nullOrCast[String, Timestamp](_, s => {
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
    case BooleanType => nullOrCast[Boolean, Timestamp](_, b => new Timestamp((if(b) 1 else 0) * 1000))
    case LongType => nullOrCast[Long, Timestamp](_, l => new Timestamp(l * 1000))
    case IntegerType => nullOrCast[Int, Timestamp](_, i => new Timestamp(i * 1000))
    case ShortType => nullOrCast[Short, Timestamp](_, s => new Timestamp(s * 1000))
    case ByteType => nullOrCast[Byte, Timestamp](_, b => new Timestamp(b * 1000))
    // TimestampWritable.decimalToTimestamp
    case DecimalType => nullOrCast[BigDecimal, Timestamp](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType => nullOrCast[Double, Timestamp](_, d => decimalToTimestamp(d))
    // TimestampWritable.floatToTimestamp
    case FloatType => nullOrCast[Float, Timestamp](_, f => decimalToTimestamp(f))
  }

  private def decimalToTimestamp(d: BigDecimal) = {
    val seconds = d.longValue()
    val bd = (d - seconds) * (1000000000)
    val nanos = bd.intValue()

    // Convert to millis
    val millis = seconds * 1000
    val t = new Timestamp(millis)

    // remaining fractional portion as nanos
    t.setNanos(nanos)
    
    t
  }

  private def timestampToDouble(t: Timestamp) = (t.getSeconds() + t.getNanos().toDouble / 1000)

  def castToLong: Any => Long = child.dataType match {
    case StringType => nullOrCast[String, Long](_, s => try s.toLong catch {
      case _: NFE => null.asInstanceOf[Long]
    })
    case BooleanType => nullOrCast[Boolean, Long](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, Long](_, t => timestampToDouble(t).toLong)
    case DecimalType => nullOrCast[BigDecimal, Long](_, _.toLong)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  def castToInt: Any => Int = child.dataType match {
    case StringType => nullOrCast[String, Int](_, s => try s.toInt catch {
      case _: NFE => null.asInstanceOf[Int]
    })
    case BooleanType => nullOrCast[Boolean, Int](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, Int](_, t => timestampToDouble(t).toInt)
    case DecimalType => nullOrCast[BigDecimal, Int](_, _.toInt)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  def castToShort: Any => Short = child.dataType match {
    case StringType => nullOrCast[String, Short](_, s => try s.toShort catch {
      case _: NFE => null.asInstanceOf[Short]
    })
    case BooleanType => nullOrCast[Boolean, Short](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, Short](_, t => timestampToDouble(t).toShort)
    case DecimalType => nullOrCast[BigDecimal, Short](_, _.toShort)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  def castToByte: Any => Byte = child.dataType match {
    case StringType => nullOrCast[String, Byte](_, s => try s.toByte catch {
      case _: NFE => null.asInstanceOf[Byte]
    })
    case BooleanType => nullOrCast[Boolean, Byte](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, Byte](_, t => timestampToDouble(t).toByte)
    case DecimalType => nullOrCast[BigDecimal, Byte](_, _.toByte)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toByte
  }

  def castToDecimal: Any => BigDecimal = child.dataType match {
    case StringType => nullOrCast[String, BigDecimal](_, s => try s.toDouble catch {
      case _: NFE => null.asInstanceOf[BigDecimal]
    })
    case BooleanType => nullOrCast[Boolean, BigDecimal](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, BigDecimal](_, t => timestampToDouble(t))
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  def castToDouble: Any => Double = child.dataType match {
    case StringType => nullOrCast[String, Double](_, s => try s.toInt catch {
      case _: NFE => null.asInstanceOf[Int]
    })
    case BooleanType => nullOrCast[Boolean, Double](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, Double](_, t => timestampToDouble(t))
    case DecimalType => nullOrCast[BigDecimal, Double](_, _.toDouble)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  def castToFloat: Any => Float = child.dataType match {
    case StringType => nullOrCast[String, Float](_, s => try s.toInt catch {
      case _: NFE => null.asInstanceOf[Int]
    })
    case BooleanType => nullOrCast[Boolean, Float](_, b => if(b) 1 else 0)
    case TimestampType => nullOrCast[Timestamp, Float](_, t => timestampToDouble(t).toFloat)
    case DecimalType => nullOrCast[BigDecimal, Float](_, _.toFloat)
    case x: NumericType => b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  def cast: Any => Any = (child.dataType, dataType) match {
    case (_, StringType) => castToString
    case (_, BinaryType) => castToBinary
    case (_, DecimalType) => castToDecimal
    case (_, TimestampType) => castToTimestamp
    case (_, BooleanType) => castToBoolean
    case (_, ByteType) => castToByte
    case (_, ShortType) => castToShort
    case (_, IntegerType) => castToInt
    case (_, FloatType) => castToFloat
    case (_, LongType) => castToLong
    case (_, DoubleType) => castToDouble
  }

  override def apply(input: Row): Any = {
    val evaluated = child.apply(input)
    if (evaluated == null) {
      null
    } else {
      if(child.dataType == dataType) evaluated else cast(evaluated)
    }
  }
}
