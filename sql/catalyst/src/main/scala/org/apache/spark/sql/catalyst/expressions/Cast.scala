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
import org.apache.spark.sql.types._

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType) extends UnaryExpression with Logging {

  override lazy val resolved = childrenResolved && resolve(child.dataType, dataType)

  override def foldable: Boolean = child.foldable

  override def nullable: Boolean = forceNullable(child.dataType, dataType) || child.nullable

  private[this] def forceNullable(from: DataType, to: DataType) = (from, to) match {
    case (StringType, _: NumericType) => true
    case (StringType, TimestampType)  => true
    case (DoubleType, TimestampType)  => true
    case (FloatType, TimestampType)   => true
    case (StringType, DateType)       => true
    case (_: NumericType, DateType)   => true
    case (BooleanType, DateType)      => true
    case (DateType, _: NumericType)   => true
    case (DateType, BooleanType)      => true
    case (DoubleType, _: DecimalType) => true
    case (FloatType, _: DecimalType)  => true
    case (_, DecimalType.Fixed(_, _)) => true // TODO: not all upcasts here can really give null
    case _                            => false
  }

  private[this] def resolvableNullability(from: Boolean, to: Boolean) = !from || to

  private[this] def resolve(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (from, to) if from == to         => true

      case (NullType, _)                    => true

      case (_, StringType)                  => true

      case (StringType, BinaryType)         => true

      case (StringType, BooleanType)        => true
      case (DateType, BooleanType)          => true
      case (TimestampType, BooleanType)     => true
      case (_: NumericType, BooleanType)    => true

      case (StringType, TimestampType)      => true
      case (BooleanType, TimestampType)     => true
      case (DateType, TimestampType)        => true
      case (_: NumericType, TimestampType)  => true

      case (_, DateType)                    => true

      case (StringType, _: NumericType)     => true
      case (BooleanType, _: NumericType)    => true
      case (DateType, _: NumericType)       => true
      case (TimestampType, _: NumericType)  => true
      case (_: NumericType, _: NumericType) => true

      case (ArrayType(from, fn), ArrayType(to, tn)) =>
        resolve(from, to) &&
          resolvableNullability(fn || forceNullable(from, to), tn)

      case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
        resolve(fromKey, toKey) &&
          (!forceNullable(fromKey, toKey)) &&
          resolve(fromValue, toValue) &&
          resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.size == toFields.size &&
          fromFields.zip(toFields).forall {
            case (fromField, toField) =>
              resolve(fromField.dataType, toField.dataType) &&
                resolvableNullability(
                  fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                  toField.nullable)
          }

      case _ => false
    }
  }

  override def toString: String = s"CAST($child, $dataType)"

  type EvaluatedType = Any

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  // UDFToString
  private[this] def castToString(from: DataType): Any => Any = from match {
    case BinaryType => buildCast[Array[Byte]](_, new String(_, "UTF-8"))
    case DateType => buildCast[Int](_, d => DateUtils.toString(d))
    case TimestampType => buildCast[Timestamp](_, timestampToString)
    case _ => buildCast[Any](_, _.toString)
  }

  // BinaryConverter
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case StringType => buildCast[String](_, _.getBytes("UTF-8"))
  }

  // UDFToBoolean
  private[this] def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, _.length() != 0)
    case TimestampType =>
      buildCast[Timestamp](_, t => t.getTime() != 0 || t.getNanos() != 0)
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
      buildCast[Decimal](_, _ != 0)
    case DoubleType =>
      buildCast[Double](_, _ != 0)
    case FloatType =>
      buildCast[Float](_, _ != 0)
  }

  // TimestampConverter
  private[this] def castToTimestamp(from: DataType): Any => Any = from match {
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
      buildCast[Int](_, d => new Timestamp(DateUtils.toJavaDate(d).getTime))
    // TimestampWritable.decimalToTimestamp
    case DecimalType() =>
      buildCast[Decimal](_, d => decimalToTimestamp(d))
    // TimestampWritable.doubleToTimestamp
    case DoubleType =>
      buildCast[Double](_, d => try {
        decimalToTimestamp(Decimal(d))
      } catch {
        case _: NumberFormatException => null
      })
    // TimestampWritable.floatToTimestamp
    case FloatType =>
      buildCast[Float](_, f => try {
        decimalToTimestamp(Decimal(f))
      } catch {
        case _: NumberFormatException => null
      })
  }

  private[this] def decimalToTimestamp(d: Decimal) = {
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

  // DateConverter
  private[this] def castToDate(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s =>
        try DateUtils.fromJavaDate(Date.valueOf(s))
        catch { case _: java.lang.IllegalArgumentException => null }
      )
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Timestamp](_, t => DateUtils.millisToDays(t.getTime))
    // Hive throws this exception as a Semantic Exception
    // It is never possible to compare result when hive return with exception,
    // so we can return null
    // NULL is more reasonable here, since the query itself obeys the grammar.
    case _ => _ => null
  }

  // LongConverter
  private[this] def castToLong(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try s.toLong catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1L else 0L)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  // IntConverter
  private[this] def castToInt(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try s.toInt catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1 else 0)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toInt)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b)
  }

  // ShortConverter
  private[this] def castToShort(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try s.toShort catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toShort else 0.toShort)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toShort)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toInt(b).toShort
  }

  // ByteConverter
  private[this] def castToByte(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try s.toByte catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1.toByte else 0.toByte)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToLong(t).toByte)
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

  private[this] def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try changePrecision(Decimal(s.toDouble), target) catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => changePrecision(if (b) Decimal(1) else Decimal(0), target))
    case DateType =>
      buildCast[Int](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Timestamp](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
    case DecimalType() =>
      b => changePrecision(b.asInstanceOf[Decimal].clone(), target)
    case LongType =>
      b => changePrecision(Decimal(b.asInstanceOf[Long]), target)
    case x: NumericType => // All other numeric types can be represented precisely as Doubles
      b => try {
        changePrecision(Decimal(x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)), target)
      } catch {
        case _: NumberFormatException => null
      }
  }

  // DoubleConverter
  private[this] def castToDouble(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try s.toDouble catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1d else 0d)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToDouble(t))
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toDouble(b)
  }

  // FloatConverter
  private[this] def castToFloat(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[String](_, s => try s.toFloat catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => if (b) 1f else 0f)
    case DateType =>
      buildCast[Int](_, d => null)
    case TimestampType =>
      buildCast[Timestamp](_, t => timestampToDouble(t).toFloat)
    case x: NumericType =>
      b => x.numeric.asInstanceOf[Numeric[Any]].toFloat(b)
  }

  private[this] def castArray(from: ArrayType, to: ArrayType): Any => Any = {
    val elementCast = cast(from.elementType, to.elementType)
    buildCast[Seq[Any]](_, _.map(v => if (v == null) null else elementCast(v)))
  }

  private[this] def castMap(from: MapType, to: MapType): Any => Any = {
    val keyCast = cast(from.keyType, to.keyType)
    val valueCast = cast(from.valueType, to.valueType)
    buildCast[Map[Any, Any]](_, _.map {
      case (key, value) => (keyCast(key), if (value == null) null else valueCast(value))
    })
  }

  private[this] def castStruct(from: StructType, to: StructType): Any => Any = {
    val casts = from.fields.zip(to.fields).map {
      case (fromField, toField) => cast(fromField.dataType, toField.dataType)
    }
    // TODO: This is very slow!
    buildCast[Row](_, row => Row(row.toSeq.zip(casts).map {
      case (v, cast) => if (v == null) null else cast(v)
    }: _*))
  }

  private[this] def cast(from: DataType, to: DataType): Any => Any = to match {
    case dt if dt == child.dataType => identity[Any]
    case StringType                 => castToString(from)
    case BinaryType                 => castToBinary(from)
    case DateType                   => castToDate(from)
    case decimal: DecimalType       => castToDecimal(from, decimal)
    case TimestampType              => castToTimestamp(from)
    case BooleanType                => castToBoolean(from)
    case ByteType                   => castToByte(from)
    case ShortType                  => castToShort(from)
    case IntegerType                => castToInt(from)
    case FloatType                  => castToFloat(from)
    case LongType                   => castToLong(from)
    case DoubleType                 => castToDouble(from)
    case array: ArrayType           => castArray(from.asInstanceOf[ArrayType], array)
    case map: MapType               => castMap(from.asInstanceOf[MapType], map)
    case struct: StructType         => castStruct(from.asInstanceOf[StructType], struct)
  }

  private[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

  override def eval(input: Row): Any = {
    val evaluated = child.eval(input)
    if (evaluated == null) null else cast(evaluated)
  }
}

object Cast {
  // `SimpleDateFormat` is not thread-safe.
  private[sql] val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }

  // `SimpleDateFormat` is not thread-safe.
  private[sql] val threadLocalDateFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): SimpleDateFormat = {
      new SimpleDateFormat("yyyy-MM-dd")
    }
  }
}
