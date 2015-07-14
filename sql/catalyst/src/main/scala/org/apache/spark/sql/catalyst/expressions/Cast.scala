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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{Interval, UTF8String}


object Cast {

  /**
   * Returns true iff we can cast `from` type to `to` type.
   */
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, StringType) => true

    case (StringType, BinaryType) => true

    case (StringType, BooleanType) => true
    case (DateType, BooleanType) => true
    case (TimestampType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

    case (StringType, TimestampType) => true
    case (BooleanType, TimestampType) => true
    case (DateType, TimestampType) => true
    case (_: NumericType, TimestampType) => true

    case (_, DateType) => true

    case (StringType, IntervalType) => true

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

    case _ => false
  }

  private def resolvableNullability(from: Boolean, to: Boolean) = !from || to

  private def forceNullable(from: DataType, to: DataType) = (from, to) match {
    case (StringType, _: NumericType) => true
    case (StringType, TimestampType) => true
    case (DoubleType, TimestampType) => true
    case (FloatType, TimestampType) => true
    case (StringType, DateType) => true
    case (_: NumericType, DateType) => true
    case (BooleanType, DateType) => true
    case (DateType, _: NumericType) => true
    case (DateType, BooleanType) => true
    case (DoubleType, _: DecimalType) => true
    case (FloatType, _: DecimalType) => true
    case (_, DecimalType.Fixed(_, _)) => true // TODO: not all upcasts here can really give null
    case _ => false
  }
}

/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType)
  extends UnaryExpression with CodegenFallback {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast ${child.dataType} to $dataType")
    }
  }

  override def nullable: Boolean = Cast.forceNullable(child.dataType, dataType) || child.nullable

  override def toString: String = s"CAST($child, $dataType)"

  // [[func]] assumes the input is no longer null because eval already does the null check.
  @inline private[this] def buildCast[T](a: Any, func: T => Any): Any = func(a.asInstanceOf[T])

  // UDFToString
  private[this] def castToString(from: DataType): Any => Any = from match {
    case BinaryType => buildCast[Array[Byte]](_, UTF8String.fromBytes)
    case DateType => buildCast[Int](_, d => UTF8String.fromString(DateTimeUtils.dateToString(d)))
    case TimestampType => buildCast[Long](_,
      t => UTF8String.fromString(DateTimeUtils.timestampToString(t)))
    case _ => buildCast[Any](_, o => UTF8String.fromString(o.toString))
  }

  // BinaryConverter
  private[this] def castToBinary(from: DataType): Any => Any = from match {
    case StringType => buildCast[UTF8String](_, _.getBytes)
  }

  // UDFToBoolean
  private[this] def castToBoolean(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, _.numBytes() != 0)
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
      buildCast[Decimal](_, _ != Decimal(0))
    case DoubleType =>
      buildCast[Double](_, _ != 0)
    case FloatType =>
      buildCast[Float](_, _ != 0)
  }

  // TimestampConverter
  private[this] def castToTimestamp(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, utfs => DateTimeUtils.stringToTimestamp(utfs).orNull)
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
      buildCast[Int](_, d => DateTimeUtils.daysToMillis(d) * 1000)
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
    (d.toBigDecimal * 1000000L).longValue()
  }
  private[this] def doubleToTimestamp(d: Double): Any = {
    if (d.isNaN || d.isInfinite) null else (d * 1000000L).toLong
  }

  // converting milliseconds to us
  private[this] def longToTimestamp(t: Long): Long = t * 1000L
  // converting us to seconds
  private[this] def timestampToLong(ts: Long): Long = math.floor(ts.toDouble / 1000000L).toLong
  // converting us to seconds in double
  private[this] def timestampToDouble(ts: Long): Double = {
    ts / 1000000.0
  }

  // DateConverter
  private[this] def castToDate(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => DateTimeUtils.stringToDate(s).orNull)
    case TimestampType =>
      // throw valid precision more than seconds, according to Hive.
      // Timestamp.nanos is in 0 to 999,999,999, no more than a second.
      buildCast[Long](_, t => DateTimeUtils.millisToDays(t / 1000L))
    // Hive throws this exception as a Semantic Exception
    // It is never possible to compare result when hive return with exception,
    // so we can return null
    // NULL is more reasonable here, since the query itself obeys the grammar.
    case _ => _ => null
  }

  // IntervalConverter
  private[this] def castToInterval(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => Interval.fromString(s.toString))
    case _ => _ => null
  }

  // LongConverter
  private[this] def castToLong(from: DataType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try s.toString.toLong catch {
        case _: NumberFormatException => null
      })
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
      buildCast[UTF8String](_, s => try s.toString.toInt catch {
        case _: NumberFormatException => null
      })
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
      buildCast[UTF8String](_, s => try s.toString.toShort catch {
        case _: NumberFormatException => null
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
      buildCast[UTF8String](_, s => try s.toString.toByte catch {
        case _: NumberFormatException => null
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
    decimalType match {
      case DecimalType.Unlimited =>
        value
      case DecimalType.Fixed(precision, scale) =>
        if (value.changePrecision(precision, scale)) value else null
    }
  }

  private[this] def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try {
        changePrecision(Decimal(new JavaBigDecimal(s.toString)), target)
      } catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => changePrecision(if (b) Decimal(1) else Decimal(0), target))
    case DateType =>
      buildCast[Int](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Long](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
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

  private[this] def castArray(from: ArrayType, to: ArrayType): Any => Any = {
    val elementCast = cast(from.elementType, to.elementType)
    buildCast[Seq[Any]](_, seq => seq.map(v => if (v == null) null else elementCast(v)))
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
    // TODO: Could be faster?
    val newRow = new GenericMutableRow(from.fields.length)
    buildCast[InternalRow](_, row => {
      var i = 0
      while (i < row.length) {
        newRow.update(i, if (row.isNullAt(i)) null else casts(i)(row(i)))
        i += 1
      }
      newRow.copy()
    })
  }

  private[this] def cast(from: DataType, to: DataType): Any => Any = to match {
    case dt if dt == child.dataType => identity[Any]
    case StringType => castToString(from)
    case BinaryType => castToBinary(from)
    case DateType => castToDate(from)
    case decimal: DecimalType => castToDecimal(from, decimal)
    case TimestampType => castToTimestamp(from)
    case IntervalType => castToInterval(from)
    case BooleanType => castToBoolean(from)
    case ByteType => castToByte(from)
    case ShortType => castToShort(from)
    case IntegerType => castToInt(from)
    case FloatType => castToFloat(from)
    case LongType => castToLong(from)
    case DoubleType => castToDouble(from)
    case array: ArrayType => castArray(from.asInstanceOf[ArrayType], array)
    case map: MapType => castMap(from.asInstanceOf[MapType], map)
    case struct: StructType => castStruct(from.asInstanceOf[StructType], struct)
  }

  private[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

  protected override def nullSafeEval(input: Any): Any = cast(input)

  private[this] class CodeHolder private() {
    // expression that can be directly assigned to result's primitive
    // similar to f in `defineCodeGen`
    private var _cg: String => String = null
    // statements to put in null safety section
    // similar to f in `nullSafeCodeGen`
    private var _ns: (String, String, String) => String = null

    // child.primitive
    def set(f: String => String): CodeHolder = {_cg = f; this}

    // child.primitive, result.primitive, result.isNull
    def set(f: (String, String, String) => String): CodeHolder = {_ns = f; this}

    def code(ctx: CodeGenContext, childPrim: String, childNull: String,
      resultPrim: String, resultNull: String, resultType: DataType): String = {
      if (_cg != null) {
        s"""
          boolean $resultNull = $childNull;
          ${ctx.javaType(resultType)} $resultPrim = ${ctx.defaultValue(resultType)};
          if (!${childNull}) {
            $resultPrim = ${_cg(childPrim)};
          }
        """
      } else {
        s"""
          boolean $resultNull = $childNull;
          ${ctx.javaType(resultType)} $resultPrim = ${ctx.defaultValue(resultType)};
          if (!${childNull}) {
            ${_ns(childPrim, resultPrim, resultNull)}
          }
        """
      }
    }
  }

  private[this] object CodeHolder {
    def apply(f: String => String): CodeHolder = (new CodeHolder).set(f)
    def apply(f: (String, String, String) => String): CodeHolder = (new CodeHolder).set(f)
  }

  private[this] def getCodeHolder(from: DataType, to: DataType, ctx: CodeGenContext) = to match {
    case StringType => castToStringCode(from, ctx)
    case BinaryType => castToBinaryCode(from)
    case DateType => castToDateCode(from)
    case decimal: DecimalType => castToDecimalCode(from, decimal)
    case TimestampType => castToTimestampCode(from)
    case BooleanType => castToBooleanCode(from)
    case ByteType => castToByteCode(from)
    case ShortType => castToShortCode(from)
    case IntegerType => castToIntCode(from)
    case FloatType => castToFloatCode(from)
    case LongType => castToLongCode(from)
    case DoubleType => castToDoubleCode(from)

    case array: ArrayType => castArrayCode(from.asInstanceOf[ArrayType], array, ctx)
    case map: MapType => castMapCode(from.asInstanceOf[MapType], map, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
    case other => null
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    val holder = getCodeHolder(child.dataType, dataType, ctx)
    if (holder != null) {
      eval.code + holder.code(ctx, eval.primitive, eval.isNull, ev.primitive, ev.isNull, dataType)
    } else {
      super.genCode(ctx, ev)
    }
  }

  private[this] def castToStringCode(from: DataType, ctx: CodeGenContext): CodeHolder = {
    from match {
      case BinaryType =>
        CodeHolder(c => s"UTF8String.fromBytes($c)")
      case DateType =>
        CodeHolder(c => s"""UTF8String.fromString(
        org.apache.spark.sql.catalyst.util.DateTimeUtils.dateToString($c))""")
      case TimestampType =>
        CodeHolder(c => s"""UTF8String.fromString(
        org.apache.spark.sql.catalyst.util.DateTimeUtils.timestampToString($c))""")
      case _ =>
        CodeHolder(c => s"UTF8String.fromString(String.valueOf($c))")
    }
  }

  private[this] def castToBinaryCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder(c => s"$c.getBytes()")
  }

  private[this] def castToDateCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) => s"""
        try {
          $evPrim = org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(
            java.sql.Date.valueOf($c.toString()));
        } catch (java.lang.IllegalArgumentException e) {
         $evNull = true;
        }
       """)
    case TimestampType =>
      CodeHolder(c => s"org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToDays($c / 1000L)")
    case _ =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
  }

  private[this] def changePrecision(d: String, decimalType: DecimalType,
      evPrim: String, evNull: String): String = {
    decimalType match {
      case DecimalType.Unlimited =>
        s"$evPrim = $d;"
      case DecimalType.Fixed(precision, scale) =>
        s"""
          if ($d.changePrecision($precision, $scale)) {
            $evPrim = $d;
          } else {
            $evNull = true;
          }
        """
    }
  }

  private[this] def castToDecimalCode(from: DataType, target: DecimalType): CodeHolder = {
    from match {
      case StringType =>
        CodeHolder((c, evPrim, evNull) =>
          s"""
            try {
              org.apache.spark.sql.types.Decimal tmpDecimal =
                new org.apache.spark.sql.types.Decimal().set(
                  new scala.math.BigDecimal(
                    new java.math.BigDecimal($c.toString())));
              ${changePrecision("tmpDecimal", target, evPrim, evNull)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """)
      case BooleanType =>
        CodeHolder((c, evPrim, evNull) =>
          s"""
            org.apache.spark.sql.types.Decimal tmpDecimal = null;
            if ($c) {
              tmpDecimal = new org.apache.spark.sql.types.Decimal().set(1);
            } else {
              tmpDecimal = new org.apache.spark.sql.types.Decimal().set(0);
            }
            ${changePrecision("tmpDecimal", target, evPrim, evNull)}
          """)
      case DateType =>
        // date can't cast to decimal in Hive
        CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
      case TimestampType =>
        // Note that we lose precision here.
        CodeHolder((c, evPrim, evNull) =>
          s"""
            org.apache.spark.sql.types.Decimal tmpDecimal =
              new org.apache.spark.sql.types.Decimal().set(
                scala.math.BigDecimal.valueOf(${timestampToDoubleCode(c)}));
            ${changePrecision("tmpDecimal", target, evPrim, evNull)}
          """)
      case DecimalType() =>
        CodeHolder((c, evPrim, evNull) =>
          s"""
            org.apache.spark.sql.types.Decimal tmpDecimal = $c.clone();
            ${changePrecision("tmpDecimal", target, evPrim, evNull)}
          """)
      case LongType =>
        CodeHolder((c, evPrim, evNull) =>
          s"""
            org.apache.spark.sql.types.Decimal tmpDecimal =
              new org.apache.spark.sql.types.Decimal().set($c);
            ${changePrecision("tmpDecimal", target, evPrim, evNull)}
          """)
      case x: NumericType =>
        // All other numeric types can be represented precisely as Doubles
        CodeHolder((c, evPrim, evNull) =>
          s"""
            try {
              org.apache.spark.sql.types.Decimal tmpDecimal =
                new org.apache.spark.sql.types.Decimal().set(
                  scala.math.BigDecimal.valueOf((double) $c));
              ${changePrecision("tmpDecimal", target, evPrim, evNull)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """
        )
    }
  }

  private[this] def castToTimestampCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp(
              java.sql.Timestamp.valueOf($c.toString()));
          } catch (java.lang.IllegalArgumentException e) {
            $evNull = true;
          }
         """
      )
    case BooleanType =>
      CodeHolder(c => s"$c ? 1L : 0")
    case _: IntegralType =>
      CodeHolder(c => longToTimeStampCode(c))
    case DateType =>
      CodeHolder(c => s"org.apache.spark.sql.catalyst.util.DateTimeUtils.daysToMillis($c) * 1000")
    case DecimalType() =>
      CodeHolder(c => decimalToTimestampCode(c))
    case DoubleType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          if (Double.isNaN($c) || Double.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * 1000000L);
          }
        """
      )
    case FloatType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          if (Float.isNaN($c) || Float.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * 1000000L);
          }
        """)
  }

  private[this] def decimalToTimestampCode(d: String): String =
    s"($d.toBigDecimal().bigDecimal().multiply(new java.math.BigDecimal(1000000L))).longValue()"
  private[this] def longToTimeStampCode(l: String): String = s"$l * 1000L"
  private[this] def timestampToIntegerCode(ts: String): String =
    s"java.lang.Math.floor((double) $ts / 1000000L)"
  private[this] def timestampToDoubleCode(ts: String): String = s"$ts / 1000000.0"

  private[this] def castToBooleanCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder(c => s"$c.numBytes() != 0")
    case TimestampType =>
      CodeHolder(c => s"$c != 0")
    case DateType =>
      // Hive would return null when cast from date to boolean
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case DecimalType() =>
      CodeHolder(c => s"!$c.isZero()")
    case n: NumericType =>
      CodeHolder(c => s"$c != 0")
  }

  private[this] def castToByteCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Byte.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """)
    case BooleanType =>
      CodeHolder(c => s"$c ? 1 : 0")
    case DateType =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case TimestampType =>
      CodeHolder(c => s"(byte) ${timestampToIntegerCode(c)}")
    case DecimalType() =>
      CodeHolder(c => s"$c.toByte()")
    case x: NumericType =>
      CodeHolder(c => s"(byte) $c")
  }

  private[this] def castToShortCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Short.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """)
    case BooleanType =>
      CodeHolder(c => s"$c ? 1 : 0")
    case DateType =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case TimestampType =>
      CodeHolder(c => s"(short) ${timestampToIntegerCode(c)}")
    case DecimalType() =>
      CodeHolder(c => s"$c.toShort()")
    case x: NumericType =>
      CodeHolder(c => s"(short) $c")
  }

  private[this] def castToIntCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Integer.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """)
    case BooleanType =>
      CodeHolder(c => s"$c ? 1 : 0")
    case DateType =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case TimestampType =>
      CodeHolder(c => s"(int) ${timestampToIntegerCode(c)}")
    case DecimalType() =>
      CodeHolder(c => s"$c.toInt()")
    case x: NumericType =>
      CodeHolder(c => s"(int) $c")
  }

  private[this] def castToLongCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Long.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """)
    case BooleanType =>
      CodeHolder(c => s"$c ? 1 : 0")
    case DateType =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case TimestampType =>
      CodeHolder(c => s"(long) ${timestampToIntegerCode(c)}")
    case DecimalType() =>
      CodeHolder(c => s"$c.toLong()")
    case x: NumericType =>
      CodeHolder(c => s"(long) $c")
  }

  private[this] def castToFloatCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Float.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """)
    case BooleanType =>
      CodeHolder(c => s"$c ? 1 : 0")
    case DateType =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case TimestampType =>
      CodeHolder(c => s"(float) (${timestampToDoubleCode(c)})")
    case DecimalType() =>
      CodeHolder(c => s"$c.toFloat()")
    case x: NumericType =>
      CodeHolder(c => s"(float) $c")
  }

  private[this] def castToDoubleCode(from: DataType): CodeHolder = from match {
    case StringType =>
      CodeHolder((c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Double.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """)
    case BooleanType =>
      CodeHolder(c => s"$c ? 1 : 0")
    case DateType =>
      CodeHolder((c, evPrim, evNull) => s"$evNull = true;")
    case TimestampType =>
      CodeHolder(c => timestampToDoubleCode(c))
    case DecimalType() =>
      CodeHolder(c => s"$c.toDouble()")
    case x: NumericType =>
      CodeHolder(c => s"(double) $c")
  }

  private[this] def castArrayCode(
      from: ArrayType, to: ArrayType, ctx: CodeGenContext): CodeHolder = {
    val elementCodeHolder = getCodeHolder(from.elementType, to.elementType, ctx)

    val arraySeqClass = "scala.collection.mutable.ArraySeq"
    val fromElementNull = ctx.freshName("feNull")
    val fromElementPrim = ctx.freshName("fePrim")
    val toElementNull = ctx.freshName("teNull")
    val toElementPrim = ctx.freshName("tePrim")
    val size = ctx.freshName("n")
    val j = ctx.freshName("j")
    val result = ctx.freshName("result")

    CodeHolder((c, evPrim, evNull) =>
      s"""
        final int $size = $c.size();
        final $arraySeqClass<Object> $result = new $arraySeqClass<Object>($size);
        for (int $j = 0; $j < $size; $j ++) {
          if ($c.apply($j) == null) {
            $result.update($j, null);
          } else {
            boolean $fromElementNull = false;
            ${ctx.boxedType(from.elementType)} $fromElementPrim =
              (${ctx.boxedType(from.elementType)}) $c.apply($j);
            ${elementCodeHolder.code(ctx, fromElementPrim,
              fromElementNull, toElementPrim, toElementNull, to.elementType)}
            if ($toElementNull) {
              $result.update($j, null);
            } else {
              $result.update($j, $toElementPrim);
            }
          }
        }
        $evPrim = $result;
      """)
  }

  private[this] def castMapCode(from: MapType, to: MapType, ctx: CodeGenContext): CodeHolder = {
    val keyCodeHolder = getCodeHolder(from.keyType, to.keyType, ctx)
    val valueCodeHolder = getCodeHolder(from.valueType, to.valueType, ctx)

    val hashMapClass = "scala.collection.mutable.HashMap"
    val fromKeyPrim = ctx.freshName("fkp")
    val fromKeyNull = ctx.freshName("fkn")
    val fromValuePrim = ctx.freshName("fvp")
    val fromValueNull = ctx.freshName("fvn")
    val toKeyPrim = ctx.freshName("tkp")
    val toKeyNull = ctx.freshName("tkn")
    val toValuePrim = ctx.freshName("tvp")
    val toValueNull = ctx.freshName("tvn")
    val result = ctx.freshName("result")


    CodeHolder((c, evPrim, evNull) =>
      s"""
        final $hashMapClass $result = new $hashMapClass();
        scala.collection.Iterator iter = $c.iterator();
        while (iter.hasNext()) {
          scala.Tuple2 kv = (scala.Tuple2) iter.next();
          boolean $fromKeyNull = false;
          ${ctx.boxedType(from.keyType)} $fromKeyPrim =
            (${ctx.boxedType(from.keyType)}) kv._1();
          ${keyCodeHolder.code(ctx, fromKeyPrim,
            fromKeyNull, toKeyPrim, toKeyNull, to.keyType)}

          boolean $fromValueNull = kv._2() == null;
          if ($fromValueNull) {
            $result.put($toKeyPrim, null);
          } else {
            ${ctx.boxedType(from.valueType)} $fromValuePrim =
              (${ctx.boxedType(from.valueType)}) kv._2();
            ${valueCodeHolder.code(ctx, fromValuePrim,
              fromValueNull, toValuePrim, toValueNull, to.valueType)}
            if ($toValueNull) {
              $result.put($toKeyPrim, null);
            } else {
              $result.put($toKeyPrim, $toValuePrim);
            }
          }
        }
        $evPrim = $result;
      """)
  }

  private[this] def castStructCode(
      from: StructType, to: StructType, ctx: CodeGenContext): CodeHolder = {

    val fieldsCodeHolder = from.fields.zip(to.fields).map {
      case (fromField, toField) => getCodeHolder(fromField.dataType, toField.dataType, ctx)
    }
    val rowClass = "org.apache.spark.sql.catalyst.expressions.GenericMutableRow"
    val result = ctx.freshName("result")
    val tmpRow = ctx.freshName("tmpRow")

    val fieldsEvalCode = fieldsCodeHolder.zipWithIndex.map { case (holder, i) => {
      val fromFieldPrim = ctx.freshName("ffp")
      val fromFieldNull = ctx.freshName("ffn")
      val toFieldPrim = ctx.freshName("tfp")
      val toFieldNull = ctx.freshName("tfn")
      val fromType = ctx.boxedType(from.fields(i).dataType)
      s"""
        boolean $fromFieldNull = $tmpRow.isNullAt($i);
        if ($fromFieldNull) {
          $result.update($i, null);
        } else {
          $fromType $fromFieldPrim = ($fromType) $tmpRow.apply($i);
          ${holder.code(ctx, fromFieldPrim,
            fromFieldNull, toFieldPrim, toFieldNull, to.fields(i).dataType)}
          if ($toFieldNull) {
            $result.update($i, null);
          } else {
            $result.update($i, $toFieldPrim);
          }
        }
       """
      }
    }.mkString("\n")

    CodeHolder((c, evPrim, evNull) =>
      s"""
        final $rowClass $result = new $rowClass(${fieldsCodeHolder.size});
        final InternalRow $tmpRow = $c;
        $fieldsEvalCode
        $evPrim = $result.copy();
      """)
  }
}
