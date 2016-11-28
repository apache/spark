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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


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

  override def toString: String = s"cast($child as ${dataType.simpleString})"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast ${child.dataType} to $dataType")
    }
  }

  override def nullable: Boolean = Cast.forceNullable(child.dataType, dataType) || child.nullable

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

  // converting seconds to us
  private[this] def longToTimestamp(t: Long): Long = t * 1000000L
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
      buildCast[UTF8String](_, s => CalendarInterval.fromString(s.toString))
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
    if (value.changePrecision(decimalType.precision, decimalType.scale)) value else null
  }

  private[this] def castToDecimal(from: DataType, target: DecimalType): Any => Any = from match {
    case StringType =>
      buildCast[UTF8String](_, s => try {
        changePrecision(Decimal(new JavaBigDecimal(s.toString)), target)
      } catch {
        case _: NumberFormatException => null
      })
    case BooleanType =>
      buildCast[Boolean](_, b => changePrecision(if (b) Decimal.ONE else Decimal.ZERO, target))
    case DateType =>
      buildCast[Int](_, d => null) // date can't cast to decimal in Hive
    case TimestampType =>
      // Note that we lose precision here.
      buildCast[Long](_, t => changePrecision(Decimal(timestampToDouble(t)), target))
    case dt: DecimalType =>
      b => changePrecision(b.asInstanceOf[Decimal].clone(), target)
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
    val newRow = new GenericMutableRow(from.fields.length)
    buildCast[InternalRow](_, row => {
      var i = 0
      while (i < row.numFields) {
        newRow.update(i,
          if (row.isNullAt(i)) null else castFuncs(i)(row.get(i, from.apply(i).dataType)))
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
    case CalendarIntervalType => castToInterval(from)
    case BooleanType => castToBoolean(from)
    case ByteType => castToByte(from)
    case ShortType => castToShort(from)
    case IntegerType => castToInt(from)
    case FloatType => castToFloat(from)
    case LongType => castToLong(from)
    case DoubleType => castToDouble(from)
    case array: ArrayType => castArray(from.asInstanceOf[ArrayType].elementType, array.elementType)
    case map: MapType => castMap(from.asInstanceOf[MapType], map)
    case struct: StructType => castStruct(from.asInstanceOf[StructType], struct)
    case udt: UserDefinedType[_]
      if udt.userClass == from.asInstanceOf[UserDefinedType[_]].userClass =>
      identity[Any]
    case _: UserDefinedType[_] =>
      throw new SparkException(s"Cannot cast $from to $to.")
  }

  private[this] lazy val cast: Any => Any = cast(child.dataType, dataType)

  protected override def nullSafeEval(input: Any): Any = cast(input)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    val nullSafeCast = nullSafeCastFunction(child.dataType, dataType, ctx)
    eval.code +
      castCode(ctx, eval.value, eval.isNull, ev.value, ev.isNull, dataType, nullSafeCast)
  }

  // three function arguments are: child.primitive, result.primitive and result.isNull
  // it returns the code snippets to be put in null safe evaluation region
  private[this] type CastFunction = (String, String, String) => String

  private[this] def nullSafeCastFunction(
      from: DataType,
      to: DataType,
      ctx: CodeGenContext): CastFunction = to match {

    case _ if from == NullType => (c, evPrim, evNull) => s"$evNull = true;"
    case _ if to == from => (c, evPrim, evNull) => s"$evPrim = $c;"
    case StringType => castToStringCode(from, ctx)
    case BinaryType => castToBinaryCode(from)
    case DateType => castToDateCode(from, ctx)
    case decimal: DecimalType => castToDecimalCode(from, decimal, ctx)
    case TimestampType => castToTimestampCode(from, ctx)
    case CalendarIntervalType => castToIntervalCode(from)
    case BooleanType => castToBooleanCode(from)
    case ByteType => castToByteCode(from)
    case ShortType => castToShortCode(from)
    case IntegerType => castToIntCode(from)
    case FloatType => castToFloatCode(from)
    case LongType => castToLongCode(from)
    case DoubleType => castToDoubleCode(from)

    case array: ArrayType =>
      castArrayCode(from.asInstanceOf[ArrayType].elementType, array.elementType, ctx)
    case map: MapType => castMapCode(from.asInstanceOf[MapType], map, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
    case udt: UserDefinedType[_]
      if udt.userClass == from.asInstanceOf[UserDefinedType[_]].userClass =>
      (c, evPrim, evNull) => s"$evPrim = $c;"
    case _: UserDefinedType[_] =>
      throw new SparkException(s"Cannot cast $from to $to.")
  }

  // Since we need to cast child expressions recursively inside ComplexTypes, such as Map's
  // Key and Value, Struct's field, we need to name out all the variable names involved in a cast.
  private[this] def castCode(ctx: CodeGenContext, childPrim: String, childNull: String,
    resultPrim: String, resultNull: String, resultType: DataType, cast: CastFunction): String = {
    s"""
      boolean $resultNull = $childNull;
      ${ctx.javaType(resultType)} $resultPrim = ${ctx.defaultValue(resultType)};
      if (!${childNull}) {
        ${cast(childPrim, resultPrim, resultNull)}
      }
    """
  }

  private[this] def castToStringCode(from: DataType, ctx: CodeGenContext): CastFunction = {
    from match {
      case BinaryType =>
        (c, evPrim, evNull) => s"$evPrim = UTF8String.fromBytes($c);"
      case DateType =>
        (c, evPrim, evNull) => s"""$evPrim = UTF8String.fromString(
          org.apache.spark.sql.catalyst.util.DateTimeUtils.dateToString($c));"""
      case TimestampType =>
        (c, evPrim, evNull) => s"""$evPrim = UTF8String.fromString(
          org.apache.spark.sql.catalyst.util.DateTimeUtils.timestampToString($c));"""
      case _ =>
        (c, evPrim, evNull) => s"$evPrim = UTF8String.fromString(String.valueOf($c));"
    }
  }

  private[this] def castToBinaryCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) => s"$evPrim = $c.getBytes();"
  }

  private[this] def castToDateCode(
      from: DataType,
      ctx: CodeGenContext): CastFunction = from match {
    case StringType =>
      val intOpt = ctx.freshName("intOpt")
      (c, evPrim, evNull) => s"""
        scala.Option<Integer> $intOpt =
          org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToDate($c);
        if ($intOpt.isDefined()) {
          $evPrim = ((Integer) $intOpt.get()).intValue();
        } else {
          $evNull = true;
        }
       """
    case TimestampType =>
      (c, evPrim, evNull) =>
        s"$evPrim = org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToDays($c / 1000L);";
    case _ =>
      (c, evPrim, evNull) => s"$evNull = true;"
  }

  private[this] def changePrecision(d: String, decimalType: DecimalType,
      evPrim: String, evNull: String): String =
    s"""
      if ($d.changePrecision(${decimalType.precision}, ${decimalType.scale})) {
        $evPrim = $d;
      } else {
        $evNull = true;
      }
    """

  private[this] def castToDecimalCode(
      from: DataType,
      target: DecimalType,
      ctx: CodeGenContext): CastFunction = {
    val tmp = ctx.freshName("tmpDecimal")
    from match {
      case StringType =>
        (c, evPrim, evNull) =>
          s"""
            try {
              Decimal $tmp = Decimal.apply(new java.math.BigDecimal($c.toString()));
              ${changePrecision(tmp, target, evPrim, evNull)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """
      case BooleanType =>
        (c, evPrim, evNull) =>
          s"""
            Decimal $tmp = $c ? Decimal.apply(1) : Decimal.apply(0);
            ${changePrecision(tmp, target, evPrim, evNull)}
          """
      case DateType =>
        // date can't cast to decimal in Hive
        (c, evPrim, evNull) => s"$evNull = true;"
      case TimestampType =>
        // Note that we lose precision here.
        (c, evPrim, evNull) =>
          s"""
            Decimal $tmp = Decimal.apply(
              scala.math.BigDecimal.valueOf(${timestampToDoubleCode(c)}));
            ${changePrecision(tmp, target, evPrim, evNull)}
          """
      case DecimalType() =>
        (c, evPrim, evNull) =>
          s"""
            Decimal $tmp = $c.clone();
            ${changePrecision(tmp, target, evPrim, evNull)}
          """
      case x: IntegralType =>
        (c, evPrim, evNull) =>
          s"""
            Decimal $tmp = Decimal.apply((long) $c);
            ${changePrecision(tmp, target, evPrim, evNull)}
          """
      case x: FractionalType =>
        // All other numeric types can be represented precisely as Doubles
        (c, evPrim, evNull) =>
          s"""
            try {
              Decimal $tmp = Decimal.apply(scala.math.BigDecimal.valueOf((double) $c));
              ${changePrecision(tmp, target, evPrim, evNull)}
            } catch (java.lang.NumberFormatException e) {
              $evNull = true;
            }
          """
    }
  }

  private[this] def castToTimestampCode(
      from: DataType,
      ctx: CodeGenContext): CastFunction = from match {
    case StringType =>
      val longOpt = ctx.freshName("longOpt")
      (c, evPrim, evNull) =>
        s"""
          scala.Option<Long> $longOpt =
            org.apache.spark.sql.catalyst.util.DateTimeUtils.stringToTimestamp($c);
          if ($longOpt.isDefined()) {
            $evPrim = ((Long) $longOpt.get()).longValue();
          } else {
            $evNull = true;
          }
         """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? 1L : 0L;"
    case _: IntegralType =>
      (c, evPrim, evNull) => s"$evPrim = ${longToTimeStampCode(c)};"
    case DateType =>
      (c, evPrim, evNull) =>
        s"$evPrim = org.apache.spark.sql.catalyst.util.DateTimeUtils.daysToMillis($c) * 1000;"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = ${decimalToTimestampCode(c)};"
    case DoubleType =>
      (c, evPrim, evNull) =>
        s"""
          if (Double.isNaN($c) || Double.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * 1000000L);
          }
        """
    case FloatType =>
      (c, evPrim, evNull) =>
        s"""
          if (Float.isNaN($c) || Float.isInfinite($c)) {
            $evNull = true;
          } else {
            $evPrim = (long)($c * 1000000L);
          }
        """
  }

  private[this] def castToIntervalCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""$evPrim = CalendarInterval.fromString($c.toString());
           if(${evPrim} == null) {
             ${evNull} = true;
           }
         """.stripMargin

  }

  private[this] def decimalToTimestampCode(d: String): String =
    s"($d.toBigDecimal().bigDecimal().multiply(new java.math.BigDecimal(1000000L))).longValue()"
  private[this] def longToTimeStampCode(l: String): String = s"$l * 1000000L"
  private[this] def timestampToIntegerCode(ts: String): String =
    s"java.lang.Math.floor((double) $ts / 1000000L)"
  private[this] def timestampToDoubleCode(ts: String): String = s"$ts / 1000000.0"

  private[this] def castToBooleanCode(from: DataType): CastFunction = from match {
    case StringType =>
      val stringUtils = StringUtils.getClass.getName.stripSuffix("$")
      (c, evPrim, evNull) =>
        s"""
          if ($stringUtils.isTrueString($c)) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c)) {
            $evPrim = false;
          } else {
            $evNull = true;
          }
        """
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = $c != 0;"
    case DateType =>
      // Hive would return null when cast from date to boolean
      (c, evPrim, evNull) => s"$evNull = true;"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = !$c.isZero();"
    case n: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = $c != 0;"
  }

  private[this] def castToByteCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Byte.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? (byte) 1 : (byte) 0;"
    case DateType =>
      (c, evPrim, evNull) => s"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = (byte) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = $c.toByte();"
    case x: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = (byte) $c;"
  }

  private[this] def castToShortCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Short.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? (short) 1 : (short) 0;"
    case DateType =>
      (c, evPrim, evNull) => s"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = (short) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = $c.toShort();"
    case x: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = (short) $c;"
  }

  private[this] def castToIntCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Integer.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? 1 : 0;"
    case DateType =>
      (c, evPrim, evNull) => s"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = (int) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = $c.toInt();"
    case x: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = (int) $c;"
  }

  private[this] def castToLongCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Long.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? 1L : 0L;"
    case DateType =>
      (c, evPrim, evNull) => s"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = (long) ${timestampToIntegerCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = $c.toLong();"
    case x: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = (long) $c;"
  }

  private[this] def castToFloatCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Float.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? 1.0f : 0.0f;"
    case DateType =>
      (c, evPrim, evNull) => s"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = (float) (${timestampToDoubleCode(c)});"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = $c.toFloat();"
    case x: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = (float) $c;"
  }

  private[this] def castToDoubleCode(from: DataType): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) =>
        s"""
          try {
            $evPrim = Double.valueOf($c.toString());
          } catch (java.lang.NumberFormatException e) {
            $evNull = true;
          }
        """
    case BooleanType =>
      (c, evPrim, evNull) => s"$evPrim = $c ? 1.0d : 0.0d;"
    case DateType =>
      (c, evPrim, evNull) => s"$evNull = true;"
    case TimestampType =>
      (c, evPrim, evNull) => s"$evPrim = ${timestampToDoubleCode(c)};"
    case DecimalType() =>
      (c, evPrim, evNull) => s"$evPrim = $c.toDouble();"
    case x: NumericType =>
      (c, evPrim, evNull) => s"$evPrim = (double) $c;"
  }

  private[this] def castArrayCode(
      fromType: DataType, toType: DataType, ctx: CodeGenContext): CastFunction = {
    val elementCast = nullSafeCastFunction(fromType, toType, ctx)
    val arrayClass = classOf[GenericArrayData].getName
    val fromElementNull = ctx.freshName("feNull")
    val fromElementPrim = ctx.freshName("fePrim")
    val toElementNull = ctx.freshName("teNull")
    val toElementPrim = ctx.freshName("tePrim")
    val size = ctx.freshName("n")
    val j = ctx.freshName("j")
    val values = ctx.freshName("values")

    (c, evPrim, evNull) =>
      s"""
        final int $size = $c.numElements();
        final Object[] $values = new Object[$size];
        for (int $j = 0; $j < $size; $j ++) {
          if ($c.isNullAt($j)) {
            $values[$j] = null;
          } else {
            boolean $fromElementNull = false;
            ${ctx.javaType(fromType)} $fromElementPrim =
              ${ctx.getValue(c, fromType, j)};
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

  private[this] def castMapCode(from: MapType, to: MapType, ctx: CodeGenContext): CastFunction = {
    val keysCast = castArrayCode(from.keyType, to.keyType, ctx)
    val valuesCast = castArrayCode(from.valueType, to.valueType, ctx)

    val mapClass = classOf[ArrayBasedMapData].getName

    val keys = ctx.freshName("keys")
    val convertedKeys = ctx.freshName("convertedKeys")
    val convertedKeysNull = ctx.freshName("convertedKeysNull")

    val values = ctx.freshName("values")
    val convertedValues = ctx.freshName("convertedValues")
    val convertedValuesNull = ctx.freshName("convertedValuesNull")

    (c, evPrim, evNull) =>
      s"""
        final ArrayData $keys = $c.keyArray();
        final ArrayData $values = $c.valueArray();
        ${castCode(ctx, keys, "false",
          convertedKeys, convertedKeysNull, ArrayType(to.keyType), keysCast)}
        ${castCode(ctx, values, "false",
          convertedValues, convertedValuesNull, ArrayType(to.valueType), valuesCast)}

        $evPrim = new $mapClass($convertedKeys, $convertedValues);
      """
  }

  private[this] def castStructCode(
      from: StructType, to: StructType, ctx: CodeGenContext): CastFunction = {

    val fieldsCasts = from.fields.zip(to.fields).map {
      case (fromField, toField) => nullSafeCastFunction(fromField.dataType, toField.dataType, ctx)
    }
    val rowClass = classOf[GenericMutableRow].getName
    val result = ctx.freshName("result")
    val tmpRow = ctx.freshName("tmpRow")

    val fieldsEvalCode = fieldsCasts.zipWithIndex.map { case (cast, i) => {
      val fromFieldPrim = ctx.freshName("ffp")
      val fromFieldNull = ctx.freshName("ffn")
      val toFieldPrim = ctx.freshName("tfp")
      val toFieldNull = ctx.freshName("tfn")
      val fromType = ctx.javaType(from.fields(i).dataType)
      s"""
        boolean $fromFieldNull = $tmpRow.isNullAt($i);
        if ($fromFieldNull) {
          $result.setNullAt($i);
        } else {
          $fromType $fromFieldPrim =
            ${ctx.getValue(tmpRow, from.fields(i).dataType, i.toString)};
          ${castCode(ctx, fromFieldPrim,
            fromFieldNull, toFieldPrim, toFieldNull, to.fields(i).dataType, cast)}
          if ($toFieldNull) {
            $result.setNullAt($i);
          } else {
            ${ctx.setColumn(result, to.fields(i).dataType, i, toFieldPrim)};
          }
        }
       """
      }
    }.mkString("\n")

    (c, evPrim, evNull) =>
      s"""
        final $rowClass $result = new $rowClass(${fieldsCasts.size});
        final InternalRow $tmpRow = $c;
        $fieldsEvalCode
        $evPrim = $result.copy();
      """
  }
}

/**
 * Cast the child expression to the target data type, but will throw error if the cast might
 * truncate, e.g. long -> int, timestamp -> data.
 */
case class UpCast(child: Expression, dataType: DataType, walkedTypePath: Seq[String])
  extends UnaryExpression with Unevaluable {
  override lazy val resolved = false
}
