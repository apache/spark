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

package org.apache.spark.sql.catalyst

import java.lang.{Iterable => JavaIterable}
import java.math.{BigDecimal => JavaBigDecimal}
import java.math.{BigInteger => JavaBigInteger}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, Period}
import java.util.{Map => JavaMap}
import javax.annotation.Nullable

import scala.language.existentials

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.Utils

/**
 * Functions to convert Scala types to Catalyst types and vice versa.
 */
object CatalystTypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  private[sql] def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case _ => false
    }
  }

  private def getConverterForType(dataType: DataType): CatalystTypeConverter[Any, Any, Any] = {
    val converter = dataType match {
      case udt: UserDefinedType[_] => UDTConverter(udt)
      case arrayType: ArrayType => ArrayConverter(arrayType.elementType)
      case mapType: MapType => MapConverter(mapType.keyType, mapType.valueType)
      case structType: StructType => StructConverter(structType)
      case CharType(length) => new CharConverter(length)
      case VarcharType(length) => new VarcharConverter(length)
      case _: StringType => StringConverter
      case DateType if SQLConf.get.datetimeJava8ApiEnabled => LocalDateConverter
      case DateType => DateConverter
      case _: TimeType => TimeConverter
      case TimestampType if SQLConf.get.datetimeJava8ApiEnabled => InstantConverter
      case TimestampType => TimestampConverter
      case TimestampNTZType => TimestampNTZConverter
      case dt: DecimalType => new DecimalConverter(dt)
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType => IntConverter
      case LongType => LongConverter
      case FloatType => FloatConverter
      case DoubleType => DoubleConverter
      case DayTimeIntervalType(_, endField) => DurationConverter(endField)
      case YearMonthIntervalType(_, endField) => PeriodConverter(endField)
      case dataType: DataType => IdentityConverter(dataType)
    }
    converter.asInstanceOf[CatalystTypeConverter[Any, Any, Any]]
  }

  /**
   * Converts a Scala type to its Catalyst equivalent (and vice versa).
   *
   * @tparam ScalaInputType The type of Scala values that can be converted to Catalyst.
   * @tparam ScalaOutputType The type of Scala values returned when converting Catalyst to Scala.
   * @tparam CatalystType The internal Catalyst type used to represent values of this Scala type.
   */
  private abstract class CatalystTypeConverter[ScalaInputType, ScalaOutputType, CatalystType]
    extends Serializable {

    /**
     * Converts a Scala type to its Catalyst equivalent while automatically handling nulls
     * and Options.
     */
    final def toCatalyst(@Nullable maybeScalaValue: Any): CatalystType = {
      maybeScalaValue match {
        case null | None => null.asInstanceOf[CatalystType]
        case opt: Some[ScalaInputType @unchecked] => toCatalystImpl(opt.get)
        case other => toCatalystImpl(other.asInstanceOf[ScalaInputType])
      }
    }

    /**
     * Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
     */
    final def toScala(row: InternalRow, column: Int): ScalaOutputType = {
      if (row.isNullAt(column)) null.asInstanceOf[ScalaOutputType] else toScalaImpl(row, column)
    }

    /**
     * Convert a Catalyst value to its Scala equivalent.
     */
    def toScala(@Nullable catalystValue: CatalystType): ScalaOutputType

    /**
     * Converts a Scala value to its Catalyst equivalent.
     * @param scalaValue the Scala value, guaranteed not to be null.
     * @return the Catalyst value.
     */
    protected def toCatalystImpl(scalaValue: ScalaInputType): CatalystType

    /**
     * Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
     * This method will only be called on non-null columns.
     */
    protected def toScalaImpl(row: InternalRow, column: Int): ScalaOutputType
  }

  private case class IdentityConverter(dataType: DataType)
    extends CatalystTypeConverter[Any, Any, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = scalaValue
    override def toScala(catalystValue: Any): Any = catalystValue
    override def toScalaImpl(row: InternalRow, column: Int): Any = row.get(column, dataType)
  }

  private case class UDTConverter[A >: Null](
      udt: UserDefinedType[A]) extends CatalystTypeConverter[A, A, Any] {
    // toCatalyst (it calls toCatalystImpl) will do null check.
    override def toCatalystImpl(scalaValue: A): Any = udt.serialize(scalaValue)

    override def toScala(catalystValue: Any): A = {
      if (catalystValue == null) null else udt.deserialize(catalystValue)
    }

    override def toScalaImpl(row: InternalRow, column: Int): A =
      toScala(row.get(column, udt.sqlType))
  }

  /** Converter for arrays, sequences, and Java iterables. */
  private case class ArrayConverter(
      elementType: DataType) extends CatalystTypeConverter[Any, Seq[Any], ArrayData] {

    private[this] val elementConverter = getConverterForType(elementType)

    override def toCatalystImpl(scalaValue: Any): ArrayData = {
      scalaValue match {
        case a: Array[_] =>
          new GenericArrayData(a.map(elementConverter.toCatalyst))
        case s: scala.collection.Seq[_] =>
          new GenericArrayData(s.map(elementConverter.toCatalyst).toArray)
        case i: JavaIterable[_] =>
          val iter = i.iterator
          val convertedIterable = scala.collection.mutable.ArrayBuffer.empty[Any]
          while (iter.hasNext) {
            val item = iter.next()
            convertedIterable += elementConverter.toCatalyst(item)
          }
          new GenericArrayData(convertedIterable.toArray)
        case other => throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3220",
          messageParameters = scala.collection.immutable.Map(
            "other" -> other.toString,
            "otherClass" -> other.getClass.getCanonicalName,
            "elementType" -> elementType.catalogString))
      }
    }

    override def toScala(catalystValue: ArrayData): Seq[Any] = {
      if (catalystValue == null) {
        null
      } else if (isPrimitive(elementType)) {
        catalystValue.toArray[Any](elementType).toImmutableArraySeq
      } else {
        val result = new Array[Any](catalystValue.numElements())
        catalystValue.foreach(elementType, (i, e) => {
          result(i) = elementConverter.toScala(e)
        })
        result.toImmutableArraySeq
      }
    }

    override def toScalaImpl(row: InternalRow, column: Int): Seq[Any] =
      toScala(row.getArray(column))
  }

  private case class MapConverter(
      keyType: DataType,
      valueType: DataType)
    extends CatalystTypeConverter[Any, Map[Any, Any], MapData] {

    private[this] val keyConverter = getConverterForType(keyType)
    private[this] val valueConverter = getConverterForType(valueType)

    override def toCatalystImpl(scalaValue: Any): MapData = {
      val keyFunction = (k: Any) => keyConverter.toCatalyst(k)
      val valueFunction = (k: Any) => valueConverter.toCatalyst(k)

      scalaValue match {
        case map: Map[_, _] => ArrayBasedMapData(map, keyFunction, valueFunction)
        case javaMap: JavaMap[_, _] => ArrayBasedMapData(javaMap, keyFunction, valueFunction)
        case other => throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3221",
          messageParameters = scala.collection.immutable.Map(
            "other" -> other.toString,
            "otherClass" -> other.getClass.getCanonicalName,
            "keyType" -> keyType.catalogString,
            "valueType" -> valueType.catalogString))
      }
    }

    override def toScala(catalystValue: MapData): Map[Any, Any] = {
      if (catalystValue == null) {
        null
      } else {
        val keys = catalystValue.keyArray().toArray[Any](keyType)
        val values = catalystValue.valueArray().toArray[Any](valueType)
        val convertedKeys =
          if (isPrimitive(keyType)) keys else keys.map(keyConverter.toScala)
        val convertedValues =
          if (isPrimitive(valueType)) values else values.map(valueConverter.toScala)

        Utils.toMap(convertedKeys, convertedValues)
      }
    }

    override def toScalaImpl(row: InternalRow, column: Int): Map[Any, Any] =
      toScala(row.getMap(column))
  }

  private case class StructConverter(
      structType: StructType) extends CatalystTypeConverter[Any, Row, InternalRow] {

    private[this] val converters = structType.fields.map { f => getConverterForType(f.dataType) }

    override def toCatalystImpl(scalaValue: Any): InternalRow = scalaValue match {
      case row: Row =>
        val ar = new Array[Any](row.size)
        var idx = 0
        while (idx < row.size) {
          ar(idx) = converters(idx).toCatalyst(row(idx))
          idx += 1
        }
        new GenericInternalRow(ar)

      case p: Product =>
        val ar = new Array[Any](structType.size)
        val iter = p.productIterator
        var idx = 0
        while (idx < structType.size) {
          ar(idx) = converters(idx).toCatalyst(iter.next())
          idx += 1
        }
        new GenericInternalRow(ar)
      case other => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3219",
        messageParameters = scala.collection.immutable.Map(
          "other" -> other.toString,
          "otherClass" -> other.getClass.getCanonicalName,
          "dataType" -> structType.catalogString))
    }

    override def toScala(row: InternalRow): Row = {
      if (row == null) {
        null
      } else {
        val ar = new Array[Any](row.numFields)
        var idx = 0
        while (idx < row.numFields) {
          ar(idx) = converters(idx).toScala(row, idx)
          idx += 1
        }
        new GenericRowWithSchema(ar, structType)
      }
    }

    override def toScalaImpl(row: InternalRow, column: Int): Row =
      toScala(row.getStruct(column, structType.size))
  }

  private class CharConverter(length: Int) extends CatalystTypeConverter[Any, String, UTF8String] {
    override def toCatalystImpl(scalaValue: Any): UTF8String =
      CharVarcharCodegenUtils.charTypeWriteSideCheck(
        StringConverter.toCatalystImpl(scalaValue), length)
    override def toScala(catalystValue: UTF8String): String = if (catalystValue == null) {
      null
    } else {
      CharVarcharCodegenUtils.charTypeWriteSideCheck(catalystValue, length).toString
    }
    override def toScalaImpl(row: InternalRow, column: Int): String =
      CharVarcharCodegenUtils.charTypeWriteSideCheck(row.getUTF8String(column), length).toString
  }

  private class VarcharConverter(length: Int)
    extends CatalystTypeConverter[Any, String, UTF8String] {
    override def toCatalystImpl(scalaValue: Any): UTF8String =
      CharVarcharCodegenUtils.varcharTypeWriteSideCheck(
        StringConverter.toCatalystImpl(scalaValue), length)
    override def toScala(catalystValue: UTF8String): String = if (catalystValue == null) {
      null
    } else {
      CharVarcharCodegenUtils.varcharTypeWriteSideCheck(catalystValue, length).toString
    }
    override def toScalaImpl(row: InternalRow, column: Int): String =
      CharVarcharCodegenUtils.varcharTypeWriteSideCheck(row.getUTF8String(column), length).toString
  }

  private object StringConverter extends CatalystTypeConverter[Any, String, UTF8String] {
    override def toCatalystImpl(scalaValue: Any): UTF8String = scalaValue match {
      case str: String => UTF8String.fromString(str)
      case utf8: UTF8String => utf8
      case chr: Char => UTF8String.fromString(chr.toString)
      case ac: Array[Char] => UTF8String.fromString(String.valueOf(ac))
      case other => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3219",
        messageParameters = scala.collection.immutable.Map(
          "other" -> other.toString,
          "otherClass" -> other.getClass.getCanonicalName,
          "dataType" -> StringType.sql))
    }
    override def toScala(catalystValue: UTF8String): String =
      if (catalystValue == null) null else catalystValue.toString
    override def toScalaImpl(row: InternalRow, column: Int): String =
      row.getUTF8String(column).toString
  }

  private object DateConverter extends CatalystTypeConverter[Any, Date, Any] {
    override def toCatalystImpl(scalaValue: Any): Int = scalaValue match {
      case d: Date => DateTimeUtils.fromJavaDate(d)
      case l: LocalDate => DateTimeUtils.localDateToDays(l)
      case other => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3219",
        messageParameters = scala.collection.immutable.Map(
          "other" -> other.toString,
          "otherClass" -> other.getClass.getCanonicalName,
          "dataType" -> DateType.sql))
    }
    override def toScala(catalystValue: Any): Date =
      if (catalystValue == null) null else DateTimeUtils.toJavaDate(catalystValue.asInstanceOf[Int])
    override def toScalaImpl(row: InternalRow, column: Int): Date =
      DateTimeUtils.toJavaDate(row.getInt(column))
  }

  private object LocalDateConverter extends CatalystTypeConverter[Any, LocalDate, Any] {
    override def toCatalystImpl(scalaValue: Any): Int =
      DateConverter.toCatalystImpl(scalaValue)
    override def toScala(catalystValue: Any): LocalDate = {
      if (catalystValue == null) null
      else DateTimeUtils.daysToLocalDate(catalystValue.asInstanceOf[Int])
    }
    override def toScalaImpl(row: InternalRow, column: Int): LocalDate =
      DateTimeUtils.daysToLocalDate(row.getInt(column))
  }

  private object TimeConverter extends CatalystTypeConverter[LocalTime, LocalTime, Any] {
    override def toCatalystImpl(scalaValue: LocalTime): Long = {
      DateTimeUtils.localTimeToMicros(scalaValue)
    }
    override def toScala(catalystValue: Any): LocalTime = {
      if (catalystValue == null) null
      else DateTimeUtils.microsToLocalTime(catalystValue.asInstanceOf[Long])
    }
    override def toScalaImpl(row: InternalRow, column: Int): LocalTime =
      DateTimeUtils.microsToLocalTime(row.getLong(column))
  }

  private object TimestampConverter extends CatalystTypeConverter[Any, Timestamp, Any] {
    override def toCatalystImpl(scalaValue: Any): Long = scalaValue match {
      case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
      case i: Instant => DateTimeUtils.instantToMicros(i)
      case other => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3219",
        messageParameters = scala.collection.immutable.Map(
          "other" -> other.toString,
          "otherClass" -> other.getClass.getCanonicalName,
          "dataType" -> TimestampType.sql))
    }
    override def toScala(catalystValue: Any): Timestamp =
      if (catalystValue == null) null
      else DateTimeUtils.toJavaTimestamp(catalystValue.asInstanceOf[Long])
    override def toScalaImpl(row: InternalRow, column: Int): Timestamp =
      DateTimeUtils.toJavaTimestamp(row.getLong(column))
  }

  private object InstantConverter extends CatalystTypeConverter[Any, Instant, Any] {
    override def toCatalystImpl(scalaValue: Any): Long =
      TimestampConverter.toCatalystImpl(scalaValue)
    override def toScala(catalystValue: Any): Instant =
      if (catalystValue == null) null
      else DateTimeUtils.microsToInstant(catalystValue.asInstanceOf[Long])
    override def toScalaImpl(row: InternalRow, column: Int): Instant =
      DateTimeUtils.microsToInstant(row.getLong(column))
  }

  private object TimestampNTZConverter
    extends CatalystTypeConverter[Any, LocalDateTime, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = scalaValue match {
      case l: LocalDateTime => DateTimeUtils.localDateTimeToMicros(l)
      case other => throw new SparkIllegalArgumentException(
        errorClass = "_LEGACY_ERROR_TEMP_3219",
        messageParameters = scala.collection.immutable.Map(
          "other" -> other.toString,
          "otherClass" -> other.getClass.getCanonicalName,
          "dataType" -> TimestampNTZType.sql))
    }

    override def toScala(catalystValue: Any): LocalDateTime =
      if (catalystValue == null) null
      else DateTimeUtils.microsToLocalDateTime(catalystValue.asInstanceOf[Long])

    override def toScalaImpl(row: InternalRow, column: Int): LocalDateTime =
      DateTimeUtils.microsToLocalDateTime(row.getLong(column))
  }

  private class DecimalConverter(dataType: DecimalType)
    extends CatalystTypeConverter[Any, JavaBigDecimal, Decimal] {

    private val nullOnOverflow = !SQLConf.get.ansiEnabled

    override def toCatalystImpl(scalaValue: Any): Decimal = {
      val decimal = scalaValue match {
        case d: BigDecimal => Decimal(d)
        case d: JavaBigDecimal => Decimal(d)
        case d: JavaBigInteger => Decimal(d)
        case d: Decimal => d
        case other => throw new SparkIllegalArgumentException(
          errorClass = "_LEGACY_ERROR_TEMP_3219",
          messageParameters = scala.collection.immutable.Map(
            "other" -> other.toString,
            "otherClass" -> other.getClass.getCanonicalName,
            "dataType" -> dataType.catalogString))
      }
      decimal.toPrecision(dataType.precision, dataType.scale, Decimal.ROUND_HALF_UP, nullOnOverflow)
    }
    override def toScala(catalystValue: Decimal): JavaBigDecimal = {
      if (catalystValue == null) null
      else catalystValue.toJavaBigDecimal
    }
    override def toScalaImpl(row: InternalRow, column: Int): JavaBigDecimal =
      row.getDecimal(column, dataType.precision, dataType.scale).toJavaBigDecimal
  }

  private abstract class PrimitiveConverter[T] extends CatalystTypeConverter[T, Any, Any] {
    final override def toScala(catalystValue: Any): Any = catalystValue
    final override def toCatalystImpl(scalaValue: T): Any = scalaValue
  }

  private object BooleanConverter extends PrimitiveConverter[Boolean] {
    override def toScalaImpl(row: InternalRow, column: Int): Boolean = row.getBoolean(column)
  }

  private object ByteConverter extends PrimitiveConverter[Byte] {
    override def toScalaImpl(row: InternalRow, column: Int): Byte = row.getByte(column)
  }

  private object ShortConverter extends PrimitiveConverter[Short] {
    override def toScalaImpl(row: InternalRow, column: Int): Short = row.getShort(column)
  }

  private object IntConverter extends PrimitiveConverter[Int] {
    override def toScalaImpl(row: InternalRow, column: Int): Int = row.getInt(column)
  }

  private object LongConverter extends PrimitiveConverter[Long] {
    override def toScalaImpl(row: InternalRow, column: Int): Long = row.getLong(column)
  }

  private object FloatConverter extends PrimitiveConverter[Float] {
    override def toScalaImpl(row: InternalRow, column: Int): Float = row.getFloat(column)
  }

  private object DoubleConverter extends PrimitiveConverter[Double] {
    override def toScalaImpl(row: InternalRow, column: Int): Double = row.getDouble(column)
  }

  private case class DurationConverter(endField: Byte)
      extends CatalystTypeConverter[Duration, Duration, Any] {
    override def toCatalystImpl(scalaValue: Duration): Long = {
      IntervalUtils.durationToMicros(scalaValue, endField)
    }
    override def toScala(catalystValue: Any): Duration = {
      if (catalystValue == null) null
      else IntervalUtils.microsToDuration(catalystValue.asInstanceOf[Long])
    }
    override def toScalaImpl(row: InternalRow, column: Int): Duration =
      IntervalUtils.microsToDuration(row.getLong(column))
  }

  private case class PeriodConverter(endField: Byte)
      extends CatalystTypeConverter[Period, Period, Any] {
    override def toCatalystImpl(scalaValue: Period): Int = {
      IntervalUtils.periodToMonths(scalaValue, endField)
    }
    override def toScala(catalystValue: Any): Period = {
      if (catalystValue == null) null
      else IntervalUtils.monthsToPeriod(catalystValue.asInstanceOf[Int])
    }
    override def toScalaImpl(row: InternalRow, column: Int): Period =
      IntervalUtils.monthsToPeriod(row.getInt(column))
  }

  /**
   * Creates a converter function that will convert Scala objects to the specified Catalyst type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  def createToCatalystConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      // Although the `else` branch here is capable of handling inbound conversion of primitives,
      // we add some special-case handling for those types here. The motivation for this relates to
      // Java method invocation costs: if we have rows that consist entirely of primitive columns,
      // then returning the same conversion function for all of the columns means that the call site
      // will be monomorphic instead of polymorphic. In microbenchmarks, this actually resulted in
      // a measurable performance impact. Note that this optimization will be unnecessary if we
      // use code generation to construct Scala Row -> Catalyst Row converters.
      def convert(maybeScalaValue: Any): Any = {
        maybeScalaValue match {
          case opt: Option[Any] => opt.orNull
          case _ => maybeScalaValue
        }
      }
      convert
    } else {
      getConverterForType(dataType).toCatalyst
    }
  }

  /**
   * Creates a converter function that will convert Catalyst types to Scala type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  def createToScalaConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      identity
    } else {
      getConverterForType(dataType).toScala
    }
  }

  /**
   *  Converts Scala objects to Catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
    case s: String => StringConverter.toCatalyst(s)
    case c: Char => StringConverter.toCatalyst(c.toString)
    case d: Date => DateConverter.toCatalyst(d)
    case ld: LocalDate => LocalDateConverter.toCatalyst(ld)
    case t: LocalTime => TimeConverter.toCatalyst(t)
    case t: Timestamp => TimestampConverter.toCatalyst(t)
    case i: Instant => InstantConverter.toCatalyst(i)
    case l: LocalDateTime => TimestampNTZConverter.toCatalyst(l)
    case d: BigDecimal =>
      new DecimalConverter(DecimalType(Math.max(d.precision, d.scale), d.scale)).toCatalyst(d)
    case d: JavaBigDecimal =>
      new DecimalConverter(DecimalType(Math.max(d.precision, d.scale), d.scale)).toCatalyst(d)
    case seq: Seq[Any] => new GenericArrayData(seq.map(convertToCatalyst).toArray)
    case r: Row => InternalRow(r.toSeq.map(convertToCatalyst): _*)
    case arr: Array[Byte] => arr
    case arr: Array[Char] => StringConverter.toCatalyst(arr)
    case arr: Array[_] => new GenericArrayData(arr.map(convertToCatalyst))
    case map: Map[_, _] =>
      ArrayBasedMapData(
        map,
        (key: Any) => convertToCatalyst(key),
        (value: Any) => convertToCatalyst(value))
    case d: Duration => DurationConverter(SECOND).toCatalyst(d)
    case p: Period => PeriodConverter(MONTH).toCatalyst(p)
    case other => other
  }

  /**
   * Converts Catalyst types used internally in rows to standard Scala types
   * This method is slow, and for batch conversion you should be using converter
   * produced by createToScalaConverter.
   */
  def convertToScala(catalystValue: Any, dataType: DataType): Any = {
    createToScalaConverter(dataType)(catalystValue)
  }
}
