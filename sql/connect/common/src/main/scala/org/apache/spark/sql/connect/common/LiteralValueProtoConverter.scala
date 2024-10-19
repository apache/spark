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

package org.apache.spark.sql.connect.common

import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time._

import scala.collection.{immutable, mutable}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.{SparkDateTimeUtils, SparkIntervalUtils}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.SparkClassUtils

object LiteralValueProtoConverter {

  /**
   * Transforms literal value to the `proto.Expression.Literal.Builder`.
   *
   * @return
   *   proto.Expression.Literal.Builder
   */
  @scala.annotation.tailrec
  def toLiteralProtoBuilder(literal: Any): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def decimalBuilder(precision: Int, scale: Int, value: String) = {
      builder.getDecimalBuilder.setPrecision(precision).setScale(scale).setValue(value)
    }

    def calendarIntervalBuilder(months: Int, days: Int, microseconds: Long) = {
      builder.getCalendarIntervalBuilder
        .setMonths(months)
        .setDays(days)
        .setMicroseconds(microseconds)
    }

    def arrayBuilder(array: Array[_]) = {
      val ab = builder.getArrayBuilder
        .setElementType(toConnectProtoType(toDataType(array.getClass.getComponentType)))
      array.foreach(x => ab.addElements(toLiteralProto(x)))
      ab
    }

    literal match {
      case v: Boolean => builder.setBoolean(v)
      case v: Byte => builder.setByte(v)
      case v: Short => builder.setShort(v)
      case v: Int => builder.setInteger(v)
      case v: Long => builder.setLong(v)
      case v: Float => builder.setFloat(v)
      case v: Double => builder.setDouble(v)
      case v: BigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: JBigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: String => builder.setString(v)
      case v: Char => builder.setString(v.toString)
      case v: Array[Char] => builder.setString(String.valueOf(v))
      case v: Array[Byte] => builder.setBinary(ByteString.copyFrom(v))
      case v: mutable.ArraySeq[_] => toLiteralProtoBuilder(v.array)
      case v: immutable.ArraySeq[_] => toLiteralProtoBuilder(v.unsafeArray)
      case v: LocalDate => builder.setDate(v.toEpochDay.toInt)
      case v: Decimal =>
        builder.setDecimal(decimalBuilder(Math.max(v.precision, v.scale), v.scale, v.toString))
      case v: Instant => builder.setTimestamp(SparkDateTimeUtils.instantToMicros(v))
      case v: Timestamp => builder.setTimestamp(SparkDateTimeUtils.fromJavaTimestamp(v))
      case v: LocalDateTime =>
        builder.setTimestampNtz(SparkDateTimeUtils.localDateTimeToMicros(v))
      case v: Date => builder.setDate(SparkDateTimeUtils.fromJavaDate(v))
      case v: Duration => builder.setDayTimeInterval(SparkIntervalUtils.durationToMicros(v))
      case v: Period => builder.setYearMonthInterval(SparkIntervalUtils.periodToMonths(v))
      case v: Array[_] => builder.setArray(arrayBuilder(v))
      case v: CalendarInterval =>
        builder.setCalendarInterval(calendarIntervalBuilder(v.months, v.days, v.microseconds))
      case null => builder.setNull(ProtoDataTypes.NullType)
      case _ => throw new UnsupportedOperationException(s"literal $literal not supported (yet).")
    }
  }

  @scala.annotation.tailrec
  def toLiteralProtoBuilder(
      literal: Any,
      dataType: DataType): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def arrayBuilder(scalaValue: Any, elementType: DataType) = {
      val ab = builder.getArrayBuilder.setElementType(toConnectProtoType(elementType))

      scalaValue match {
        case a: Array[_] =>
          a.foreach(item => ab.addElements(toLiteralProto(item, elementType)))
        case s: scala.collection.Seq[_] =>
          s.foreach(item => ab.addElements(toLiteralProto(item, elementType)))
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }

      ab
    }

    def mapBuilder(scalaValue: Any, keyType: DataType, valueType: DataType) = {
      val mb = builder.getMapBuilder
        .setKeyType(toConnectProtoType(keyType))
        .setValueType(toConnectProtoType(valueType))

      scalaValue match {
        case map: scala.collection.Map[_, _] =>
          map.foreach { case (k, v) =>
            mb.addKeys(toLiteralProto(k, keyType))
            mb.addValues(toLiteralProto(v, valueType))
          }
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }

      mb
    }

    def structBuilder(scalaValue: Any, structType: StructType) = {
      val sb = builder.getStructBuilder.setStructType(toConnectProtoType(structType))
      val dataTypes = structType.fields.map(_.dataType)

      scalaValue match {
        case p: Product =>
          val iter = p.productIterator
          var idx = 0
          while (idx < structType.size) {
            sb.addElements(toLiteralProto(iter.next(), dataTypes(idx)))
            idx += 1
          }
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }

      sb
    }

    (literal, dataType) match {
      case (v: mutable.ArraySeq[_], ArrayType(_, _)) =>
        toLiteralProtoBuilder(v.array, dataType)
      case (v: immutable.ArraySeq[_], ArrayType(_, _)) =>
        toLiteralProtoBuilder(v.unsafeArray, dataType)
      case (v: Array[Byte], ArrayType(_, _)) =>
        toLiteralProtoBuilder(v)
      case (v, ArrayType(elementType, _)) =>
        builder.setArray(arrayBuilder(v, elementType))
      case (v, MapType(keyType, valueType, _)) =>
        builder.setMap(mapBuilder(v, keyType, valueType))
      case (v, structType: StructType) =>
        builder.setStruct(structBuilder(v, structType))
      case (v: Option[_], _: DataType) =>
        if (v.isDefined) {
          toLiteralProtoBuilder(v.get)
        } else {
          builder.setNull(toConnectProtoType(dataType))
        }
      case _ => toLiteralProtoBuilder(literal)
    }
  }

  def create[T: TypeTag](v: T): proto.Expression.Literal.Builder = Try {
    val ScalaReflection.Schema(dataType, _) = ScalaReflection.schemaFor[T]
    toLiteralProtoBuilder(v, dataType)
  }.getOrElse {
    toLiteralProtoBuilder(v)
  }

  /**
   * Transforms literal value to the `proto.Expression.Literal`.
   *
   * @return
   *   proto.Expression.Literal
   */
  def toLiteralProto(literal: Any): proto.Expression.Literal =
    toLiteralProtoBuilder(literal).build()

  def toLiteralProto(literal: Any, dataType: DataType): proto.Expression.Literal =
    toLiteralProtoBuilder(literal, dataType).build()

  private[sql] def toDataType(clz: Class[_]): DataType = clz match {
    // primitive types
    case JShort.TYPE => ShortType
    case JInteger.TYPE => IntegerType
    case JLong.TYPE => LongType
    case JDouble.TYPE => DoubleType
    case JByte.TYPE => ByteType
    case JFloat.TYPE => FloatType
    case JBoolean.TYPE => BooleanType
    case JChar.TYPE => StringType

    // java classes
    case _ if clz == classOf[LocalDate] || clz == classOf[Date] => DateType
    case _ if clz == classOf[Instant] || clz == classOf[Timestamp] => TimestampType
    case _ if clz == classOf[LocalDateTime] => TimestampNTZType
    case _ if clz == classOf[Duration] => DayTimeIntervalType.DEFAULT
    case _ if clz == classOf[Period] => YearMonthIntervalType.DEFAULT
    case _ if clz == classOf[JBigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[Array[Byte]] => BinaryType
    case _ if clz == classOf[Array[Char]] => StringType
    case _ if clz == classOf[JShort] => ShortType
    case _ if clz == classOf[JInteger] => IntegerType
    case _ if clz == classOf[JLong] => LongType
    case _ if clz == classOf[JDouble] => DoubleType
    case _ if clz == classOf[JByte] => ByteType
    case _ if clz == classOf[JFloat] => FloatType
    case _ if clz == classOf[JBoolean] => BooleanType

    // other scala classes
    case _ if clz == classOf[String] => StringType
    case _ if clz == classOf[BigInt] || clz == classOf[BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[CalendarInterval] => CalendarIntervalType
    case _ if clz.isArray => ArrayType(toDataType(clz.getComponentType))
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }

  def toCatalystValue(literal: proto.Expression.Literal): Any = {
    literal.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.NULL => null

      case proto.Expression.Literal.LiteralTypeCase.BINARY => literal.getBinary.toByteArray

      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN => literal.getBoolean

      case proto.Expression.Literal.LiteralTypeCase.BYTE => literal.getByte.toByte

      case proto.Expression.Literal.LiteralTypeCase.SHORT => literal.getShort.toShort

      case proto.Expression.Literal.LiteralTypeCase.INTEGER => literal.getInteger

      case proto.Expression.Literal.LiteralTypeCase.LONG => literal.getLong

      case proto.Expression.Literal.LiteralTypeCase.FLOAT => literal.getFloat

      case proto.Expression.Literal.LiteralTypeCase.DOUBLE => literal.getDouble

      case proto.Expression.Literal.LiteralTypeCase.DECIMAL =>
        Decimal(literal.getDecimal.getValue)

      case proto.Expression.Literal.LiteralTypeCase.STRING => literal.getString

      case proto.Expression.Literal.LiteralTypeCase.DATE =>
        SparkDateTimeUtils.toJavaDate(literal.getDate)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP =>
        SparkDateTimeUtils.toJavaTimestamp(literal.getTimestamp)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
        SparkDateTimeUtils.microsToLocalDateTime(literal.getTimestampNtz)

      case proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
        new CalendarInterval(
          literal.getCalendarInterval.getMonths,
          literal.getCalendarInterval.getDays,
          literal.getCalendarInterval.getMicroseconds)

      case proto.Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL =>
        SparkIntervalUtils.monthsToPeriod(literal.getYearMonthInterval)

      case proto.Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL =>
        SparkIntervalUtils.microsToDuration(literal.getDayTimeInterval)

      case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
        toCatalystArray(literal.getArray)

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Literal Type: ${other.getNumber} (${other.name})")
    }
  }

  private def getConverter(dataType: proto.DataType): proto.Expression.Literal => Any = {
    if (dataType.hasShort) { v =>
      v.getShort.toShort
    } else if (dataType.hasInteger) { v =>
      v.getInteger
    } else if (dataType.hasLong) { v =>
      v.getLong
    } else if (dataType.hasDouble) { v =>
      v.getDouble
    } else if (dataType.hasByte) { v =>
      v.getByte.toByte
    } else if (dataType.hasFloat) { v =>
      v.getFloat
    } else if (dataType.hasBoolean) { v =>
      v.getBoolean
    } else if (dataType.hasString) { v =>
      v.getString
    } else if (dataType.hasBinary) { v =>
      v.getBinary.toByteArray
    } else if (dataType.hasDate) { v =>
      v.getDate
    } else if (dataType.hasTimestamp) { v =>
      v.getTimestamp
    } else if (dataType.hasTimestampNtz) { v =>
      v.getTimestampNtz
    } else if (dataType.hasDayTimeInterval) { v =>
      v.getDayTimeInterval
    } else if (dataType.hasYearMonthInterval) { v =>
      v.getYearMonthInterval
    } else if (dataType.hasDecimal) { v =>
      Decimal(v.getDecimal.getValue)
    } else if (dataType.hasCalendarInterval) { v =>
      val interval = v.getCalendarInterval
      new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
    } else if (dataType.hasArray) { v =>
      toCatalystArray(v.getArray)
    } else if (dataType.hasMap) { v =>
      toCatalystMap(v.getMap)
    } else if (dataType.hasStruct) { v =>
      toCatalystStruct(v.getStruct)
    } else {
      throw InvalidPlanInput(s"Unsupported Literal Type: $dataType)")
    }
  }

  def toCatalystArray(array: proto.Expression.Literal.Array): Array[_] = {
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
        tag: ClassTag[T]): Array[T] = {
      val builder = mutable.ArrayBuilder.make[T]
      val elementList = array.getElementsList
      builder.sizeHint(elementList.size())
      val iter = elementList.iterator()
      while (iter.hasNext) {
        builder += converter(iter.next())
      }
      builder.result()
    }

    makeArrayData(getConverter(array.getElementType))
  }

  def toCatalystMap(map: proto.Expression.Literal.Map): mutable.Map[_, _] = {
    def makeMapData[K, V](
        keyConverter: proto.Expression.Literal => K,
        valueConverter: proto.Expression.Literal => V)(implicit
        tagK: ClassTag[K],
        tagV: ClassTag[V]): mutable.Map[K, V] = {
      val builder = mutable.HashMap.empty[K, V]
      val keys = map.getKeysList.asScala
      val values = map.getValuesList.asScala
      builder.sizeHint(keys.size)
      keys.zip(values).foreach { case (key, value) =>
        builder += ((keyConverter(key), valueConverter(value)))
      }
      builder
    }

    makeMapData(getConverter(map.getKeyType), getConverter(map.getValueType))
  }

  def toCatalystStruct(struct: proto.Expression.Literal.Struct): Any = {
    def toTuple[A <: Object](data: Seq[A]): Product = {
      try {
        val tupleClass = SparkClassUtils.classForName(s"scala.Tuple${data.length}")
        tupleClass.getConstructors.head.newInstance(data: _*).asInstanceOf[Product]
      } catch {
        case _: Exception =>
          throw InvalidPlanInput(s"Unsupported Literal: ${data.mkString("Array(", ", ", ")")})")
      }
    }

    val elements = struct.getElementsList.asScala
    val dataTypes = struct.getStructType.getStruct.getFieldsList.asScala.map(_.getDataType)
    val structData = elements
      .zip(dataTypes)
      .map { case (element, dataType) =>
        getConverter(dataType)(element)
      }
      .asInstanceOf[scala.collection.Seq[Object]]
      .toSeq

    toTuple(structData)
  }
}
