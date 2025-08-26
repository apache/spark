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

  @scala.annotation.tailrec
  private def toLiteralProtoBuilderInternal(
      literal: Any,
      options: ToLiteralProtoOptions): proto.Expression.Literal.Builder = {
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
      if (options.useDeprecatedDataTypeFields) {
        ab.setElementType(toConnectProtoType(toDataType(array.getClass.getComponentType)))
      } else {
        ab.setDataType(
          proto.DataType.Array
            .newBuilder()
            .setElementType(toConnectProtoType(toDataType(array.getClass.getComponentType)))
            .setContainsNull(true)
            .build())
      }
      array.foreach(x => ab.addElements(toLiteralProtoWithOptions(x, None, options)))
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
      case v: mutable.ArraySeq[_] => toLiteralProtoBuilderInternal(v.array, options)
      case v: immutable.ArraySeq[_] => toLiteralProtoBuilderInternal(v.unsafeArray, options)
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
      case v: LocalTime =>
        builder.setTime(
          builder.getTimeBuilder
            .setNano(SparkDateTimeUtils.localTimeToNanos(v))
            .setPrecision(TimeType.DEFAULT_PRECISION))
      case v: Array[_] => builder.setArray(arrayBuilder(v))
      case v: CalendarInterval =>
        builder.setCalendarInterval(calendarIntervalBuilder(v.months, v.days, v.microseconds))
      case null => builder.setNull(ProtoDataTypes.NullType)
      case _ => throw new UnsupportedOperationException(s"literal $literal not supported (yet).")
    }
  }

  @scala.annotation.tailrec
  private def toLiteralProtoBuilderInternal(
      literal: Any,
      dataType: DataType,
      options: ToLiteralProtoOptions): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def arrayBuilder(scalaValue: Any, elementType: DataType, containsNull: Boolean) = {
      val ab = builder.getArrayBuilder
      if (options.useDeprecatedDataTypeFields) {
        ab.setElementType(toConnectProtoType(elementType))
      } else {
        ab.setDataType(
          proto.DataType.Array
            .newBuilder()
            .setElementType(toConnectProtoType(elementType))
            .setContainsNull(containsNull)
            .build())
      }
      scalaValue match {
        case a: Array[_] =>
          a.foreach(item =>
            ab.addElements(toLiteralProtoWithOptions(item, Some(elementType), options)))
        case s: scala.collection.Seq[_] =>
          s.foreach(item =>
            ab.addElements(toLiteralProtoWithOptions(item, Some(elementType), options)))
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }

      ab
    }

    def mapBuilder(
        scalaValue: Any,
        keyType: DataType,
        valueType: DataType,
        valueContainsNull: Boolean) = {
      val mb = builder.getMapBuilder
      if (options.useDeprecatedDataTypeFields) {
        mb.setKeyType(toConnectProtoType(keyType))
        mb.setValueType(toConnectProtoType(valueType))
      } else {
        mb.setDataType(
          proto.DataType.Map
            .newBuilder()
            .setKeyType(toConnectProtoType(keyType))
            .setValueType(toConnectProtoType(valueType))
            .setValueContainsNull(valueContainsNull)
            .build())
      }

      scalaValue match {
        case map: scala.collection.Map[_, _] =>
          map.foreach { case (k, v) =>
            mb.addKeys(toLiteralProtoWithOptions(k, Some(keyType), options))
            mb.addValues(toLiteralProtoWithOptions(v, Some(valueType), options))
          }
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }

      mb
    }

    def structBuilder(scalaValue: Any, structType: StructType) = {
      val sb = builder.getStructBuilder
      val fields = structType.fields

      scalaValue match {
        case p: Product =>
          val iter = p.productIterator
          var idx = 0
          if (options.useDeprecatedDataTypeFields) {
            while (idx < structType.size) {
              val field = fields(idx)
              val literalProto =
                toLiteralProtoWithOptions(iter.next(), Some(field.dataType), options)
              sb.addElements(literalProto)
              idx += 1
            }
            sb.setStructType(toConnectProtoType(structType))
          } else {
            val dataTypeStruct = proto.DataType.Struct.newBuilder()
            while (idx < structType.size) {
              val field = fields(idx)
              val literalProto =
                toLiteralProtoWithOptions(iter.next(), Some(field.dataType), options)
              sb.addElements(literalProto)

              val fieldBuilder = dataTypeStruct
                .addFieldsBuilder()
                .setName(field.name)
                .setNullable(field.nullable)

              if (LiteralValueProtoConverter.getInferredDataType(literalProto).isEmpty) {
                fieldBuilder.setDataType(toConnectProtoType(field.dataType))
              }

              // Set metadata if available
              if (field.metadata != Metadata.empty) {
                fieldBuilder.setMetadata(field.metadata.json)
              }

              idx += 1
            }
            sb.setDataTypeStruct(dataTypeStruct.build())
          }
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }

      sb
    }

    (literal, dataType) match {
      case (v: mutable.ArraySeq[_], ArrayType(_, _)) =>
        toLiteralProtoBuilderInternal(v.array, dataType, options)
      case (v: immutable.ArraySeq[_], ArrayType(_, _)) =>
        toLiteralProtoBuilderInternal(v.unsafeArray, dataType, options)
      case (v: Array[Byte], ArrayType(_, _)) =>
        toLiteralProtoBuilderInternal(v, options)
      case (v, ArrayType(elementType, containsNull)) =>
        builder.setArray(arrayBuilder(v, elementType, containsNull))
      case (v, MapType(keyType, valueType, valueContainsNull)) =>
        builder.setMap(mapBuilder(v, keyType, valueType, valueContainsNull))
      case (v, structType: StructType) =>
        builder.setStruct(structBuilder(v, structType))
      case (v: Option[_], _: DataType) =>
        if (v.isDefined) {
          toLiteralProtoBuilderInternal(v.get, options)
        } else {
          builder.setNull(toConnectProtoType(dataType))
        }
      case (v: LocalTime, timeType: TimeType) =>
        builder.setTime(
          builder.getTimeBuilder
            .setNano(SparkDateTimeUtils.localTimeToNanos(v))
            .setPrecision(timeType.precision))
      case _ => toLiteralProtoBuilderInternal(literal, options)
    }

  }

  /**
   * Transforms literal value to the `proto.Expression.Literal.Builder`.
   *
   * @return
   *   proto.Expression.Literal.Builder
   */
  def toLiteralProtoBuilder(literal: Any): proto.Expression.Literal.Builder = {
    toLiteralProtoBuilderInternal(
      literal,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  def toLiteralProtoBuilder(
      literal: Any,
      dataType: DataType): proto.Expression.Literal.Builder = {
    toLiteralProtoBuilderInternal(
      literal,
      dataType,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  def toLiteralProtoBuilderWithOptions(
      literal: Any,
      dataTypeOpt: Option[DataType],
      options: ToLiteralProtoOptions): proto.Expression.Literal.Builder = {
    dataTypeOpt match {
      case Some(dataType) =>
        toLiteralProtoBuilderInternal(literal, dataType, options)
      case None =>
        toLiteralProtoBuilderInternal(literal, options)
    }
  }

  def create[T: TypeTag](v: T): proto.Expression.Literal.Builder = Try {
    val ScalaReflection.Schema(dataType, _) = ScalaReflection.schemaFor[T]
    toLiteralProtoBuilder(v, dataType)
  }.getOrElse {
    toLiteralProtoBuilder(v)
  }

  case class ToLiteralProtoOptions(useDeprecatedDataTypeFields: Boolean)

  /**
   * Transforms literal value to the `proto.Expression.Literal`.
   *
   * @return
   *   proto.Expression.Literal
   */
  def toLiteralProto(literal: Any): proto.Expression.Literal =
    toLiteralProtoBuilderInternal(
      literal,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true)).build()

  def toLiteralProto(literal: Any, dataType: DataType): proto.Expression.Literal =
    toLiteralProtoBuilderInternal(
      literal,
      dataType,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true)).build()

  def toLiteralProtoWithOptions(
      literal: Any,
      dataTypeOpt: Option[DataType],
      options: ToLiteralProtoOptions): proto.Expression.Literal = {
    dataTypeOpt match {
      case Some(dataType) =>
        toLiteralProtoBuilderInternal(literal, dataType, options).build()
      case None =>
        toLiteralProtoBuilderInternal(literal, options).build()
    }
  }

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

      case proto.Expression.Literal.LiteralTypeCase.STRUCT =>
        toCatalystStruct(literal.getStruct)

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Literal Type: ${other.getNumber} (${other.name})")
    }
  }

  private def getConverter(dataType: proto.DataType): proto.Expression.Literal => Any = {
    dataType.getKindCase match {
      case proto.DataType.KindCase.SHORT => v => v.getShort.toShort
      case proto.DataType.KindCase.INTEGER => v => v.getInteger
      case proto.DataType.KindCase.LONG => v => v.getLong
      case proto.DataType.KindCase.DOUBLE => v => v.getDouble
      case proto.DataType.KindCase.BYTE => v => v.getByte.toByte
      case proto.DataType.KindCase.FLOAT => v => v.getFloat
      case proto.DataType.KindCase.BOOLEAN => v => v.getBoolean
      case proto.DataType.KindCase.STRING => v => v.getString
      case proto.DataType.KindCase.BINARY => v => v.getBinary.toByteArray
      case proto.DataType.KindCase.DATE => v => v.getDate
      case proto.DataType.KindCase.TIMESTAMP => v => v.getTimestamp
      case proto.DataType.KindCase.TIMESTAMP_NTZ => v => v.getTimestampNtz
      case proto.DataType.KindCase.DAY_TIME_INTERVAL => v => v.getDayTimeInterval
      case proto.DataType.KindCase.YEAR_MONTH_INTERVAL => v => v.getYearMonthInterval
      case proto.DataType.KindCase.DECIMAL => v => Decimal(v.getDecimal.getValue)
      case proto.DataType.KindCase.CALENDAR_INTERVAL =>
        v =>
          val interval = v.getCalendarInterval
          new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
      case proto.DataType.KindCase.ARRAY =>
        v => toCatalystArrayInternal(v.getArray, dataType.getArray)
      case proto.DataType.KindCase.MAP =>
        v => toCatalystMapInternal(v.getMap, dataType.getMap)
      case proto.DataType.KindCase.STRUCT =>
        v => toCatalystStructInternal(v.getStruct, dataType.getStruct)
      case _ =>
        throw InvalidPlanInput(s"Unsupported Literal Type: ${dataType.getKindCase}")
    }
  }

  private def getInferredDataType(
      literal: proto.Expression.Literal,
      recursive: Boolean = false): Option[proto.DataType] = {
    if (literal.hasNull) {
      return Some(literal.getNull)
    }

    val builder = proto.DataType.newBuilder()
    literal.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.BINARY =>
        builder.setBinary(proto.DataType.Binary.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
        builder.setBoolean(proto.DataType.Boolean.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.BYTE =>
        builder.setByte(proto.DataType.Byte.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.SHORT =>
        builder.setShort(proto.DataType.Short.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.INTEGER =>
        builder.setInteger(proto.DataType.Integer.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.LONG =>
        builder.setLong(proto.DataType.Long.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.FLOAT =>
        builder.setFloat(proto.DataType.Float.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
        builder.setDouble(proto.DataType.Double.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.DATE =>
        builder.setDate(proto.DataType.Date.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP =>
        builder.setTimestamp(proto.DataType.Timestamp.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
        builder.setTimestampNtz(proto.DataType.TimestampNTZ.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
        builder.setCalendarInterval(proto.DataType.CalendarInterval.newBuilder.build())
      case proto.Expression.Literal.LiteralTypeCase.STRUCT =>
        if (recursive) {
          val struct = literal.getStruct
          val size = struct.getElementsCount
          val structTypeBuilder = proto.DataType.Struct.newBuilder
          var i = 0
          while (i < size) {
            val field = struct.getDataTypeStruct.getFields(i)
            if (field.hasDataType) {
              structTypeBuilder.addFields(field)
            } else {
              val element = struct.getElements(i)
              getInferredDataType(element, recursive = true) match {
                case Some(dataType) =>
                  val fieldBuilder = structTypeBuilder.addFieldsBuilder()
                  fieldBuilder.setName(field.getName)
                  fieldBuilder.setDataType(dataType)
                  fieldBuilder.setNullable(field.getNullable)
                  if (field.hasMetadata) {
                    fieldBuilder.setMetadata(field.getMetadata)
                  }
                case None => return None
              }
            }
            i += 1
          }
          builder.setStruct(structTypeBuilder.build())
        } else {
          builder.setStruct(proto.DataType.Struct.newBuilder.build())
        }
      case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
        if (recursive) {
          val arrayType = literal.getArray.getDataType
          val elementTypeOpt = if (arrayType.hasElementType) {
            Some(arrayType.getElementType)
          } else if (literal.getArray.getElementsCount > 0) {
            getInferredDataType(literal.getArray.getElements(0), recursive = true)
          } else {
            None
          }
          if (elementTypeOpt.isDefined) {
            builder.setArray(
              proto.DataType.Array
                .newBuilder()
                .setElementType(elementTypeOpt.get)
                .setContainsNull(arrayType.getContainsNull)
                .build())
          } else {
            return None
          }
        } else {
          builder.setArray(proto.DataType.Array.newBuilder.build())
        }
      case proto.Expression.Literal.LiteralTypeCase.MAP =>
        if (recursive) {
          val mapType = literal.getMap.getDataType
          val keyTypeOpt = if (mapType.hasKeyType) {
            Some(mapType.getKeyType)
          } else if (literal.getMap.getKeysCount > 0) {
            getInferredDataType(literal.getMap.getKeys(0), recursive = true)
          } else {
            None
          }
          val valueTypeOpt = if (mapType.hasValueType) {
            Some(mapType.getValueType)
          } else if (literal.getMap.getValuesCount > 0) {
            getInferredDataType(literal.getMap.getValues(0), recursive = true)
          } else {
            None
          }
          if (keyTypeOpt.isDefined && valueTypeOpt.isDefined) {
            builder.setMap(
              proto.DataType.Map.newBuilder
                .setKeyType(keyTypeOpt.get)
                .setValueType(valueTypeOpt.get)
                .setValueContainsNull(mapType.getValueContainsNull)
                .build())
          } else {
            return None
          }
        } else {
          builder.setMap(proto.DataType.Map.newBuilder.build())
        }
      case _ =>
        // Not all data types support inferring the data type from the literal at the moment.
        // e.g. the type of DayTimeInterval contains extra information like start_field and
        // end_field and cannot be inferred from the literal.
        return None
    }
    Some(builder.build())
  }

  private def toCatalystArrayInternal(
      array: proto.Expression.Literal.Array,
      arrayType: proto.DataType.Array): Array[_] = {
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
        tag: ClassTag[T]): Array[T] = {
      val size = array.getElementsCount
      if (size > 0) {
        Array.tabulate(size)(i => converter(array.getElements(i)))
      } else {
        Array.empty[T]
      }
    }

    makeArrayData(getConverter(arrayType.getElementType))
  }

  def getProtoArrayType(array: proto.Expression.Literal.Array): proto.DataType.Array = {
    if (array.hasDataType) {
      val literal = proto.Expression.Literal.newBuilder().setArray(array).build()
      getInferredDataType(literal, recursive = true) match {
        case Some(dataType) => dataType.getArray
        case None => throw InvalidPlanInput("Cannot infer data type from this array literal.")
      }
    } else if (array.hasElementType) {
      // For backward compatibility, we still support the old way to
      // define the type of the array.
      proto.DataType.Array.newBuilder
        .setElementType(array.getElementType)
        .setContainsNull(true)
        .build()
    } else {
      throw InvalidPlanInput("Data type information is missing in the array literal.")
    }
  }

  def toCatalystArray(array: proto.Expression.Literal.Array): Array[_] = {
    toCatalystArrayInternal(array, getProtoArrayType(array))
  }

  private def toCatalystMapInternal(
      map: proto.Expression.Literal.Map,
      mapType: proto.DataType.Map): mutable.Map[_, _] = {
    def makeMapData[K, V](
        keyConverter: proto.Expression.Literal => K,
        valueConverter: proto.Expression.Literal => V)(implicit
        tagK: ClassTag[K],
        tagV: ClassTag[V]): mutable.Map[K, V] = {
      val size = map.getKeysCount
      if (size > 0) {
        val m = mutable.LinkedHashMap.empty[K, V]
        m.sizeHint(size)
        m.addAll(Iterator.tabulate(size)(i =>
          (keyConverter(map.getKeys(i)), valueConverter(map.getValues(i)))))
      } else {
        mutable.Map.empty[K, V]
      }
    }

    makeMapData(getConverter(mapType.getKeyType), getConverter(mapType.getValueType))
  }

  def getProtoMapType(map: proto.Expression.Literal.Map): proto.DataType.Map = {
    if (map.hasDataType) {
      val literal = proto.Expression.Literal.newBuilder().setMap(map).build()
      getInferredDataType(literal, recursive = true) match {
        case Some(dataType) => dataType.getMap
        case None => throw InvalidPlanInput("Cannot infer data type from this map literal.")
      }
    } else if (map.hasKeyType && map.hasValueType) {
      // For backward compatibility, we still support the old way to
      // define the type of the map.
      proto.DataType.Map.newBuilder
        .setKeyType(map.getKeyType)
        .setValueType(map.getValueType)
        .setValueContainsNull(true)
        .build()
    } else {
      throw InvalidPlanInput("Data type information is missing in the map literal.")
    }
  }

  def toCatalystMap(map: proto.Expression.Literal.Map): mutable.Map[_, _] = {
    toCatalystMapInternal(map, getProtoMapType(map))
  }

  private def toCatalystStructInternal(
      struct: proto.Expression.Literal.Struct,
      structType: proto.DataType.Struct): Any = {
    def toTuple[A <: Object](data: Seq[A]): Product = {
      try {
        val tupleClass = SparkClassUtils.classForName(s"scala.Tuple${data.length}")
        tupleClass.getConstructors.head.newInstance(data: _*).asInstanceOf[Product]
      } catch {
        case _: Exception =>
          throw InvalidPlanInput(s"Unsupported Literal: ${data.mkString("Array(", ", ", ")")})")
      }
    }

    val size = struct.getElementsCount
    val structData = Seq.tabulate(size) { i =>
      val element = struct.getElements(i)
      val dataType = structType.getFields(i).getDataType
      getConverter(dataType)(element).asInstanceOf[Object]
    }
    toTuple(structData)
  }

  def getProtoStructType(struct: proto.Expression.Literal.Struct): proto.DataType.Struct = {
    if (struct.hasDataTypeStruct) {
      val literal = proto.Expression.Literal.newBuilder().setStruct(struct).build()
      getInferredDataType(literal, recursive = true) match {
        case Some(dataType) => dataType.getStruct
        case None => throw InvalidPlanInput("Cannot infer data type from this struct literal.")
      }
    } else if (struct.hasStructType) {
      // For backward compatibility, we still support the old way to
      // define and convert struct types.
      struct.getStructType.getStruct
    } else {
      throw InvalidPlanInput("Data type information is missing in the struct literal.")
    }
  }

  def toCatalystStruct(struct: proto.Expression.Literal.Struct): Any = {
    toCatalystStructInternal(struct, getProtoStructType(struct))
  }
}
