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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.{SparkDateTimeUtils, SparkIntervalUtils}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

object LiteralValueProtoConverter {

  private def setNullValue(
      builder: proto.Expression.Literal.Builder,
      dataType: DataType,
      needDataType: Boolean): proto.Expression.Literal.Builder = {
    if (needDataType) {
      builder.setNull(toConnectProtoType(dataType))
    } else {
      // No need data type but still set the null type to indicate that
      // the value is null.
      builder.setNull(ProtoDataTypes.NullType)
    }
  }

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
      array.foreach { x =>
        ab.addElements(toLiteralProtoBuilderInternal(x, options).build())
      }
      if (options.useDeprecatedDataTypeFields) {
        ab.setElementType(toConnectProtoType(toDataType(array.getClass.getComponentType)))
      }
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
      case v: immutable.ArraySeq[_] =>
        toLiteralProtoBuilderInternal(v.unsafeArray, options)
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

  private def toLiteralProtoBuilderInternal(
      literal: Any,
      dataType: DataType,
      options: ToLiteralProtoOptions): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def arrayBuilder(scalaValue: Any, elementType: DataType) = {
      val ab = builder.getArrayBuilder
      scalaValue match {
        case a: Array[_] =>
          a.foreach { item =>
            ab.addElements(toLiteralProtoBuilderInternal(item, elementType, options).build())
          }
        case s: scala.collection.Seq[_] =>
          s.foreach { item =>
            ab.addElements(toLiteralProtoBuilderInternal(item, elementType, options).build())
          }
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }
      if (options.useDeprecatedDataTypeFields) {
        ab.setElementType(toConnectProtoType(elementType))
      }
      ab
    }

    def mapBuilder(scalaValue: Any, keyType: DataType, valueType: DataType) = {
      val mb = builder.getMapBuilder
      scalaValue match {
        case map: scala.collection.Map[_, _] =>
          map.foreach { case (k, v) =>
            mb.addKeys(toLiteralProtoBuilderInternal(k, keyType, options).build())
            mb.addValues(toLiteralProtoBuilderInternal(v, valueType, options).build())
          }
        case other =>
          throw new IllegalArgumentException(s"literal $other not supported (yet).")
      }
      if (options.useDeprecatedDataTypeFields) {
        mb.setKeyType(toConnectProtoType(keyType))
        mb.setValueType(toConnectProtoType(valueType))
      }
      mb
    }

    def structBuilder(scalaValue: Any, structType: StructType) = {
      val sb = builder.getStructBuilder
      val fields = structType.fields

      val iter = scalaValue match {
        case p: Product =>
          p.productIterator
        case r: Row =>
          r.toSeq.iterator
        case other =>
          throw new IllegalArgumentException(
            s"literal ${other.getClass.getName}($other) not supported (yet).")
      }

      var idx = 0
      while (idx < structType.size) {
        val field = fields(idx)
        val literalProto =
          toLiteralProtoBuilderInternal(iter.next(), field.dataType, options)
        sb.addElements(literalProto)
        idx += 1
      }
      if (options.useDeprecatedDataTypeFields) {
        sb.setStructType(toConnectProtoType(structType))
      }

      sb
    }

    (literal, dataType) match {
      case (v: Option[_], _) =>
        if (v.isDefined) {
          toLiteralProtoBuilderInternal(v.get, dataType, options)
        } else {
          setNullValue(builder, dataType, options.useDeprecatedDataTypeFields)
        }
      case (null, _) =>
        setNullValue(builder, dataType, options.useDeprecatedDataTypeFields)
      case (v: mutable.ArraySeq[_], ArrayType(_, _)) =>
        toLiteralProtoBuilderInternal(v.array, dataType, options)
      case (v: immutable.ArraySeq[_], ArrayType(_, _)) =>
        toLiteralProtoBuilderInternal(v.unsafeArray, dataType, options)
      case (v: Array[Byte], ArrayType(_, _)) =>
        toLiteralProtoBuilderInternal(v, options)
      case (v, ArrayType(elementType, _)) =>
        builder.setArray(arrayBuilder(v, elementType))
      case (v, MapType(keyType, valueType, _)) =>
        builder.setMap(mapBuilder(v, keyType, valueType))
      case (v, structType: StructType) =>
        builder.setStruct(structBuilder(v, structType))
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
    toLiteralProtoBuilderWithOptions(
      literal,
      None,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  def toLiteralProtoBuilder(
      literal: Any,
      dataType: DataType): proto.Expression.Literal.Builder = {
    toLiteralProtoBuilderWithOptions(
      literal,
      Some(dataType),
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  private def setDataTypeForRootLiteral(
      builder: proto.Expression.Literal.Builder,
      dataType: DataType): proto.Expression.Literal.Builder = {
    if (builder.getLiteralTypeCase ==
        proto.Expression.Literal.LiteralTypeCase.LITERALTYPE_NOT_SET) {
      throw new IllegalArgumentException("Literal type should be set first")
    }
    // To be compatible with the current Scala behavior, we should convert bytes to binary.
    val protoDataType = toConnectProtoType(dataType, bytesToBinary = true)
    // If the value is not null and the data type is trivial, we don't need to
    // set the data type field, because it will be inferred from the literal value, saving space.
    val needDataType = protoDataType.getKindCase match {
      case proto.DataType.KindCase.ARRAY => true
      case proto.DataType.KindCase.STRUCT => true
      case proto.DataType.KindCase.MAP => true
      case _ => builder.getLiteralTypeCase == proto.Expression.Literal.LiteralTypeCase.NULL
    }
    if (needDataType) {
      builder.setDataType(protoDataType)
    }
    builder
  }

  def toLiteralProtoBuilderWithOptions(
      literal: Any,
      dataTypeOpt: Option[DataType],
      options: ToLiteralProtoOptions): proto.Expression.Literal.Builder = {
    dataTypeOpt match {
      case Some(dataType) =>
        val builder = toLiteralProtoBuilderInternal(literal, dataType, options)
        if (!options.useDeprecatedDataTypeFields) {
          setDataTypeForRootLiteral(builder, dataType)
        }
        builder
      case None =>
        val builder = toLiteralProtoBuilderInternal(literal, options)
        if (!options.useDeprecatedDataTypeFields) {
          @scala.annotation.tailrec
          def unwrapArraySeq(value: Any): Any = value match {
            case arraySeq: mutable.ArraySeq[_] => unwrapArraySeq(arraySeq.array)
            case arraySeq: immutable.ArraySeq[_] => unwrapArraySeq(arraySeq.unsafeArray)
            case _ => value
          }
          unwrapArraySeq(literal) match {
            case null =>
              setDataTypeForRootLiteral(builder, NullType)
            case value =>
              setDataTypeForRootLiteral(builder, toDataType(value.getClass))
          }
        }
        builder
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
  def toLiteralProto(literal: Any): proto.Expression.Literal = {
    toLiteralProtoWithOptions(
      literal,
      None,
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  def toLiteralProto(literal: Any, dataType: DataType): proto.Expression.Literal = {
    toLiteralProtoWithOptions(
      literal,
      Some(dataType),
      ToLiteralProtoOptions(useDeprecatedDataTypeFields = true))
  }

  def toLiteralProtoWithOptions(
      literal: Any,
      dataTypeOpt: Option[DataType],
      options: ToLiteralProtoOptions): proto.Expression.Literal = {
    toLiteralProtoBuilderWithOptions(literal, dataTypeOpt, options).build()
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
    case _ if clz == classOf[LocalTime] => TimeType(TimeType.DEFAULT_PRECISION)
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
    case _ if clz == classOf[JChar] => StringType

    // other scala classes
    case _ if clz == classOf[String] => StringType
    case _ if clz == classOf[BigInt] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[Decimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[CalendarInterval] => CalendarIntervalType
    case _ if clz.isArray => ArrayType(toDataType(clz.getComponentType))
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }

  def toScalaValue(literal: proto.Expression.Literal): Any = {
    getScalaConverter(getProtoDataType(literal))(literal)
  }

  private def getScalaConverter(dataType: proto.DataType): proto.Expression.Literal => Any = {
    val converter: proto.Expression.Literal => Any = dataType.getKindCase match {
      case proto.DataType.KindCase.NULL =>
        v => throw InvalidPlanInput(s"Expected null value, but got ${v.getLiteralTypeCase}")
      case proto.DataType.KindCase.SHORT => v => v.getShort.toShort
      case proto.DataType.KindCase.INTEGER => v => v.getInteger
      case proto.DataType.KindCase.LONG => v => v.getLong
      case proto.DataType.KindCase.DOUBLE => v => v.getDouble
      case proto.DataType.KindCase.BYTE => v => v.getByte.toByte
      case proto.DataType.KindCase.FLOAT => v => v.getFloat
      case proto.DataType.KindCase.BOOLEAN => v => v.getBoolean
      case proto.DataType.KindCase.STRING => v => v.getString
      case proto.DataType.KindCase.BINARY => v => v.getBinary.toByteArray
      case proto.DataType.KindCase.DATE =>
        v => SparkDateTimeUtils.toJavaDate(v.getDate)
      case proto.DataType.KindCase.TIMESTAMP =>
        v => SparkDateTimeUtils.toJavaTimestamp(v.getTimestamp)
      case proto.DataType.KindCase.TIMESTAMP_NTZ =>
        v => SparkDateTimeUtils.microsToLocalDateTime(v.getTimestampNtz)
      case proto.DataType.KindCase.DAY_TIME_INTERVAL =>
        v => SparkIntervalUtils.microsToDuration(v.getDayTimeInterval)
      case proto.DataType.KindCase.YEAR_MONTH_INTERVAL =>
        v => SparkIntervalUtils.monthsToPeriod(v.getYearMonthInterval)
      case proto.DataType.KindCase.TIME =>
        v => SparkDateTimeUtils.nanosToLocalTime(v.getTime.getNano)
      case proto.DataType.KindCase.DECIMAL => v => Decimal(v.getDecimal.getValue)
      case proto.DataType.KindCase.CALENDAR_INTERVAL =>
        v =>
          val interval = v.getCalendarInterval
          new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
      case proto.DataType.KindCase.ARRAY =>
        v => toScalaArrayInternal(v, dataType.getArray)
      case proto.DataType.KindCase.MAP =>
        v => toScalaMapInternal(v, dataType.getMap)
      case proto.DataType.KindCase.STRUCT =>
        v => toScalaStructInternal(v, dataType.getStruct)
      case _ =>
        throw InvalidPlanInput(s"Unsupported Literal Type: ${dataType.getKindCase}")
    }
    v => if (v.hasNull) null else converter(v)
  }

  private def isCompatible(
      literalTypeCase: proto.Expression.Literal.LiteralTypeCase,
      dataTypeCase: proto.DataType.KindCase): Boolean = {
    (literalTypeCase, dataTypeCase) match {
      case (proto.Expression.Literal.LiteralTypeCase.NULL, _) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.BINARY, proto.DataType.KindCase.BINARY) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.BOOLEAN, proto.DataType.KindCase.BOOLEAN) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.BYTE, proto.DataType.KindCase.BYTE) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.SHORT, proto.DataType.KindCase.SHORT) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.INTEGER, proto.DataType.KindCase.INTEGER) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.LONG, proto.DataType.KindCase.LONG) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.FLOAT, proto.DataType.KindCase.FLOAT) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.DOUBLE, proto.DataType.KindCase.DOUBLE) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.DECIMAL, proto.DataType.KindCase.DECIMAL) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.STRING, proto.DataType.KindCase.STRING) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.DATE, proto.DataType.KindCase.DATE) =>
        true
      case (
            proto.Expression.Literal.LiteralTypeCase.TIMESTAMP,
            proto.DataType.KindCase.TIMESTAMP) =>
        true
      case (
            proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ,
            proto.DataType.KindCase.TIMESTAMP_NTZ) =>
        true
      case (
            proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL,
            proto.DataType.KindCase.CALENDAR_INTERVAL) =>
        true
      case (
            proto.Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL,
            proto.DataType.KindCase.DAY_TIME_INTERVAL) =>
        true
      case (
            proto.Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL,
            proto.DataType.KindCase.YEAR_MONTH_INTERVAL) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.TIME, proto.DataType.KindCase.TIME) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.ARRAY, proto.DataType.KindCase.ARRAY) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.MAP, proto.DataType.KindCase.MAP) =>
        true
      case (proto.Expression.Literal.LiteralTypeCase.STRUCT, proto.DataType.KindCase.STRUCT) =>
        true
      case _ => false
    }
  }

  def getProtoDataType(literal: proto.Expression.Literal): proto.DataType = {
    val dataType = if (literal.hasDataType) {
      literal.getDataType
    } else {
      // For backward compatibility, we still support the old way to
      // define the data type of the literal.
      if (literal.getLiteralTypeCase == proto.Expression.Literal.LiteralTypeCase.NULL) {
        literal.getNull
      } else {
        val builder = proto.DataType.newBuilder()
        literal.getLiteralTypeCase match {
          case proto.Expression.Literal.LiteralTypeCase.BINARY =>
            builder.setBinary(proto.DataType.Binary.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
            builder.setBoolean(proto.DataType.Boolean.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.BYTE =>
            builder.setByte(proto.DataType.Byte.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.SHORT =>
            builder.setShort(proto.DataType.Short.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.INTEGER =>
            builder.setInteger(proto.DataType.Integer.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.LONG =>
            builder.setLong(proto.DataType.Long.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.FLOAT =>
            builder.setFloat(proto.DataType.Float.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
            builder.setDouble(proto.DataType.Double.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.DECIMAL =>
            val decimal = Decimal.apply(literal.getDecimal.getValue)
            var precision = decimal.precision
            if (literal.getDecimal.hasPrecision) {
              precision = math.max(precision, literal.getDecimal.getPrecision)
            }
            var scale = decimal.scale
            if (literal.getDecimal.hasScale) {
              scale = math.max(scale, literal.getDecimal.getScale)
            }
            builder.setDecimal(
              proto.DataType.Decimal
                .newBuilder()
                .setPrecision(math.max(precision, scale))
                .setScale(scale)
                .build())
          case proto.Expression.Literal.LiteralTypeCase.STRING =>
            builder.setString(proto.DataType.String.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.DATE =>
            builder.setDate(proto.DataType.Date.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP =>
            builder.setTimestamp(proto.DataType.Timestamp.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
            builder.setTimestampNtz(proto.DataType.TimestampNTZ.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
            builder.setCalendarInterval(proto.DataType.CalendarInterval.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL =>
            builder.setYearMonthInterval(proto.DataType.YearMonthInterval.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL =>
            builder.setDayTimeInterval(proto.DataType.DayTimeInterval.newBuilder().build())
          case proto.Expression.Literal.LiteralTypeCase.TIME =>
            val timeBuilder = proto.DataType.Time.newBuilder()
            if (literal.getTime.hasPrecision) {
              timeBuilder.setPrecision(literal.getTime.getPrecision)
            }
            builder.setTime(timeBuilder.build())
          case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
            if (literal.getArray.hasElementType) {
              builder.setArray(
                proto.DataType.Array
                  .newBuilder()
                  .setElementType(literal.getArray.getElementType)
                  .setContainsNull(true)
                  .build())
            } else {
              throw InvalidPlanInput("Data type information is missing in the array literal.")
            }
          case proto.Expression.Literal.LiteralTypeCase.MAP =>
            if (literal.getMap.hasKeyType && literal.getMap.hasValueType) {
              builder.setMap(
                proto.DataType.Map
                  .newBuilder()
                  .setKeyType(literal.getMap.getKeyType)
                  .setValueType(literal.getMap.getValueType)
                  .setValueContainsNull(true)
                  .build())
            } else {
              throw InvalidPlanInput("Data type information is missing in the map literal.")
            }
          case proto.Expression.Literal.LiteralTypeCase.STRUCT =>
            if (literal.getStruct.hasStructType) {
              builder.setStruct(literal.getStruct.getStructType.getStruct)
            } else {
              throw InvalidPlanInput("Data type information is missing in the struct literal.")
            }
          case _ =>
            throw InvalidPlanInput(
              s"Unsupported Literal Type: ${literal.getLiteralTypeCase.name}" +
                s"(${literal.getLiteralTypeCase.getNumber})")
        }
        builder.build()
      }
    }

    if (!isCompatible(literal.getLiteralTypeCase, dataType.getKindCase)) {
      throw InvalidPlanInput(
        s"Incompatible data type ${dataType.getKindCase} " +
          s"for literal ${literal.getLiteralTypeCase}")
    }

    dataType
  }

  private def toScalaArrayInternal(
      literal: proto.Expression.Literal,
      arrayType: proto.DataType.Array): Array[_] = {
    if (!literal.hasArray) {
      throw InvalidPlanInput("Array literal is not set.")
    }
    val array = literal.getArray
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
        tag: ClassTag[T]): Array[T] = {
      val size = array.getElementsCount
      if (size > 0) {
        Array.tabulate(size)(i => converter(array.getElements(i)))
      } else {
        Array.empty[T]
      }
    }

    makeArrayData(getScalaConverter(arrayType.getElementType))
  }

  private def toScalaMapInternal(
      literal: proto.Expression.Literal,
      mapType: proto.DataType.Map): mutable.Map[_, _] = {
    if (!literal.hasMap) {
      throw InvalidPlanInput("Map literal is not set.")
    }
    val map = literal.getMap
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

    makeMapData(getScalaConverter(mapType.getKeyType), getScalaConverter(mapType.getValueType))
  }

  private def toScalaStructInternal(
      literal: proto.Expression.Literal,
      structType: proto.DataType.Struct): Any = {
    if (!literal.hasStruct) {
      throw InvalidPlanInput("Struct literal is not set.")
    }
    val struct = literal.getStruct
    val structData = Array.tabulate(struct.getElementsCount) { i =>
      val element = struct.getElements(i)
      val dataType = structType.getFields(i).getDataType
      getScalaConverter(dataType)(element)
    }
    new GenericRowWithSchema(structData, DataTypeProtoConverter.toCatalystStructType(structType))
  }

  def getDataType(literal: proto.Expression.Literal): DataType = {
    DataTypeProtoConverter.toCatalystType(getProtoDataType(literal))
  }
}
