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

package org.apache.spark.sql.connect.planner

import scala.collection.convert.ImplicitConversions._

import org.apache.spark.connect.proto
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

/**
 * This object offers methods to convert to/from connect proto to catalyst types.
 */
object DataTypeProtoConverter {
  def toCatalystType(t: proto.DataType): DataType = {
    t.getKindCase match {
      case proto.DataType.KindCase.NULL => NullType

      case proto.DataType.KindCase.BINARY => BinaryType

      case proto.DataType.KindCase.BOOLEAN => BooleanType

      case proto.DataType.KindCase.BYTE => ByteType
      case proto.DataType.KindCase.SHORT => ShortType
      case proto.DataType.KindCase.INTEGER => IntegerType
      case proto.DataType.KindCase.LONG => LongType

      case proto.DataType.KindCase.FLOAT => FloatType
      case proto.DataType.KindCase.DOUBLE => DoubleType
      case proto.DataType.KindCase.DECIMAL => toCatalystDecimalType(t.getDecimal)

      case proto.DataType.KindCase.STRING => StringType
      case proto.DataType.KindCase.CHAR => CharType(t.getChar.getLength)
      case proto.DataType.KindCase.VAR_CHAR => VarcharType(t.getVarChar.getLength)

      case proto.DataType.KindCase.DATE => DateType
      case proto.DataType.KindCase.TIMESTAMP => TimestampType
      case proto.DataType.KindCase.TIMESTAMP_NTZ => TimestampNTZType

      case proto.DataType.KindCase.CALENDAR_INTERVAL => CalendarIntervalType
      case proto.DataType.KindCase.YEAR_MONTH_INTERVAL =>
        toCatalystYearMonthIntervalType(t.getYearMonthInterval)
      case proto.DataType.KindCase.DAY_TIME_INTERVAL =>
        toCatalystDayTimeIntervalType(t.getDayTimeInterval)

      case proto.DataType.KindCase.ARRAY => toCatalystArrayType(t.getArray)
      case proto.DataType.KindCase.STRUCT => toCatalystStructType(t.getStruct)
      case proto.DataType.KindCase.MAP => toCatalystMapType(t.getMap)
      case _ =>
        throw InvalidPlanInput(s"Does not support convert ${t.getKindCase} to catalyst types.")
    }
  }

  private def toCatalystDecimalType(t: proto.DataType.Decimal): DecimalType = {
    (t.hasPrecision, t.hasScale) match {
      case (true, true) => DecimalType(t.getPrecision, t.getScale)
      case (true, false) => new DecimalType(t.getPrecision)
      case _ => new DecimalType()
    }
  }

  private def toCatalystYearMonthIntervalType(t: proto.DataType.YearMonthInterval) = {
    (t.hasStartField, t.hasEndField) match {
      case (true, true) => YearMonthIntervalType(t.getStartField.toByte, t.getEndField.toByte)
      case (true, false) => YearMonthIntervalType(t.getStartField.toByte)
      case _ => YearMonthIntervalType()
    }
  }

  private def toCatalystDayTimeIntervalType(t: proto.DataType.DayTimeInterval) = {
    (t.hasStartField, t.hasEndField) match {
      case (true, true) => DayTimeIntervalType(t.getStartField.toByte, t.getEndField.toByte)
      case (true, false) => DayTimeIntervalType(t.getStartField.toByte)
      case _ => DayTimeIntervalType()
    }
  }

  private def toCatalystArrayType(t: proto.DataType.Array): ArrayType = {
    ArrayType(toCatalystType(t.getElementType), t.getContainsNull)
  }

  private def toCatalystStructType(t: proto.DataType.Struct): StructType = {
    // TODO: support metadata
    val fields = t.getFieldsList.toSeq.map { protoField =>
      StructField(
        name = protoField.getName,
        dataType = toCatalystType(protoField.getDataType),
        nullable = protoField.getNullable,
        metadata = Metadata.empty)
    }
    StructType.apply(fields)
  }

  private def toCatalystMapType(t: proto.DataType.Map): MapType = {
    MapType(toCatalystType(t.getKeyType), toCatalystType(t.getValueType), t.getValueContainsNull)
  }

  def toConnectProtoType(t: DataType): proto.DataType = {
    t match {
      case NullType =>
        proto.DataType
          .newBuilder()
          .setNull(proto.DataType.NULL.getDefaultInstance)
          .build()

      case BooleanType =>
        proto.DataType
          .newBuilder()
          .setBoolean(proto.DataType.Boolean.getDefaultInstance)
          .build()

      case BinaryType =>
        proto.DataType
          .newBuilder()
          .setBinary(proto.DataType.Binary.getDefaultInstance)
          .build()

      case ByteType =>
        proto.DataType
          .newBuilder()
          .setByte(proto.DataType.Byte.getDefaultInstance)
          .build()

      case ShortType =>
        proto.DataType
          .newBuilder()
          .setShort(proto.DataType.Short.getDefaultInstance)
          .build()

      case IntegerType =>
        proto.DataType
          .newBuilder()
          .setInteger(proto.DataType.Integer.getDefaultInstance)
          .build()

      case LongType =>
        proto.DataType
          .newBuilder()
          .setLong(proto.DataType.Long.getDefaultInstance)
          .build()

      case FloatType =>
        proto.DataType
          .newBuilder()
          .setFloat(proto.DataType.Float.getDefaultInstance)
          .build()

      case DoubleType =>
        proto.DataType
          .newBuilder()
          .setDouble(proto.DataType.Double.getDefaultInstance)
          .build()

      case DecimalType.Fixed(precision, scale) =>
        proto.DataType
          .newBuilder()
          .setDecimal(
            proto.DataType.Decimal.newBuilder().setPrecision(precision).setScale(scale).build())
          .build()

      case StringType =>
        proto.DataType
          .newBuilder()
          .setString(proto.DataType.String.getDefaultInstance)
          .build()

      case CharType(length) =>
        proto.DataType
          .newBuilder()
          .setChar(proto.DataType.Char.newBuilder().setLength(length).build())
          .build()

      case VarcharType(length) =>
        proto.DataType
          .newBuilder()
          .setVarChar(proto.DataType.VarChar.newBuilder().setLength(length).build())
          .build()

      case DateType =>
        proto.DataType
          .newBuilder()
          .setDate(proto.DataType.Date.getDefaultInstance)
          .build()

      case TimestampType =>
        proto.DataType
          .newBuilder()
          .setTimestamp(proto.DataType.Timestamp.getDefaultInstance)
          .build()

      case TimestampNTZType =>
        proto.DataType
          .newBuilder()
          .setTimestampNtz(proto.DataType.TimestampNTZ.getDefaultInstance)
          .build()

      case CalendarIntervalType =>
        proto.DataType
          .newBuilder()
          .setCalendarInterval(proto.DataType.CalendarInterval.getDefaultInstance)
          .build()

      case YearMonthIntervalType(startField, endField) =>
        proto.DataType
          .newBuilder()
          .setYearMonthInterval(
            proto.DataType.YearMonthInterval
              .newBuilder()
              .setStartField(startField)
              .setEndField(endField)
              .build())
          .build()

      case DayTimeIntervalType(startField, endField) =>
        proto.DataType
          .newBuilder()
          .setDayTimeInterval(
            proto.DataType.DayTimeInterval
              .newBuilder()
              .setStartField(startField)
              .setEndField(endField)
              .build())
          .build()

      case ArrayType(elementType: DataType, containsNull: Boolean) =>
        proto.DataType
          .newBuilder()
          .setArray(
            proto.DataType.Array
              .newBuilder()
              .setElementType(toConnectProtoType(elementType))
              .setContainsNull(containsNull)
              .build())
          .build()

      case StructType(fields: Array[StructField]) =>
        // TODO: support metadata
        val protoFields = fields.toSeq.map {
          case StructField(
                name: String,
                dataType: DataType,
                nullable: Boolean,
                metadata: Metadata) =>
            proto.DataType.StructField
              .newBuilder()
              .setName(name)
              .setDataType(toConnectProtoType(dataType))
              .setNullable(nullable)
              .build()
        }
        proto.DataType
          .newBuilder()
          .setStruct(
            proto.DataType.Struct
              .newBuilder()
              .addAllFields(protoFields)
              .build())
          .build()

      case MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean) =>
        proto.DataType
          .newBuilder()
          .setMap(
            proto.DataType.Map
              .newBuilder()
              .setKeyType(toConnectProtoType(keyType))
              .setValueType(toConnectProtoType(valueType))
              .setValueContainsNull(valueContainsNull)
              .build())
          .build()

      case _ =>
        throw InvalidPlanInput(s"Does not support convert ${t.typeName} to connect proto types.")
    }
  }

  def toSaveMode(mode: proto.WriteOperation.SaveMode): SaveMode = {
    mode match {
      case proto.WriteOperation.SaveMode.SAVE_MODE_APPEND => SaveMode.Append
      case proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE => SaveMode.Ignore
      case proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE => SaveMode.Overwrite
      case proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS => SaveMode.ErrorIfExists
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot convert from WriteOperaton.SaveMode to Spark SaveMode: ${mode.getNumber}")
    }
  }

  def toSaveModeProto(mode: SaveMode): proto.WriteOperation.SaveMode = {
    mode match {
      case SaveMode.Append => proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
      case SaveMode.Ignore => proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE
      case SaveMode.Overwrite => proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
      case SaveMode.ErrorIfExists => proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot convert from SaveMode to WriteOperation.SaveMode: ${mode.name()}")
    }
  }
}
