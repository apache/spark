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
package org.apache.spark.sql.protobuf

import scala.jdk.CollectionConverters._

import com.google.protobuf.{BoolValue, ByteString, BytesValue, DoubleValue, Duration, DynamicMessage, FloatValue, Int32Value, Int64Value, StringValue, Timestamp, UInt32Value, UInt64Value, WireFormat}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.ANSI_STYLE
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.protobuf.utils.ProtobufUtils
import org.apache.spark.sql.protobuf.utils.ProtobufUtils.{toFieldStr, ProtoMatchedField}
import org.apache.spark.sql.types._

/**
 * A serializer to serialize data in catalyst format to data in Protobuf format.
 */
private[sql] class ProtobufSerializer(
    rootCatalystType: DataType,
    rootDescriptor: Descriptor,
    nullable: Boolean)
    extends Logging {

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val converter: Any => Any = {
    assert(
      rootCatalystType.isInstanceOf[StructType],
      "ProtobufSerializer's root catalyst type must be a struct type")
    val baseConverter =
      try {
        rootCatalystType match {
          case st: StructType =>
            newStructConverter(st, rootDescriptor, Nil, Nil).asInstanceOf[Any => Any]
        }
      } catch {
        case ise: AnalysisException =>
          throw QueryCompilationErrors.cannotConvertSqlTypeToProtobufError(
            rootDescriptor.getName,
            rootCatalystType,
            ise)
      }
    if (nullable) { (data: Any) =>
      if (data == null) {
        null
      } else {
        baseConverter.apply(data)
      }
    } else {
      baseConverter
    }
  }

  private type Converter = (SpecializedGetters, Int) => Any

  private def newConverter(
      catalystType: DataType,
      fieldDescriptor: FieldDescriptor,
      catalystPath: Seq[String],
      protoPath: Seq[String]): Converter = {
    (catalystType, fieldDescriptor.getJavaType) match {
      case (NullType, _) =>
        (getter, ordinal) => null
      case (BooleanType, BOOLEAN) =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, INT) =>
        (getter, ordinal) => getter.getByte(ordinal).toInt
      case (ShortType, INT) =>
        (getter, ordinal) => getter.getShort(ordinal).toInt
      case (IntegerType, INT) =>
        (getter, ordinal) => {
          getter.getInt(ordinal)
        }
      case (LongType, INT) if fieldDescriptor.getLiteType == WireFormat.FieldType.UINT32 =>
        (getter, ordinal) => {
          getter.getLong(ordinal).toInt
        }
      case (LongType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (DecimalType(), LONG)
        if fieldDescriptor.getLiteType == WireFormat.FieldType.UINT64 =>
        (getter, ordinal) => {
          getter.getDecimal(ordinal, 20, 0).toUnscaledLong
        }
      case (FloatType, FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (StringType, ENUM) =>
        val enumSymbols: Set[String] =
          fieldDescriptor.getEnumType.getValues.asScala.map(e => e.toString).toSet
        (getter, ordinal) =>
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw QueryExecutionErrors.cannotConvertCatalystValueToProtobufEnumTypeError(
              catalystPath,
              toFieldStr(protoPath),
              data,
              enumSymbols.mkString("\"", "\", \"", "\""))
          }
          fieldDescriptor.getEnumType.findValueByName(data)
      case (IntegerType, ENUM) =>
        val enumValues: Set[Int] =
          fieldDescriptor.getEnumType.getValues.asScala.map(e => e.getNumber).toSet
        (getter, ordinal) =>
          val data = getter.getInt(ordinal)
          if (!enumValues.contains(data)) {
            throw QueryExecutionErrors.cannotConvertCatalystValueToProtobufEnumTypeError(
              catalystPath,
              toFieldStr(protoPath),
              data.toString,
              enumValues.mkString(", "))
          }
          fieldDescriptor.getEnumType.findValueByNumber(data)
      case (StringType, STRING) =>
        (getter, ordinal) => {
          String.valueOf(getter.getUTF8String(ordinal))
        }

      case (BinaryType, BYTE_STRING) =>
        (getter, ordinal) => getter.getBinary(ordinal)

      case (DateType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (TimestampType, MESSAGE) =>
        (getter, ordinal) =>
          val millis = DateTimeUtils.microsToMillis(getter.getLong(ordinal))
          Timestamp
            .newBuilder()
            .setSeconds((millis / 1000))
            .setNanos(((millis % 1000) * 1000000).toInt)
            .build()

      case (ArrayType(et, containsNull), _) =>
        val elementConverter =
          newConverter(et, fieldDescriptor, catalystPath :+ "element", protoPath :+ "element")
        (getter, ordinal) => {
          val arrayData = getter.getArray(ordinal)
          val len = arrayData.numElements()
          val result = new Array[Any](len)
          var i = 0
          while (i < len) {
            if (containsNull && arrayData.isNullAt(i)) {
              result(i) = null
            } else {
              result(i) = elementConverter(arrayData, i)
            }
            i += 1
          }
          // Protobuf writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      // Handle serializing primitives back into well known wrapper types.
      case (BooleanType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == BoolValue.getDescriptor.getFullName =>
        (getter, ordinal) =>
          BoolValue.of(getter.getBoolean(ordinal))

      case (IntegerType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == Int32Value.getDescriptor.getFullName =>
        (getter, ordinal) =>
          Int32Value.of(getter.getInt(ordinal))

      case (IntegerType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == UInt32Value.getDescriptor.getFullName =>
        (getter, ordinal) =>
          UInt32Value.of(getter.getInt(ordinal))

      case (LongType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == Int64Value.getDescriptor.getFullName =>
        (getter, ordinal) =>
          Int64Value.of(getter.getLong(ordinal))

      case (LongType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == UInt64Value.getDescriptor.getFullName =>
        (getter, ordinal) =>
          UInt64Value.of(getter.getLong(ordinal))

      case (StringType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == StringValue.getDescriptor.getFullName =>
        (getter, ordinal) =>
          StringValue.of(getter.getUTF8String(ordinal).toString)

      case (BinaryType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == BytesValue.getDescriptor.getFullName =>
        (getter, ordinal) =>
          BytesValue.of(ByteString.copyFrom(getter.getBinary(ordinal)))

      case (FloatType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == FloatValue.getDescriptor.getFullName =>
        (getter, ordinal) =>
          FloatValue.of(getter.getFloat(ordinal))

      case (DoubleType, MESSAGE)
        if fieldDescriptor.getMessageType.getFullName == DoubleValue.getDescriptor.getFullName =>
        (getter, ordinal) =>
          DoubleValue.of(getter.getDouble(ordinal))

      case (st: StructType, MESSAGE) =>
        val structConverter =
          newStructConverter(st, fieldDescriptor.getMessageType, catalystPath, protoPath)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      case (MapType(kt, vt, valueContainsNull), MESSAGE) =>
        var keyField: FieldDescriptor = null
        var valueField: FieldDescriptor = null
        fieldDescriptor.getMessageType.getFields.asScala.foreach { field =>
          field.getName match {
            case "key" =>
              keyField = field
            case "value" =>
              valueField = field
          }
        }

        val keyConverter = newConverter(kt, keyField, catalystPath :+ "key", protoPath :+ "key")
        val valueConverter =
          newConverter(vt, valueField, catalystPath :+ "value", protoPath :+ "value")

        (getter, ordinal) =>
          val mapData = getter.getMap(ordinal)
          val len = mapData.numElements()
          val list = new java.util.ArrayList[DynamicMessage]()
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var i = 0
          while (i < len) {
            val result = DynamicMessage.newBuilder(fieldDescriptor.getMessageType)
            if (valueContainsNull && valueArray.isNullAt(i)) {
              result.setField(keyField, keyConverter(keyArray, i))
              result.setField(valueField, valueField.getDefaultValue)
            } else {
              result.setField(keyField, keyConverter(keyArray, i))
              result.setField(valueField, valueConverter(valueArray, i))
            }
            list.add(result.build())
            i += 1
          }
          list

      case (DayTimeIntervalType(startField, endField), MESSAGE) =>
        (getter, ordinal) =>
          val dayTimeIntervalString =
            IntervalUtils.toDayTimeIntervalString(getter.getLong(ordinal)
              , ANSI_STYLE, startField, endField)
          val calendarInterval = IntervalUtils.fromIntervalString(dayTimeIntervalString)

          val millis = DateTimeUtils.microsToMillis(calendarInterval.microseconds)
          val duration = Duration
            .newBuilder()
            .setSeconds((millis / 1000))
            .setNanos(((millis % 1000) * 1000000).toInt)

          if (duration.getSeconds < 0 && duration.getNanos > 0) {
            duration.setSeconds(duration.getSeconds + 1)
            duration.setNanos(duration.getNanos - 1000000000)
          } else if (duration.getSeconds > 0 && duration.getNanos < 0) {
            duration.setSeconds(duration.getSeconds - 1)
            duration.setNanos(duration.getNanos + 1000000000)
          }
          duration.build()

      case _ =>
        throw QueryCompilationErrors.cannotConvertCatalystTypeToProtobufTypeError(
          catalystPath,
          toFieldStr(protoPath),
          catalystType,
          s"${fieldDescriptor} ${fieldDescriptor.toProto.getLabel} ${fieldDescriptor.getJavaType}" +
            s" ${fieldDescriptor.getType}")
    }
  }

  private def newStructConverter(
      catalystStruct: StructType,
      descriptor: Descriptor,
      catalystPath: Seq[String],
      protoPath: Seq[String]): InternalRow => DynamicMessage = {

    val protoSchemaHelper =
      new ProtobufUtils.ProtoSchemaHelper(descriptor, catalystStruct, protoPath, catalystPath)

    protoSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = false)
    protoSchemaHelper.validateNoExtraRequiredProtoFields()

    val (protoIndices, fieldConverters: Array[Converter]) = protoSchemaHelper.matchedFields
      .map { case ProtoMatchedField(catalystField, _, protoField) =>
        val converter = newConverter(
          catalystField.dataType,
          protoField,
          catalystPath :+ catalystField.name,
          protoPath :+ protoField.getName)
        (protoField, converter)
      }
      .toArray
      .unzip

    val numFields = catalystStruct.length
    row: InternalRow =>
      val result = DynamicMessage.newBuilder(descriptor)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          if (!protoIndices(i).isRepeated() &&
            protoIndices(i).getJavaType() != FieldDescriptor.JavaType.MESSAGE &&
            protoIndices(i).isRequired()) {
            result.setField(protoIndices(i), protoIndices(i).getDefaultValue())
          }
        } else {
          result.setField(protoIndices(i), fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      result.build()
  }
}
