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
package org.apache.spark.sql.proto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.DynamicMessage
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.proto.ProtoUtils.{toFieldStr, ProtoMatchedField}
import org.apache.spark.sql.proto.SchemaConverters.{IncompatibleSchemaException, UnsupportedProtoValueException}
import org.apache.spark.sql.types._

/**
 * A serializer to serialize data in catalyst format to data in proto format.
 */
private[sql] class ProtoSerializer(
                                    rootCatalystType: DataType,
                                    rootProtoType: Descriptor,
                                    nullable: Boolean,
                                    positionalFieldMatch: Boolean) extends Logging {

  def this(rootCatalystType: DataType, rootProtoType: Descriptor, nullable: Boolean) = {
    this(rootCatalystType, rootProtoType, nullable, positionalFieldMatch = false)
  }

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val converter: Any => Any = {
    val baseConverter = try {
      rootCatalystType match {
        case st: StructType =>
          newStructConverter(st, rootProtoType, Nil, Nil).asInstanceOf[Any => Any]
      }
    } catch {
      case ise: IncompatibleSchemaException => throw new IncompatibleSchemaException(
        s"Cannot convert SQL type ${rootCatalystType.sql} to Proto type ${rootProtoType.getName}.", ise)
    }
    if (nullable) {
      (data: Any) =>
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
                            protoType: FieldDescriptor,
                            catalystPath: Seq[String],
                            protoPath: Seq[String]): Converter = {
    val errorPrefix = s"Cannot convert SQL ${toFieldStr(catalystPath)} " +
      s"to Proto ${toFieldStr(protoPath)} because "
    (catalystType, protoType.getJavaType) match {
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
      case (LongType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (StringType, ENUM) =>
        val enumSymbols: Set[String] = protoType.getEnumType.getValues.asScala.map(
          e => e.toString).toSet
        (getter, ordinal) =>
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(errorPrefix +
              s""""$data" cannot be written since it's not defined in enum """ +
              enumSymbols.mkString("\"", "\", \"", "\""))
          }
          protoType.getEnumType.findValueByName(data)
      case (StringType, STRING) =>
        (getter, ordinal) => {
          String.valueOf(getter.getUTF8String(ordinal))
        }

      case (BinaryType, BYTE_STRING) =>
        (getter, ordinal) => getter.getBinary(ordinal)

      case (DateType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (TimestampType, LONG) => protoType.getContainingType match {
        // For backward compatibility, if the Proto type is Long and it is not logical type
        // (the `null` case), output the timestamp value as with millisecond precision.
        case null => (getter, ordinal) =>
          DateTimeUtils.microsToMillis(getter.getLong(ordinal))
        case other => throw new IncompatibleSchemaException(errorPrefix +
          s"SQL type ${TimestampType.sql} cannot be converted to Proto logical type $other")
      }

      case (TimestampNTZType, LONG) => protoType.getContainingType match {
        // To keep consistent with TimestampType, if the Proto type is Long and it is not
        // logical type (the `null` case), output the TimestampNTZ as long value
        // in millisecond precision.
        case null => (getter, ordinal) =>
          DateTimeUtils.microsToMillis(getter.getLong(ordinal))
        case other => throw new IncompatibleSchemaException(errorPrefix +
          s"SQL type ${TimestampNTZType.sql} cannot be converted to Proto logical type $other")
      }

      case (ArrayType(et, containsNull), _) =>
        val elementConverter = newConverter(
          et, protoType,
          catalystPath :+ "element", protoPath :+ "element")
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
          // proto writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      case (st: StructType, MESSAGE) =>
        val structConverter = newStructConverter(
          st, protoType.getMessageType, catalystPath, protoPath)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      case (MapType(kt, vt, valueContainsNull), _) if kt == StringType =>
        val valueConverter = newConverter(
          vt, protoType, catalystPath :+ "value", protoPath :+ "value")
        (getter, ordinal) =>
          val mapData = getter.getMap(ordinal)
          val len = mapData.numElements()
          val result = new java.util.HashMap[String, Any](len)
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var i = 0
          while (i < len) {
            val key = keyArray.getUTF8String(i).toString
            if (valueContainsNull && valueArray.isNullAt(i)) {
              result.put(key, null)
            } else {
              result.put(key, valueConverter(valueArray, i))
            }
            i += 1
          }
          result

      case (_: YearMonthIntervalType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (_: DayTimeIntervalType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)

      case _ =>
        throw new IncompatibleSchemaException(errorPrefix +
          s"schema is incompatible (sqlType = ${catalystType}, " +
          s"protoType = ${protoType.getJavaType})")
    }
  }

  private def newStructConverter(
                                  catalystStruct: StructType,
                                  protoStruct: Descriptor,
                                  catalystPath: Seq[String],
                                  protoPath: Seq[String]): InternalRow => DynamicMessage = {

    val protoSchemaHelper = new ProtoUtils.ProtoSchemaHelper(
      protoStruct, catalystStruct, protoPath, catalystPath, positionalFieldMatch)

    protoSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = false)
    protoSchemaHelper.validateNoExtraRequiredProtoFields()

    val (protoIndices, fieldConverters) = protoSchemaHelper.matchedFields.map {
      case ProtoMatchedField(catalystField, _, protoField) =>
        val converter = newConverter(catalystField.dataType,
          protoField,
          catalystPath :+ catalystField.name, protoPath :+ protoField.getName)
        (protoField, converter)
    }.toArray.unzip

    val numFields = catalystStruct.length
    row: InternalRow =>
      val result = DynamicMessage.newBuilder(protoStruct)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          throw new UnsupportedProtoValueException(
            s"Cannot set ${protoIndices(i).getName} a Null, Proto does not allow Null values")
        } else {
          result.setField(protoIndices(i), fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      result.build()
  }
}
