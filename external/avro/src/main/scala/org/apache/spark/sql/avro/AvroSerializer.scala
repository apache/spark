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

package org.apache.spark.sql.avro

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type.NULL
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

/**
 * A serializer to serialize data in catalyst format to data in avro format.
 */
class AvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean) {

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val converter: Any => Any = {
    val actualAvroType = resolveNullableType(rootAvroType, nullable)
    val baseConverter = rootCatalystType match {
      case st: StructType =>
        newStructConverter(st, actualAvroType).asInstanceOf[Any => Any]
      case _ =>
        val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
        val converter = newConverter(rootCatalystType, actualAvroType)
        (data: Any) =>
          tmpRow.update(0, data)
          converter.apply(tmpRow, 0)
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

  private def newConverter(catalystType: DataType, avroType: Schema): Converter = {
    catalystType match {
      case NullType =>
        (getter, ordinal) => null
      case BooleanType =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case ByteType =>
        (getter, ordinal) => getter.getByte(ordinal).toInt
      case ShortType =>
        (getter, ordinal) => getter.getShort(ordinal).toInt
      case IntegerType =>
        (getter, ordinal) => getter.getInt(ordinal)
      case LongType =>
        (getter, ordinal) => getter.getLong(ordinal)
      case FloatType =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case DoubleType =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case d: DecimalType =>
        (getter, ordinal) => getter.getDecimal(ordinal, d.precision, d.scale).toString
      case StringType =>
        (getter, ordinal) => new Utf8(getter.getUTF8String(ordinal).getBytes)
      case BinaryType =>
        (getter, ordinal) => ByteBuffer.wrap(getter.getBinary(ordinal))
      case DateType =>
        (getter, ordinal) => getter.getInt(ordinal) * DateTimeUtils.MILLIS_PER_DAY
      case TimestampType => avroType.getLogicalType match {
          case _: TimestampMillis => (getter, ordinal) => getter.getLong(ordinal) / 1000
          case _: TimestampMicros => (getter, ordinal) => getter.getLong(ordinal)
          // For backward compatibility, if the Avro type is Long and it is not logical type,
          // output the timestamp value as with millisecond precision.
          case null => (getter, ordinal) => getter.getLong(ordinal) / 1000
          case other => throw new IncompatibleSchemaException(
            s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
        }

      case ArrayType(et, containsNull) =>
        val elementConverter = newConverter(
          et, resolveNullableType(avroType.getElementType, containsNull))
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
          // avro writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      case st: StructType =>
        val structConverter = newStructConverter(st, avroType)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      case MapType(kt, vt, valueContainsNull) if kt == StringType =>
        val valueConverter = newConverter(
          vt, resolveNullableType(avroType.getValueType, valueContainsNull))
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

      case other =>
        throw new IncompatibleSchemaException(s"Unexpected type: $other")
    }
  }

  private def newStructConverter(
      catalystStruct: StructType, avroStruct: Schema): InternalRow => Record = {
    val avroFields = avroStruct.getFields
    assert(avroFields.size() == catalystStruct.length)
    val fieldConverters = catalystStruct.zip(avroFields.asScala).map {
      case (f1, f2) => newConverter(f1.dataType, resolveNullableType(f2.schema(), f1.nullable))
    }
    val numFields = catalystStruct.length
    (row: InternalRow) =>
      val result = new Record(avroStruct)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          result.put(i, null)
        } else {
          result.put(i, fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      result
  }

  private def resolveNullableType(avroType: Schema, nullable: Boolean): Schema = {
    if (nullable) {
      // avro uses union to represent nullable type.
      val fields = avroType.getTypes.asScala
      assert(fields.length == 2)
      val actualType = fields.filter(_.getType != NULL)
      assert(actualType.length == 1)
      actualType.head
    } else {
      avroType
    }
  }
}
