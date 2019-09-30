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

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._

/**
 * A serializer to serialize data in catalyst format to data in avro format.
 */
class AvroSerializer(rootCatalystType: DataType,
                     rootAvroType: Schema,
                     nullable: Boolean,
                     logicalTypeUpdater: AvroLogicalTypeCatalystMapper)
  extends Logging {

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
          converter.apply(DataSerializer(tmpRow, 0))
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

  private type Converter = DataSerializer => Any

  private lazy val decimalConversions = new DecimalConversion()

  private def newConverter(catalystType: DataType, avroType: Schema): Converter = {
    (catalystType, avroType.getType) match {
      case _ if logicalTypeUpdater.serialize.isDefinedAt(avroType.getLogicalType) =>
        logicalTypeUpdater.serialize(avroType.getLogicalType)
      case (NullType, NULL) =>
        _ => null
      case (BooleanType, BOOLEAN) =>
        dataSerializer => dataSerializer
          .getter
          .getBoolean(dataSerializer.ordinal)
      case (ByteType, INT) =>
        dataSerializer => dataSerializer
          .getter
          .getByte(dataSerializer.ordinal).toInt
      case (ShortType, INT) =>
        dataSerializer => dataSerializer
          .getter
          .getShort(dataSerializer.ordinal).toInt
      case (IntegerType, INT) =>
        dataSerializer => dataSerializer
          .getter
          .getInt(dataSerializer.ordinal)
      case (LongType, LONG) =>
        dataSerializer => dataSerializer
          .getter
          .getLong(dataSerializer.ordinal)
      case (FloatType, FLOAT) =>
        dataSerializer => dataSerializer
          .getter
          .getFloat(dataSerializer.ordinal)
      case (DoubleType, DOUBLE) =>
        dataSerializer => dataSerializer
          .getter
          .getDouble(dataSerializer.ordinal)
      case (d: DecimalType, FIXED)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        dataSerializer =>
          val decimal = dataSerializer
            .getter
            .getDecimal(dataSerializer.ordinal, d.precision, d.scale)
          decimalConversions.toFixed(decimal.toJavaBigDecimal, avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (d: DecimalType, BYTES)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        dataSerializer =>
          val decimal = dataSerializer
            .getter
            .getDecimal(dataSerializer.ordinal, d.precision, d.scale)
          decimalConversions.toBytes(decimal.toJavaBigDecimal, avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (StringType, ENUM) =>
        val enumSymbols: Set[String] = avroType.getEnumSymbols.asScala.toSet
        dataSerializer =>
          val data = dataSerializer
            .getter
            .getUTF8String(dataSerializer.ordinal)
            .toString
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(
              "Cannot write \"" + data + "\" since it's not defined in enum \"" +
                enumSymbols.mkString("\", \"") + "\"")
          }
          new EnumSymbol(avroType, data)

      case (StringType, STRING) =>
        dataSerializer => new Utf8(dataSerializer
          .getter
          .getUTF8String(dataSerializer.ordinal)
          .getBytes)

      case (BinaryType, FIXED) =>
        val size = avroType.getFixedSize()
        dataSerializer =>
          val data: Array[Byte] = dataSerializer
            .getter
            .getBinary(dataSerializer.ordinal)
          if (data.length != size) {
            throw new IncompatibleSchemaException(
              s"Cannot write ${data.length} ${if (data.length > 1) "bytes" else "byte"} of " +
                "binary data into FIXED Type with size of " +
                s"$size ${if (size > 1) "bytes" else "byte"}")
          }
          new Fixed(avroType, data)

      case (BinaryType, BYTES) =>
        dataSerializer => ByteBuffer.wrap(dataSerializer
          .getter
          .getBinary(dataSerializer.ordinal))

      case (DateType, INT) =>
        dataSerializer => dataSerializer
          .getter
          .getInt(dataSerializer.ordinal)

      case (TimestampType, LONG) => avroType.getLogicalType match {
          case _: TimestampMillis => dataSerializer => dataSerializer
            .getter
            .getLong(dataSerializer.ordinal) / 1000
          case _: TimestampMicros => dataSerializer => dataSerializer
            .getter
            .getLong(dataSerializer.ordinal)
          // For backward compatibility, if the Avro type is Long and it is not logical type,
          // output the timestamp value as with millisecond precision.
          case null => dataSerializer => dataSerializer
            .getter
            .getLong(dataSerializer.ordinal) / 1000
          case other => throw new IncompatibleSchemaException(
            s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
        }

      case (ArrayType(et, containsNull), ARRAY) =>
        val elementConverter = newConverter(
          et, resolveNullableType(avroType.getElementType, containsNull))
        dataSerializer => {
          val arrayData = dataSerializer
            .getter
            .getArray(dataSerializer.ordinal)
          val len = arrayData.numElements()
          val result = new Array[Any](len)
          var i = 0
          while (i < len) {
            if (containsNull && arrayData.isNullAt(i)) {
              result(i) = null
            } else {
              result(i) = elementConverter(DataSerializer(arrayData, i))
            }
            i += 1
          }
          // avro writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      case (st: StructType, RECORD) =>
        val structConverter = newStructConverter(st, avroType)
        val numFields = st.length
        dataSerializer => structConverter(dataSerializer
          .getter
          .getStruct(dataSerializer.ordinal, numFields))

      case (MapType(kt, vt, valueContainsNull), MAP) if kt == StringType =>
        val valueConverter = newConverter(
          vt, resolveNullableType(avroType.getValueType, valueContainsNull))
        dataSerializer =>
          val mapData = dataSerializer.getter.getMap(dataSerializer.ordinal)
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
              result.put(key, valueConverter(DataSerializer(valueArray, i)))
            }
            i += 1
          }
          result

      case other =>
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystType to " +
          s"Avro type $avroType.")
    }
  }

  private def newStructConverter(
      catalystStruct: StructType, avroStruct: Schema): InternalRow => Record = {
    if (avroStruct.getType != RECORD || avroStruct.getFields.size() != catalystStruct.length) {
      throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystStruct to " +
        s"Avro type $avroStruct.")
    }

    val (avroIndices: Array[Int], fieldConverters: Array[Converter]) =
      catalystStruct.map { catalystField =>
        val avroField = avroStruct.getField(catalystField.name)
        if (avroField == null) {
          throw new IncompatibleSchemaException(
            s"Cannot convert Catalyst type $catalystStruct to Avro type $avroStruct.")
        }
        val converter = newConverter(catalystField.dataType, resolveNullableType(
          avroField.schema(), catalystField.nullable))
        (avroField.pos(), converter)
      }.toArray.unzip

    val numFields = catalystStruct.length
    row: InternalRow =>
      val result = new Record(avroStruct)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          result.put(avroIndices(i), null)
        } else {
          result.put(avroIndices(i), fieldConverters(i).apply(DataSerializer(row, i)))
        }
        i += 1
      }
      result
  }

  private def resolveNullableType(avroType: Schema, nullable: Boolean): Schema = {
    if (avroType.getType == Type.UNION && nullable) {
      // avro uses union to represent nullable type.
      val fields = avroType.getTypes.asScala
      assert(fields.length == 2)
      val actualType = fields.filter(_.getType != Type.NULL)
      assert(actualType.length == 1)
      actualType.head
    } else {
      if (nullable) {
        logWarning("Writing avro files with non-nullable avro schema with nullable catalyst " +
          "schema will throw runtime exception if there is a record with null value.")
      }
      avroType
    }
  }
}
