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

package org.apache.spark.sql.execution.datasources.parquet

import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{HashMap => JHashMap}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api._

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A `parquet.hadoop.api.WriteSupport` for Row objects.
 */
private[parquet] class RowWriteSupport extends WriteSupport[InternalRow] with Logging {

  private[parquet] var writer: RecordConsumer = null
  private[parquet] var attributes: Array[Attribute] = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    val origAttributesStr: String = configuration.get(RowWriteSupport.SPARK_ROW_SCHEMA)
    val metadata = new JHashMap[String, String]()
    metadata.put(CatalystReadSupport.SPARK_METADATA_KEY, origAttributesStr)

    if (attributes == null) {
      attributes = ParquetTypesConverter.convertFromString(origAttributesStr).toArray
    }

    log.debug(s"write support initialized for requested schema $attributes")
    new WriteSupport.WriteContext(ParquetTypesConverter.convertFromAttributes(attributes), metadata)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    writer = recordConsumer
    log.debug(s"preparing for write with schema $attributes")
  }

  override def write(record: InternalRow): Unit = {
    val attributesSize = attributes.size
    if (attributesSize > record.numFields) {
      throw new IndexOutOfBoundsException("Trying to write more fields than contained in row " +
        s"($attributesSize > ${record.numFields})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributesSize) {
      // null values indicate optional fields but we do not check currently
      if (!record.isNullAt(index)) {
        writer.startField(attributes(index).name, index)
        writeValue(attributes(index).dataType, record.get(index, attributes(index).dataType))
        writer.endField(attributes(index).name, index)
      }
      index = index + 1
    }
    writer.endMessage()
  }

  private[parquet] def writeValue(schema: DataType, value: Any): Unit = {
    if (value != null) {
      schema match {
        case t: UserDefinedType[_] => writeValue(t.sqlType, value)
        case t @ ArrayType(_, _) => writeArray(
          t,
          value.asInstanceOf[CatalystConverter.ArrayScalaType])
        case t @ MapType(_, _, _) => writeMap(
          t,
          value.asInstanceOf[CatalystConverter.MapScalaType])
        case t @ StructType(_) => writeStruct(
          t,
          value.asInstanceOf[CatalystConverter.StructScalaType])
        case _ => writePrimitive(schema.asInstanceOf[AtomicType], value)
      }
    }
  }

  private[parquet] def writePrimitive(schema: DataType, value: Any): Unit = {
    if (value != null) {
      schema match {
        case BooleanType => writer.addBoolean(value.asInstanceOf[Boolean])
        case ByteType => writer.addInteger(value.asInstanceOf[Byte])
        case ShortType => writer.addInteger(value.asInstanceOf[Short])
        case IntegerType | DateType => writer.addInteger(value.asInstanceOf[Int])
        case LongType => writer.addLong(value.asInstanceOf[Long])
        case TimestampType => writeTimestamp(value.asInstanceOf[Long])
        case FloatType => writer.addFloat(value.asInstanceOf[Float])
        case DoubleType => writer.addDouble(value.asInstanceOf[Double])
        case StringType => writer.addBinary(
          Binary.fromByteArray(value.asInstanceOf[UTF8String].getBytes))
        case BinaryType => writer.addBinary(
          Binary.fromByteArray(value.asInstanceOf[Array[Byte]]))
        case DecimalType.Fixed(precision, _) =>
          writeDecimal(value.asInstanceOf[Decimal], precision)
        case _ => sys.error(s"Do not know how to writer $schema to consumer")
      }
    }
  }

  private[parquet] def writeStruct(
      schema: StructType,
      struct: CatalystConverter.StructScalaType): Unit = {
    if (struct != null) {
      val fields = schema.fields.toArray
      writer.startGroup()
      var i = 0
      while(i < fields.length) {
        if (!struct.isNullAt(i)) {
          writer.startField(fields(i).name, i)
          writeValue(fields(i).dataType, struct.get(i, fields(i).dataType))
          writer.endField(fields(i).name, i)
        }
        i = i + 1
      }
      writer.endGroup()
    }
  }

  private[parquet] def writeArray(
      schema: ArrayType,
      array: CatalystConverter.ArrayScalaType): Unit = {
    val elementType = schema.elementType
    writer.startGroup()
    if (array.numElements() > 0) {
      if (schema.containsNull) {
        writer.startField(CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME, 0)
        var i = 0
        while (i < array.numElements()) {
          writer.startGroup()
          if (!array.isNullAt(i)) {
            writer.startField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
            writeValue(elementType, array.get(i, elementType))
            writer.endField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
          }
          writer.endGroup()
          i = i + 1
        }
        writer.endField(CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME, 0)
      } else {
        writer.startField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
        var i = 0
        while (i < array.numElements()) {
          writeValue(elementType, array.get(i, elementType))
          i = i + 1
        }
        writer.endField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
      }
    }
    writer.endGroup()
  }

  private[parquet] def writeMap(
      schema: MapType,
      map: CatalystConverter.MapScalaType): Unit = {
    writer.startGroup()
    val length = map.numElements()
    if (length > 0) {
      writer.startField(CatalystConverter.MAP_SCHEMA_NAME, 0)
      map.foreach(schema.keyType, schema.valueType, (key, value) => {
        writer.startGroup()
        writer.startField(CatalystConverter.MAP_KEY_SCHEMA_NAME, 0)
        writeValue(schema.keyType, key)
        writer.endField(CatalystConverter.MAP_KEY_SCHEMA_NAME, 0)
        if (value != null) {
          writer.startField(CatalystConverter.MAP_VALUE_SCHEMA_NAME, 1)
          writeValue(schema.valueType, value)
          writer.endField(CatalystConverter.MAP_VALUE_SCHEMA_NAME, 1)
        }
        writer.endGroup()
      })
      writer.endField(CatalystConverter.MAP_SCHEMA_NAME, 0)
    }
    writer.endGroup()
  }

  // Scratch array used to write decimals as fixed-length byte array
  private[this] var reusableDecimalBytes = new Array[Byte](16)

  private[parquet] def writeDecimal(decimal: Decimal, precision: Int): Unit = {
    val numBytes = CatalystSchemaConverter.minBytesForPrecision(precision)

    def longToBinary(unscaled: Long): Binary = {
      var i = 0
      var shift = 8 * (numBytes - 1)
      while (i < numBytes) {
        reusableDecimalBytes(i) = (unscaled >> shift).toByte
        i += 1
        shift -= 8
      }
      Binary.fromByteArray(reusableDecimalBytes, 0, numBytes)
    }

    def bigIntegerToBinary(unscaled: BigInteger): Binary = {
      unscaled.toByteArray match {
        case bytes if bytes.length == numBytes =>
          Binary.fromByteArray(bytes)

        case bytes if bytes.length <= reusableDecimalBytes.length =>
          val signedByte = (if (bytes.head < 0) -1 else 0).toByte
          java.util.Arrays.fill(reusableDecimalBytes, 0, numBytes - bytes.length, signedByte)
          System.arraycopy(bytes, 0, reusableDecimalBytes, numBytes - bytes.length, bytes.length)
          Binary.fromByteArray(reusableDecimalBytes, 0, numBytes)

        case bytes =>
          reusableDecimalBytes = new Array[Byte](bytes.length)
          bigIntegerToBinary(unscaled)
      }
    }

    val binary = if (numBytes <= 8) {
      longToBinary(decimal.toUnscaledLong)
    } else {
      bigIntegerToBinary(decimal.toJavaBigDecimal.unscaledValue())
    }

    writer.addBinary(binary)
  }

  // array used to write Timestamp as Int96 (fixed-length binary)
  private[this] val int96buf = new Array[Byte](12)

  private[parquet] def writeTimestamp(ts: Long): Unit = {
    val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(ts)
    val buf = ByteBuffer.wrap(int96buf)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putLong(timeOfDayNanos)
    buf.putInt(julianDay)
    writer.addBinary(Binary.fromByteArray(int96buf))
  }
}

// Optimized for non-nested rows
private[parquet] class MutableRowWriteSupport extends RowWriteSupport {
  override def write(record: InternalRow): Unit = {
    val attributesSize = attributes.size
    if (attributesSize > record.numFields) {
      throw new IndexOutOfBoundsException("Trying to write more fields than contained in row " +
        s"($attributesSize > ${record.numFields})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributesSize) {
      // null values indicate optional fields but we do not check currently
      if (!record.isNullAt(index) && !record.isNullAt(index)) {
        writer.startField(attributes(index).name, index)
        consumeType(attributes(index).dataType, record, index)
        writer.endField(attributes(index).name, index)
      }
      index = index + 1
    }
    writer.endMessage()
  }

  private def consumeType(
      ctype: DataType,
      record: InternalRow,
      index: Int): Unit = {
    ctype match {
      case BooleanType => writer.addBoolean(record.getBoolean(index))
      case ByteType => writer.addInteger(record.getByte(index))
      case ShortType => writer.addInteger(record.getShort(index))
      case IntegerType | DateType => writer.addInteger(record.getInt(index))
      case LongType => writer.addLong(record.getLong(index))
      case TimestampType => writeTimestamp(record.getLong(index))
      case FloatType => writer.addFloat(record.getFloat(index))
      case DoubleType => writer.addDouble(record.getDouble(index))
      case StringType =>
        writer.addBinary(Binary.fromByteArray(record.getUTF8String(index).getBytes))
      case BinaryType =>
        writer.addBinary(Binary.fromByteArray(record.getBinary(index)))
      case DecimalType.Fixed(precision, scale) =>
        writeDecimal(record.getDecimal(index, precision, scale), precision)
      case _ => sys.error(s"Unsupported datatype $ctype, cannot write to consumer")
    }
  }
}

private[parquet] object RowWriteSupport {
  val SPARK_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.attributes"

  def getSchema(configuration: Configuration): Seq[Attribute] = {
    val schemaString = configuration.get(RowWriteSupport.SPARK_ROW_SCHEMA)
    if (schemaString == null) {
      throw new RuntimeException("Missing schema!")
    }
    ParquetTypesConverter.convertFromString(schemaString)
  }

  def setSchema(schema: Seq[Attribute], configuration: Configuration) {
    val encoded = ParquetTypesConverter.convertToString(schema)
    configuration.set(SPARK_ROW_SCHEMA, encoded)
    configuration.set(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }
}
