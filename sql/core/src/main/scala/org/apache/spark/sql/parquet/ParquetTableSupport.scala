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

package org.apache.spark.sql.parquet

import java.util.{HashMap => JHashMap}

import org.apache.hadoop.conf.Configuration
import parquet.column.ParquetProperties
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{ReadSupport, WriteSupport}
import parquet.io.api._
import parquet.schema.MessageType

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.types._

/**
 * A `parquet.io.api.RecordMaterializer` for Rows.
 *
 *@param root The root group converter for the record.
 */
private[parquet] class RowRecordMaterializer(root: CatalystConverter)
  extends RecordMaterializer[Row] {

  def this(parquetSchema: MessageType, attributes: Seq[Attribute]) =
    this(CatalystConverter.createRootConverter(parquetSchema, attributes))

  override def getCurrentRecord: Row = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root.asInstanceOf[GroupConverter]
}

/**
 * A `parquet.hadoop.api.ReadSupport` for Row objects.
 */
private[parquet] class RowReadSupport extends ReadSupport[Row] with Logging {

  override def prepareForRead(
      conf: Configuration,
      stringMap: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[Row] = {
    log.debug(s"preparing for read with Parquet file schema $fileSchema")
    // Note: this very much imitates AvroParquet
    val parquetSchema = readContext.getRequestedSchema
    var schema: Seq[Attribute] = null

    if (readContext.getReadSupportMetadata != null) {
      // first try to find the read schema inside the metadata (can result from projections)
      if (
        readContext
          .getReadSupportMetadata
          .get(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA) != null) {
        schema = ParquetTypesConverter.convertFromString(
          readContext.getReadSupportMetadata.get(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
      } else {
        // if unavailable, try the schema that was read originally from the file or provided
        // during the creation of the Parquet relation
        if (readContext.getReadSupportMetadata.get(RowReadSupport.SPARK_METADATA_KEY) != null) {
          schema = ParquetTypesConverter.convertFromString(
            readContext.getReadSupportMetadata.get(RowReadSupport.SPARK_METADATA_KEY))
        }
      }
    }
    // if both unavailable, fall back to deducing the schema from the given Parquet schema
    // TODO: Why it can be null?
    if (schema == null)  {
      log.debug("falling back to Parquet read schema")
      schema = ParquetTypesConverter.convertToAttributes(
        parquetSchema, false, true)
    }
    log.debug(s"list of attributes that will be read: $schema")
    new RowRecordMaterializer(parquetSchema, schema)
  }

  override def init(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType): ReadContext = {
    var parquetSchema = fileSchema
    val metadata = new JHashMap[String, String]()
    val requestedAttributes = RowReadSupport.getRequestedSchema(configuration)

    if (requestedAttributes != null) {
      // If the parquet file is thrift derived, there is a good chance that
      // it will have the thrift class in metadata.
      val isThriftDerived = keyValueMetaData.keySet().contains("thrift.class")
      parquetSchema = ParquetTypesConverter
        .convertFromAttributes(requestedAttributes, isThriftDerived)
      metadata.put(
        RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
        ParquetTypesConverter.convertToString(requestedAttributes))
    }

    val origAttributesStr: String = configuration.get(RowWriteSupport.SPARK_ROW_SCHEMA)
    if (origAttributesStr != null) {
      metadata.put(RowReadSupport.SPARK_METADATA_KEY, origAttributesStr)
    }

    new ReadSupport.ReadContext(parquetSchema, metadata)
  }
}

private[parquet] object RowReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"
  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  private def getRequestedSchema(configuration: Configuration): Seq[Attribute] = {
    val schemaString = configuration.get(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
    if (schemaString == null) null else ParquetTypesConverter.convertFromString(schemaString)
  }
}

/**
 * A `parquet.hadoop.api.WriteSupport` for Row ojects.
 */
private[parquet] class RowWriteSupport extends WriteSupport[Row] with Logging {

  private[parquet] var writer: RecordConsumer = null
  private[parquet] var attributes: Array[Attribute] = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    val origAttributesStr: String = configuration.get(RowWriteSupport.SPARK_ROW_SCHEMA)
    val metadata = new JHashMap[String, String]()
    metadata.put(RowReadSupport.SPARK_METADATA_KEY, origAttributesStr)

    if (attributes == null) {
      attributes = ParquetTypesConverter.convertFromString(origAttributesStr).toArray
    }

    log.debug(s"write support initialized for requested schema $attributes")
    ParquetRelation.enableLogForwarding()
    new WriteSupport.WriteContext(ParquetTypesConverter.convertFromAttributes(attributes), metadata)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    writer = recordConsumer
    log.debug(s"preparing for write with schema $attributes")
  }

  override def write(record: Row): Unit = {
    val attributesSize = attributes.size
    if (attributesSize > record.size) {
      throw new IndexOutOfBoundsException(
        s"Trying to write more fields than contained in row ($attributesSize > ${record.size})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributesSize) {
      // null values indicate optional fields but we do not check currently
      if (record(index) != null) {
        writer.startField(attributes(index).name, index)
        writeValue(attributes(index).dataType, record(index))
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
          value.asInstanceOf[CatalystConverter.ArrayScalaType[_]])
        case t @ MapType(_, _, _) => writeMap(
          t,
          value.asInstanceOf[CatalystConverter.MapScalaType[_, _]])
        case t @ StructType(_) => writeStruct(
          t,
          value.asInstanceOf[CatalystConverter.StructScalaType[_]])
        case _ => writePrimitive(schema.asInstanceOf[NativeType], value)
      }
    }
  }

  private[parquet] def writePrimitive(schema: DataType, value: Any): Unit = {
    if (value != null) {
      schema match {
        case StringType => writer.addBinary(
          Binary.fromByteArray(
            value.asInstanceOf[String].getBytes("utf-8")
          )
        )
        case BinaryType => writer.addBinary(
          Binary.fromByteArray(value.asInstanceOf[Array[Byte]]))
        case IntegerType => writer.addInteger(value.asInstanceOf[Int])
        case ShortType => writer.addInteger(value.asInstanceOf[Short])
        case LongType => writer.addLong(value.asInstanceOf[Long])
        case TimestampType => writeTimestamp(value.asInstanceOf[java.sql.Timestamp])
        case ByteType => writer.addInteger(value.asInstanceOf[Byte])
        case DoubleType => writer.addDouble(value.asInstanceOf[Double])
        case FloatType => writer.addFloat(value.asInstanceOf[Float])
        case BooleanType => writer.addBoolean(value.asInstanceOf[Boolean])
        case DateType => writer.addInteger(value.asInstanceOf[Int])
        case d: DecimalType =>
          if (d.precisionInfo == None || d.precisionInfo.get.precision > 18) {
            sys.error(s"Unsupported datatype $d, cannot write to consumer")
          }
          writeDecimal(value.asInstanceOf[Decimal], d.precisionInfo.get.precision)
        case _ => sys.error(s"Do not know how to writer $schema to consumer")
      }
    }
  }

  private[parquet] def writeStruct(
      schema: StructType,
      struct: CatalystConverter.StructScalaType[_]): Unit = {
    if (struct != null) {
      val fields = schema.fields.toArray
      writer.startGroup()
      var i = 0
      while(i < fields.size) {
        if (struct(i) != null) {
          writer.startField(fields(i).name, i)
          writeValue(fields(i).dataType, struct(i))
          writer.endField(fields(i).name, i)
        }
        i = i + 1
      }
      writer.endGroup()
    }
  }

  private[parquet] def writeArray(
      schema: ArrayType,
      array: CatalystConverter.ArrayScalaType[_]): Unit = {
    val elementType = schema.elementType
    writer.startGroup()
    if (array.size > 0) {
      if (schema.containsNull) {
        writer.startField(CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME, 0)
        var i = 0
        while (i < array.size) {
          writer.startGroup()
          if (array(i) != null) {
            writer.startField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
            writeValue(elementType, array(i))
            writer.endField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
          }
          writer.endGroup()
          i = i + 1
        }
        writer.endField(CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME, 0)
      } else {
        writer.startField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
        var i = 0
        while (i < array.size) {
          writeValue(elementType, array(i))
          i = i + 1
        }
        writer.endField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
      }
    }
    writer.endGroup()
  }

  private[parquet] def writeMap(
      schema: MapType,
      map: CatalystConverter.MapScalaType[_, _]): Unit = {
    writer.startGroup()
    if (map.size > 0) {
      writer.startField(CatalystConverter.MAP_SCHEMA_NAME, 0)
      for ((key, value) <- map) {
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
      }
      writer.endField(CatalystConverter.MAP_SCHEMA_NAME, 0)
    }
    writer.endGroup()
  }

  // Scratch array used to write decimals as fixed-length binary
  private val scratchBytes = new Array[Byte](8)

  private[parquet] def writeDecimal(decimal: Decimal, precision: Int): Unit = {
    val numBytes = ParquetTypesConverter.BYTES_FOR_PRECISION(precision)
    val unscaledLong = decimal.toUnscaledLong
    var i = 0
    var shift = 8 * (numBytes - 1)
    while (i < numBytes) {
      scratchBytes(i) = (unscaledLong >> shift).toByte
      i += 1
      shift -= 8
    }
    writer.addBinary(Binary.fromByteArray(scratchBytes, 0, numBytes))
  }

  private[parquet] def writeTimestamp(ts: java.sql.Timestamp): Unit = {
    val binaryNanoTime = CatalystTimestampConverter.convertFromTimestamp(ts)
    writer.addBinary(binaryNanoTime)
  }
}

// Optimized for non-nested rows
private[parquet] class MutableRowWriteSupport extends RowWriteSupport {
  override def write(record: Row): Unit = {
    val attributesSize = attributes.size
    if (attributesSize > record.size) {
      throw new IndexOutOfBoundsException(
        s"Trying to write more fields than contained in row ($attributesSize > ${record.size})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributesSize) {
      // null values indicate optional fields but we do not check currently
      if (record(index) != null && record(index) != Nil) {
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
      record: Row,
      index: Int): Unit = {
    ctype match {
      case StringType => writer.addBinary(
        Binary.fromByteArray(record(index).asInstanceOf[String].getBytes("utf-8")))
      case BinaryType => writer.addBinary(
        Binary.fromByteArray(record(index).asInstanceOf[Array[Byte]]))
      case IntegerType => writer.addInteger(record.getInt(index))
      case ShortType => writer.addInteger(record.getShort(index))
      case LongType => writer.addLong(record.getLong(index))
      case ByteType => writer.addInteger(record.getByte(index))
      case DoubleType => writer.addDouble(record.getDouble(index))
      case FloatType => writer.addFloat(record.getFloat(index))
      case BooleanType => writer.addBoolean(record.getBoolean(index))
      case DateType => writer.addInteger(record.getInt(index))
      case TimestampType => writeTimestamp(record(index).asInstanceOf[java.sql.Timestamp])
      case d: DecimalType =>
        if (d.precisionInfo == None || d.precisionInfo.get.precision > 18) {
          sys.error(s"Unsupported datatype $d, cannot write to consumer")
        }
        writeDecimal(record(index).asInstanceOf[Decimal], d.precisionInfo.get.precision)
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

