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

import org.apache.hadoop.conf.Configuration

import parquet.column.ParquetProperties
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{ReadSupport, WriteSupport}
import parquet.io.api._
import parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.catalyst.types._

/**
 * A `parquet.io.api.RecordMaterializer` for Rows.
 *
 *@param root The root group converter for the record.
 */
private[parquet] class RowRecordMaterializer(root: CatalystConverter)
  extends RecordMaterializer[Row] {

  def this(parquetSchema: MessageType) =
    this(CatalystConverter.createRootConverter(parquetSchema))

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
    log.debug(s"preparing for read with file schema $fileSchema")
    new RowRecordMaterializer(readContext.getRequestedSchema)
  }

  override def init(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType): ReadContext = {
    val requested_schema_string =
      configuration.get(RowReadSupport.PARQUET_ROW_REQUESTED_SCHEMA, fileSchema.toString)
    val requested_schema =
      MessageTypeParser.parseMessageType(requested_schema_string)
    log.debug(s"read support initialized for requested schema $requested_schema")
    ParquetRelation.enableLogForwarding()
    new ReadContext(requested_schema, keyValueMetaData)
  }
}

private[parquet] object RowReadSupport {
  val PARQUET_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"
}

/**
 * A `parquet.hadoop.api.WriteSupport` for Row ojects.
 */
private[parquet] class RowWriteSupport extends WriteSupport[Row] with Logging {


  def setSchema(schema: Seq[Attribute], configuration: Configuration) {
    configuration.set(
      RowWriteSupport.PARQUET_ROW_SCHEMA,
      StructType.fromAttributes(schema).toString)
    configuration.set(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }

  private[parquet] var writer: RecordConsumer = null
  private[parquet] var attributes: Seq[Attribute] = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    attributes = DataType(configuration.get(RowWriteSupport.PARQUET_ROW_SCHEMA)) match {
      case s: StructType => s.toAttributes
      case other => sys.error(s"Can convert $attributes to row")
    }
    log.debug(s"write support initialized for requested schema $attributes")
    ParquetRelation.enableLogForwarding()
    new WriteSupport.WriteContext(
      ParquetTypesConverter.convertFromAttributes(attributes),
      new java.util.HashMap[java.lang.String, java.lang.String]())
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    writer = recordConsumer
    log.debug(s"preparing for write with schema $attributes")
  }

  override def write(record: Row): Unit = {
    if (attributes.size > record.size) {
      throw new IndexOutOfBoundsException(
        s"Trying to write more fields than contained in row (${attributes.size}>${record.size})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributes.size) {
      // null values indicate optional fields but we do not check currently
      if (record(index) != null && record(index) != Nil) {
        writer.startField(attributes(index).name, index)
        writeValue(attributes(index).dataType, record(index))
        writer.endField(attributes(index).name, index)
      }
      index = index + 1
    }
    writer.endMessage()
  }

  private[parquet] def writeValue(schema: DataType, value: Any): Unit = {
    if (value != null && value != Nil) {
      schema match {
        case t @ ArrayType(_) => writeArray(
          t,
          value.asInstanceOf[CatalystConverter.ArrayScalaType[_]])
        case t @ MapType(_, _) => writeMap(
          t,
          value.asInstanceOf[CatalystConverter.MapScalaType[_, _]])
        case t @ StructType(_) => writeStruct(
          t,
          value.asInstanceOf[CatalystConverter.StructScalaType[_]])
        case _ => writePrimitive(schema.asInstanceOf[PrimitiveType], value)
      }
    }
  }

  private[parquet] def writePrimitive(schema: PrimitiveType, value: Any): Unit = {
    if (value != null && value != Nil) {
      schema match {
        case StringType => writer.addBinary(
          Binary.fromByteArray(
            value.asInstanceOf[String].getBytes("utf-8")
          )
        )
        case IntegerType => writer.addInteger(value.asInstanceOf[Int])
        case LongType => writer.addLong(value.asInstanceOf[Long])
        case DoubleType => writer.addDouble(value.asInstanceOf[Double])
        case FloatType => writer.addFloat(value.asInstanceOf[Float])
        case BooleanType => writer.addBoolean(value.asInstanceOf[Boolean])
        case _ => sys.error(s"Do not know how to writer $schema to consumer")
      }
    }
  }

  private[parquet] def writeStruct(
      schema: StructType,
      struct: CatalystConverter.StructScalaType[_]): Unit = {
    if (struct != null && struct != Nil) {
      val fields = schema.fields.toArray
      writer.startGroup()
      var i = 0
      while(i < fields.size) {
        if (struct(i) != null && struct(i) != Nil) {
          writer.startField(fields(i).name, i)
          writeValue(fields(i).dataType, struct(i))
          writer.endField(fields(i).name, i)
        }
        i = i + 1
      }
      writer.endGroup()
    }
  }

  // TODO: support null values, see
  // https://issues.apache.org/jira/browse/SPARK-1649
  private[parquet] def writeArray(
      schema: ArrayType,
      array: CatalystConverter.ArrayScalaType[_]): Unit = {
    val elementType = schema.elementType
    writer.startGroup()
    if (array.size > 0) {
      writer.startField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
      var i = 0
      while(i < array.size) {
        writeValue(elementType, array(i))
        i = i + 1
      }
      writer.endField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
    }
    writer.endGroup()
  }

  // TODO: support null values, see
  // https://issues.apache.org/jira/browse/SPARK-1649
  private[parquet] def writeMap(
      schema: MapType,
      map: CatalystConverter.MapScalaType[_, _]): Unit = {
    writer.startGroup()
    if (map.size > 0) {
      writer.startField(CatalystConverter.MAP_SCHEMA_NAME, 0)
      writer.startGroup()
      writer.startField(CatalystConverter.MAP_KEY_SCHEMA_NAME, 0)
      for(key <- map.keys) {
        writeValue(schema.keyType, key)
      }
      writer.endField(CatalystConverter.MAP_KEY_SCHEMA_NAME, 0)
      writer.startField(CatalystConverter.MAP_VALUE_SCHEMA_NAME, 1)
      for(value <- map.values) {
        writeValue(schema.valueType, value)
      }
      writer.endField(CatalystConverter.MAP_VALUE_SCHEMA_NAME, 1)
      writer.endGroup()
      writer.endField(CatalystConverter.MAP_SCHEMA_NAME, 0)
    }
    writer.endGroup()
  }
}

// Optimized for non-nested rows
private[parquet] class MutableRowWriteSupport extends RowWriteSupport {
  override def write(record: Row): Unit = {
    if (attributes.size > record.size) {
      throw new IndexOutOfBoundsException(
        s"Trying to write more fields than contained in row (${attributes.size}>${record.size})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributes.size) {
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
        Binary.fromByteArray(
          record(index).asInstanceOf[String].getBytes("utf-8")
        )
      )
      case IntegerType => writer.addInteger(record.getInt(index))
      case LongType => writer.addLong(record.getLong(index))
      case DoubleType => writer.addDouble(record.getDouble(index))
      case FloatType => writer.addFloat(record.getFloat(index))
      case BooleanType => writer.addBoolean(record.getBoolean(index))
      case _ => sys.error(s"Unsupported datatype $ctype, cannot write to consumer")
    }
  }
}

private[parquet] object RowWriteSupport {
  val PARQUET_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.schema"
}

