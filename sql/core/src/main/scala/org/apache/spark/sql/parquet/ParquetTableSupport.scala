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
private[parquet] class RowRecordMaterializer(root: CatalystGroupConverter)
  extends RecordMaterializer[Row] {

  def this(parquetSchema: MessageType) =
    this(new CatalystGroupConverter(ParquetTypesConverter.convertToAttributes(parquetSchema)))

  override def getCurrentRecord: Row = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root
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
  def setSchema(schema: MessageType, configuration: Configuration) {
    // for testing
    this.schema = schema
    // TODO: could use Attributes themselves instead of Parquet schema?
    configuration.set(
      RowWriteSupport.PARQUET_ROW_SCHEMA,
      schema.toString)
    configuration.set(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }

  def getSchema(configuration: Configuration): MessageType = {
    MessageTypeParser.parseMessageType(configuration.get(RowWriteSupport.PARQUET_ROW_SCHEMA))
  }

  private var schema: MessageType = null
  private var writer: RecordConsumer = null
  private var attributes: Seq[Attribute] = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    schema = if (schema == null) getSchema(configuration) else schema
    attributes = ParquetTypesConverter.convertToAttributes(schema)
    log.debug(s"write support initialized for requested schema $schema")
    ParquetRelation.enableLogForwarding()
    new WriteSupport.WriteContext(
      schema,
      new java.util.HashMap[java.lang.String, java.lang.String]())
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    writer = recordConsumer
    log.debug(s"preparing for write with schema $schema")
  }

  // TODO: add groups (nested fields)
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
        ParquetTypesConverter.consumeType(writer, attributes(index).dataType, record, index)
        writer.endField(attributes(index).name, index)
      }
      index = index + 1
    }
    writer.endMessage()
  }
}

private[parquet] object RowWriteSupport {
  val PARQUET_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.schema"
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record to a `Row` object.
 *
 * @param schema The corresponding Catalyst schema in the form of a list of attributes.
 */
private[parquet] class CatalystGroupConverter(
    schema: Seq[Attribute],
    protected[parquet] val current: ParquetRelation.RowType) extends GroupConverter {

  def this(schema: Seq[Attribute]) = this(schema, new ParquetRelation.RowType(schema.length))

  val converters: Array[Converter] = schema.map {
    a => a.dataType match {
      case ctype: NativeType =>
        // note: for some reason matching for StringType fails so use this ugly if instead
        if (ctype == StringType) {
          new CatalystPrimitiveStringConverter(this, schema.indexOf(a))
        } else {
          new CatalystPrimitiveConverter(this, schema.indexOf(a))
        }
      case _ => throw new RuntimeException(
        s"unable to convert datatype ${a.dataType.toString} in CatalystGroupConverter")
    }
  }.toArray

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  private[parquet] def getCurrentRecord: ParquetRelation.RowType = current

  override def start(): Unit = {
    var i = 0
    while (i < schema.length) {
      current.setNullAt(i)
      i = i + 1
    }
  }

  override def end(): Unit = {}
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveConverter(
    parent: CatalystGroupConverter,
    fieldIndex: Int) extends PrimitiveConverter {
  // TODO: consider refactoring these together with ParquetTypesConverter
  override def addBinary(value: Binary): Unit =
    parent.getCurrentRecord.update(fieldIndex, value.getBytes)

  override def addBoolean(value: Boolean): Unit =
    parent.getCurrentRecord.setBoolean(fieldIndex, value)

  override def addDouble(value: Double): Unit =
    parent.getCurrentRecord.setDouble(fieldIndex, value)

  override def addFloat(value: Float): Unit =
    parent.getCurrentRecord.setFloat(fieldIndex, value)

  override def addInt(value: Int): Unit =
    parent.getCurrentRecord.setInt(fieldIndex, value)

  override def addLong(value: Long): Unit =
    parent.getCurrentRecord.setLong(fieldIndex, value)
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet strings (fixed-length byte arrays)
 * into Catalyst Strings.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveStringConverter(
    parent: CatalystGroupConverter,
    fieldIndex: Int) extends CatalystPrimitiveConverter(parent, fieldIndex) {
  override def addBinary(value: Binary): Unit =
    parent.getCurrentRecord.setString(fieldIndex, value.toStringUsingUTF8)
}
