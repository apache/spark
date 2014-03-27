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
import org.apache.spark.sql.parquet.ParquetRelation.RowType
import org.apache.spark.sql.parquet.CatalystConverter.FieldType

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

private[parquet] object CatalystConverter {
  type FieldType = StructField

  protected[parquet] def createConverter(field: FieldType, fieldIndex: Int, parent: CatalystConverter): Converter = {
    val fieldType: DataType = field.dataType
    fieldType match {
      case ArrayType(elementType: DataType) => {
        val numberOfFields = 10 // TODO: where do we get this info from?
        val subArray = new ParquetRelation.RowType(numberOfFields)
        parent.updateField(fieldIndex, subArray)
        new CatalystArrayConverter(elementType, fieldIndex, parent, subArray)
      }
      case StructType(fields: Seq[StructField]) => {
        val subArray = new ParquetRelation.RowType(fields.size)
        parent.updateField(fieldIndex, subArray)
        new CatalystGroupConverter(fields, fieldIndex, parent, subArray) // TODO: check if field type changes
      }
      case ctype: NativeType =>
        // note: for some reason matching for StringType fails so use this ugly if instead
        if (ctype == StringType) {
          new CatalystPrimitiveStringConverter(parent, fieldIndex)
        } else {
          new CatalystPrimitiveConverter(parent, fieldIndex)
        }
      case _ => throw new RuntimeException(
        s"unable to convert datatype ${field.dataType.toString} in CatalystGroupConverter")
    }
  }
}

trait CatalystConverter {

  // the number of fields this group has
  val size: Int

  // the index of this converter in the parent
  protected[parquet] val index: Int

  // the data (possibly nested)
  protected[parquet] val current: ParquetRelation.RowType

  // the parent converter
  protected[parquet] val parent: CatalystConverter

  // should be only called in root group converter!
  def getCurrentRecord: ParquetRelation.RowType = current

  // for child converters to update upstream values
  protected [parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    current.update(fieldIndex, value)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit = {
    current.setBoolean(fieldIndex, value)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateInt(fieldIndex: Int, value: Int): Unit = {
    current.setInt(fieldIndex, value)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateLong(fieldIndex: Int, value: Long): Unit = {
    current.setLong(fieldIndex, value)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateDouble(fieldIndex: Int, value: Double): Unit = {
    current.setDouble(fieldIndex, value)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateFloat(fieldIndex: Int, value: Float): Unit = {
    current.setFloat(fieldIndex, value)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit = {
    current.update(fieldIndex, value.getBytes)
    if (parent != null) parent.updateField(index, current)
  }

  protected [parquet] def updateString(fieldIndex: Int, value: Binary): Unit = {
    current.setString(fieldIndex, value.toStringUsingUTF8)
    if (parent != null) parent.updateField(index, current)
  }

  def start(): Unit = {
    var i = 0
    while (i < size) {
      current.setNullAt(i)
      i = i + 1
    }
  }

  def end(): Unit = {}
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object.
 *
 * @param schema The corresponding Catalyst schema in the form of a list of attributes.
 */
class CatalystGroupConverter(
    private[parquet] val schema: Seq[FieldType],
    protected[parquet] val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] val current: RowType) extends GroupConverter with CatalystConverter {

  def this(schema: Seq[FieldType], index:Int, parent: CatalystConverter) =
    this(schema, index, parent, new ParquetRelation.RowType(schema.length))

  def this(attributes: Seq[Attribute]) =
    this(attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)), 0, null)

  protected [parquet] val converters: Array[Converter] =
    schema.map(field => CatalystConverter.createConverter(field, schema.indexOf(field), this)).toArray

  override val size = schema.size

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveConverter(
    parent: CatalystConverter,
    fieldIndex: Int) extends PrimitiveConverter {
  // TODO: consider refactoring these together with ParquetTypesConverter
  override def addBinary(value: Binary): Unit =
    parent.updateBinary(fieldIndex, value)

  override def addBoolean(value: Boolean): Unit =
    parent.updateBoolean(fieldIndex, value)

  override def addDouble(value: Double): Unit =
    parent.updateDouble(fieldIndex, value)

  override def addFloat(value: Float): Unit =
    parent.updateFloat(fieldIndex, value)

  override def addInt(value: Int): Unit =
    parent.updateInt(fieldIndex, value)

  override def addLong(value: Long): Unit =
    parent.updateLong(fieldIndex, value)
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet strings (fixed-length byte arrays)
 * into Catalyst Strings.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveStringConverter(
    parent: CatalystConverter,
    fieldIndex: Int) extends CatalystPrimitiveConverter(parent, fieldIndex) {
  override def addBinary(value: Binary): Unit =
    parent.updateString(fieldIndex, value)
}

class CatalystArrayConverter(
    val elementType: DataType,
    val size: Int,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] val current: ParquetRelation.RowType)
  extends GroupConverter with CatalystConverter {

  private var arrayOffset: Int = 0

  def this(elementType: DataType, index: Int, parent: CatalystConverter, current: ParquetRelation.RowType) =
    this(elementType, current.size, index, parent, current)

  def this(elementType: DataType, size: Int, index: Int, parent: CatalystConverter) =
    this(elementType, size, index, parent, new ParquetRelation.RowType(size))

  protected[parquet] val converter: Converter = CatalystConverter.createConverter(
    new CatalystConverter.FieldType("values", elementType, false), 0, this)

  override def getConverter(fieldIndex: Int): Converter = converter

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    super.updateField(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateString(fieldIndex: Int, value: Binary): Unit = {
    super.updateString(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit = {
    super.updateBoolean(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit = {
    super.updateInt(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit = {
    super.updateLong(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit = {
    super.updateDouble(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit = {
    super.updateFloat(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }

  override protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit = {
    super.updateBinary(arrayOffset + fieldIndex, value)
    arrayOffset = arrayOffset + 1
  }
}

