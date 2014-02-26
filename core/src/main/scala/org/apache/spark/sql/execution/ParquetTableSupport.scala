package org.apache.spark.sql.execution

import org.apache.hadoop.conf.Configuration

import org.apache.spark.Logging

import parquet.io.api._
import parquet.schema.{MessageTypeParser, MessageType}
import parquet.hadoop.api.{WriteSupport, ReadSupport}
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.ParquetOutputFormat
import parquet.column.ParquetProperties

import org.apache.spark.sql.catalyst.expressions.{Row, GenericRow, Attribute}
import org.apache.spark.sql.catalyst.types._

/**
 * A [[parquet.io.api.RecordMaterializer]] for Rows.
 *
 * @param root The root group converter for the record.
 */
class RowRecordMaterializer(root: CatalystGroupConverter) extends RecordMaterializer[Row] {

  def this(parquetSchema: MessageType) = this(new CatalystGroupConverter(ParquetTypesConverter.convertToAttributes(parquetSchema)))

  override def getCurrentRecord: Row = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root
}

/**
 * A [[parquet.hadoop.api.ReadSupport]] for Row objects.
 */
class RowReadSupport extends ReadSupport[Row] with Logging {

  override def prepareForRead(
                               conf: Configuration,
                               stringMap: java.util.Map[String, String],
                               fileSchema: MessageType,
                               readContext: ReadContext): RecordMaterializer[Row] = {
    log.debug(s"preparing for read with schema ${fileSchema.toString}")
    new RowRecordMaterializer(readContext.getRequestedSchema)
  }

  override def init(
                     configuration: Configuration,
                     keyValueMetaData: java.util.Map[String, String],
                     fileSchema: MessageType): ReadContext = {
    val requested_schema_string = configuration.get(RowReadSupport.PARQUET_ROW_REQUESTED_SCHEMA, fileSchema.toString)
    val requested_schema = MessageTypeParser.parseMessageType(requested_schema_string)
    log.debug(s"read support initialized for original schema ${requested_schema.toString}")
    new ReadContext(requested_schema, keyValueMetaData)
  }
}

object RowReadSupport {
  val PARQUET_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"
}

/**
 * A [[parquet.hadoop.api.WriteSupport]] for Row ojects.
 */
class RowWriteSupport extends WriteSupport[Row] with Logging {
  def setSchema(schema: MessageType, configuration: Configuration) {
    // for testing
    this.schema = schema
    // TODO: could use Attributes themselves instead of Parquet schema?
    configuration.set(RowWriteSupport.PARQUET_ROW_SCHEMA, schema.toString)
    configuration.set(ParquetOutputFormat.WRITER_VERSION, ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }

  def getSchema(configuration: Configuration): MessageType = {
    return MessageTypeParser.parseMessageType(configuration.get(RowWriteSupport.PARQUET_ROW_SCHEMA))
  }

  private var schema: MessageType = null
  private var writer: RecordConsumer = null
  private var attributes: Seq[Attribute] = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    schema = if(schema == null)
      getSchema(configuration)
    else
      schema
    attributes = ParquetTypesConverter.convertToAttributes(schema)
    new WriteSupport.WriteContext(schema, new java.util.HashMap[java.lang.String, java.lang.String]());
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    writer = recordConsumer
  }

  // TODO: add groups (nested fields)
  override def write(record: Row): Unit = {
    writer.startMessage()
    attributes.zipWithIndex.foreach {
      case (attribute, index) => {
        if(record(index) != null && record(index) != Nil) { // null values indicate optional fields but we do not check currently
          writer.startField(attribute.name, index)
          ParquetTypesConverter.consumeType(writer, attribute.dataType, record, index)
          writer.endField(attribute.name, index)
        }
      }
    }
    writer.endMessage()
  }
}

object RowWriteSupport {
  val PARQUET_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.schema"
}

/**
 * A [[parquet.io.api.GroupConverter]] that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object.
 *
 * @param schema The corresponding Shark schema in the form of a list of attributes.
 */
class CatalystGroupConverter(schema: Seq[Attribute]) extends GroupConverter {
  var current: GenericRow = new GenericRow(Seq())
  // initialization may not strictly be required
  val currentData: Array[Any] = new Array[Any](schema.length)

  val converters: Array[Converter] = schema.map {
    a => a.dataType match {
      case ctype: NativeType =>
        // note: for some reason matching for StringType fails so use this ugly if instead
        if (ctype == StringType) new CatalystPrimitiveStringConverter(this, schema.indexOf(a))
        else new CatalystPrimitiveConverter(this, schema.indexOf(a))
      case _ => throw new RuntimeException("unable to convert datatype in CatalystGroupConverter")
    }
  }.toArray

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def getCurrentRecord: GenericRow = current

  override def start(): Unit = {
    for (i <- 0 until schema.length) {
      currentData.update(i, Nil)
    }
  }

  override def end(): Unit = {
    current = new GenericRow(currentData)
  }
}

/**
 * A [[parquet.io.api.PrimitiveConverter]] that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
class CatalystPrimitiveConverter(parent: CatalystGroupConverter, fieldIndex: Int) extends PrimitiveConverter {
  // TODO: consider refactoring these together with ParquetTypesConverter
  override def addBinary(value: Binary): Unit = parent.currentData.update(fieldIndex, value.getBytes.asInstanceOf[BinaryType.JvmType])

  override def addBoolean(value: Boolean): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[BooleanType.JvmType])

  override def addDouble(value: Double): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[DoubleType.JvmType])

  override def addFloat(value: Float): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[FloatType.JvmType])

  override def addInt(value: Int): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[IntegerType.JvmType])

  override def addLong(value: Long): Unit = parent.currentData.update(fieldIndex, value.asInstanceOf[LongType.JvmType])
}

/**
 * A [[parquet.io.api.PrimitiveConverter]] that converts Parquet strings (fixed-length byte arrays) into Catalyst Strings.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
class CatalystPrimitiveStringConverter(parent: CatalystGroupConverter, fieldIndex: Int) extends CatalystPrimitiveConverter(parent, fieldIndex) {
  override def addBinary(value: Binary): Unit = parent.currentData.update(fieldIndex, value.toStringUsingUTF8.asInstanceOf[StringType.JvmType])
}
