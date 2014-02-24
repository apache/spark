package org.apache.spark.sql

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.plans.logical.BaseRelation
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.ArrayType
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row, AttributeReference, Attribute}

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import parquet.schema.{PrimitiveType => ParquetPrimitiveType}
import parquet.schema.{Type => ParquetType}

import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.Type.Repetition
import parquet.hadoop.{ParquetWriter, ParquetFileReader}
import parquet.hadoop.metadata.ParquetMetadata

import scala.collection.JavaConversions._
import java.io.File

/**
 * Relation formed by underlying Parquet file that contains data stored in columnar form.
 *
 * @param tableName The name of the relation.
 * @param path The path to the Parquet file.
 */
case class ParquetRelation(val tableName: String, val path: String)

  extends BaseRelation {

  private def parquetMetaData: ParquetMetadata = readMetaData

  /** Schema derived from ParquetFile **/
  def parquetSchema: MessageType = parquetMetaData.getFileMetaData.getSchema

  /** Attributes **/
  val attributes = ParquetTypesConverter.convertToAttributes(parquetSchema)

  /** Output **/
  val output = attributes

  private def readMetaData: ParquetMetadata = ParquetFileReader.readFooter(new Configuration(), new Path(path))

  override def isPartitioned = false // Parquet files have no concepts of keys, therefore no Partitioner
  // Note: we could allow Block level access; needs to be thought through
}

object ParquetRelation {
  def apply(path: Path, tableName: String = "ParquetTable") =
    new ParquetRelation(tableName, path.toUri.toString)
}

object ParquetTypesConverter {
  def toDataType(parquetType : ParquetPrimitiveTypeName): DataType = parquetType match {
    // for now map binary to string type
    // TODO: figure out how Parquet uses strings or why we can't use them in a MessageType schema
    case ParquetPrimitiveTypeName.BINARY => StringType
    case ParquetPrimitiveTypeName.BOOLEAN => BooleanType
    case ParquetPrimitiveTypeName.DOUBLE => DoubleType
    case ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => ArrayType(ByteType)
    case ParquetPrimitiveTypeName.FLOAT => FloatType
    case ParquetPrimitiveTypeName.INT32 => IntegerType
    case ParquetPrimitiveTypeName.INT64 => LongType
    case ParquetPrimitiveTypeName.INT96 => LongType // TODO: is there an equivalent?
    case _ => sys.error(s"Unsupported parquet datatype")
  }

  def fromDataType(ctype: DataType): ParquetPrimitiveTypeName = ctype match {
    case StringType => ParquetPrimitiveTypeName.BINARY
    case BooleanType => ParquetPrimitiveTypeName.BOOLEAN
    case DoubleType => ParquetPrimitiveTypeName.DOUBLE
    case ArrayType(ByteType) => ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
    case FloatType => ParquetPrimitiveTypeName.FLOAT
    case IntegerType => ParquetPrimitiveTypeName.INT32
    case LongType => ParquetPrimitiveTypeName.INT64
    case _ => sys.error(s"Unsupported datatype")
  }

  def consumeType(consumer: RecordConsumer, ctype: DataType, record: Row, index: Int): Unit = {
    ctype match {
      case StringType => consumer.addBinary(
        Binary.fromByteArray(
          record(index).asInstanceOf[String].getBytes("utf-8")
        )
      )
      case IntegerType => consumer.addInteger(record.getInt(index))
      case LongType => consumer.addLong(record.getLong(index))
      case DoubleType => consumer.addDouble(record.getDouble(index))
      case FloatType => consumer.addFloat(record.getFloat(index))
      case BooleanType => consumer.addBoolean(record.getBoolean(index))
      case _ => sys.error(s"Unsupported datatype, cannot write to consumer")
    }
  }

  def getSchema(schemaString : String) : MessageType = MessageTypeParser.parseMessageType(schemaString)

  def convertToAttributes(parquetSchema: MessageType) : Seq[Attribute] = {
    parquetSchema.getColumns.map {
      case (desc) => {
        val ctype = toDataType(desc.getType)
        val name: String = desc.getPath.mkString(".")
        new AttributeReference(name, ctype, false)()
      }
    }
  }

  // TODO: allow nesting?
  def convertFromAttributes(attributes: Seq[Attribute]): MessageType = {
    val fields: Seq[ParquetType] = attributes.map {
      a => new ParquetPrimitiveType(Repetition.OPTIONAL, fromDataType(a.dataType), a.name)
    }
    new MessageType("root", fields)
  }
}

object ParquetTestData {

  val testSchema =
    """message myrecord {
      |optional boolean myboolean;
      |optional int32 myint;
      |optional binary mystring;
      |optional int64 mylong;
      |optional float myfloat;
      |optional double mydouble;
      |}""".stripMargin

  val subTestSchema =
    """
      |message myrecord {
      |optional boolean myboolean;
      |optional int64 mylong;
      |}
    """.stripMargin

  val testFile = new File("/tmp/testParquetFile").getAbsoluteFile

  lazy val testData = ParquetRelation(new Path(testFile.toURI))

  def writeFile = {
    testFile.delete
    val path: Path = new Path(testFile.toURI)
    val configuration: Configuration = new Configuration
    val schema: MessageType = MessageTypeParser.parseMessageType(testSchema)

    val writeSupport = new RowWriteSupport()
    writeSupport.setSchema(schema, configuration)
    val writer = new ParquetWriter(path, writeSupport)
    for(i <- 0 until 15) {
      val data = new Array[Any](6)
      if(i % 3 ==0)
        data.update(0, true)
      else
        data.update(0, false)
      if(i % 5 == 0)
        data.update(1, 5)
      else
        data.update(1, null) // optional
      data.update(2, "abc")
      data.update(3, 1L<<33)
      data.update(4, 2.5F)
      data.update(5, 4.5D)
      writer.write(new GenericRow(data))
    }
    writer.close()
  }
}