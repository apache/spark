package org.apache.spark.sql

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.plans.logical.BaseRelation
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.ArrayType
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}

import scala.collection.JavaConversions._

/**
 * Relation formed by underlying Parquet file that contains data stored in columnar form.
 *
 * @param parquetSchema The requested schema (may contain projections w.r.t. Parquet file).
 * @param path The path to the Parquet file.
 */
case class ParquetRelation(val tableName: String, val parquetSchema: MessageType, val path: Path)

  extends BaseRelation {

  /** Attributes */
  val attributes = ParquetTypesConverter.convertToAttributes(parquetSchema)

  val output = attributes

  /* TODO: implement low-level Parquet metadata store access */
  // val metaData: ParquetMetadata
  // def getBlocks: java.util.List[BlockMetaData] = metaData.getBlocks
  // def getColumns: java.util.List[ColumnDescriptor] = metaData.getFileMetaData.getSchema.getColumns

  val numberOfBlocks = 1 // TODO: see comment above
}

object ParquetRelation {
  def apply(schemaString : String, path: Path, tableName: String = "ParquetTable") =
    new ParquetRelation(tableName, ParquetTypesConverter.getSchema(schemaString), path)
}

object ParquetTypesConverter {
  def toDataType(parquetType : ParquetPrimitiveTypeName) : DataType = parquetType match {
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

  def getSchema(schemaString : String) : MessageType = MessageTypeParser.parseMessageType(schemaString)

  def convertToAttributes(parquetSchema: MessageType) : List[Attribute] = {
    parquetSchema.getColumns.map {
      case (desc) => {
        val ctype = toDataType(desc.getType)
        val name = desc.getPath.toString
        new AttributeReference(name, ctype, false)()
      }
    }.toList
  }
}
