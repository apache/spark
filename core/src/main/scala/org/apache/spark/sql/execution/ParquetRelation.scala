package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, InsertIntoCreatedTable, BaseRelation}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.ArrayType
import org.apache.spark.sql.catalyst.expressions.{Row, GenericRow, AttributeReference, Attribute}

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import parquet.schema.{PrimitiveType => ParquetPrimitiveType}
import parquet.schema.{Type => ParquetType}

import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.Type.Repetition
import parquet.hadoop.{Footer, ParquetFileWriter, ParquetWriter, ParquetFileReader}
import parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}

import scala.collection.JavaConversions._
import java.io.{IOException, FileNotFoundException, File}
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.util.ContextUtil
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog

/**
 * Relation formed by underlying Parquet file that contains data stored in columnar form.
 *
 * @param tableName The name of the relation.
 * @param path The path to the Parquet file.
 */
case class ParquetRelation(val tableName: String, val path: String)

  extends BaseRelation {

  /** Schema derived from ParquetFile **/
  def parquetSchema: MessageType =
    ParquetTypesConverter
      .readMetaData(new Path(path))
      .getFileMetaData
      .getSchema

  /** Attributes **/
  val attributes = ParquetTypesConverter
    .convertToAttributes(parquetSchema)

  /** Output **/
  override val output = attributes

  override def isPartitioned = false // Parquet files have no concepts of keys, therefore no Partitioner
  // Note: we could allow Block level access; needs to be thought through
}

object ParquetRelation {
  def apply(path: Path, tableName: String = "ParquetTable") =
    new ParquetRelation(tableName, path.toUri.toString)
}

object RegisterParquetTables extends Rule[LogicalPlan] {
  var catalog: OverrideCatalog = null

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ InsertIntoTable(relation: ParquetRelation, _, _, _) => {
      catalog.overrideTable(Some("parquet"), relation.tableName, relation)
      p
    }
  }
}

/**
 * Creates any tables required for query execution.
 * For example, because of a CREATE TABLE X AS statement.
 */
object CreateParquetTable extends Rule[LogicalPlan] {
  var catalog: OverrideCatalog = null

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case InsertIntoCreatedTable(db, tableName, child) => {
      if(catalog == null)
        throw new NullPointerException("Catalog was not set inside CreateParquetTable")

      val databaseName = db.getOrElse("parquet") // see TODO in [[CreateTables]]
      val job = new Job()
      val conf = ContextUtil.getConfiguration(job)
      val prefix = "tmp"

      val uri = FileSystem.getDefaultUri(conf)
      val path = new Path(
        new Path(uri),
          new Path(
            new Path(prefix),
            new Path(
              new Path(databaseName),
              new Path(
                new Path(tableName),
                new Path("data")))))
      // TODO: add checking: directory exists, etc

      ParquetTypesConverter.writeMetaData(child.output, path)
      val relation = new ParquetRelation(tableName, path.toString)

      catalog.overrideTable(Some("parquet"), tableName, relation)

      InsertIntoTable(
        relation.asInstanceOf[BaseRelation],
        Map.empty,
        child,
        overwrite = false)
    }
  }
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

  // todo: proper exception handling, warning if path exists
  def writeMetaData(attributes: Seq[Attribute], path: Path) {
    val job = new Job()
    val conf = ContextUtil.getConfiguration(job)
    val fileSystem = FileSystem.get(conf)

    if(fileSystem.exists(path) && !fileSystem.getFileStatus(path).isDir)
      throw new IOException(s"Expected to write to directory ${path.toString} but found file")

    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)

    if(fileSystem.exists(metadataPath))
      fileSystem.delete(metadataPath, true)

    val extraMetadata = new java.util.HashMap[String, String]()
    extraMetadata.put("path", path.toString)
    // TODO: add table name, etc.?

    val parquetSchema: MessageType = ParquetTypesConverter.convertFromAttributes(attributes)
    val metaData: FileMetaData = new FileMetaData(
      parquetSchema,
      new java.util.HashMap[String, String](),
      "Shark")

    ParquetFileWriter.writeMetadataFile(
      conf,
      path,
      new Footer(
        path,
        new ParquetMetadata(
          metaData,
          Nil)
      ) :: Nil)
  }

  /**
   * Try to read Parquet metadata at the given Path. We first see if there is a summary file
   * in the parent directory. If so, this is used. Else we read the actual footer at the given
   * location.
   * @param path The path at which we expect one (or more) Parquet files.
   * @return The [[ParquetMetadata]] containing among other things the schema.
   */
  def readMetaData(path: Path): ParquetMetadata = {
    val job = new Job()
    val conf = ContextUtil.getConfiguration(job)
    val fs: FileSystem = path.getFileSystem(conf)

    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)

    if(fs.exists(metadataPath) && fs.isFile(metadataPath))
      // TODO: improve exception handling, etc.
      ParquetFileReader.readFooter(conf, metadataPath)
    else {
      if(!fs.exists(path) || !fs.isFile(path))
        throw new FileNotFoundException(s"Could not find file ${path.toString} when trying to read metadata")

      ParquetFileReader.readFooter(conf, path)
    }
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
    val job = new Job()
    val configuration: Configuration = ContextUtil.getConfiguration(job)
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
      writer.write(new GenericRow(data.toSeq))
    }
    writer.close()
  }
}