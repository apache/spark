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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.mapreduce.Job

import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetOutputFormat, Footer, ParquetFileWriter, ParquetFileReader}
import parquet.hadoop.metadata.{CompressionCodecName, FileMetaData, ParquetMetadata}
import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.{Type => ParquetType, PrimitiveType => ParquetPrimitiveType, MessageType, MessageTypeParser}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import parquet.schema.Type.Repetition

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LeafNode}
import org.apache.spark.sql.catalyst.types._

// Implicits
import scala.collection.JavaConversions._

/**
 * Relation that consists of data stored in a Parquet columnar format.
 *
 * Users should interact with parquet files though a SchemaRDD, created by a [[SQLContext]] instead
 * of using this class directly.
 *
 * {{{
 *   val parquetRDD = sqlContext.parquetFile("path/to/parquet.file")
 * }}}
 *
 * @param path The path to the Parquet file.
 */
private[sql] case class ParquetRelation(val path: String)
    extends LeafNode with MultiInstanceRelation {
  self: Product =>

  /** Schema derived from ParquetFile */
  def parquetSchema: MessageType =
    ParquetTypesConverter
      .readMetaData(new Path(path))
      .getFileMetaData
      .getSchema

  /** Attributes */
  override val output =
    ParquetTypesConverter
      .convertToAttributes(parquetSchema)

  override def newInstance = ParquetRelation(path).asInstanceOf[this.type]

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
  override def equals(other: Any) = other match {
    case p: ParquetRelation =>
      p.path == path && p.output == output
    case _ => false
  }
}

private[sql] object ParquetRelation {

  def enableLogForwarding() {
    // Note: Logger.getLogger("parquet") has a default logger
    // that appends to Console which needs to be cleared.
    val parquetLogger = java.util.logging.Logger.getLogger("parquet")
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
    // TODO(witgo): Need to set the log level ?
    // if(parquetLogger.getLevel != null) parquetLogger.setLevel(null)
    if (!parquetLogger.getUseParentHandlers) parquetLogger.setUseParentHandlers(true)
  }

  // The element type for the RDDs that this relation maps to.
  type RowType = org.apache.spark.sql.catalyst.expressions.GenericMutableRow

  // The compression type
  type CompressionType = parquet.hadoop.metadata.CompressionCodecName

  // The default compression
  val defaultCompression = CompressionCodecName.GZIP

  /**
   * Creates a new ParquetRelation and underlying Parquetfile for the given LogicalPlan. Note that
   * this is used inside [[org.apache.spark.sql.execution.SparkStrategies SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Parquetfile. The relation is empty
   * but is initialized with ParquetMetadata and can be inserted into.
   *
   * @param pathString The directory the Parquetfile will be stored in.
   * @param child The child node that will be used for extracting the schema.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation with inferred metadata.
   */
  def create(pathString: String,
             child: LogicalPlan,
             conf: Configuration): ParquetRelation = {
    if (!child.resolved) {
      throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create Parquet table from unresolved child (when schema is not available)")
    }
    createEmpty(pathString, child.output, false, conf)
  }

  /**
   * Creates an empty ParquetRelation and underlying Parquetfile that only
   * consists of the Metadata for the given schema.
   *
   * @param pathString The directory the Parquetfile will be stored in.
   * @param attributes The schema of the relation.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation.
   */
  def createEmpty(pathString: String,
                  attributes: Seq[Attribute],
                  allowExisting: Boolean,
                  conf: Configuration): ParquetRelation = {
    val path = checkPath(pathString, allowExisting, conf)
    if (conf.get(ParquetOutputFormat.COMPRESSION) == null) {
      conf.set(ParquetOutputFormat.COMPRESSION, ParquetRelation.defaultCompression.name())
    }
    ParquetRelation.enableLogForwarding()
    ParquetTypesConverter.writeMetaData(attributes, path, conf)
    new ParquetRelation(path.toString)
  }

  private def checkPath(pathStr: String, allowExisting: Boolean, conf: Configuration): Path = {
    if (pathStr == null) {
      throw new IllegalArgumentException("Unable to create ParquetRelation: path is null")
    }
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to create ParquetRelation: incorrectly formatted path $pathStr")
    }
    val path = origPath.makeQualified(fs)
    if (!allowExisting && fs.exists(path)) {
      sys.error(s"File $pathStr already exists.")
    }

    if (fs.exists(path) &&
        !fs.getFileStatus(path)
        .getPermission
        .getUserAction
        .implies(FsAction.READ_WRITE)) {
      throw new IOException(
        s"Unable to create ParquetRelation: path $path not read-writable")
    }
    path
  }
}

private[parquet] object ParquetTypesConverter {
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
    case ParquetPrimitiveTypeName.INT96 =>
      // TODO: add BigInteger type? TODO(andre) use DecimalType instead????
      sys.error("Warning: potential loss of precision: converting INT96 to long")
      LongType
    case _ => sys.error(
      s"Unsupported parquet datatype $parquetType")
  }

  def fromDataType(ctype: DataType): ParquetPrimitiveTypeName = ctype match {
    case StringType => ParquetPrimitiveTypeName.BINARY
    case BooleanType => ParquetPrimitiveTypeName.BOOLEAN
    case DoubleType => ParquetPrimitiveTypeName.DOUBLE
    case ArrayType(ByteType) => ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
    case FloatType => ParquetPrimitiveTypeName.FLOAT
    case IntegerType => ParquetPrimitiveTypeName.INT32
    case LongType => ParquetPrimitiveTypeName.INT64
    case _ => sys.error(s"Unsupported datatype $ctype")
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
      case _ => sys.error(s"Unsupported datatype $ctype, cannot write to consumer")
    }
  }

  def getSchema(schemaString : String) : MessageType =
    MessageTypeParser.parseMessageType(schemaString)

  def convertToAttributes(parquetSchema: MessageType) : Seq[Attribute] = {
    parquetSchema.getColumns.map {
      case (desc) =>
        val ctype = toDataType(desc.getType)
        val name: String = desc.getPath.mkString(".")
        new AttributeReference(name, ctype, false)()
    }
  }

  // TODO: allow nesting?
  def convertFromAttributes(attributes: Seq[Attribute]): MessageType = {
    val fields: Seq[ParquetType] = attributes.map {
      a => new ParquetPrimitiveType(Repetition.OPTIONAL, fromDataType(a.dataType), a.name)
    }
    new MessageType("root", fields)
  }

  def writeMetaData(attributes: Seq[Attribute], origPath: Path, conf: Configuration) {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to write Parquet metadata: path is null")
    }
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to write Parquet metadata: path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (fs.exists(path) && !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(s"Expected to write to directory $path but found file")
    }
    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)
    if (fs.exists(metadataPath)) {
      try {
        fs.delete(metadataPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(s"Unable to delete previous PARQUET_METADATA_FILE at $metadataPath")
      }
    }
    val extraMetadata = new java.util.HashMap[String, String]()
    extraMetadata.put("path", path.toString)
    // TODO: add extra data, e.g., table name, date, etc.?

    val parquetSchema: MessageType =
      ParquetTypesConverter.convertFromAttributes(attributes)
    val metaData: FileMetaData = new FileMetaData(
      parquetSchema,
      extraMetadata,
      "Spark")

    ParquetRelation.enableLogForwarding()
    ParquetFileWriter.writeMetadataFile(
      conf,
      path,
      new Footer(path, new ParquetMetadata(metaData, Nil)) :: Nil)
  }

  /**
   * Try to read Parquet metadata at the given Path. We first see if there is a summary file
   * in the parent directory. If so, this is used. Else we read the actual footer at the given
   * location.
   * @param origPath The path at which we expect one (or more) Parquet files.
   * @return The `ParquetMetadata` containing among other things the schema.
   */
  def readMetaData(origPath: Path): ParquetMetadata = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to read Parquet metadata: path is null")
    }
    val job = new Job()
    // TODO: since this is called from ParquetRelation (LogicalPlan) we don't have access
    // to SparkContext's hadoopConfig; in principle the default FileSystem may be different(?!)
    val conf = ContextUtil.getConfiguration(job)
    val fs: FileSystem = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(s"Incorrectly formatted Parquet metadata path $origPath")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"Expected $path for be a directory with Parquet files/metadata")
    }
    ParquetRelation.enableLogForwarding()
    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)
    // if this is a new table that was just created we will find only the metadata file
    if (fs.exists(metadataPath) && fs.isFile(metadataPath)) {
      ParquetFileReader.readFooter(conf, metadataPath)
    } else {
      // there may be one or more Parquet files in the given directory
      val footers = ParquetFileReader.readFooters(conf, fs.getFileStatus(path))
      // TODO: for now we assume that all footers (if there is more than one) have identical
      // metadata; we may want to add a check here at some point
      if (footers.size() == 0) {
        throw new IllegalArgumentException(s"Could not find Parquet metadata at path $path")
      }
      footers(0).getParquetMetadata
    }
  }
}
