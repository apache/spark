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

import java.io.{IOException, FileNotFoundException}

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.permission.FsAction

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, BaseRelation}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.ArrayType
import org.apache.spark.sql.catalyst.expressions.{Row, AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.analysis.UnresolvedException

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import parquet.schema.{PrimitiveType => ParquetPrimitiveType}
import parquet.schema.{Type => ParquetType}
import parquet.schema.Type.Repetition
import parquet.io.api.{Binary, RecordConsumer}
import parquet.hadoop.{Footer, ParquetFileWriter, ParquetFileReader}
import parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}
import parquet.hadoop.util.ContextUtil

import scala.collection.JavaConversions._

/**
 * Relation formed by underlying Parquet file that contains data stored in columnar form.
 * Note that there are currently two ways to import a ParquetRelation:
 *
 * {{{
 * // a) create the Relation "manually" and register via the OverrideCatalog, e.g.:
 * scala> ParquetTestData.writeFile
 *
 * scala> TestHive.catalog.overrideTable(None, "psrc", ParquetTestData.testData)
 * res1: Option[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = None
 *
 * scala> val query = sql("SELECT * FROM psrc")
 * query: org.apache.spark.sql.ExecutedQuery =
 * SELECT * FROM psrc
 * === Query Plan ===
 * ParquetTableScan [myboolean#6,myint#7,mystring#8,mylong#9L,myfloat#10,mydouble#11],
 * (ParquetRelation testData, file:/tmp/testParquetFile), None
 *
 * scala> query.collect
 * res2: Array[org.apache.spark.sql.Row] = Array([true,5,abc,8589934592,2.5,4.5], ...
 *
 * // b) "manually" resolve the relation by modifying the logical plan
 * scala> import org.apache.spark.sql.execution.ParquetRelation
 * import org.apache.spark.sql.execution.ParquetRelation
 *
 * scala> ParquetTestData.writeFile
 *
 * scala> val filename = ParquetTestData.testFile.toString
 * filename: String = /tmp/testParquetFile
 *
 * scala> val query_string = "SELECT * FROM psrc"
 * query_string: String = SELECT * FROM psrc
 *
 * scala> val query = TestHive.parseSql(query_string).transform {
 *    | case relation @ UnresolvedRelation(databaseName, name, alias) =>
 *    | if (name == "psrc") ParquetRelation(name, filename)
 *    | else relation
 *    | }
 * query: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
 * Project [*]
 * ParquetRelation psrc, /tmp/testParquetFile
 *
 * scala> executePlan(query).toRdd.collect()
 * res8: Array[org.apache.spark.sql.Row] = Array([true,5,abc,8589934592,2.5,4.5], ...
 * }}}
 *
 * @param tableName The name of the relation that can be used in queries.
 * @param path The path to the Parquet file.
 */
case class ParquetRelation(val tableName: String, val path: String) extends BaseRelation {

  // TODO: Figure out resolution of unresolved ParquetRelation (a simple MetaStore?)
  // to make this more user friendly

  /** Schema derived from ParquetFile **/
  def parquetSchema: MessageType =
    ParquetTypesConverter
      .readMetaData(new Path(path))
      .getFileMetaData
      .getSchema

  /** Attributes **/
  val attributes =
    ParquetTypesConverter
    .convertToAttributes(parquetSchema)

  /** Output **/
  override val output = attributes

  // Parquet files have no concepts of keys, therefore no Partitioner
  // Note: we could allow Block level access; needs to be thought through
  override def isPartitioned = false
}

object ParquetRelation {

  // The element type for the RDDs that this relation maps to.
  type RowType = org.apache.spark.sql.catalyst.expressions.GenericMutableRow

  /**
   * Creates a new ParquetRelation and underlying Parquetfile for the given
   * LogicalPlan. Note that this is used insider [[SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Parquetfile.
   * The relation is empty but is initialized with ParquetMetadata and
   * can be inserted into.
   *
   * @param pathString The directory the Parquetfile will be stored in.
   * @param child The child node that will be used for extracting the schema.
   * @param conf A configuration configuration to be used.
   * @param tableName The name of the resulting relation.
   * @return An empty ParquetRelation inferred metadata.
   */
  def create(pathString: String,
             child: LogicalPlan,
             conf: Configuration,
             tableName: Option[String]): ParquetRelation = {
    if (!child.resolved) {
      throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create Parquet table from unresolved child (when schema is not available)")
    }

    val name = s"${tableName.getOrElse(child.nodeName)}_parquet"
    val path = checkPath(pathString, conf)
    ParquetTypesConverter.writeMetaData(child.output, path, conf)
    new ParquetRelation(name, path.toString)
  }

  private def checkPath(pathStr: String, conf: Configuration): Path = {
    val path = new Path(pathStr)
    val fs = path.getFileSystem(conf)
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
    case ParquetPrimitiveTypeName.INT96 => {
      // TODO: add BigInteger type? TODO(andre) use DecimalType instead????
      sys.error("Warning: potential loss of precision: converting INT96 to long")
      LongType
    }
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

  def writeMetaData(attributes: Seq[Attribute], path: Path, conf: Configuration) {
    val fileSystem = FileSystem.get(conf)

    if (fileSystem.exists(path) && !fileSystem.getFileStatus(path).isDir) {
      throw new IOException(s"Expected to write to directory $path but found file")
    }

    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)

    if (fileSystem.exists(metadataPath)) {
      try {
        fileSystem.delete(metadataPath, true)
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

    ParquetFileWriter.writeMetadataFile(
      conf,
      path,
      new Footer(path, new ParquetMetadata(metaData, Nil)) :: Nil)
  }

  /**
   * Try to read Parquet metadata at the given Path. We first see if there is a summary file
   * in the parent directory. If so, this is used. Else we read the actual footer at the given
   * location.
   * @param path The path at which we expect one (or more) Parquet files.
   * @return The `ParquetMetadata` containing among other things the schema.
   */
  def readMetaData(path: Path): ParquetMetadata = {
    val job = new Job()
    val conf = ContextUtil.getConfiguration(job)
    val fs: FileSystem = path.getFileSystem(conf)

    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)

    if (fs.exists(metadataPath) && fs.isFile(metadataPath)) {
      // TODO: improve exception handling, etc.
      ParquetFileReader.readFooter(conf, metadataPath)
    } else {
      if (!fs.exists(path) || !fs.isFile(path)) {
        throw new FileNotFoundException(
          s"Could not find file ${path.toString} when trying to read metadata")
      }
      ParquetFileReader.readFooter(conf, path)
    }
  }
}
