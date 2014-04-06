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
import parquet.schema.{Type => ParquetType, PrimitiveType => ParquetPrimitiveType, MessageType, MessageTypeParser, GroupType => ParquetGroupType, OriginalType => ParquetOriginalType, ConversionPatterns}
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
  def isPrimitiveType(ctype: DataType): Boolean = classOf[PrimitiveType] isAssignableFrom ctype.getClass

  def toPrimitiveDataType(parquetType : ParquetPrimitiveTypeName): DataType = parquetType match {
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

  /**
   * Converts a given Parquet `Type` into the corresponding
   * [[org.apache.spark.sql.catalyst.types.DataType]].
   *
   * Note that we apply the following conversion rules:
   * <ul>
   *   <li> Primitive types are converter to the corresponding primitive type.</li>
   *   <li> Group types that have a single field with repetition `REPEATED` or themselves
   *        have repetition level `REPEATED` are converted to an [[ArrayType]] with the
   *        corresponding field type (possibly primitive) as element type.</li>
   *   <li> Other group types are converted as follows:<ul>
   *      <li> If they have a single field, they are converted into a [[StructType]] with
   *           the corresponding field type.</li>
   *      <li> If they have more than one field and repetition level `REPEATED` they are
   *           converted into an [[ArrayType]] with the corresponding [[StructType]] as complex
   *           element type.</li>
   *      <li> Otherwise they are converted into a [[StructType]] with the corresponding
   *           field types.</li></ul></li>
   * </ul>
   * Note that fields are determined to be `nullable` if and only if their Parquet repetition
   * level is not `REQUIRED`.
   *
   * @param parquetType The type to convert.
   * @return The corresponding Catalyst type.
   */
  def toDataType(parquetType: ParquetType): DataType = {
    if (parquetType.isPrimitive) {
      toPrimitiveDataType(parquetType.asPrimitiveType.getPrimitiveTypeName)
    }
    else {
      val groupType = parquetType.asGroupType()
      parquetType.getOriginalType match {
        // if the schema was constructed programmatically there may be hints how to convert
        // it inside the metadata via the OriginalType field
        case ParquetOriginalType.LIST => { // TODO: check enums!
          val fields = groupType.getFields.map {
            field => new StructField(field.getName, toDataType(field), field.getRepetition != Repetition.REQUIRED)
          }
          if (fields.size == 1) {
            new ArrayType(fields.apply(0).dataType)
          } else {
            new ArrayType(StructType(fields))
          }
        }
        case _ => { // everything else nested becomes a Struct, unless it has a single repeated field
          // in which case it becomes an array (this should correspond to the inverse operation of
          // parquet.schema.ConversionPatterns.listType)
          if (groupType.getFieldCount == 1 &&
              (groupType.getFields.apply(0).getRepetition == Repetition.REPEATED ||
                groupType.getRepetition == Repetition.REPEATED)) {
            val elementType = toDataType(groupType.getFields.apply(0))
            new ArrayType(elementType)
          } else {
            val fields = groupType
              .getFields
              .map(ptype => new StructField(
              ptype.getName,
              toDataType(ptype),
              ptype.getRepetition != Repetition.REQUIRED))

            if (groupType.getFieldCount == 1) {
              new StructType(fields)
            } else {
              if (parquetType.getRepetition == Repetition.REPEATED) {
                new ArrayType(StructType(fields))
              } else {
                new StructType(fields)
              }
            }
          }
        }
      }
    }
  }

  /**
   * For a given Catalyst [[org.apache.spark.sql.catalyst.types.DataType]] return
   * the name of the corresponding Parquet primitive type or None if the given type
   * is not primitive.
   *
   * @param ctype The type to convert
   * @return The name of the corresponding Parquet primitive type
   */
  def fromPrimitiveDataType(ctype: DataType): Option[ParquetPrimitiveTypeName] = ctype match {
    case StringType => Some(ParquetPrimitiveTypeName.BINARY)
    case BooleanType => Some(ParquetPrimitiveTypeName.BOOLEAN)
    case DoubleType => Some(ParquetPrimitiveTypeName.DOUBLE)
    case ArrayType(ByteType) => Some(ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
    case FloatType => Some(ParquetPrimitiveTypeName.FLOAT)
    case IntegerType => Some(ParquetPrimitiveTypeName.INT32)
    case LongType => Some(ParquetPrimitiveTypeName.INT64)
    case _ => None
  }

  /**
   * Converts a given Catalyst [[org.apache.spark.sql.catalyst.types.DataType]] into
   * the corrponsing Parquet `Type`.
   *
   * The conversion follows the rules below:
   * <ul>
   *   <li> Primitive types are converted into Parquet's primitive types.</li>
   *   <li> [[org.apache.spark.sql.catalyst.types.StructType]]s are converted
   *   into Parquet's `GroupType` with the corresponding field types.</li>
   *   <li> [[org.apache.spark.sql.catalyst.types.ArrayType]]s are handled as follows:<ul>
   *     <li> If their element is complex, that is of type
   *          [[org.apache.spark.sql.catalyst.types.StructType]], they are converted
   *          into a `GroupType` with the corresponding field types of the struct and
   *          original type of the `GroupType` is set to `LIST`.</li>
   *     <li> Otherwise, that is they contain a primitive they are converted into a `GroupType`
   *     that is also a list but has only a single field of the type corresponding to
   *     the element type.</li></ul></li>
   * </ul>
   * Parquet's repetition level is set according to the following rule:
   * <ul>
   *   <li> If the call to `fromDataType` is recursive inside an enclosing `ArrayType`, then
   *   the repetition level is set to `REPEATED`.</li>
   *   <li> Otherwise, if the attribute whose type is converted is `nullable`, the Parquet
   *   type gets repetition level `OPTIONAL` and otherwise `REQUIRED`.</li>
   * </ul>
   * The single expection to this rule is an [[org.apache.spark.sql.catalyst.types.ArrayType]]
   * that contains a [[org.apache.spark.sql.catalyst.types.StructType]], whose repetition level
   * is always set to `REPEATED`.
   *
   @param ctype The type to convert.
   * @param name The name of the [[org.apache.spark.sql.catalyst.expressions.Attribute]] whose type is converted
   * @param nullable When true indicates that the attribute is nullable
   * @param inArray When true indicates that this is a nested attribute inside an array.
   * @return The corresponding Parquet type.
   */
  def fromDataType(
      ctype: DataType,
      name: String,
      nullable: Boolean = true,
      inArray: Boolean = false): ParquetType = {
    val repetition =
      if (inArray) Repetition.REPEATED
      else {
        if (nullable) Repetition.OPTIONAL
        else Repetition.REQUIRED
      }
    val primitiveType = fromPrimitiveDataType(ctype)
    if (primitiveType.isDefined) {
      new ParquetPrimitiveType(repetition, primitiveType.get, name)
    } else {
      ctype match {
        case ArrayType(elementType: DataType) => {
          elementType match {
            case StructType(fields) => { // first case: array of structs
              val parquetFieldTypes = fields.map(
                f => fromDataType(f.dataType, f.name, f.nullable, inArray = false))
              assert(fields.size > 1, "Found struct inside array with a single field.. error parsin Catalyst schema")
              new ParquetGroupType(Repetition.REPEATED, name, ParquetOriginalType.LIST, parquetFieldTypes)
              //ConversionPatterns.listType(Repetition.REPEATED, name, parquetFieldTypes)
            }
            case _ => { // second case: array of primitive types
              val parquetElementType = fromDataType(
                elementType,
                CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
                nullable = false,
                inArray = true)
              ConversionPatterns.listType(repetition, name, parquetElementType)
            }
          }
        }
        // TODO: test structs inside arrays
        case StructType(structFields) => {
          val fields = structFields.map {
            field => fromDataType(field.dataType, field.name, field.nullable, inArray = false)
          }
          new ParquetGroupType(repetition, name, fields)
        }
        case _ => sys.error(s"Unsupported datatype $ctype")
      }
    }
  }

  def consumeType(
      consumer: RecordConsumer,
      ctype: DataType,
      record: Row,
      index: Int): Unit = {
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

  def getSchema(schemaString: String) : MessageType =
    MessageTypeParser.parseMessageType(schemaString)

  def convertToAttributes(parquetSchema: ParquetType): Seq[Attribute] = {
    parquetSchema
      .asGroupType()
      .getFields
      .map(
        field =>
          new AttributeReference(
            field.getName,
            toDataType(field),
            field.getRepetition != Repetition.REQUIRED)())
  }

  def convertFromAttributes(attributes: Seq[Attribute]): MessageType = {
    val fields = attributes.map(
      attribute =>
        fromDataType(attribute.dataType, attribute.name, attribute.nullable))
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
