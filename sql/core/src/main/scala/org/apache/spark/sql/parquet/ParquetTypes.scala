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

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import parquet.format.converter.ParquetMetadataConverter
import parquet.hadoop.{ParquetFileReader, Footer, ParquetFileWriter}
import parquet.hadoop.metadata.{ParquetMetadata, FileMetaData}
import parquet.hadoop.util.ContextUtil
import parquet.schema.{Type => ParquetType, Types => ParquetTypes, PrimitiveType => ParquetPrimitiveType, MessageType}
import parquet.schema.{GroupType => ParquetGroupType, OriginalType => ParquetOriginalType, ConversionPatterns, DecimalMetadata}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import parquet.schema.Type.Repetition

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.types._

// Implicits
import scala.collection.JavaConversions._

/** A class representing Parquet info fields we care about, for passing back to Parquet */
private[parquet] case class ParquetTypeInfo(
  primitiveType: ParquetPrimitiveTypeName,
  originalType: Option[ParquetOriginalType] = None,
  decimalMetadata: Option[DecimalMetadata] = None,
  length: Option[Int] = None)

private[parquet] object ParquetTypesConverter extends Logging {
  def isPrimitiveType(ctype: DataType): Boolean =
    classOf[PrimitiveType] isAssignableFrom ctype.getClass

  def toPrimitiveDataType(
      parquetType: ParquetPrimitiveType,
      binaryAsString: Boolean): DataType = {
    val originalType = parquetType.getOriginalType
    val decimalInfo = parquetType.getDecimalMetadata
    parquetType.getPrimitiveTypeName match {
      case ParquetPrimitiveTypeName.BINARY
        if (originalType == ParquetOriginalType.UTF8 || binaryAsString) => StringType
      case ParquetPrimitiveTypeName.BINARY => BinaryType
      case ParquetPrimitiveTypeName.BOOLEAN => BooleanType
      case ParquetPrimitiveTypeName.DOUBLE => DoubleType
      case ParquetPrimitiveTypeName.FLOAT => FloatType
      case ParquetPrimitiveTypeName.INT32 => IntegerType
      case ParquetPrimitiveTypeName.INT64 => LongType
      case ParquetPrimitiveTypeName.INT96 =>
        // TODO: add BigInteger type? TODO(andre) use DecimalType instead????
        sys.error("Potential loss of precision: cannot convert INT96")
      case ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
        if (originalType == ParquetOriginalType.DECIMAL && decimalInfo.getPrecision <= 18) =>
          // TODO: for now, our reader only supports decimals that fit in a Long
          DecimalType(decimalInfo.getPrecision, decimalInfo.getScale)
      case _ => sys.error(
        s"Unsupported parquet datatype $parquetType")
    }
  }

  /**
   * Converts a given Parquet `Type` into the corresponding
   * [[org.apache.spark.sql.types.DataType]].
   *
   * We apply the following conversion rules:
   * <ul>
   *   <li> Primitive types are converter to the corresponding primitive type.</li>
   *   <li> Group types that have a single field that is itself a group, which has repetition
   *        level `REPEATED`, are treated as follows:<ul>
   *          <li> If the nested group has name `values`, the surrounding group is converted
   *               into an [[ArrayType]] with the corresponding field type (primitive or
   *               complex) as element type.</li>
   *          <li> If the nested group has name `map` and two fields (named `key` and `value`),
   *               the surrounding group is converted into a [[MapType]]
   *               with the corresponding key and value (value possibly complex) types.
   *               Note that we currently assume map values are not nullable.</li>
   *   <li> Other group types are converted into a [[StructType]] with the corresponding
   *        field types.</li></ul></li>
   * </ul>
   * Note that fields are determined to be `nullable` if and only if their Parquet repetition
   * level is not `REQUIRED`.
   *
   * @param parquetType The type to convert.
   * @return The corresponding Catalyst type.
   */
  def toDataType(parquetType: ParquetType, isBinaryAsString: Boolean): DataType = {
    def correspondsToMap(groupType: ParquetGroupType): Boolean = {
      if (groupType.getFieldCount != 1 || groupType.getFields.apply(0).isPrimitive) {
        false
      } else {
        // This mostly follows the convention in ``parquet.schema.ConversionPatterns``
        val keyValueGroup = groupType.getFields.apply(0).asGroupType()
        keyValueGroup.getRepetition == Repetition.REPEATED &&
          keyValueGroup.getName == CatalystConverter.MAP_SCHEMA_NAME &&
          keyValueGroup.getFieldCount == 2 &&
          keyValueGroup.getFields.apply(0).getName == CatalystConverter.MAP_KEY_SCHEMA_NAME &&
          keyValueGroup.getFields.apply(1).getName == CatalystConverter.MAP_VALUE_SCHEMA_NAME
      }
    }

    def correspondsToArray(groupType: ParquetGroupType): Boolean = {
      groupType.getFieldCount == 1 &&
        groupType.getFieldName(0) == CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME &&
        groupType.getFields.apply(0).getRepetition == Repetition.REPEATED
    }

    if (parquetType.isPrimitive) {
      toPrimitiveDataType(parquetType.asPrimitiveType, isBinaryAsString)
    } else {
      val groupType = parquetType.asGroupType()
      parquetType.getOriginalType match {
        // if the schema was constructed programmatically there may be hints how to convert
        // it inside the metadata via the OriginalType field
        case ParquetOriginalType.LIST => { // TODO: check enums!
          assert(groupType.getFieldCount == 1)
          val field = groupType.getFields.apply(0)
          if (field.getName == CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME) {
            val bag = field.asGroupType()
            assert(bag.getFieldCount == 1)
            ArrayType(toDataType(bag.getFields.apply(0), isBinaryAsString), containsNull = true)
          } else {
            ArrayType(toDataType(field, isBinaryAsString), containsNull = false)
          }
        }
        case ParquetOriginalType.MAP => {
          assert(
            !groupType.getFields.apply(0).isPrimitive,
            "Parquet Map type malformatted: expected nested group for map!")
          val keyValueGroup = groupType.getFields.apply(0).asGroupType()
          assert(
            keyValueGroup.getFieldCount == 2,
            "Parquet Map type malformatted: nested group should have 2 (key, value) fields!")
          assert(keyValueGroup.getFields.apply(0).getRepetition == Repetition.REQUIRED)

          val keyType = toDataType(keyValueGroup.getFields.apply(0), isBinaryAsString)
          val valueType = toDataType(keyValueGroup.getFields.apply(1), isBinaryAsString)
          MapType(keyType, valueType,
            keyValueGroup.getFields.apply(1).getRepetition != Repetition.REQUIRED)
        }
        case _ => {
          // Note: the order of these checks is important!
          if (correspondsToMap(groupType)) { // MapType
            val keyValueGroup = groupType.getFields.apply(0).asGroupType()
            assert(keyValueGroup.getFields.apply(0).getRepetition == Repetition.REQUIRED)

            val keyType = toDataType(keyValueGroup.getFields.apply(0), isBinaryAsString)
            val valueType = toDataType(keyValueGroup.getFields.apply(1), isBinaryAsString)
            MapType(keyType, valueType,
              keyValueGroup.getFields.apply(1).getRepetition != Repetition.REQUIRED)
          } else if (correspondsToArray(groupType)) { // ArrayType
            val field = groupType.getFields.apply(0)
            if (field.getName == CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME) {
              val bag = field.asGroupType()
              assert(bag.getFieldCount == 1)
              ArrayType(toDataType(bag.getFields.apply(0), isBinaryAsString), containsNull = true)
            } else {
              ArrayType(toDataType(field, isBinaryAsString), containsNull = false)
            }
          } else { // everything else: StructType
            val fields = groupType
              .getFields
              .map(ptype => new StructField(
              ptype.getName,
              toDataType(ptype, isBinaryAsString),
              ptype.getRepetition != Repetition.REQUIRED))
            StructType(fields)
          }
        }
      }
    }
  }

  /**
   * For a given Catalyst [[org.apache.spark.sql.types.DataType]] return
   * the name of the corresponding Parquet primitive type or None if the given type
   * is not primitive.
   *
   * @param ctype The type to convert
   * @return The name of the corresponding Parquet type properties
   */
  def fromPrimitiveDataType(ctype: DataType): Option[ParquetTypeInfo] = ctype match {
    case StringType => Some(ParquetTypeInfo(
      ParquetPrimitiveTypeName.BINARY, Some(ParquetOriginalType.UTF8)))
    case BinaryType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.BINARY))
    case BooleanType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.BOOLEAN))
    case DoubleType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.DOUBLE))
    case FloatType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.FLOAT))
    case IntegerType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.INT32))
    // There is no type for Byte or Short so we promote them to INT32.
    case ShortType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.INT32))
    case ByteType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.INT32))
    case LongType => Some(ParquetTypeInfo(ParquetPrimitiveTypeName.INT64))
    case DecimalType.Fixed(precision, scale) if precision <= 18 =>
      // TODO: for now, our writer only supports decimals that fit in a Long
      Some(ParquetTypeInfo(ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        Some(ParquetOriginalType.DECIMAL),
        Some(new DecimalMetadata(precision, scale)),
        Some(BYTES_FOR_PRECISION(precision))))
    case _ => None
  }

  /**
   * Compute the FIXED_LEN_BYTE_ARRAY length needed to represent a given DECIMAL precision.
   */
  private[parquet] val BYTES_FOR_PRECISION = Array.tabulate[Int](38) { precision =>
    var length = 1
    while (math.pow(2.0, 8 * length - 1) < math.pow(10.0, precision)) {
      length += 1
    }
    length
  }

  /**
   * Converts a given Catalyst [[org.apache.spark.sql.types.DataType]] into
   * the corresponding Parquet `Type`.
   *
   * The conversion follows the rules below:
   * <ul>
   *   <li> Primitive types are converted into Parquet's primitive types.</li>
   *   <li> [[org.apache.spark.sql.types.StructType]]s are converted
   *        into Parquet's `GroupType` with the corresponding field types.</li>
   *   <li> [[org.apache.spark.sql.types.ArrayType]]s are converted
   *        into a 2-level nested group, where the outer group has the inner
   *        group as sole field. The inner group has name `values` and
   *        repetition level `REPEATED` and has the element type of
   *        the array as schema. We use Parquet's `ConversionPatterns` for this
   *        purpose.</li>
   *   <li> [[org.apache.spark.sql.types.MapType]]s are converted
   *        into a nested (2-level) Parquet `GroupType` with two fields: a key
   *        type and a value type. The nested group has repetition level
   *        `REPEATED` and name `map`. We use Parquet's `ConversionPatterns`
   *        for this purpose</li>
   * </ul>
   * Parquet's repetition level is generally set according to the following rule:
   * <ul>
   *   <li> If the call to `fromDataType` is recursive inside an enclosing `ArrayType` or
   *   `MapType`, then the repetition level is set to `REPEATED`.</li>
   *   <li> Otherwise, if the attribute whose type is converted is `nullable`, the Parquet
   *   type gets repetition level `OPTIONAL` and otherwise `REQUIRED`.</li>
   * </ul>
   *
   *@param ctype The type to convert
   * @param name The name of the [[org.apache.spark.sql.catalyst.expressions.Attribute]]
   *             whose type is converted
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
      if (inArray) {
        Repetition.REPEATED
      } else {
        if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
      }
    val typeInfo = fromPrimitiveDataType(ctype)
    typeInfo.map {
      case ParquetTypeInfo(primitiveType, originalType, decimalMetadata, length) =>
        val builder = ParquetTypes.primitive(primitiveType, repetition).as(originalType.orNull)
        for (len <- length) {
          builder.length(len)
        }
        for (metadata <- decimalMetadata) {
          builder.precision(metadata.getPrecision).scale(metadata.getScale)
        }
        builder.named(name)
    }.getOrElse {
      ctype match {
        case udt: UserDefinedType[_] => {
          fromDataType(udt.sqlType, name, nullable, inArray)
        }
        case ArrayType(elementType, false) => {
          val parquetElementType = fromDataType(
            elementType,
            CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
            nullable = false,
            inArray = true)
          ConversionPatterns.listType(repetition, name, parquetElementType)
        }
        case ArrayType(elementType, true) => {
          val parquetElementType = fromDataType(
            elementType,
            CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
            nullable = true,
            inArray = false)
          ConversionPatterns.listType(
            repetition,
            name,
            new ParquetGroupType(
              Repetition.REPEATED,
              CatalystConverter.ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME,
              parquetElementType))
        }
        case StructType(structFields) => {
          val fields = structFields.map {
            field => fromDataType(field.dataType, field.name, field.nullable, inArray = false)
          }
          new ParquetGroupType(repetition, name, fields.toSeq)
        }
        case MapType(keyType, valueType, valueContainsNull) => {
          val parquetKeyType =
            fromDataType(
              keyType,
              CatalystConverter.MAP_KEY_SCHEMA_NAME,
              nullable = false,
              inArray = false)
          val parquetValueType =
            fromDataType(
              valueType,
              CatalystConverter.MAP_VALUE_SCHEMA_NAME,
              nullable = valueContainsNull,
              inArray = false)
          ConversionPatterns.mapType(
            repetition,
            name,
            parquetKeyType,
            parquetValueType)
        }
        case _ => sys.error(s"Unsupported datatype $ctype")
      }
    }
  }

  def convertToAttributes(parquetSchema: ParquetType, isBinaryAsString: Boolean): Seq[Attribute] = {
    parquetSchema
      .asGroupType()
      .getFields
      .map(
        field =>
          new AttributeReference(
            field.getName,
            toDataType(field, isBinaryAsString),
            field.getRepetition != Repetition.REQUIRED)())
  }

  def convertFromAttributes(attributes: Seq[Attribute]): MessageType = {
    val fields = attributes.map(
      attribute =>
        fromDataType(attribute.dataType, attribute.name, attribute.nullable))
    new MessageType("root", fields)
  }

  def convertFromString(string: String): Seq[Attribute] = {
    Try(DataType.fromJson(string)).getOrElse(DataType.fromCaseClassString(string)) match {
      case s: StructType => s.toAttributes
      case other => sys.error(s"Can convert $string to row")
    }
  }

  def convertToString(schema: Seq[Attribute]): String = {
    StructType.fromAttributes(schema).json
  }

  def writeMetaData(attributes: Seq[Attribute], origPath: Path, conf: Configuration): Unit = {
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
    extraMetadata.put(
      RowReadSupport.SPARK_METADATA_KEY,
      ParquetTypesConverter.convertToString(attributes))
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
   * @param configuration The Hadoop configuration to use.
   * @return The `ParquetMetadata` containing among other things the schema.
   */
  def readMetaData(origPath: Path, configuration: Option[Configuration]): ParquetMetadata = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to read Parquet metadata: path is null")
    }
    val job = new Job()
    val conf = configuration.getOrElse(ContextUtil.getConfiguration(job))
    val fs: FileSystem = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(s"Incorrectly formatted Parquet metadata path $origPath")
    }
    val path = origPath.makeQualified(fs)

    val children =
      fs
        .globStatus(path)
        .flatMap { status => if(status.isDir) fs.listStatus(status.getPath) else List(status) }
        .filterNot { status =>
          val name = status.getPath.getName
          (name(0) == '.' || name(0) == '_') && name != ParquetFileWriter.PARQUET_METADATA_FILE
        }

    ParquetRelation.enableLogForwarding()

    // NOTE (lian): Parquet "_metadata" file can be very slow if the file consists of lots of row
    // groups. Since Parquet schema is replicated among all row groups, we only need to touch a
    // single row group to read schema related metadata. Notice that we are making assumptions that
    // all data in a single Parquet file have the same schema, which is normally true.
    children
      // Try any non-"_metadata" file first...
      .find(_.getPath.getName != ParquetFileWriter.PARQUET_METADATA_FILE)
      // ... and fallback to "_metadata" if no such file exists (which implies the Parquet file is
      // empty, thus normally the "_metadata" file is expected to be fairly small).
      .orElse(children.find(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE))
      .map(ParquetFileReader.readFooter(conf, _, ParquetMetadataConverter.NO_FILTER))
      .getOrElse(
        throw new IllegalArgumentException(s"Could not find Parquet metadata at path $path"))
  }

  /**
   * Reads in Parquet Metadata from the given path and tries to extract the schema
   * (Catalyst attributes) from the application-specific key-value map. If this
   * is empty it falls back to converting from the Parquet file schema which
   * may lead to an upcast of types (e.g., {byte, short} to int).
   *
   * @param origPath The path at which we expect one (or more) Parquet files.
   * @param conf The Hadoop configuration to use.
   * @return A list of attributes that make up the schema.
   */
  def readSchemaFromFile(
      origPath: Path,
      conf: Option[Configuration],
      isBinaryAsString: Boolean): Seq[Attribute] = {
    val keyValueMetadata: java.util.Map[String, String] =
      readMetaData(origPath, conf)
        .getFileMetaData
        .getKeyValueMetaData
    if (keyValueMetadata.get(RowReadSupport.SPARK_METADATA_KEY) != null) {
      convertFromString(keyValueMetadata.get(RowReadSupport.SPARK_METADATA_KEY))
    } else {
      val attributes = convertToAttributes(
        readMetaData(origPath, conf).getFileMetaData.getSchema, isBinaryAsString)
      log.info(s"Falling back to schema conversion from Parquet types; result: $attributes")
      attributes
    }
  }
}
