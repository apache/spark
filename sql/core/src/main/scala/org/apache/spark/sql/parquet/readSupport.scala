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

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import parquet.column.Dictionary
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{InitContext, ReadSupport}
import parquet.io.api._
import parquet.schema.{PrimitiveType => ParquetPrimitiveType, _}

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{MutableRow, SpecificMutableRow}
import org.apache.spark.sql.types._

private[parquet] object SparkRowReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"

  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  val SPARK_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.attributes"
}

private[parquet] class SparkRowReadSupport extends ReadSupport[Row] with Logging {
  import SparkRowReadSupport._

  override def init(context: InitContext): ReadContext = {
    val conf = context.getConfiguration
    val maybeRequestedSchema = Option(conf.get(SPARK_ROW_REQUESTED_SCHEMA))
    val maybeRowSchema = Option(conf.get(SPARK_ROW_SCHEMA))

    val fullParquetSchema = context.getFileSchema
    val parquetRequestedSchema =
      maybeRequestedSchema.map { value =>
        val requestedFieldNames = StructType.fromString(value).fieldNames.toSet
        pruneFields(fullParquetSchema, requestedFieldNames)
      }.getOrElse(fullParquetSchema)

    val metadata =
      Map.empty[String, String] ++
        maybeRequestedSchema.map(SPARK_ROW_REQUESTED_SCHEMA -> _) ++
        maybeRowSchema.map(SPARK_ROW_SCHEMA -> _)

    new ReadContext(parquetRequestedSchema, metadata)
  }

  override def prepareForRead(
      configuration: Configuration,
      keyValueMetaData: util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[Row] = {

    logDebug(s"Preparing for reading with Parquet file schema $fileSchema")

    val parquetRequestedSchema = readContext.getRequestedSchema
    val metadata = readContext.getReadSupportMetadata

    val maybeSparkSchema = Option(metadata.get(SPARK_METADATA_KEY))
    val maybeSparkRequestedSchema = Option(metadata.get(SPARK_ROW_REQUESTED_SCHEMA))

    val sparkSchema =
      maybeSparkRequestedSchema
        .orElse(maybeSparkSchema)
        .map(ParquetTypesConverter.convertFromString)
        .getOrElse(
          ParquetTypesConverter.convertToAttributes(
            parquetRequestedSchema,
            isBinaryAsString = false,
            isInt96AsTimestamp = true))

    new SparkRowRecordMaterializer(parquetRequestedSchema, StructType.fromAttributes(sparkSchema))
  }

  /**
   * Removes those Parquet fields that are not requested.
   */
  private def pruneFields(
      schema: MessageType,
      requestedFieldNames: Set[String]): MessageType = {
    val requestedFields = schema.getFields.filter(f => requestedFieldNames.contains(f.getName))
    new MessageType("root", requestedFields: _*)
  }
}

private[parquet] class SparkRowRecordMaterializer(
    parquetSchema: MessageType,
    sparkSchema: StructType)
  extends RecordMaterializer[Row] {

  private val rootConverter = new CatalystRowConverter(parquetSchema, sparkSchema, NoopUpdater)

  override def getCurrentRecord: Row = rootConverter.currentRow

  override def getRootConverter: GroupConverter = rootConverter
}

/**
 * A [[ParentContainerUpdater]] is used by a Parquet converter to set converted values to some
 * corresponding parent container. For example, a converter for a `Struct` field may set converted
 * values to a [[MutableRow]]; or a converter for array element may append converted values to an
 * [[ArrayBuffer]].
 */
private[parquet] trait ParentContainerUpdater {
  def set(value: Any): Unit = ()
  def setBoolean(value: Boolean): Unit = set(value)
  def setByte(value: Byte): Unit = set(value)
  def setShort(value: Short): Unit = set(value)
  def setInt(value: Int): Unit = set(value)
  def setLong(value: Long): Unit = set(value)
  def setFloat(value: Float): Unit = set(value)
  def setDouble(value: Double): Unit = set(value)
}

/** A no-op updater used for root converter (who doesn't have a parent). */
private[parquet] object NoopUpdater extends ParentContainerUpdater

/**
 * This Parquet converter converts Parquet records to Spark SQL rows.
 *
 * @param parquetType Parquet type of Parquet records
 * @param sparkType A Spark SQL struct type that corresponds to the Parquet record type
 * @param updater An updater which takes care of the converted row object
 */
private[parquet] class CatalystRowConverter(
    parquetType: GroupType,
    sparkType: StructType,
    updater: ParentContainerUpdater)
  extends GroupConverter {

  /**
   * Updater used together with [[CatalystRowConverter]].
   *
   * @constructor Constructs a [[RowUpdater]] which sets converted filed values to the `ordinal`-th
   *              cell in `row`.
   */
  private final class RowUpdater(row: MutableRow, ordinal: Int) extends ParentContainerUpdater {
    override def set(value: Any): Unit = row(ordinal) = value
    override def setBoolean(value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(value: Float): Unit = row.setFloat(ordinal, value)
  }

  /**
   * Represents the converted row object once an entire Parquet record is converted.
   */
  val currentRow = new SpecificMutableRow(sparkType.map(_.dataType))

  // Converters for each field.
  private val fieldConverters: Array[Converter] = {
    parquetType.getFields.zip(sparkType).zipWithIndex.map {
      case ((parquetFieldType, sparkField), ordinal) =>
        newConverter(parquetFieldType, sparkField.dataType, new RowUpdater(currentRow, ordinal))
    }.toArray
  }

  override def getConverter(fieldIndex: Int): Converter = fieldConverters(fieldIndex)

  override def end(): Unit = updater.set(currentRow)

  override def start(): Unit = {
    var i = 0
    while (i < currentRow.length) {
      currentRow.setNullAt(i)
      i += 1
    }
  }

  /**
   * Creates a converter for the given Parquet type `parquetType` and Spark SQL data type
   * `sparkType`. Converted values are handled by `updater`.
   */
  private def newConverter(
      parquetType: Type,
      sparkType: DataType,
      updater: ParentContainerUpdater): Converter = {

    sparkType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType | BinaryType =>
        new CatalystPrimitiveConverter(updater)

      case ByteType =>
        new PrimitiveConverter {
          override def addInt(value: Int): Unit =
            updater.setByte(value.asInstanceOf[ByteType#JvmType])
        }

      case ShortType =>
        new PrimitiveConverter {
          override def addInt(value: Int): Unit =
            updater.setShort(value.asInstanceOf[ShortType#JvmType])
        }

      case t: DecimalType =>
        new CatalystDecimalConverter(t, updater)

      case StringType =>
        new CatalystStringConverter(updater)

      case TimestampType =>
        new PrimitiveConverter {
          override def addBinary(value: Binary): Unit = {
            updater.set(CatalystTimestampConverter.convertToTimestamp(value))
          }
        }

      case DateType =>
        new PrimitiveConverter {
          override def addInt(value: Int): Unit = {
            updater.setInt(value.asInstanceOf[DateType#JvmType])
          }
        }

      case t: ArrayType =>
        new CatalystArrayConverter(parquetType.asGroupType(), t, updater)

      case t: MapType =>
        new CatalystMapConverter(parquetType.asGroupType(), t, updater)

      case t: StructType =>
        new CatalystRowConverter(parquetType.asGroupType(), t, updater)

      case t: UserDefinedType[_] =>
        val sparkTypeForUDT = t.sqlType
        val parquetTypeForUDT = ParquetTypesConverter.fromDataType(sparkTypeForUDT, "<placeholder>")
        newConverter(parquetTypeForUDT, sparkTypeForUDT, updater)

      case _ =>
        throw new RuntimeException(
          s"Unable to create Parquet converter for data type ${sparkType.json}")
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Other converters
  /////////////////////////////////////////////////////////////////////////////

  /** Parquet converter for Parquet primitive types. */
  private final class CatalystPrimitiveConverter(updater: ParentContainerUpdater)
    extends PrimitiveConverter {

    override def addBoolean(value: Boolean): Unit = updater.setBoolean(value)
    override def addInt(value: Int): Unit = updater.setInt(value)
    override def addLong(value: Long): Unit = updater.setLong(value)
    override def addFloat(value: Float): Unit = updater.setFloat(value)
    override def addDouble(value: Double): Unit = updater.setDouble(value)
    override def addBinary(value: Binary): Unit = updater.set(value.getBytes)
  }

  /**
   * Parquet converter for strings. A dictionary is used to avoid unnecessary string decoding cost.
   */
  private final class CatalystStringConverter(updater: ParentContainerUpdater)
    extends PrimitiveConverter {

    private var expandedDictionary: Array[String] = null

    override def hasDictionarySupport: Boolean = true

    override def setDictionary(dictionary: Dictionary): Unit = {
      this.expandedDictionary = Array.tabulate(dictionary.getMaxId + 1) {
        dictionary.decodeToBinary(_).toStringUsingUTF8
      }
    }

    override def addValueFromDictionary(dictionaryId: Int): Unit = {
      updater.set(expandedDictionary(dictionaryId))
    }

    override def addBinary(value: Binary): Unit = updater.set(value.toStringUsingUTF8)
  }

  /**
   * Parquet converter for decimals.
   *
   * @todo Handle decimals stored as INT32 and INT64
   */
  private final class CatalystDecimalConverter(
      decimalType: DecimalType,
      updater: ParentContainerUpdater)
    extends PrimitiveConverter {

    override def addBinary(value: Binary): Unit = {
      updater.set(toDecimal(value))
    }

    private def toDecimal(value: Binary): Decimal = {
      val precision = decimalType.precision
      val scale = decimalType.scale
      val bytes = value.getBytes

      require(bytes.length <= 16, "Decimal field too large to read")

      var unscaled = 0L
      var i = 0

      while (i < bytes.length) {
        unscaled = (unscaled << 8) | (bytes(i) & 0xff)
        i += 1
      }

      val bits = 8 * bytes.length
      unscaled = (unscaled << (64 - bits)) >> (64 - bits)
      Decimal(unscaled, precision, scale)
    }
  }

  /** Parquet converter for arrays. */
  private final class CatalystArrayConverter(
      parquetSchema: GroupType,
      sparkSchema: ArrayType,
      updater: ParentContainerUpdater)
    extends GroupConverter {

    // TODO This is slow! Needs specialization.
    private val currentArray = ArrayBuffer.empty[Any]

    private val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = sparkSchema.elementType

      if (isElementType(repeatedType, elementType)) {
        newConverter(repeatedType, elementType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentArray += value
        })
      } else {
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = updater.set(currentArray)

    override def start(): Unit = currentArray.clear()

    private def isElementType(repeatedType: Type, elementType: DataType): Boolean = {
      (repeatedType, elementType) match {
        case (t: ParquetPrimitiveType, _) => true
        case (t: GroupType, _) if t.getFieldCount > 1 => true
        case (t: GroupType, StructType(Array(f))) if f.name == t.getFieldName(0) => true
        case _ => false
      }
    }

    /** Array element converter */
    private final class ElementConverter(
        parquetType: Type,
        sparkType: DataType)
      extends GroupConverter {

      private var currentElement: Any = _

      private val converter = newConverter(parquetType, sparkType, new ParentContainerUpdater {
        override def set(value: Any): Unit = currentElement = value
      })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = currentArray += currentElement

      override def start(): Unit = currentElement = null
    }
  }

  /** Parquet converter for maps */
  private final class CatalystMapConverter(
      parquetType: GroupType,
      sparkType: MapType,
      updater: ParentContainerUpdater)
    extends GroupConverter {

    private val currentMap = mutable.Map.empty[Any, Any]

    private val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        sparkType.keyType,
        sparkType.valueType)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit = updater.set(currentMap)

    override def start(): Unit = currentMap.clear()

    /** Parquet converter for key-value pairs within the map. */
    private final class KeyValueConverter(
        parquetKeyType: Type,
        parquetValueType: Type,
        sparkKeyType: DataType,
        sparkValueType: DataType)
      extends GroupConverter {

      private var currentKey: Any = _

      private var currentValue: Any = _

      private val converters = Array(
        // Converter for keys
        newConverter(parquetKeyType, sparkKeyType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentKey = value
        }),

        // Converter for values
        newConverter(parquetValueType, sparkValueType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentValue = value
        }))

      override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

      override def end(): Unit = currentMap(currentKey) = currentValue

      override def start(): Unit = {
        currentKey = null
        currentValue = null
      }
    }
  }
}
