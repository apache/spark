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
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.types._

private[parquet] object SparkRowReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"

  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  val SPARK_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.attributes"
}

private[parquet] class SparkRowReadSupport extends ReadSupport[Row] with Logging {
  override def init(context: InitContext): ReadContext = {
    val conf = context.getConfiguration
    val maybeRequestedSchema = Option(conf.get(SparkRowReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
    val maybeRowSchema = Option(conf.get(SparkRowReadSupport.SPARK_ROW_SCHEMA))

    val fullParquetSchema = context.getFileSchema
    val parquetRequestedSchema =
      maybeRequestedSchema.map { value =>
        val requestedFieldNames = StructType.fromString(value).fieldNames.toSet
        pruneFields(fullParquetSchema, requestedFieldNames)
      }.getOrElse(fullParquetSchema)

    val metadata =
      Map.empty[String, String] ++
        maybeRequestedSchema.map(SparkRowReadSupport.SPARK_ROW_REQUESTED_SCHEMA -> _) ++
        maybeRowSchema.map(SparkRowReadSupport.SPARK_ROW_SCHEMA -> _)

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

    val maybeSparkSchema = Option(metadata.get(SparkRowReadSupport.SPARK_METADATA_KEY))
    val maybeSparkRequestedSchema = Option(metadata.get(SparkRowReadSupport.SPARK_ROW_REQUESTED_SCHEMA))

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

private[parquet] class SparkRowRecordMaterializer(parquetSchema: MessageType, sparkSchema: StructType)
  extends RecordMaterializer[Row] {

  private val rootConverter = {
    val noopUpdater = new ParentContainerUpdater {}
    new StructConverter(parquetSchema, sparkSchema, noopUpdater)
  }

  override def getCurrentRecord: Row = rootConverter.currentRow

  override def getRootConverter: GroupConverter = rootConverter
}

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

private[parquet] class StructConverter(
    parquetType: GroupType,
    sparkType: StructType,
    updater: ParentContainerUpdater)
  extends GroupConverter {

  val currentRow = new SpecificMutableRow(sparkType.map(_.dataType))

  private val fieldConverters: Array[Converter] = {
    parquetType.getFields.zip(sparkType).zipWithIndex.map {
      case ((parquetFieldType, sparkField), ordinal) =>
        newConverter(parquetFieldType, sparkField.dataType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentRow(ordinal) = value
          override def setBoolean(value: Boolean): Unit = currentRow.setBoolean(ordinal, value)
          override def setByte(value: Byte): Unit = currentRow.setByte(ordinal, value)
          override def setShort(value: Short): Unit = currentRow.setShort(ordinal, value)
          override def setInt(value: Int): Unit = currentRow.setInt(ordinal, value)
          override def setLong(value: Long): Unit = currentRow.setLong(ordinal, value)
          override def setDouble(value: Double): Unit = currentRow.setDouble(ordinal, value)
          override def setFloat(value: Float): Unit = currentRow.setFloat(ordinal, value)
        })
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

  private def newConverter(
      parquetType: Type,
      sparkType: DataType,
      updater: ParentContainerUpdater): Converter = {

    sparkType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType | BinaryType =>
        new SimplePrimitiveConverter(updater)

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
        new DecimalConverter(t, updater)

      case StringType =>
        new StringConverter(updater)

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
        new ArrayConverter(parquetType.asGroupType(), t, updater)

      case t: MapType =>
        new MapConverter(parquetType.asGroupType(), t, updater)

      case t: StructType =>
        new StructConverter(parquetType.asGroupType(), t, updater)

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

  class SimplePrimitiveConverter(updater: ParentContainerUpdater) extends PrimitiveConverter {
    override def addBoolean(value: Boolean): Unit = updater.setBoolean(value)
    override def addInt(value: Int): Unit = updater.setInt(value)
    override def addLong(value: Long): Unit = updater.setLong(value)
    override def addFloat(value: Float): Unit = updater.setFloat(value)
    override def addDouble(value: Double): Unit = updater.setDouble(value)
    override def addBinary(value: Binary): Unit = updater.set(value.getBytes)
  }

  class StringConverter(updater: ParentContainerUpdater) extends PrimitiveConverter {
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

  class DecimalConverter(decimalType: DecimalType, updater: ParentContainerUpdater)
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

  class ArrayConverter(
      parquetSchema: GroupType,
      sparkSchema: ArrayType,
      updater: ParentContainerUpdater)
    extends GroupConverter {

    // TODO This is slow! Needs specialization.
    private val array = ArrayBuffer.empty[Any]

    private val elementConverter: Converter = {
      val repeatedType = parquetSchema.getType(0)
      val elementType = sparkSchema.elementType

      if (isElementType(repeatedType, elementType)) {
        newConverter(repeatedType, elementType, new ParentContainerUpdater {
          override def set(value: Any): Unit = array += value
        })
      } else {
        new ElementConverter(repeatedType.asGroupType().getType(0), elementType)
      }
    }

    override def getConverter(fieldIndex: Int): Converter = elementConverter

    override def end(): Unit = updater.set(array)

    override def start(): Unit = array.clear()

    private def isElementType(repeatedType: Type, elementType: DataType): Boolean = {
      (repeatedType, elementType) match {
        case (t: ParquetPrimitiveType, _) => true
        case (t: GroupType, _) if t.getFieldCount > 1 => true
        case (t: GroupType, StructType(Array(f))) if f.name == t.getFieldName(0) => true
        case _ => false
      }
    }

    /** Array element converter */
    private class ElementConverter(
        parquetType: Type,
        sparkType: DataType)
      extends GroupConverter {

      private var currentElement: Any = _

      private val converter = newConverter(parquetType, sparkType, new ParentContainerUpdater {
        override def set(value: Any): Unit = currentElement = value
      })

      override def getConverter(fieldIndex: Int): Converter = converter

      override def end(): Unit = array += currentElement

      override def start(): Unit = currentElement = null
    }
  }

  class MapConverter(
      parquetType: GroupType,
      sparkType: MapType,
      updater: ParentContainerUpdater)
    extends GroupConverter {

    private val map = mutable.Map.empty[Any, Any]

    private val keyValueConverter = {
      val repeatedType = parquetType.getType(0).asGroupType()
      new KeyValueConverter(
        repeatedType.getType(0),
        repeatedType.getType(1),
        sparkType.keyType,
        sparkType.valueType)
    }

    override def getConverter(fieldIndex: Int): Converter = keyValueConverter

    override def end(): Unit = updater.set(map)

    override def start(): Unit = map.clear()

    private class KeyValueConverter(
        parquetKeyType: Type,
        parquetValueType: Type,
        sparkKeyType: DataType,
        sparkValueType: DataType)
      extends GroupConverter {

      private var currentKey: Any = _

      private var currentValue: Any = _

      private val keyConverter =
        newConverter(parquetKeyType, sparkKeyType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentKey = value
        })

      private val valueConverter =
        newConverter(parquetValueType, sparkValueType, new ParentContainerUpdater {
          override def set(value: Any): Unit = currentValue = value
        })

      override def getConverter(fieldIndex: Int): Converter = {
        if (fieldIndex == 0) keyConverter else valueConverter
      }

      override def end(): Unit = map(currentKey) = currentValue

      override def start(): Unit = {
        currentKey = null
        currentValue = null
      }
    }
  }
}
