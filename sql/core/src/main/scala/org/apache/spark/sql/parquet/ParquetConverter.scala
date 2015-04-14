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

import java.sql.Timestamp
import java.util.{Calendar, TimeZone}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import jodd.datetime.JDateTime
import parquet.column.Dictionary
import parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import parquet.schema.{GroupType, PrimitiveType => ParquetPrimitiveType, Type}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.timestamp.NanoTime
import org.apache.spark.sql.types._

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
 * @param sparkType Spark SQL schema that corresponds to the Parquet record type
 * @param updater An updater which takes care of the converted row object
 */
private[parquet] class CatalystRowConverter(
    parquetType: GroupType,
    sparkType: StructType,
    updater: ParentContainerUpdater)
  extends GroupConverter {

  /**
   * Updater used together with [[CatalystRowConverter]].  It sets converted filed values to the
   * `ordinal`-th cell in `row`.
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

  /** Represents the converted row object once an entire Parquet record is converted. */
  val currentRow = new SpecificMutableRow(sparkType.map(_.dataType))

  // Converters for each field.
  private val fieldConverters: Array[Converter] = {
    parquetType.getFields.zip(sparkType).zipWithIndex.map {
      case ((parquetFieldType, sparkField), ordinal) =>
        // Converted field value should be set to the `ordinal`-th cell of `currentRow`
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
            // DateType is not specialized in `SpecificMutableRow`, have to box it here.
            updater.set(value.asInstanceOf[DateType#JvmType])
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

  /**
   * Parquet converter for Parquet primitive types.  Note that not all Spark SQL primitive types
   * are handled by this converter.  Parquet primitive types are only a subset of those of Spark
   * SQL.  For example, BYTE, SHORT and INT in Spark SQL are all covered by INT32 in Parquet.
   */
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
   * Parquet converter for strings. A dictionary is used to minimize string decoding cost.
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
   * Parquet converter for fixed-precision decimals.
   *
   * @todo Handle fixed-precision decimals stored as INT32 and INT64
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

  /**
   * Parquet converter for arrays.  Spark SQL arrays are represented as Parquet lists.  Standard
   * Parquet lists are represented as a 3-level group annotated by `LIST`:
   * {{{
   *   <list-repetition> group <name> (LIST) {            <-- parquetSchema points here
   *     repeated group list {
   *       <element-repetition> <element-type> element;
   *     }
   *   }
   * }}}
   * The `parquetSchema` constructor argument points to the outermost group.
   *
   * However, before this representation is standardized, some Parquet libraries/tools also use some
   * non-standard formats to represent list-like structures.  Backwards-compatibility rules for
   * handling these cases are described in Parquet format spec.
   *
   * @see https://github.com/apache/incubator-parquet-format/blob/master/LogicalTypes.md#lists
   */
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

    // scalastyle:off
    /**
     * Returns whether the given type is the element type of a list or is a syntactic group with
     * one field that is the element type.  This is determined by checking whether the type can be
     * a syntactic group and by checking whether a potential syntactic group matches the expected
     * schema.
     * {{{
     *   <list-repetition> group <name> (LIST) {
     *     repeated group list {                          <-- repeatedType points here
     *       <element-repetition> <element-type> element;
     *     }
     *   }
     * }}}
     * In short, here we handle Parquet list backwards-compatibility rules on the read path.  This
     * method is based on `AvroIndexedRecordConverter.isElementType`.
     *
     * @see https://github.com/apache/incubator-parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
     */
    // scalastyle:on
    private def isElementType(repeatedType: Type, elementType: DataType): Boolean = {
      (repeatedType, elementType) match {
        case (t: ParquetPrimitiveType, _) => true
        case (t: GroupType, _) if t.getFieldCount > 1 => true
        case (t: GroupType, StructType(Array(f))) if f.name == t.getFieldName(0) => true
        case _ => false
      }
    }

    /** Array element converter */
    private final class ElementConverter(parquetType: Type, sparkType: DataType)
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

private[sql] object CatalystConverter {
  // The type internally used for fields
  type FieldType = StructField

  // This is mostly Parquet convention (see, e.g., `ConversionPatterns`).
  // Note that "array" for the array elements is chosen by ParquetAvro.
  // Using a different value will result in Parquet silently dropping columns.
  val ARRAY_CONTAINS_NULL_BAG_SCHEMA_NAME = "bag"
  val ARRAY_ELEMENTS_SCHEMA_NAME = "array"
  // SPARK-4520: Thrift generated parquet files have different array element
  // schema names than avro. Thrift parquet uses array_schema_name + "_tuple"
  // as opposed to "array" used by default. For more information, check
  // TestThriftSchemaConverter.java in parquet.thrift.
  val THRIFT_ARRAY_ELEMENTS_SCHEMA_NAME_SUFFIX = "_tuple"
  val MAP_KEY_SCHEMA_NAME = "key"
  val MAP_VALUE_SCHEMA_NAME = "value"
  val MAP_SCHEMA_NAME = "map"

  // TODO: consider using Array[T] for arrays to avoid boxing of primitive types
  type ArrayScalaType[T] = Seq[T]
  type StructScalaType[T] = Row
  type MapScalaType[K, V] = Map[K, V]
}

private[parquet] object CatalystTimestampConverter {
  // TODO most part of this comes from Hive-0.14
  // Hive code might have some issues, so we need to keep an eye on it.
  // Also we use NanoTime and Int96Values from parquet-examples.
  // We utilize jodd to convert between NanoTime and Timestamp
  val parquetTsCalendar = new ThreadLocal[Calendar]
  def getCalendar: Calendar = {
    // this is a cache for the calendar instance.
    if (parquetTsCalendar.get == null) {
      parquetTsCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")))
    }
    parquetTsCalendar.get
  }
  val NANOS_PER_SECOND: Long = 1000000000
  val SECONDS_PER_MINUTE: Long = 60
  val MINUTES_PER_HOUR: Long = 60
  val NANOS_PER_MILLI: Long = 1000000

  def convertToTimestamp(value: Binary): Timestamp = {
    val nt = NanoTime.fromBinary(value)
    val timeOfDayNanos = nt.getTimeOfDayNanos
    val julianDay = nt.getJulianDay
    val jDateTime = new JDateTime(julianDay.toDouble)
    val calendar = getCalendar
    calendar.set(Calendar.YEAR, jDateTime.getYear)
    calendar.set(Calendar.MONTH, jDateTime.getMonth - 1)
    calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay)

    // written in command style
    var remainder = timeOfDayNanos
    calendar.set(
      Calendar.HOUR_OF_DAY,
      (remainder / (NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR)).toInt)
    remainder = remainder % (NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR)
    calendar.set(
      Calendar.MINUTE, (remainder / (NANOS_PER_SECOND * SECONDS_PER_MINUTE)).toInt)
    remainder = remainder % (NANOS_PER_SECOND * SECONDS_PER_MINUTE)
    calendar.set(Calendar.SECOND, (remainder / NANOS_PER_SECOND).toInt)
    val nanos = remainder % NANOS_PER_SECOND
    val ts = new Timestamp(calendar.getTimeInMillis)
    ts.setNanos(nanos.toInt)
    ts
  }

  def convertFromTimestamp(ts: Timestamp): Binary = {
    val calendar = getCalendar
    calendar.setTime(ts)
    val jDateTime = new JDateTime(calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH))
    // Hive-0.14 didn't set hour before get day number, while the day number should
    // has something to do with hour, since julian day number grows at 12h GMT
    // here we just follow what hive does.
    val julianDay = jDateTime.getJulianDayNumber

    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    val minute = calendar.get(Calendar.MINUTE)
    val second = calendar.get(Calendar.SECOND)
    val nanos = ts.getNanos
    // Hive-0.14 would use hours directly, that might be wrong, since the day starts
    // from 12h in Julian. here we just follow what hive does.
    val nanosOfDay = nanos + second * NANOS_PER_SECOND +
      minute * NANOS_PER_SECOND * SECONDS_PER_MINUTE +
      hour * NANOS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR
    NanoTime(julianDay, nanosOfDay).toBinary
  }
}
