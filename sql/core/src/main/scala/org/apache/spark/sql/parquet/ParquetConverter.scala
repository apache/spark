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
import java.util.{TimeZone, Calendar}

import scala.collection.mutable.{Buffer, ArrayBuffer, HashMap}

import jodd.datetime.JDateTime
import parquet.column.Dictionary
import parquet.io.api.{PrimitiveConverter, GroupConverter, Binary, Converter}
import parquet.schema.MessageType

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.CatalystConverter.FieldType
import org.apache.spark.sql.types._
import org.apache.spark.sql.parquet.timestamp.NanoTime

/**
 * Collection of converters of Parquet types (group and primitive types) that
 * model arrays and maps. The conversions are partly based on the AvroParquet
 * converters that are part of Parquet in order to be able to process these
 * types.
 *
 * There are several types of converters:
 * <ul>
 *   <li>[[org.apache.spark.sql.parquet.CatalystPrimitiveConverter]] for primitive
 *   (numeric, boolean and String) types</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystNativeArrayConverter]] for arrays
 *   of native JVM element types; note: currently null values are not supported!</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystArrayConverter]] for arrays of
 *   arbitrary element types (including nested element types); note: currently
 *   null values are not supported!</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystStructConverter]] for structs</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystMapConverter]] for maps; note:
 *   currently null values are not supported!</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystPrimitiveRowConverter]] for rows
 *   of only primitive element types</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystGroupConverter]] for other nested
 *   records, including the top-level row record</li>
 * </ul>
 */

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

  protected[parquet] def createConverter(
      field: FieldType,
      fieldIndex: Int,
      parent: CatalystConverter): Converter = {
    val fieldType: DataType = field.dataType
    fieldType match {
      case udt: UserDefinedType[_] => {
        createConverter(field.copy(dataType = udt.sqlType), fieldIndex, parent)
      }
      // For native JVM types we use a converter with native arrays
      case ArrayType(elementType: NativeType, false) => {
        new CatalystNativeArrayConverter(elementType, fieldIndex, parent)
      }
      // This is for other types of arrays, including those with nested fields
      case ArrayType(elementType: DataType, false) => {
        new CatalystArrayConverter(elementType, fieldIndex, parent)
      }
      case ArrayType(elementType: DataType, true) => {
        new CatalystArrayContainsNullConverter(elementType, fieldIndex, parent)
      }
      case StructType(fields: Array[StructField]) => {
        new CatalystStructConverter(fields, fieldIndex, parent)
      }
      case MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean) => {
        new CatalystMapConverter(
          Array(
            new FieldType(MAP_KEY_SCHEMA_NAME, keyType, false),
            new FieldType(MAP_VALUE_SCHEMA_NAME, valueType, valueContainsNull)),
          fieldIndex,
          parent)
      }
      // Strings, Shorts and Bytes do not have a corresponding type in Parquet
      // so we need to treat them separately
      case StringType =>
        new CatalystPrimitiveStringConverter(parent, fieldIndex)
      case ShortType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addInt(value: Int): Unit =
            parent.updateShort(fieldIndex, value.asInstanceOf[ShortType.JvmType])
        }
      }
      case ByteType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addInt(value: Int): Unit =
            parent.updateByte(fieldIndex, value.asInstanceOf[ByteType.JvmType])
        }
      }
      case DateType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addInt(value: Int): Unit =
            parent.updateDate(fieldIndex, value.asInstanceOf[DateType.JvmType])
        }
      }
      case d: DecimalType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addBinary(value: Binary): Unit =
            parent.updateDecimal(fieldIndex, value, d)
        }
      }
      case TimestampType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addBinary(value: Binary): Unit =
            parent.updateTimestamp(fieldIndex, value)
        }
      }
      // All other primitive types use the default converter
      case ctype: PrimitiveType => { // note: need the type tag here!
        new CatalystPrimitiveConverter(parent, fieldIndex)
      }
      case _ => throw new RuntimeException(
        s"unable to convert datatype ${field.dataType.toString} in CatalystConverter")
    }
  }

  protected[parquet] def createRootConverter(
      parquetSchema: MessageType,
      attributes: Seq[Attribute]): CatalystConverter = {
    // For non-nested types we use the optimized Row converter
    if (attributes.forall(a => ParquetTypesConverter.isPrimitiveType(a.dataType))) {
      new CatalystPrimitiveRowConverter(attributes.toArray)
    } else {
      new CatalystGroupConverter(attributes.toArray)
    }
  }
}

private[parquet] abstract class CatalystConverter extends GroupConverter {
  /**
   * The number of fields this group has
   */
  protected[parquet] val size: Int

  /**
   * The index of this converter in the parent
   */
  protected[parquet] val index: Int

  /**
   * The parent converter
   */
  protected[parquet] val parent: CatalystConverter

  /**
   * Called by child converters to update their value in its parent (this).
   * Note that if possible the more specific update methods below should be used
   * to avoid auto-boxing of native JVM types.
   *
   * @param fieldIndex
   * @param value
   */
  protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit

  protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateDate(fieldIndex: Int, value: Int): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateShort(fieldIndex: Int, value: Short): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateByte(fieldIndex: Int, value: Byte): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit =
    updateField(fieldIndex, value.getBytes)

  protected[parquet] def updateString(fieldIndex: Int, value: Array[Byte]): Unit =
    updateField(fieldIndex, UTF8String(value))

  protected[parquet] def updateTimestamp(fieldIndex: Int, value: Binary): Unit =
    updateField(fieldIndex, readTimestamp(value))

  protected[parquet] def updateDecimal(fieldIndex: Int, value: Binary, ctype: DecimalType): Unit =
    updateField(fieldIndex, readDecimal(new Decimal(), value, ctype))

  protected[parquet] def isRootConverter: Boolean = parent == null

  protected[parquet] def clearBuffer(): Unit

  /**
   * Should only be called in the root (group) converter!
   *
   * @return
   */
  def getCurrentRecord: Row = throw new UnsupportedOperationException

  /**
   * Read a decimal value from a Parquet Binary into "dest". Only supports decimals that fit in
   * a long (i.e. precision <= 18)
   */
  protected[parquet] def readDecimal(dest: Decimal, value: Binary, ctype: DecimalType): Unit = {
    val precision = ctype.precisionInfo.get.precision
    val scale = ctype.precisionInfo.get.scale
    val bytes = value.getBytes
    require(bytes.length <= 16, "Decimal field too large to read")
    var unscaled = 0L
    var i = 0
    while (i < bytes.length) {
      unscaled = (unscaled << 8) | (bytes(i) & 0xFF)
      i += 1
    }
    // Make sure unscaled has the right sign, by sign-extending the first bit
    val numBits = 8 * bytes.length
    unscaled = (unscaled << (64 - numBits)) >> (64 - numBits)
    dest.set(unscaled, precision, scale)
  }

  /**
   * Read a Timestamp value from a Parquet Int96Value
   */
  protected[parquet] def readTimestamp(value: Binary): Timestamp = {
    CatalystTimestampConverter.convertToTimestamp(value)
  }
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object.
 *
 * @param schema The corresponding Catalyst schema in the form of a list of attributes.
 */
private[parquet] class CatalystGroupConverter(
    protected[parquet] val schema: Array[FieldType],
    protected[parquet] val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var current: ArrayBuffer[Any],
    protected[parquet] var buffer: ArrayBuffer[Row])
  extends CatalystConverter {

  def this(schema: Array[FieldType], index: Int, parent: CatalystConverter) =
    this(
      schema,
      index,
      parent,
      current = null,
      buffer = new ArrayBuffer[Row](
        CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  /**
   * This constructor is used for the root converter only!
   */
  def this(attributes: Array[Attribute]) =
    this(attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)), 0, null)

  protected [parquet] val converters: Array[Converter] =
    schema.zipWithIndex.map {
      case (field, idx) => CatalystConverter.createConverter(field, idx, this)
    }.toArray

  override val size = schema.size

  override def getCurrentRecord: Row = {
    assert(isRootConverter, "getCurrentRecord should only be called in root group converter!")
    // TODO: use iterators if possible
    // Note: this will ever only be called in the root converter when the record has been
    // fully processed. Therefore it will be difficult to use mutable rows instead, since
    // any non-root converter never would be sure when it would be safe to re-use the buffer.
    new GenericRow(current.toArray)
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // for child converters to update upstream values
  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    current.update(fieldIndex, value)
  }

  override protected[parquet] def clearBuffer(): Unit = buffer.clear()

  override def start(): Unit = {
    current = ArrayBuffer.fill(size)(null)
    converters.foreach {
      converter => if (!converter.isPrimitive) {
        converter.asInstanceOf[CatalystConverter].clearBuffer
      }
    }
  }

  override def end(): Unit = {
    if (!isRootConverter) {
      assert(current != null) // there should be no empty groups
      buffer.append(new GenericRow(current.toArray))
      parent.updateField(index, new GenericRow(buffer.toArray.asInstanceOf[Array[Any]]))
    }
  }
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object. Note that his
 * converter is optimized for rows of primitive types (non-nested records).
 */
private[parquet] class CatalystPrimitiveRowConverter(
    protected[parquet] val schema: Array[FieldType],
    protected[parquet] var current: MutableRow)
  extends CatalystConverter {

  // This constructor is used for the root converter only
  def this(attributes: Array[Attribute]) =
    this(
      attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)),
      new SpecificMutableRow(attributes.map(_.dataType)))

  protected [parquet] val converters: Array[Converter] =
    schema.zipWithIndex.map {
      case (field, idx) => CatalystConverter.createConverter(field, idx, this)
    }.toArray

  override val size = schema.size

  override val index = 0

  override val parent = null

  // Should be only called in root group converter!
  override def getCurrentRecord: Row = current

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // for child converters to update upstream values
  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    throw new UnsupportedOperationException // child converters should use the
    // specific update methods below
  }

  override protected[parquet] def clearBuffer(): Unit = {}

  override def start(): Unit = {
    var i = 0
    while (i < size) {
      current.setNullAt(i)
      i = i + 1
    }
  }

  override def end(): Unit = {}

  // Overridden here to avoid auto-boxing for primitive types
  override protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit =
    current.setBoolean(fieldIndex, value)

  override protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit =
    current.setInt(fieldIndex, value)

  override protected[parquet] def updateDate(fieldIndex: Int, value: Int): Unit =
    current.update(fieldIndex, value)

  override protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit =
    current.setLong(fieldIndex, value)

  override protected[parquet] def updateShort(fieldIndex: Int, value: Short): Unit =
    current.setShort(fieldIndex, value)

  override protected[parquet] def updateByte(fieldIndex: Int, value: Byte): Unit =
    current.setByte(fieldIndex, value)

  override protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit =
    current.setDouble(fieldIndex, value)

  override protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit =
    current.setFloat(fieldIndex, value)

  override protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit =
    current.update(fieldIndex, value.getBytes)

  override protected[parquet] def updateString(fieldIndex: Int, value: Array[Byte]): Unit =
    current.update(fieldIndex, UTF8String(value))

  override protected[parquet] def updateTimestamp(fieldIndex: Int, value: Binary): Unit =
    current.update(fieldIndex, readTimestamp(value))

  override protected[parquet] def updateDecimal(
      fieldIndex: Int, value: Binary, ctype: DecimalType): Unit = {
    var decimal = current(fieldIndex).asInstanceOf[Decimal]
    if (decimal == null) {
      decimal = new Decimal
      current(fieldIndex) = decimal
    }
    readDecimal(decimal, value, ctype)
  }
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveConverter(
    parent: CatalystConverter,
    fieldIndex: Int) extends PrimitiveConverter {
  override def addBinary(value: Binary): Unit =
    parent.updateBinary(fieldIndex, value)

  override def addBoolean(value: Boolean): Unit =
    parent.updateBoolean(fieldIndex, value)

  override def addDouble(value: Double): Unit =
    parent.updateDouble(fieldIndex, value)

  override def addFloat(value: Float): Unit =
    parent.updateFloat(fieldIndex, value)

  override def addInt(value: Int): Unit =
    parent.updateInt(fieldIndex, value)

  override def addLong(value: Long): Unit =
    parent.updateLong(fieldIndex, value)
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet Binary to Catalyst String.
 * Supports dictionaries to reduce Binary to String conversion overhead.
 *
 * Follows pattern in Parquet of using dictionaries, where supported, for String conversion.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveStringConverter(parent: CatalystConverter, fieldIndex: Int)
  extends CatalystPrimitiveConverter(parent, fieldIndex) {

  private[this] var dict: Array[Array[Byte]] = null

  override def hasDictionarySupport: Boolean = true

  override def setDictionary(dictionary: Dictionary):Unit =
    dict = Array.tabulate(dictionary.getMaxId + 1) { dictionary.decodeToBinary(_).getBytes }

  override def addValueFromDictionary(dictionaryId: Int): Unit =
    parent.updateString(fieldIndex, dict(dictionaryId))

  override def addBinary(value: Binary): Unit =
    parent.updateString(fieldIndex, value.getBytes)
}

private[parquet] object CatalystArrayConverter {
  val INITIAL_ARRAY_SIZE = 20
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

/**
 * A `parquet.io.api.GroupConverter` that converts a single-element groups that
 * match the characteristics of an array (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.types.ArrayType]].
 *
 * @param elementType The type of the array elements (complex or primitive)
 * @param index The position of this (array) field inside its parent converter
 * @param parent The parent converter
 * @param buffer A data buffer
 */
private[parquet] class CatalystArrayConverter(
    val elementType: DataType,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var buffer: Buffer[Any])
  extends CatalystConverter {

  def this(elementType: DataType, index: Int, parent: CatalystConverter) =
    this(
      elementType,
      index,
      parent,
      new ArrayBuffer[Any](CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  protected[parquet] val converter: Converter = CatalystConverter.createConverter(
    new CatalystConverter.FieldType(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      elementType,
      false),
    fieldIndex=0,
    parent=this)

  override def getConverter(fieldIndex: Int): Converter = converter

  // arrays have only one (repeated) field, which is its elements
  override val size = 1

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    // fieldIndex is ignored (assumed to be zero but not checked)
    if(value == null) {
      throw new IllegalArgumentException("Null values inside Parquet arrays are not supported!")
    }
    buffer += value
  }

  override protected[parquet] def clearBuffer(): Unit = {
    buffer.clear()
  }

  override def start(): Unit = {
    if (!converter.isPrimitive) {
      converter.asInstanceOf[CatalystConverter].clearBuffer
    }
  }

  override def end(): Unit = {
    assert(parent != null)
    // here we need to make sure to use ArrayScalaType
    parent.updateField(index, buffer.toArray.toSeq)
    clearBuffer()
  }
}

/**
 * A `parquet.io.api.GroupConverter` that converts a single-element groups that
 * match the characteristics of an array (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.types.ArrayType]].
 *
 * @param elementType The type of the array elements (native)
 * @param index The position of this (array) field inside its parent converter
 * @param parent The parent converter
 * @param capacity The (initial) capacity of the buffer
 */
private[parquet] class CatalystNativeArrayConverter(
    val elementType: NativeType,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var capacity: Int = CatalystArrayConverter.INITIAL_ARRAY_SIZE)
  extends CatalystConverter {

  type NativeType = elementType.JvmType

  private var buffer: Array[NativeType] = elementType.classTag.newArray(capacity)

  private var elements: Int = 0

  protected[parquet] val converter: Converter = CatalystConverter.createConverter(
    new CatalystConverter.FieldType(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      elementType,
      false),
    fieldIndex=0,
    parent=this)

  override def getConverter(fieldIndex: Int): Converter = converter

  // arrays have only one (repeated) field, which is its elements
  override val size = 1

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit =
    throw new UnsupportedOperationException

  // Overridden here to avoid auto-boxing for primitive types
  override protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateShort(fieldIndex: Int, value: Short): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateByte(fieldIndex: Int, value: Byte): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.getBytes.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateString(fieldIndex: Int, value: Array[Byte]): Unit = {
    checkGrowBuffer()
    buffer(elements) = UTF8String(value).asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def clearBuffer(): Unit = {
    elements = 0
  }

  override def start(): Unit = {}

  override def end(): Unit = {
    assert(parent != null)
    // here we need to make sure to use ArrayScalaType
    parent.updateField(
      index,
      buffer.slice(0, elements).toSeq)
    clearBuffer()
  }

  private def checkGrowBuffer(): Unit = {
    if (elements >= capacity) {
      val newCapacity = 2 * capacity
      val tmp: Array[NativeType] = elementType.classTag.newArray(newCapacity)
      Array.copy(buffer, 0, tmp, 0, capacity)
      buffer = tmp
      capacity = newCapacity
    }
  }
}

/**
 * A `parquet.io.api.GroupConverter` that converts a single-element groups that
 * match the characteristics of an array contains null (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.types.ArrayType]].
 *
 * @param elementType The type of the array elements (complex or primitive)
 * @param index The position of this (array) field inside its parent converter
 * @param parent The parent converter
 * @param buffer A data buffer
 */
private[parquet] class CatalystArrayContainsNullConverter(
    val elementType: DataType,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var buffer: Buffer[Any])
  extends CatalystConverter {

  def this(elementType: DataType, index: Int, parent: CatalystConverter) =
    this(
      elementType,
      index,
      parent,
      new ArrayBuffer[Any](CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  protected[parquet] val converter: Converter = new CatalystConverter {

    private var current: Any = null

    val converter = CatalystConverter.createConverter(
      new CatalystConverter.FieldType(
        CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
        elementType,
        false),
      fieldIndex = 0,
      parent = this)

    override def getConverter(fieldIndex: Int): Converter = converter

    override def end(): Unit = parent.updateField(index, current)

    override def start(): Unit = {
      current = null
    }

    override protected[parquet] val size: Int = 1
    override protected[parquet] val index: Int = 0
    override protected[parquet] val parent = CatalystArrayContainsNullConverter.this

    override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
      current = value
    }

    override protected[parquet] def clearBuffer(): Unit = {}
  }

  override def getConverter(fieldIndex: Int): Converter = converter

  // arrays have only one (repeated) field, which is its elements
  override val size = 1

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    buffer += value
  }

  override protected[parquet] def clearBuffer(): Unit = {
    buffer.clear()
  }

  override def start(): Unit = {}

  override def end(): Unit = {
    assert(parent != null)
    // here we need to make sure to use ArrayScalaType
    parent.updateField(index, buffer.toArray.toSeq)
    clearBuffer()
  }
}

/**
 * This converter is for multi-element groups of primitive or complex types
 * that have repetition level optional or required (so struct fields).
 *
 * @param schema The corresponding Catalyst schema in the form of a list of
 *               attributes.
 * @param index
 * @param parent
 */
private[parquet] class CatalystStructConverter(
    override protected[parquet] val schema: Array[FieldType],
    override protected[parquet] val index: Int,
    override protected[parquet] val parent: CatalystConverter)
  extends CatalystGroupConverter(schema, index, parent) {

  override protected[parquet] def clearBuffer(): Unit = {}

  // TODO: think about reusing the buffer
  override def end(): Unit = {
    assert(!isRootConverter)
    // here we need to make sure to use StructScalaType
    // Note: we need to actually make a copy of the array since we
    // may be in a nested field
    parent.updateField(index, new GenericRow(current.toArray))
  }
}

/**
 * A `parquet.io.api.GroupConverter` that converts two-element groups that
 * match the characteristics of a map (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.types.MapType]].
 *
 * @param schema
 * @param index
 * @param parent
 */
private[parquet] class CatalystMapConverter(
    protected[parquet] val schema: Array[FieldType],
    override protected[parquet] val index: Int,
    override protected[parquet] val parent: CatalystConverter)
  extends CatalystConverter {

  private val map = new HashMap[Any, Any]()

  private val keyValueConverter = new CatalystConverter {
    private var currentKey: Any = null
    private var currentValue: Any = null
    val keyConverter = CatalystConverter.createConverter(schema(0), 0, this)
    val valueConverter = CatalystConverter.createConverter(schema(1), 1, this)

    override def getConverter(fieldIndex: Int): Converter = {
      if (fieldIndex == 0) keyConverter else valueConverter
    }

    override def end(): Unit = CatalystMapConverter.this.map += currentKey -> currentValue

    override def start(): Unit = {
      currentKey = null
      currentValue = null
    }

    override protected[parquet] val size: Int = 2
    override protected[parquet] val index: Int = 0
    override protected[parquet] val parent: CatalystConverter = CatalystMapConverter.this

    override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
      fieldIndex match {
        case 0 =>
          currentKey = value
        case 1 =>
          currentValue = value
        case _ =>
          new RuntimePermission(s"trying to update Map with fieldIndex $fieldIndex")
      }
    }

    override protected[parquet] def clearBuffer(): Unit = {}
  }

  override protected[parquet] val size: Int = 1

  override protected[parquet] def clearBuffer(): Unit = {}

  override def start(): Unit = {
    map.clear()
  }

  override def end(): Unit = {
    // here we need to make sure to use MapScalaType
    parent.updateField(index, map.toMap)
  }

  override def getConverter(fieldIndex: Int): Converter = keyValueConverter

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit =
    throw new UnsupportedOperationException
}
