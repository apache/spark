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

package org.apache.spark.sql.catalyst.json

import java.io.Writer

import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.VariantVal
import org.apache.spark.util.ArrayImplicits._

/**
 * `JackGenerator` can only be initialized with a `StructType`, a `MapType` or an `ArrayType`.
 * Once it is initialized with `StructType`, it can be used to write out a struct or an array of
 * struct. Once it is initialized with `MapType`, it can be used to write out a map or an array
 * of map. An exception will be thrown if trying to write out a struct if it is initialized with
 * a `MapType`, and vice verse.
 */
class JacksonGenerator(
    dataType: DataType,
    writer: Writer,
    options: JSONOptions) {
  // A `ValueWriter` is responsible for writing a field of an `InternalRow` to appropriate
  // JSON data. Here we are using `SpecializedGetters` rather than `InternalRow` so that
  // we can directly access data in `ArrayData` without the help of `SpecificMutableRow`.
  private type ValueWriter = (SpecializedGetters, Int) => Unit

  // `JackGenerator` can only be initialized with a `StructType`, a `MapType`, a `ArrayType` or a
  // `VariantType`.
  require(dataType.isInstanceOf[StructType] || dataType.isInstanceOf[MapType]
    || dataType.isInstanceOf[ArrayType] || dataType.isInstanceOf[VariantType],
    s"JacksonGenerator only supports to be initialized with a ${StructType.simpleString}, " +
      s"${MapType.simpleString}, ${ArrayType.simpleString} or ${VariantType.simpleString} but " +
      s"got ${dataType.catalogString}")

  // `ValueWriter`s for all fields of the schema
  private lazy val rootFieldWriters: Array[ValueWriter] = dataType match {
    case st: StructType => st.map(_.dataType).map(makeWriter).toArray
    case _ => throw QueryExecutionErrors.initialTypeNotTargetDataTypeError(
      dataType, StructType.simpleString)
  }

  // `ValueWriter` for array data storing rows of the schema.
  private lazy val arrElementWriter: ValueWriter = dataType match {
    case at: ArrayType => makeWriter(at.elementType)
    case _: StructType | _: MapType => makeWriter(dataType)
    case _ => throw QueryExecutionErrors.initialTypeNotTargetDataTypesError(dataType)
  }

  private lazy val mapElementWriter: ValueWriter = dataType match {
    case mt: MapType => makeWriter(mt.valueType)
    case _ => throw QueryExecutionErrors.initialTypeNotTargetDataTypeError(
      dataType, MapType.simpleString)
  }

  private val gen = {
    val generator = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
    if (options.pretty) {
      generator.setPrettyPrinter(
        new DefaultPrettyPrinter(PrettyPrinter.DEFAULT_SEPARATORS.withRootSeparator("")))
    }
    if (options.writeNonAsciiCharacterAsCodePoint) {
      generator.setHighestNonEscapedChar(0x7F)
    }
    generator
  }

  private val lineSeparator: String = options.lineSeparatorInWrite

  private val timestampFormatter = TimestampFormatter(
    options.timestampFormatInWrite,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInWrite,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false,
    forTimestampNTZ = true)
  private val dateFormatter = DateFormatter(
    options.dateFormatInWrite,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)

  private def makeWriter(dataType: DataType): ValueWriter = dataType match {
    case NullType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNull()

    case BooleanType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeBoolean(row.getBoolean(ordinal))

    case ByteType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getByte(ordinal))

    case ShortType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getShort(ordinal))

    case IntegerType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getInt(ordinal))

    case LongType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getLong(ordinal))

    case FloatType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getFloat(ordinal))

    case DoubleType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getDouble(ordinal))

    case _: StringType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeString(row.getUTF8String(ordinal).toString)

    case TimestampType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val timestampString = timestampFormatter.format(row.getLong(ordinal))
        gen.writeString(timestampString)

    case TimestampNTZType =>
      (row: SpecializedGetters, ordinal: Int) =>
      val timestampString =
        timestampNTZFormatter.format(DateTimeUtils.microsToLocalDateTime(row.getLong(ordinal)))
      gen.writeString(timestampString)

    case DateType =>
      (row: SpecializedGetters, ordinal: Int) =>
        val dateString = dateFormatter.format(row.getInt(ordinal))
        gen.writeString(dateString)

    case CalendarIntervalType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeString(row.getInterval(ordinal).toString)

    case YearMonthIntervalType(start, end) =>
      (row: SpecializedGetters, ordinal: Int) =>
        val ymString = IntervalUtils.toYearMonthIntervalString(
          row.getInt(ordinal),
          IntervalStringStyles.ANSI_STYLE,
          start,
          end)
        gen.writeString(ymString)

    case DayTimeIntervalType(start, end) =>
      (row: SpecializedGetters, ordinal: Int) =>
        val dtString = IntervalUtils.toDayTimeIntervalString(
          row.getLong(ordinal),
          IntervalStringStyles.ANSI_STYLE,
          start,
          end)
        gen.writeString(dtString)

    case BinaryType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeBinary(row.getBinary(ordinal))

    case dt: DecimalType =>
      (row: SpecializedGetters, ordinal: Int) =>
        gen.writeNumber(row.getDecimal(ordinal, dt.precision, dt.scale).toJavaBigDecimal)

    case st: StructType =>
      val fieldWriters = st.map(_.dataType).map(makeWriter)
      (row: SpecializedGetters, ordinal: Int) =>
        writeObject(writeFields(row.getStruct(ordinal, st.length), st, fieldWriters))

    case at: ArrayType =>
      val elementWriter = makeWriter(at.elementType)
      (row: SpecializedGetters, ordinal: Int) =>
        writeArray(writeArrayData(row.getArray(ordinal), elementWriter))

    case mt: MapType =>
      val valueWriter = makeWriter(mt.valueType)
      (row: SpecializedGetters, ordinal: Int) =>
        writeObject(writeMapData(row.getMap(ordinal), mt, valueWriter))

    case VariantType =>
      (row: SpecializedGetters, ordinal: Int) => write(row.getVariant(ordinal))

    // For UDT values, they should be in the SQL type's corresponding value type.
    // We should not see values in the user-defined class at here.
    // For example, VectorUDT's SQL type is an array of double. So, we should expect that v is
    // an ArrayData at here, instead of a Vector.
    case t: UserDefinedType[_] =>
      makeWriter(t.sqlType)

    case _ =>
      (row: SpecializedGetters, ordinal: Int) =>
        val v = row.get(ordinal, dataType)
        throw QueryExecutionErrors.failToConvertValueToJsonError(v, v.getClass, dataType)
  }

  private def writeObject(f: => Unit): Unit = {
    gen.writeStartObject()
    f
    gen.writeEndObject()
  }

  private def writeFields(
      row: InternalRow, schema: StructType, fieldWriters: Seq[ValueWriter]): Unit = {
    var i = 0
    while (i < row.numFields) {
      val field = schema(i)
      if (!row.isNullAt(i)) {
        gen.writeFieldName(field.name)
        fieldWriters(i).apply(row, i)
      } else if (!options.ignoreNullFields ||
        (options.writeNullIfWithDefaultValue && field.getExistenceDefaultValue().isDefined)) {
        gen.writeFieldName(field.name)
        gen.writeNull()
      }
      i += 1
    }
  }

  private def writeArray(f: => Unit): Unit = {
    gen.writeStartArray()
    f
    gen.writeEndArray()
  }

  private def writeArrayData(
      array: ArrayData, fieldWriter: ValueWriter): Unit = {
    var i = 0
    while (i < array.numElements()) {
      if (!array.isNullAt(i)) {
        fieldWriter.apply(array, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  private def writeMapData(
      map: MapData, mapType: MapType, fieldWriter: ValueWriter): Unit = {
    val keyArray = map.keyArray()
    val valueArray = map.valueArray()
    var i = 0
    while (i < map.numElements()) {
      gen.writeFieldName(keyArray.get(i, mapType.keyType).toString)
      if (!valueArray.isNullAt(i)) {
        fieldWriter.apply(valueArray, i)
      } else {
        gen.writeNull()
      }
      i += 1
    }
  }

  def close(): Unit = gen.close()

  def flush(): Unit = gen.flush()

  /**
   * Transforms a single `InternalRow` to JSON object using Jackson.
   * This api calling will be validated through accessing `rootFieldWriters`.
   *
   * @param row The row to convert
   */
  def write(row: InternalRow): Unit = {
    writeObject(writeFields(
      fieldWriters = rootFieldWriters.toImmutableArraySeq,
      row = row,
      schema = dataType.asInstanceOf[StructType]))
  }

  /**
   * Transforms multiple `InternalRow`s or `MapData`s to JSON array using Jackson
   *
   * @param array The array of rows or maps to convert
   */
  def write(array: ArrayData): Unit = writeArray(writeArrayData(array, arrElementWriter))

  /**
   * Transforms a single `MapData` to JSON object using Jackson
   * This api calling will will be validated through accessing `mapElementWriter`.
   *
   * @param map a map to convert
   */
  def write(map: MapData): Unit = {
    writeObject(writeMapData(
      fieldWriter = mapElementWriter,
      map = map,
      mapType = dataType.asInstanceOf[MapType]))
  }

  def write(v: VariantVal): Unit = {
    gen.writeRawValue(v.toJson(options.zoneId))
  }

  def writeLineEnding(): Unit = {
    // Note that JSON uses writer with UTF-8 charset. This string will be written out as UTF-8.
    gen.writeRaw(lineSeparator)
  }
}
