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

import java.nio.{ByteBuffer, ByteOrder}
import java.util

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}

import org.apache.spark.Logging
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificMutableRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.parquet.CatalystSchemaConverter.{MAX_PRECISION_FOR_INT32, MAX_PRECISION_FOR_INT64, minBytesForPrecision}
import org.apache.spark.sql.types._

private[parquet] class CatalystWriteSupport extends WriteSupport[InternalRow] with Logging {
  // A `ValueWriter` is responsible for writing a field of an `InternalRow` to the record consumer.
  // Here we are using `SpecializedGetters` rather than `InternalRow` so that we can directly access
  // data in `ArrayData` without the help of `SpecificMutableRow`.
  private type ValueWriter = (SpecializedGetters, Int) => Unit

  // Schema of the `InternalRow`s to be written
  private var schema: StructType = _

  // `ValueWriter`s for all fields of the schema
  private var rootFieldWriters: Seq[ValueWriter] = _

  // The Parquet `RecordConsumer` to which all `InternalRow`s are written
  private var recordConsumer: RecordConsumer = _

  // Whether we should write standard Parquet data conforming to parquet-format spec or not
  private var followParquetFormatSpec: Boolean = _

  // Reusable byte array used to write timestamps as Parquet INT96 values
  private val timestampBuffer = new Array[Byte](12)

  // Reusable byte array used to write decimal values
  private val decimalBuffer = new Array[Byte](minBytesForPrecision(DecimalType.MAX_PRECISION))

  override def init(configuration: Configuration): WriteContext = {
    val schemaString = configuration.get(CatalystWriteSupport.SPARK_ROW_SCHEMA)
    schema = StructType.fromString(schemaString)
    rootFieldWriters = schema.map(_.dataType).map(makeWriter)

    assert(configuration.get(SQLConf.PARQUET_FOLLOW_PARQUET_FORMAT_SPEC.key) != null)
    followParquetFormatSpec =
      configuration.getBoolean(
        SQLConf.PARQUET_FOLLOW_PARQUET_FORMAT_SPEC.key,
        SQLConf.PARQUET_FOLLOW_PARQUET_FORMAT_SPEC.defaultValue.get)

    val messageType = new CatalystSchemaConverter(configuration).convert(schema)
    val metadata = Map(CatalystReadSupport.SPARK_METADATA_KEY -> schemaString).asJava

    logDebug(
      s"""Initialized Parquet WriteSupport with Catalyst schema:
         |${schema.prettyJson}
         |and corresponding Parquet message type:
         |$messageType
       """.stripMargin)

    new WriteContext(messageType, metadata)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    this.recordConsumer = recordConsumer
  }

  override def write(row: InternalRow): Unit = {
    consumeMessage(writeFields(row, schema, rootFieldWriters))
  }

  private def writeFields(
      row: InternalRow, schema: StructType, fieldWriters: Seq[ValueWriter]): Unit = {
    var i = 0
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        consumeField(schema(i).name, i) {
          fieldWriters(i).apply(row, i)
        }
      }
      i += 1
    }
  }

  private def makeWriter(dataType: DataType): ValueWriter = {
    dataType match {
      case BooleanType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addBoolean(row.getBoolean(ordinal))

      case ByteType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addInteger(row.getByte(ordinal))

      case ShortType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addInteger(row.getShort(ordinal))

      case IntegerType | DateType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addInteger(row.getInt(ordinal))

      case LongType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addLong(row.getLong(ordinal))

      case FloatType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addFloat(row.getFloat(ordinal))

      case DoubleType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addDouble(row.getDouble(ordinal))

      case StringType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addBinary(Binary.fromByteArray(row.getUTF8String(ordinal).getBytes))

      case TimestampType =>
        // TODO Writes `TimestampType` values as `TIMESTAMP_MICROS` once parquet-mr implements it
        (row: SpecializedGetters, ordinal: Int) => {
          // Actually Spark SQL `TimestampType` only has microsecond precision.
          val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(row.getLong(ordinal))
          val buf = ByteBuffer.wrap(timestampBuffer)
          buf.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay)
          recordConsumer.addBinary(Binary.fromByteArray(timestampBuffer))
        }

      case BinaryType =>
        (row: SpecializedGetters, ordinal: Int) =>
          recordConsumer.addBinary(Binary.fromByteArray(row.getBinary(ordinal)))

      case DecimalType.Fixed(precision, scale) =>
        makeDecimalWriter(precision, scale)

      case t: StructType =>
        val fieldWriters = t.map(_.dataType).map(makeWriter)
        (row: SpecializedGetters, ordinal: Int) =>
          consumeGroup(writeFields(row.getStruct(ordinal, t.length), t, fieldWriters))

      case ArrayType(elementType, _) if followParquetFormatSpec =>
        makeThreeLevelArrayWriter(elementType, "list", "element")

      case ArrayType(elementType, true) if !followParquetFormatSpec =>
        makeThreeLevelArrayWriter(elementType, "bag", "array")

      case ArrayType(elementType, false) if !followParquetFormatSpec =>
        makeTwoLevelArrayWriter(elementType, "array")

      case t: MapType if followParquetFormatSpec =>
        makeMapWriter(t, "key_value")

      case t: MapType if !followParquetFormatSpec =>
        makeMapWriter(t, "map")

      case t: UserDefinedType[_] =>
        makeWriter(t.sqlType)

      case _ =>
        sys.error(s"Unsupported data type $dataType.")
    }
  }

  private def makeDecimalWriter(precision: Int, scale: Int): ValueWriter = {
    assert(
      precision <= DecimalType.MAX_PRECISION,
      s"Precision overflow: $precision is greater than ${DecimalType.MAX_PRECISION}")

    val numBytes = minBytesForPrecision(precision)

    val int32Writer =
      (row: SpecializedGetters, ordinal: Int) =>
        recordConsumer.addInteger(row.getDecimal(ordinal, precision, scale).toUnscaledLong.toInt)

    val int64Writer =
      (row: SpecializedGetters, ordinal: Int) =>
        recordConsumer.addLong(row.getDecimal(ordinal, precision, scale).toUnscaledLong)

    val binaryWriterUsingUnscaledLong =
      (row: SpecializedGetters, ordinal: Int) => {
        // When the precision is low enough (<= 18) to squeeze the decimal value into a `Long`, we
        // can build a fixed-length byte array with length `numBytes` using the unscaled `Long`
        // value and the `decimalBuffer` for better performance.
        val unscaled = row.getDecimal(ordinal, precision, scale).toUnscaledLong
        var i = 0
        var shift = 8 * (numBytes - 1)

        while (i < numBytes) {
          decimalBuffer(i) = (unscaled >> shift).toByte
          i += 1
          shift -= 8
        }

        recordConsumer.addBinary(Binary.fromByteArray(decimalBuffer, 0, numBytes))
      }

    val binaryWriterUsingUnscaledBytes =
      (row: SpecializedGetters, ordinal: Int) => {
        val decimal = row.getDecimal(ordinal, precision, scale)
        val bytes = decimal.toJavaBigDecimal.unscaledValue().toByteArray
        val fixedLengthBytes = if (bytes.length == numBytes) {
          // If the length of the underlying byte array of the unscaled `BigInteger` happens to be
          // `numBytes`, just reuse it, so that we don't bother copying it to `decimalBuffer`.
          bytes
        } else {
          // Otherwise, the length must be less than `numBytes`.  In this case we copy contents of
          // the underlying bytes with padding sign bytes to `decimalBuffer` to form the result
          // fixed-length byte array.
          val signByte = if (bytes.head < 0) -1: Byte else 0: Byte
          util.Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte)
          System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length)
          decimalBuffer
        }

        recordConsumer.addBinary(Binary.fromByteArray(fixedLengthBytes, 0, numBytes))
      }

    followParquetFormatSpec match {
      // Standard mode, writes decimals with precision <= 9 as INT32
      case true if precision <= MAX_PRECISION_FOR_INT32 => int32Writer

      // Standard mode, writes decimals with precision <= 18 as INT64
      case true if precision <= MAX_PRECISION_FOR_INT64 => int64Writer

      // Legacy mode, writes decimals with precision <= 18 as FIXED_LEN_BYTE_ARRAY
      case false if precision <= MAX_PRECISION_FOR_INT64 => binaryWriterUsingUnscaledLong

      // All other cases:
      //  - Standard mode, writes decimals with precision > 18 as FIXED_LEN_BYTE_ARRAY
      //  - Legacy mode, writes decimals with precision > 18 as FIXED_LEN_BYTE_ARRAY
      case _ => binaryWriterUsingUnscaledBytes
    }
  }

  private def makeThreeLevelArrayWriter(
      elementType: DataType, repeatedGroupName: String, elementFieldName: String): ValueWriter = {
    val elementWriter = makeWriter(elementType)

    (row: SpecializedGetters, ordinal: Int) => {
      consumeGroup {
        val array = row.getArray(ordinal)
        if (array.numElements() > 0) {
          consumeField(repeatedGroupName, 0) {
            var i = 0
            while (i < array.numElements()) {
              consumeGroup {
                if (!array.isNullAt(i)) {
                  consumeField(elementFieldName, 0)(elementWriter.apply(array, i))
                }
              }
              i += 1
            }
          }
        }
      }
    }
  }

  private def makeTwoLevelArrayWriter(
      elementType: DataType, repeatedFieldName: String): ValueWriter = {
    val elementWriter = makeWriter(elementType)

    (row: SpecializedGetters, ordinal: Int) => {
      consumeGroup {
        val array = row.getArray(ordinal)
        if (array.numElements() > 0) {
          consumeField(repeatedFieldName, 0) {
            var i = 0
            while (i < array.numElements()) {
              elementWriter.apply(array, i)
              i += 1
            }
          }
        }
      }
    }
  }

  private def makeMapWriter(mapType: MapType, repeatedGroupName: String): ValueWriter = {
    val keyType = mapType.keyType
    val valueType = mapType.valueType
    val keyWriter = makeWriter(keyType)
    val valueWriter = makeWriter(valueType)
    val mutableRow = new SpecificMutableRow(keyType :: valueType :: Nil)

    (row: SpecializedGetters, ordinal: Int) => {
      consumeGroup {
        val map = row.getMap(ordinal)
        if (map.numElements() > 0) {
          consumeField(repeatedGroupName, 0) {
            var i = 0
            while (i < map.numElements()) {
              consumeGroup {
                mutableRow.update(0, map.keyArray().get(i, keyType))
                consumeField("key", 0)(keyWriter.apply(mutableRow, 0))
                if (!map.valueArray().isNullAt(i)) {
                  mutableRow.update(1, map.valueArray().get(i, valueType))
                  consumeField("value", 1)(valueWriter.apply(mutableRow, 1))
                }
              }
              i += 1
            }
          }
        }
      }
    }
  }

  private def consumeMessage(f: => Unit): Unit = {
    recordConsumer.startMessage()
    f
    recordConsumer.endMessage()
  }

  private def consumeGroup(f: => Unit): Unit = {
    recordConsumer.startGroup()
    f
    recordConsumer.endGroup()
  }

  private def consumeField(field: String, index: Int)(f: => Unit): Unit = {
    recordConsumer.startField(field, index)
    f
    recordConsumer.endField(field, index)
  }
}

private[parquet] object CatalystWriteSupport {
  val SPARK_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.attributes"

  def setSchema(schema: StructType, configuration: Configuration): Unit = {
    schema.map(_.name).foreach(CatalystSchemaConverter.checkFieldName)
    configuration.set(SPARK_ROW_SCHEMA, schema.json)
    configuration.set(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
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
}
