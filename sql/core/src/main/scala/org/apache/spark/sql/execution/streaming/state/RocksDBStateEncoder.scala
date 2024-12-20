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

package org.apache.spark.sql.execution.streaming.state

import java.io.ByteArrayOutputStream
import java.lang.Double.{doubleToRawLongBits, longBitsToDouble}
import java.lang.Float.{floatToRawIntBits, intBitsToFloat}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.streaming.StateStoreColumnFamilySchemaUtils
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider.{STATE_ENCODING_NUM_VERSION_BYTES, STATE_ENCODING_VERSION, VIRTUAL_COL_FAMILY_PREFIX_BYTES}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

sealed trait RocksDBKeyStateEncoder {
  def supportPrefixKeyScan: Boolean
  def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte]
  def encodeKey(row: UnsafeRow): Array[Byte]
  def decodeKey(keyBytes: Array[Byte]): UnsafeRow
  def getColumnFamilyIdBytes(): Array[Byte]
}

sealed trait RocksDBValueStateEncoder {
  def supportsMultipleValuesPerKey: Boolean
  def encodeValue(row: UnsafeRow): Array[Byte]
  def decodeValue(valueBytes: Array[Byte]): UnsafeRow
  def decodeValues(valueBytes: Array[Byte]): Iterator[UnsafeRow]
}

/**
 * The DataEncoder can encode UnsafeRows into raw bytes in two ways:
 *    - Using the direct byte layout of the UnsafeRow
 *    - Converting the UnsafeRow into an Avro row, and encoding that
 * In both of these cases, the raw bytes that are written into RockDB have
 * headers, footers and other metadata, but they also have data that is provided
 * by the callers. The metadata in each row does not need to be written as Avro or UnsafeRow,
 * but the actual data provided by the caller does.
 * The classes that use this trait require specialized partial encoding which makes them much
 * easier to cache and use, which is why each DataEncoder deals with multiple schemas.
 */
trait DataEncoder {
  /**
   * Encodes a complete key row into bytes. Used as the primary key for state lookups.
   *
   * @param row An UnsafeRow containing all key columns as defined in the keySchema
   * @return Serialized byte array representation of the key
   */
  def encodeKey(row: UnsafeRow): Array[Byte]

  /**
   * Encodes the non-prefix portion of a key row. Used with prefix scan and
   * range scan state lookups where the key is split into prefix and remaining portions.
   *
   * For prefix scans: Encodes columns after the prefix columns
   * For range scans: Encodes columns not included in the ordering columns
   *
   * @param row An UnsafeRow containing only the remaining key columns
   * @return Serialized byte array of the remaining key portion
   * @throws UnsupportedOperationException if called on an encoder that doesn't support split keys
   */
  def encodeRemainingKey(row: UnsafeRow): Array[Byte]

  /**
   * Encodes key columns used for range scanning, ensuring proper sort order in RocksDB.
   *
   * This method handles special encoding for numeric types to maintain correct sort order:
   * - Adds sign byte markers for numeric types
   * - Flips bits for negative floating point values
   * - Preserves null ordering
   *
   * @param row An UnsafeRow containing the columns needed for range scan
   *            (specified by orderingOrdinals)
   * @return Serialized bytes that will maintain correct sort order in RocksDB
   * @throws UnsupportedOperationException if called on an encoder that doesn't support range scans
   */
  def encodePrefixKeyForRangeScan(row: UnsafeRow): Array[Byte]

  /**
   * Encodes a value row into bytes.
   *
   * @param row An UnsafeRow containing the value columns as defined in the valueSchema
   * @return Serialized byte array representation of the value
   */
  def encodeValue(row: UnsafeRow): Array[Byte]

  /**
   * Decodes a complete key from its serialized byte form.
   *
   * For NoPrefixKeyStateEncoder: Decodes the entire key
   * For PrefixKeyScanStateEncoder: Decodes only the prefix portion
   *
   * @param bytes Serialized byte array containing the encoded key
   * @return UnsafeRow containing the decoded key columns
   * @throws UnsupportedOperationException for unsupported encoder types
   */
  def decodeKey(bytes: Array[Byte]): UnsafeRow

  /**
   * Decodes the remaining portion of a split key from its serialized form.
   *
   * For PrefixKeyScanStateEncoder: Decodes columns after the prefix
   * For RangeKeyScanStateEncoder: Decodes non-ordering columns
   *
   * @param bytes Serialized byte array containing the encoded remaining key portion
   * @return UnsafeRow containing the decoded remaining key columns
   * @throws UnsupportedOperationException if called on an encoder that doesn't support split keys
   */
  def decodeRemainingKey(bytes: Array[Byte]): UnsafeRow

  /**
   * Decodes range scan key bytes back into an UnsafeRow, preserving proper ordering.
   *
   * This method reverses the special encoding done by encodePrefixKeyForRangeScan:
   * - Interprets sign byte markers
   * - Reverses bit flipping for negative floating point values
   * - Handles null values
   *
   * @param bytes Serialized byte array containing the encoded range scan key
   * @return UnsafeRow containing the decoded range scan columns
   * @throws UnsupportedOperationException if called on an encoder that doesn't support range scans
   */
  def decodePrefixKeyForRangeScan(bytes: Array[Byte]): UnsafeRow

  /**
   * Decodes a value from its serialized byte form.
   *
   * @param bytes Serialized byte array containing the encoded value
   * @return UnsafeRow containing the decoded value columns
   */
  def decodeValue(bytes: Array[Byte]): UnsafeRow
}

abstract class RocksDBDataEncoder(
    keyStateEncoderSpec: KeyStateEncoderSpec,
    valueSchema: StructType) extends DataEncoder {

  val keySchema = keyStateEncoderSpec.keySchema
  val reusedKeyRow = new UnsafeRow(keyStateEncoderSpec.keySchema.length)
  val reusedValueRow = new UnsafeRow(valueSchema.length)

  // bit masks used for checking sign or flipping all bits for negative float/double values
  val floatFlipBitMask = 0xFFFFFFFF
  val floatSignBitMask = 0x80000000

  val doubleFlipBitMask = 0xFFFFFFFFFFFFFFFFL
  val doubleSignBitMask = 0x8000000000000000L

  // Byte markers used to identify whether the value is null, negative or positive
  // To ensure sorted ordering, we use the lowest byte value for negative numbers followed by
  // positive numbers and then null values.
  val negativeValMarker: Byte = 0x00.toByte
  val positiveValMarker: Byte = 0x01.toByte
  val nullValMarker: Byte = 0x02.toByte


  def unsupportedOperationForKeyStateEncoder(
      operation: String
  ): UnsupportedOperationException = {
    new UnsupportedOperationException(
      s"Method $operation not supported for encoder spec type " +
        s"${keyStateEncoderSpec.getClass.getSimpleName}")
  }

  /**
   * Encode the UnsafeRow of N bytes as a N+1 byte array.
   * @note This creates a new byte array and memcopies the UnsafeRow to the new array.
   */
  def encodeUnsafeRow(row: UnsafeRow): Array[Byte] = {
    val bytesToEncode = row.getBytes
    val encodedBytes = new Array[Byte](bytesToEncode.length + STATE_ENCODING_NUM_VERSION_BYTES)
    Platform.putByte(encodedBytes, Platform.BYTE_ARRAY_OFFSET, STATE_ENCODING_VERSION)
    // Platform.BYTE_ARRAY_OFFSET is the recommended way to memcopy b/w byte arrays. See Platform.
    Platform.copyMemory(
      bytesToEncode, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, Platform.BYTE_ARRAY_OFFSET + STATE_ENCODING_NUM_VERSION_BYTES,
      bytesToEncode.length)
    encodedBytes
  }

  def decodeToUnsafeRow(bytes: Array[Byte], numFields: Int): UnsafeRow = {
    if (bytes != null) {
      val row = new UnsafeRow(numFields)
      decodeToUnsafeRow(bytes, row)
    } else {
      null
    }
  }

  def decodeToUnsafeRow(bytes: Array[Byte], reusedRow: UnsafeRow): UnsafeRow = {
    if (bytes != null) {
      // Platform.BYTE_ARRAY_OFFSET is the recommended way refer to the 1st offset. See Platform.
      reusedRow.pointTo(
        bytes,
        Platform.BYTE_ARRAY_OFFSET + STATE_ENCODING_NUM_VERSION_BYTES,
        bytes.length - STATE_ENCODING_NUM_VERSION_BYTES)
      reusedRow
    } else {
      null
    }
  }
}

class UnsafeRowDataEncoder(
    keyStateEncoderSpec: KeyStateEncoderSpec,
    valueSchema: StructType) extends RocksDBDataEncoder(keyStateEncoderSpec, valueSchema) {

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    encodeUnsafeRow(row)
  }

  override def encodeRemainingKey(row: UnsafeRow): Array[Byte] = {
    encodeUnsafeRow(row)
  }

  override def encodePrefixKeyForRangeScan(row: UnsafeRow): Array[Byte] = {
    assert(keyStateEncoderSpec.isInstanceOf[RangeKeyScanStateEncoderSpec])
    val rsk = keyStateEncoderSpec.asInstanceOf[RangeKeyScanStateEncoderSpec]
    val rangeScanKeyFieldsWithOrdinal = rsk.orderingOrdinals.map { ordinal =>
      val field = rsk.keySchema(ordinal)
      (field, ordinal)
    }
    val writer = new UnsafeRowWriter(rsk.orderingOrdinals.length)
    writer.resetRowWriter()
    rangeScanKeyFieldsWithOrdinal.zipWithIndex.foreach { case (fieldWithOrdinal, idx) =>
      val field = fieldWithOrdinal._1
      val value = row.get(idx, field.dataType)
      // Note that we cannot allocate a smaller buffer here even if the value is null
      // because the effective byte array is considered variable size and needs to have
      // the same size across all rows for the ordering to work as expected.
      val bbuf = ByteBuffer.allocate(field.dataType.defaultSize + 1)
      bbuf.order(ByteOrder.BIG_ENDIAN)
      if (value == null) {
        bbuf.put(nullValMarker)
        writer.write(idx, bbuf.array())
      } else {
        field.dataType match {
          case BooleanType =>
          case ByteType =>
            val byteVal = value.asInstanceOf[Byte]
            val signCol = if (byteVal < 0) {
              negativeValMarker
            } else {
              positiveValMarker
            }
            bbuf.put(signCol)
            bbuf.put(byteVal)
            writer.write(idx, bbuf.array())

          case ShortType =>
            val shortVal = value.asInstanceOf[Short]
            val signCol = if (shortVal < 0) {
              negativeValMarker
            } else {
              positiveValMarker
            }
            bbuf.put(signCol)
            bbuf.putShort(shortVal)
            writer.write(idx, bbuf.array())

          case IntegerType =>
            val intVal = value.asInstanceOf[Int]
            val signCol = if (intVal < 0) {
              negativeValMarker
            } else {
              positiveValMarker
            }
            bbuf.put(signCol)
            bbuf.putInt(intVal)
            writer.write(idx, bbuf.array())

          case LongType =>
            val longVal = value.asInstanceOf[Long]
            val signCol = if (longVal < 0) {
              negativeValMarker
            } else {
              positiveValMarker
            }
            bbuf.put(signCol)
            bbuf.putLong(longVal)
            writer.write(idx, bbuf.array())

          case FloatType =>
            val floatVal = value.asInstanceOf[Float]
            val rawBits = floatToRawIntBits(floatVal)
            // perform sign comparison using bit manipulation to ensure NaN values are handled
            // correctly
            if ((rawBits & floatSignBitMask) != 0) {
              // for negative values, we need to flip all the bits to ensure correct ordering
              val updatedVal = rawBits ^ floatFlipBitMask
              bbuf.put(negativeValMarker)
              // convert the bits back to float
              bbuf.putFloat(intBitsToFloat(updatedVal))
            } else {
              bbuf.put(positiveValMarker)
              bbuf.putFloat(floatVal)
            }
            writer.write(idx, bbuf.array())

          case DoubleType =>
            val doubleVal = value.asInstanceOf[Double]
            val rawBits = doubleToRawLongBits(doubleVal)
            // perform sign comparison using bit manipulation to ensure NaN values are handled
            // correctly
            if ((rawBits & doubleSignBitMask) != 0) {
              // for negative values, we need to flip all the bits to ensure correct ordering
              val updatedVal = rawBits ^ doubleFlipBitMask
              bbuf.put(negativeValMarker)
              // convert the bits back to double
              bbuf.putDouble(longBitsToDouble(updatedVal))
            } else {
              bbuf.put(positiveValMarker)
              bbuf.putDouble(doubleVal)
            }
            writer.write(idx, bbuf.array())
        }
      }
    }
    encodeUnsafeRow(writer.getRow())
  }

  override def encodeValue(row: UnsafeRow): Array[Byte] = encodeUnsafeRow(row)

  override def decodeKey(bytes: Array[Byte]): UnsafeRow = {
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) =>
        decodeToUnsafeRow(bytes, reusedKeyRow)
      case PrefixKeyScanStateEncoderSpec(_, numColsPrefixKey) =>
        decodeToUnsafeRow(bytes, numFields = numColsPrefixKey)
      case _ => throw unsupportedOperationForKeyStateEncoder("decodeKey")
    }
  }

  override def decodeRemainingKey(bytes: Array[Byte]): UnsafeRow = {
    keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(_, numColsPrefixKey) =>
        decodeToUnsafeRow(bytes, numFields = numColsPrefixKey)
      case RangeKeyScanStateEncoderSpec(_, orderingOrdinals) =>
        decodeToUnsafeRow(bytes, keySchema.length - orderingOrdinals.length)
      case _ => throw unsupportedOperationForKeyStateEncoder("decodeRemainingKey")
    }
  }

  override def decodePrefixKeyForRangeScan(bytes: Array[Byte]): UnsafeRow = {
    assert(keyStateEncoderSpec.isInstanceOf[RangeKeyScanStateEncoderSpec])
    val rsk = keyStateEncoderSpec.asInstanceOf[RangeKeyScanStateEncoderSpec]
    val writer = new UnsafeRowWriter(rsk.orderingOrdinals.length)
    val rangeScanKeyFieldsWithOrdinal = rsk.orderingOrdinals.map { ordinal =>
      val field = rsk.keySchema(ordinal)
      (field, ordinal)
    }
    writer.resetRowWriter()
    val row = decodeToUnsafeRow(bytes, numFields = rsk.orderingOrdinals.length)
    rangeScanKeyFieldsWithOrdinal.zipWithIndex.foreach { case (fieldWithOrdinal, idx) =>
      val field = fieldWithOrdinal._1

      val value = row.getBinary(idx)
      val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      bbuf.order(ByteOrder.BIG_ENDIAN)
      val isNullOrSignCol = bbuf.get()
      if (isNullOrSignCol == nullValMarker) {
        // set the column to null and skip reading the next byte(s)
        writer.setNullAt(idx)
      } else {
        field.dataType match {
          case BooleanType =>
          case ByteType =>
            writer.write(idx, bbuf.get)

          case ShortType =>
            writer.write(idx, bbuf.getShort)

          case IntegerType =>
            writer.write(idx, bbuf.getInt)

          case LongType =>
            writer.write(idx, bbuf.getLong)

          case FloatType =>
            if (isNullOrSignCol == negativeValMarker) {
              // if the number is negative, get the raw binary bits for the float
              // and flip the bits back
              val updatedVal = floatToRawIntBits(bbuf.getFloat) ^ floatFlipBitMask
              writer.write(idx, intBitsToFloat(updatedVal))
            } else {
              writer.write(idx, bbuf.getFloat)
            }

          case DoubleType =>
            if (isNullOrSignCol == negativeValMarker) {
              // if the number is negative, get the raw binary bits for the double
              // and flip the bits back
              val updatedVal = doubleToRawLongBits(bbuf.getDouble) ^ doubleFlipBitMask
              writer.write(idx, longBitsToDouble(updatedVal))
            } else {
              writer.write(idx, bbuf.getDouble)
            }
        }
      }
    }
    writer.getRow()
  }

  override def decodeValue(bytes: Array[Byte]): UnsafeRow = decodeToUnsafeRow(bytes, reusedValueRow)
}

class AvroStateEncoder(
    keyStateEncoderSpec: KeyStateEncoderSpec,
    valueSchema: StructType) extends RocksDBDataEncoder(keyStateEncoderSpec, valueSchema)
    with Logging {

  private val avroEncoder = createAvroEnc(keyStateEncoderSpec, valueSchema)
  // Avro schema used by the avro encoders
  private lazy val keyAvroType: Schema = SchemaConverters.toAvroType(keySchema)
  private lazy val keyProj = UnsafeProjection.create(keySchema)

  private lazy val valueAvroType: Schema = SchemaConverters.toAvroType(valueSchema)
  private lazy val valueProj = UnsafeProjection.create(valueSchema)

  // Prefix Key schema and projection definitions used by the Avro Serializers
  // and Deserializers
  private lazy val prefixKeySchema = keyStateEncoderSpec match {
    case PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey) =>
      StructType(keySchema.take (numColsPrefixKey))
    case _ => throw unsupportedOperationForKeyStateEncoder("prefixKeySchema")
  }
  private lazy val prefixKeyAvroType = SchemaConverters.toAvroType(prefixKeySchema)
  private lazy val prefixKeyProj = UnsafeProjection.create(prefixKeySchema)

  // Range Key schema nd projection definitions used by the Avro Serializers and
  // Deserializers
  private lazy val rangeScanKeyFieldsWithOrdinal = keyStateEncoderSpec match {
    case RangeKeyScanStateEncoderSpec(keySchema, orderingOrdinals) =>
      orderingOrdinals.map { ordinal =>
        val field = keySchema(ordinal)
        (field, ordinal)
      }
    case _ =>
      throw unsupportedOperationForKeyStateEncoder("rangeScanKey")
  }

  private lazy val rangeScanAvroSchema = StateStoreColumnFamilySchemaUtils.convertForRangeScan(
    StructType(rangeScanKeyFieldsWithOrdinal.map(_._1).toArray))

  private lazy val rangeScanAvroType = SchemaConverters.toAvroType(rangeScanAvroSchema)

  private lazy val rangeScanAvroProjection = UnsafeProjection.create(rangeScanAvroSchema)

  // Existing remainder key schema definitions
  // Remaining Key schema and projection definitions used by the Avro Serializers
  // and Deserializers
  private lazy val remainingKeySchema = keyStateEncoderSpec match {
    case PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey) =>
      StructType(keySchema.drop(numColsPrefixKey))
    case RangeKeyScanStateEncoderSpec(keySchema, orderingOrdinals) =>
      StructType(0.until(keySchema.length).diff(orderingOrdinals).map(keySchema(_)))
    case _ => throw unsupportedOperationForKeyStateEncoder("remainingKeySchema")
  }

  private lazy val remainingKeyAvroType = SchemaConverters.toAvroType(remainingKeySchema)

  private lazy val remainingKeyAvroProjection = UnsafeProjection.create(remainingKeySchema)

  private def getAvroSerializer(schema: StructType): AvroSerializer = {
    val avroType = SchemaConverters.toAvroType(schema)
    new AvroSerializer(schema, avroType, nullable = false)
  }

  private def getAvroDeserializer(schema: StructType): AvroDeserializer = {
    val avroType = SchemaConverters.toAvroType(schema)
    val avroOptions = AvroOptions(Map.empty)
    new AvroDeserializer(avroType, schema,
      avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
      avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
  }

  /**
   * Creates an AvroEncoder that handles both key and value serialization/deserialization.
   * This method sets up the complete encoding infrastructure needed for state store operations.
   *
   * The encoder handles different key encoding specifications:
   * - NoPrefixKeyStateEncoderSpec: Simple key encoding without prefix
   * - PrefixKeyScanStateEncoderSpec: Keys with prefix for efficient scanning
   * - RangeKeyScanStateEncoderSpec: Keys with ordering requirements for range scans
   *
   * For prefix scan cases, it also creates separate encoders for the suffix portion of keys.
   *
   * @param keyStateEncoderSpec Specification for how to encode keys
   * @param valueSchema Schema for the values to be encoded
   * @return An AvroEncoder containing all necessary serializers and deserializers
   */
  private def createAvroEnc(
      keyStateEncoderSpec: KeyStateEncoderSpec,
      valueSchema: StructType): AvroEncoder = {
    val valueSerializer = getAvroSerializer(valueSchema)
    val valueDeserializer = getAvroDeserializer(valueSchema)

    // Get key schema based on encoder spec type
    val keySchema = keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(schema) =>
        schema
      case PrefixKeyScanStateEncoderSpec(schema, numColsPrefixKey) =>
        StructType(schema.take(numColsPrefixKey))
      case RangeKeyScanStateEncoderSpec(schema, orderingOrdinals) =>
        val remainingSchema = {
          0.until(schema.length).diff(orderingOrdinals).map { ordinal =>
            schema(ordinal)
          }
        }
        StructType(remainingSchema)
    }

    // Handle suffix key schema for prefix scan case
    val suffixKeySchema = keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(schema, numColsPrefixKey) =>
        Some(StructType(schema.drop(numColsPrefixKey)))
      case _ =>
        None
    }

    val keySerializer = getAvroSerializer(keySchema)
    val keyDeserializer = getAvroDeserializer(keySchema)

    // Create the AvroEncoder with all components
    AvroEncoder(
      keySerializer,
      keyDeserializer,
      valueSerializer,
      valueDeserializer,
      suffixKeySchema.map(getAvroSerializer),
      suffixKeySchema.map(getAvroDeserializer)
    )
  }

  /**
   * This method takes an UnsafeRow, and serializes to a byte array using Avro encoding.
   */
  def encodeUnsafeRowToAvro(
      row: UnsafeRow,
      avroSerializer: AvroSerializer,
      valueAvroType: Schema,
      out: ByteArrayOutputStream): Array[Byte] = {
    // InternalRow -> Avro.GenericDataRecord
    val avroData =
      avroSerializer.serialize(row)
    out.reset()
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    val writer = new GenericDatumWriter[Any](
      valueAvroType) // Defining Avro writer for this struct type
    writer.write(avroData, encoder) // Avro.GenericDataRecord -> byte array
    encoder.flush()
    out.toByteArray
  }

  /**
   * This method takes a byte array written using Avro encoding, and
   * deserializes to an UnsafeRow using the Avro deserializer
   */
  def decodeFromAvroToUnsafeRow(
      valueBytes: Array[Byte],
      avroDeserializer: AvroDeserializer,
      valueAvroType: Schema,
      valueProj: UnsafeProjection): UnsafeRow = {
    if (valueBytes != null) {
      val reader = new GenericDatumReader[Any](valueAvroType)
      val decoder = DecoderFactory.get().binaryDecoder(
        valueBytes, 0, valueBytes.length, null)
      // bytes -> Avro.GenericDataRecord
      val genericData = reader.read(null, decoder)
      // Avro.GenericDataRecord -> InternalRow
      val internalRow = avroDeserializer.deserialize(
        genericData).orNull.asInstanceOf[InternalRow]
      // InternalRow -> UnsafeRow
      valueProj.apply(internalRow)
    } else {
      null
    }
  }

  private val out = new ByteArrayOutputStream

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) =>
        encodeUnsafeRowToAvro(row, avroEncoder.keySerializer, keyAvroType, out)
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        encodeUnsafeRowToAvro(row, avroEncoder.keySerializer, prefixKeyAvroType, out)
      case _ => throw unsupportedOperationForKeyStateEncoder("encodeKey")
    }
  }

  override def encodeRemainingKey(row: UnsafeRow): Array[Byte] = {
    keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        encodeUnsafeRowToAvro(row, avroEncoder.suffixKeySerializer.get, remainingKeyAvroType, out)
      case RangeKeyScanStateEncoderSpec(_, _) =>
        encodeUnsafeRowToAvro(row, avroEncoder.keySerializer, remainingKeyAvroType, out)
      case _ => throw unsupportedOperationForKeyStateEncoder("encodeRemainingKey")
    }
  }

  /**
   * Encodes an UnsafeRow into an Avro-compatible byte array format for range scan operations.
   *
   * This method transforms row data into a binary format that preserves ordering when
   * used in range scans.
   * For each field in the row:
   * - A marker byte is written to indicate null status or sign (for numeric types)
   * - The value is written in big-endian format
   *
   * Special handling is implemented for:
   * - Null values: marked with nullValMarker followed by zero bytes
   * - Negative numbers: marked with negativeValMarker
   * - Floating point numbers: bit manipulation to handle sign and NaN values correctly
   *
   * @param row The UnsafeRow to encode
   * @param avroType The Avro schema defining the structure for encoding
   * @return Array[Byte] containing the Avro-encoded data that preserves ordering for range scans
   * @throws UnsupportedOperationException if a field's data type is not supported for range
   *                                       scan encoding
   */
  override def encodePrefixKeyForRangeScan(row: UnsafeRow): Array[Byte] = {
    val record = new GenericData.Record(rangeScanAvroType)
    var fieldIdx = 0
    rangeScanKeyFieldsWithOrdinal.zipWithIndex.foreach { case (fieldWithOrdinal, idx) =>
      val field = fieldWithOrdinal._1
      val value = row.get(idx, field.dataType)

      // Create marker byte buffer
      val markerBuffer = ByteBuffer.allocate(1)
      markerBuffer.order(ByteOrder.BIG_ENDIAN)

      if (value == null) {
        markerBuffer.put(nullValMarker)
        record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))
        record.put(fieldIdx + 1, ByteBuffer.wrap(new Array[Byte](field.dataType.defaultSize)))
      } else {
        field.dataType match {
          case BooleanType =>
            markerBuffer.put(positiveValMarker)
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))
            val valueBuffer = ByteBuffer.allocate(1)
            valueBuffer.put(if (value.asInstanceOf[Boolean]) 1.toByte else 0.toByte)
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case ByteType =>
            val byteVal = value.asInstanceOf[Byte]
            markerBuffer.put(if (byteVal < 0) negativeValMarker else positiveValMarker)
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))

            val valueBuffer = ByteBuffer.allocate(1)
            valueBuffer.order(ByteOrder.BIG_ENDIAN)
            valueBuffer.put(byteVal)
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case ShortType =>
            val shortVal = value.asInstanceOf[Short]
            markerBuffer.put(if (shortVal < 0) negativeValMarker else positiveValMarker)
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))

            val valueBuffer = ByteBuffer.allocate(2)
            valueBuffer.order(ByteOrder.BIG_ENDIAN)
            valueBuffer.putShort(shortVal)
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case IntegerType =>
            val intVal = value.asInstanceOf[Int]
            markerBuffer.put(if (intVal < 0) negativeValMarker else positiveValMarker)
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))

            val valueBuffer = ByteBuffer.allocate(4)
            valueBuffer.order(ByteOrder.BIG_ENDIAN)
            valueBuffer.putInt(intVal)
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case LongType =>
            val longVal = value.asInstanceOf[Long]
            markerBuffer.put(if (longVal < 0) negativeValMarker else positiveValMarker)
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))

            val valueBuffer = ByteBuffer.allocate(8)
            valueBuffer.order(ByteOrder.BIG_ENDIAN)
            valueBuffer.putLong(longVal)
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case FloatType =>
            val floatVal = value.asInstanceOf[Float]
            val rawBits = floatToRawIntBits(floatVal)
            markerBuffer.put(if ((rawBits & floatSignBitMask) != 0) {
              negativeValMarker
            } else {
              positiveValMarker
            })
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))

            val valueBuffer = ByteBuffer.allocate(4)
            valueBuffer.order(ByteOrder.BIG_ENDIAN)
            if ((rawBits & floatSignBitMask) != 0) {
              val updatedVal = rawBits ^ floatFlipBitMask
              valueBuffer.putFloat(intBitsToFloat(updatedVal))
            } else {
              valueBuffer.putFloat(floatVal)
            }
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case DoubleType =>
            val doubleVal = value.asInstanceOf[Double]
            val rawBits = doubleToRawLongBits(doubleVal)
            markerBuffer.put(if ((rawBits & doubleSignBitMask) != 0) {
              negativeValMarker
            } else {
              positiveValMarker
            })
            record.put(fieldIdx, ByteBuffer.wrap(markerBuffer.array()))

            val valueBuffer = ByteBuffer.allocate(8)
            valueBuffer.order(ByteOrder.BIG_ENDIAN)
            if ((rawBits & doubleSignBitMask) != 0) {
              val updatedVal = rawBits ^ doubleFlipBitMask
              valueBuffer.putDouble(longBitsToDouble(updatedVal))
            } else {
              valueBuffer.putDouble(doubleVal)
            }
            record.put(fieldIdx + 1, ByteBuffer.wrap(valueBuffer.array()))

          case _ => throw new UnsupportedOperationException(
            s"Range scan encoding not supported for data type: ${field.dataType}")
        }
      }
      fieldIdx += 2
    }

    out.reset()
    val writer = new GenericDatumWriter[GenericRecord](rangeScanAvroType)
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def encodeValue(row: UnsafeRow): Array[Byte] =
    encodeUnsafeRowToAvro(row, avroEncoder.valueSerializer, valueAvroType, out)

  override def decodeKey(bytes: Array[Byte]): UnsafeRow = {
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) =>
        decodeFromAvroToUnsafeRow(bytes, avroEncoder.keyDeserializer, keyAvroType, keyProj)
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        decodeFromAvroToUnsafeRow(
          bytes, avroEncoder.keyDeserializer, prefixKeyAvroType, prefixKeyProj)
      case _ => throw unsupportedOperationForKeyStateEncoder("decodeKey")
    }
  }


  override def decodeRemainingKey(bytes: Array[Byte]): UnsafeRow = {
    keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        decodeFromAvroToUnsafeRow(bytes,
          avroEncoder.suffixKeyDeserializer.get, remainingKeyAvroType, remainingKeyAvroProjection)
      case RangeKeyScanStateEncoderSpec(_, _) =>
        decodeFromAvroToUnsafeRow(
          bytes, avroEncoder.keyDeserializer, remainingKeyAvroType, remainingKeyAvroProjection)
      case _ => throw unsupportedOperationForKeyStateEncoder("decodeRemainingKey")
    }
  }

  /**
   * Decodes an Avro-encoded byte array back into an UnsafeRow for range scan operations.
   *
   * This method reverses the encoding process performed by encodePrefixKeyForRangeScan:
   * - Reads the marker byte to determine null status or sign
   * - Reconstructs the original values from big-endian format
   * - Handles special cases for floating point numbers by reversing bit manipulations
   *
   * The decoding process preserves the original data types and values, including:
   * - Null values marked by nullValMarker
   * - Sign information for numeric types
   * - Proper restoration of negative floating point values
   *
   * @param bytes The Avro-encoded byte array to decode
   * @param avroType The Avro schema defining the structure for decoding
   * @return UnsafeRow containing the decoded data
   * @throws UnsupportedOperationException if a field's data type is not supported for range
   *                                       scan decoding
   */
  override def decodePrefixKeyForRangeScan(bytes: Array[Byte]): UnsafeRow = {
    val reader = new GenericDatumReader[GenericRecord](rangeScanAvroType)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, 0, bytes.length, null)
    val record = reader.read(null, decoder)

    val rowWriter = new UnsafeRowWriter(rangeScanKeyFieldsWithOrdinal.length)
    rowWriter.resetRowWriter()

    var fieldIdx = 0
    rangeScanKeyFieldsWithOrdinal.zipWithIndex.foreach { case (fieldWithOrdinal, idx) =>
      val field = fieldWithOrdinal._1

      val markerBytes = record.get(fieldIdx).asInstanceOf[ByteBuffer].array()
      val markerBuf = ByteBuffer.wrap(markerBytes)
      markerBuf.order(ByteOrder.BIG_ENDIAN)
      val marker = markerBuf.get()

      if (marker == nullValMarker) {
        rowWriter.setNullAt(idx)
      } else {
        field.dataType match {
          case BooleanType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            rowWriter.write(idx, bytes(0) == 1)

          case ByteType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            val valueBuf = ByteBuffer.wrap(bytes)
            valueBuf.order(ByteOrder.BIG_ENDIAN)
            rowWriter.write(idx, valueBuf.get())

          case ShortType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            val valueBuf = ByteBuffer.wrap(bytes)
            valueBuf.order(ByteOrder.BIG_ENDIAN)
            rowWriter.write(idx, valueBuf.getShort())

          case IntegerType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            val valueBuf = ByteBuffer.wrap(bytes)
            valueBuf.order(ByteOrder.BIG_ENDIAN)
            rowWriter.write(idx, valueBuf.getInt())

          case LongType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            val valueBuf = ByteBuffer.wrap(bytes)
            valueBuf.order(ByteOrder.BIG_ENDIAN)
            rowWriter.write(idx, valueBuf.getLong())

          case FloatType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            val valueBuf = ByteBuffer.wrap(bytes)
            valueBuf.order(ByteOrder.BIG_ENDIAN)
            if (marker == negativeValMarker) {
              val floatVal = valueBuf.getFloat
              val updatedVal = floatToRawIntBits(floatVal) ^ floatFlipBitMask
              rowWriter.write(idx, intBitsToFloat(updatedVal))
            } else {
              rowWriter.write(idx, valueBuf.getFloat())
            }

          case DoubleType =>
            val bytes = record.get(fieldIdx + 1).asInstanceOf[ByteBuffer].array()
            val valueBuf = ByteBuffer.wrap(bytes)
            valueBuf.order(ByteOrder.BIG_ENDIAN)
            if (marker == negativeValMarker) {
              val doubleVal = valueBuf.getDouble
              val updatedVal = doubleToRawLongBits(doubleVal) ^ doubleFlipBitMask
              rowWriter.write(idx, longBitsToDouble(updatedVal))
            } else {
              rowWriter.write(idx, valueBuf.getDouble())
            }

          case _ => throw new UnsupportedOperationException(
            s"Range scan decoding not supported for data type: ${field.dataType}")
        }
      }
      fieldIdx += 2
    }

    rowWriter.getRow()
  }

  override def decodeValue(bytes: Array[Byte]): UnsafeRow =
    decodeFromAvroToUnsafeRow(
      bytes, avroEncoder.valueDeserializer, valueAvroType, valueProj)
}

abstract class RocksDBKeyStateEncoderBase(
    useColumnFamilies: Boolean,
    virtualColFamilyId: Option[Short] = None) extends RocksDBKeyStateEncoder {
  def offsetForColFamilyPrefix: Int =
    if (useColumnFamilies) VIRTUAL_COL_FAMILY_PREFIX_BYTES else 0

  val out = new ByteArrayOutputStream

  /**
   * Get Byte Array for the virtual column family id that is used as prefix for
   * key state rows.
   */
  override def getColumnFamilyIdBytes(): Array[Byte] = {
    assert(useColumnFamilies, "Cannot return virtual Column Family Id Bytes" +
      " because multiple Column is not supported for this encoder")
    val encodedBytes = new Array[Byte](VIRTUAL_COL_FAMILY_PREFIX_BYTES)
    Platform.putShort(encodedBytes, Platform.BYTE_ARRAY_OFFSET, virtualColFamilyId.get)
    encodedBytes
  }

  /**
   * Encode and put column family Id as a prefix to a pre-allocated byte array.
   *
   * @param numBytes - size of byte array to be created for storing key row (without
   *                 column family prefix)
   * @return Array[Byte] for an array byte to put encoded key bytes
   *         Int for a starting offset to put the encoded key bytes
   */
  protected def encodeColumnFamilyPrefix(numBytes: Int): (Array[Byte], Int) = {
    val encodedBytes = new Array[Byte](numBytes + offsetForColFamilyPrefix)
    var offset = Platform.BYTE_ARRAY_OFFSET
    if (useColumnFamilies) {
      Platform.putShort(encodedBytes, Platform.BYTE_ARRAY_OFFSET, virtualColFamilyId.get)
      offset = Platform.BYTE_ARRAY_OFFSET + offsetForColFamilyPrefix
    }
    (encodedBytes, offset)
  }

  /**
   * Get starting offset for decoding an encoded key byte array.
   */
  protected def decodeKeyStartOffset: Int = {
    if (useColumnFamilies) {
      Platform.BYTE_ARRAY_OFFSET + VIRTUAL_COL_FAMILY_PREFIX_BYTES
    } else Platform.BYTE_ARRAY_OFFSET
  }
}

/**
 * Factory object for creating state encoders used by RocksDB state store.
 *
 * The encoders created by this object handle serialization and deserialization of state data,
 * supporting both key and value encoding with various access patterns
 * (e.g., prefix scan, range scan).
 */
object RocksDBStateEncoder extends Logging {

  /**
   * Creates a key encoder based on the specified encoding strategy and configuration.
   *
   * @param dataEncoder The underlying encoder that handles the actual data encoding/decoding
   * @param keyStateEncoderSpec Specification defining the key encoding strategy
   *                            (no prefix, prefix scan, or range scan)
   * @param useColumnFamilies Whether to use RocksDB column families for storage
   * @param virtualColFamilyId Optional column family identifier when column families are enabled
   * @return A configured RocksDBKeyStateEncoder instance
   */
  def getKeyEncoder(
      dataEncoder: RocksDBDataEncoder,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      virtualColFamilyId: Option[Short] = None): RocksDBKeyStateEncoder = {
    keyStateEncoderSpec.toEncoder(dataEncoder, useColumnFamilies, virtualColFamilyId)
  }

  /**
   * Creates a value encoder that supports either single or multiple values per key.
   *
   * @param dataEncoder The underlying encoder that handles the actual data encoding/decoding
   * @param valueSchema Schema defining the structure of values to be encoded
   * @param useMultipleValuesPerKey If true, creates an encoder that can handle multiple values
   *                                per key; if false, creates an encoder for single values
   * @return A configured RocksDBValueStateEncoder instance
   */
  def getValueEncoder(
      dataEncoder: RocksDBDataEncoder,
      valueSchema: StructType,
      useMultipleValuesPerKey: Boolean): RocksDBValueStateEncoder = {
    if (useMultipleValuesPerKey) {
      new MultiValuedStateEncoder(dataEncoder, valueSchema)
    } else {
      new SingleValueStateEncoder(dataEncoder, valueSchema)
    }
  }

  /**
   * Encodes a virtual column family ID into a byte array suitable for RocksDB.
   *
   * This method creates a fixed-size byte array prefixed with the virtual column family ID,
   * which is used to partition data within RocksDB.
   *
   * @param virtualColFamilyId The column family identifier to encode
   * @return A byte array containing the encoded column family ID
   */
  def getColumnFamilyIdBytes(virtualColFamilyId: Short): Array[Byte] = {
    val encodedBytes = new Array[Byte](VIRTUAL_COL_FAMILY_PREFIX_BYTES)
    Platform.putShort(encodedBytes, Platform.BYTE_ARRAY_OFFSET, virtualColFamilyId)
    encodedBytes
  }
}

/**
 * RocksDB Key Encoder for UnsafeRow that supports prefix scan
 *
 * @param dataEncoder - the encoder that handles actual encoding/decoding of data
 * @param keySchema - schema of the key to be encoded
 * @param numColsPrefixKey - number of columns to be used for prefix key
 * @param useColumnFamilies - if column family is enabled for this encoder
 */
class PrefixKeyScanStateEncoder(
    dataEncoder: RocksDBDataEncoder,
    keySchema: StructType,
    numColsPrefixKey: Int,
    useColumnFamilies: Boolean = false,
    virtualColFamilyId: Option[Short] = None)
  extends RocksDBKeyStateEncoderBase(useColumnFamilies, virtualColFamilyId) with Logging {

  private val prefixKeyFieldsWithIdx: Seq[(StructField, Int)] = {
    keySchema.zipWithIndex.take(numColsPrefixKey)
  }

  private val remainingKeyFieldsWithIdx: Seq[(StructField, Int)] = {
    keySchema.zipWithIndex.drop(numColsPrefixKey)
  }

  private val prefixKeyProjection: UnsafeProjection = {
    val refs = prefixKeyFieldsWithIdx.map(x => BoundReference(x._2, x._1.dataType, x._1.nullable))
    UnsafeProjection.create(refs)
  }

  private val remainingKeyProjection: UnsafeProjection = {
    val refs = remainingKeyFieldsWithIdx.map(x =>
      BoundReference(x._2, x._1.dataType, x._1.nullable))
    UnsafeProjection.create(refs)
  }

  // This is quite simple to do - just bind sequentially, as we don't change the order.
  private val restoreKeyProjection: UnsafeProjection = UnsafeProjection.create(keySchema)

  // Reusable objects
  private val joinedRowOnKey = new JoinedRow()

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    val prefixKeyEncoded = dataEncoder.encodeKey(extractPrefixKey(row))
    val remainingEncoded = dataEncoder.encodeRemainingKey(remainingKeyProjection(row))

    val (encodedBytes, startingOffset) = encodeColumnFamilyPrefix(
      prefixKeyEncoded.length + remainingEncoded.length + 4
    )

    Platform.putInt(encodedBytes, startingOffset, prefixKeyEncoded.length)
    Platform.copyMemory(prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, startingOffset + 4, prefixKeyEncoded.length)
    // NOTE: We don't put the length of remainingEncoded as we can calculate later
    // on deserialization.
    Platform.copyMemory(remainingEncoded, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, startingOffset + 4 + prefixKeyEncoded.length,
      remainingEncoded.length)

    encodedBytes
  }

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    val prefixKeyEncodedLen = Platform.getInt(keyBytes, decodeKeyStartOffset)
    val prefixKeyEncoded = new Array[Byte](prefixKeyEncodedLen)
    Platform.copyMemory(keyBytes, decodeKeyStartOffset + 4,
      prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET, prefixKeyEncodedLen)

    // Here we calculate the remainingKeyEncodedLen leveraging the length of keyBytes
    val remainingKeyEncodedLen = keyBytes.length - 4 - prefixKeyEncodedLen -
      offsetForColFamilyPrefix

    val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
    Platform.copyMemory(keyBytes, decodeKeyStartOffset + 4 + prefixKeyEncodedLen,
      remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET, remainingKeyEncodedLen)

    val prefixKeyDecoded = dataEncoder.decodeKey(
      prefixKeyEncoded)
    val remainingKeyDecoded = dataEncoder.decodeRemainingKey(remainingKeyEncoded)

    restoreKeyProjection(joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded))
  }

  private def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    prefixKeyProjection(key)
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    val prefixKeyEncoded = dataEncoder.encodeKey(prefixKey)
    val (prefix, startingOffset) = encodeColumnFamilyPrefix(
      prefixKeyEncoded.length + 4
    )

    Platform.putInt(prefix, startingOffset, prefixKeyEncoded.length)
    Platform.copyMemory(prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET, prefix,
      startingOffset + 4, prefixKeyEncoded.length)
    prefix
  }

  override def supportPrefixKeyScan: Boolean = true
}

/**
 * RocksDB Key Encoder for UnsafeRow that supports range scan for fixed size fields
 *
 * To encode a row for range scan, we first project the orderingOrdinals from the oridinal
 * UnsafeRow into another UnsafeRow; we then rewrite that new UnsafeRow's fields in BIG_ENDIAN
 * to allow for scanning keys in sorted order using the byte-wise comparison method that
 * RocksDB uses.
 *
 * Then, for the rest of the fields, we project those into another UnsafeRow.
 * We then effectively join these two UnsafeRows together, and finally take those bytes
 * to get the resulting row.
 *
 * We cannot support variable sized fields in the range scan because the UnsafeRow format
 * stores variable sized fields as offset and length pointers to the actual values,
 * thereby changing the required ordering.
 *
 * Note that we also support "null" values being passed for these fixed size fields. We prepend
 * a single byte to indicate whether the column value is null or not. We cannot change the
 * nullability on the UnsafeRow itself as the expected ordering would change if non-first
 * columns are marked as null. If the first col is null, those entries will appear last in
 * the iterator. If non-first columns are null, ordering based on the previous columns will
 * still be honored. For rows with null column values, ordering for subsequent columns
 * will also be maintained within those set of rows. We use the same byte to also encode whether
 * the value is negative or not. For negative float/double values, we flip all the bits to ensure
 * the right lexicographical ordering. For the rationale around this, please check the link
 * here: https://en.wikipedia.org/wiki/IEEE_754#Design_rationale
 *
 * @param dataEncoder - the encoder that handles the actual encoding/decoding of data
 * @param keySchema - schema of the key to be encoded
 * @param orderingOrdinals - the ordinals for which the range scan is constructed
 * @param useColumnFamilies - if column family is enabled for this encoder
 */
class RangeKeyScanStateEncoder(
    dataEncoder: RocksDBDataEncoder,
    keySchema: StructType,
    orderingOrdinals: Seq[Int],
    useColumnFamilies: Boolean = false,
    virtualColFamilyId: Option[Short] = None)
  extends RocksDBKeyStateEncoderBase(useColumnFamilies, virtualColFamilyId) with Logging {

  private val rangeScanKeyFieldsWithOrdinal: Seq[(StructField, Int)] = {
    orderingOrdinals.map { ordinal =>
      val field = keySchema(ordinal)
      (field, ordinal)
    }
  }

  private def isFixedSize(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: BooleanType | _: ShortType | _: IntegerType | _: LongType |
      _: FloatType | _: DoubleType => true
    case _ => false
  }

  // verify that only fixed sized columns are used for ordering
  rangeScanKeyFieldsWithOrdinal.foreach { case (field, ordinal) =>
    if (!isFixedSize(field.dataType)) {
      // NullType is technically fixed size, but not supported for ordering
      if (field.dataType == NullType) {
        throw StateStoreErrors.nullTypeOrderingColsNotSupported(field.name, ordinal.toString)
      } else {
        throw StateStoreErrors.variableSizeOrderingColsNotSupported(field.name, ordinal.toString)
      }
    }
  }

  private val remainingKeyFieldsWithOrdinal: Seq[(StructField, Int)] = {
    0.to(keySchema.length - 1).diff(orderingOrdinals).map { ordinal =>
      val field = keySchema(ordinal)
      (field, ordinal)
    }
  }

  private val rangeScanKeyProjection: UnsafeProjection = {
    val refs = rangeScanKeyFieldsWithOrdinal.map(x =>
      BoundReference(x._2, x._1.dataType, x._1.nullable))
    UnsafeProjection.create(refs)
  }

  private val remainingKeyProjection: UnsafeProjection = {
    val refs = remainingKeyFieldsWithOrdinal.map(x =>
      BoundReference(x._2, x._1.dataType, x._1.nullable))
    UnsafeProjection.create(refs)
  }

  // The original schema that we might get could be:
  //    [foo, bar, baz, buzz]
  // We might order by bar and buzz, leading to:
  //    [bar, buzz, foo, baz]
  // We need to create a projection that sends, for example, the buzz at index 1 to index
  // 3. Thus, for every record in the original schema, we compute where it would be in
  // the joined row and created a projection based on that.
  private val restoreKeyProjection: UnsafeProjection = {
    val refs = keySchema.zipWithIndex.map { case (field, originalOrdinal) =>
      val ordinalInJoinedRow = if (orderingOrdinals.contains(originalOrdinal)) {
          orderingOrdinals.indexOf(originalOrdinal)
      } else {
          orderingOrdinals.length +
            remainingKeyFieldsWithOrdinal.indexWhere(_._2 == originalOrdinal)
      }

      BoundReference(ordinalInJoinedRow, field.dataType, field.nullable)
    }
    UnsafeProjection.create(refs)
  }

  // Reusable objects
  private val joinedRowOnKey = new JoinedRow()

  private def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    rangeScanKeyProjection(key)
  }

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    // This prefix key has the columns specified by orderingOrdinals
    val prefixKey = extractPrefixKey(row)
    val rangeScanKeyEncoded = dataEncoder.encodePrefixKeyForRangeScan(prefixKey)

    val result = if (orderingOrdinals.length < keySchema.length) {
      val remainingEncoded = dataEncoder.encodeRemainingKey(remainingKeyProjection(row))
      val (encodedBytes, startingOffset) = encodeColumnFamilyPrefix(
        rangeScanKeyEncoded.length + remainingEncoded.length + 4
      )

      Platform.putInt(encodedBytes, startingOffset,
        rangeScanKeyEncoded.length)
      Platform.copyMemory(rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        encodedBytes, startingOffset + 4, rangeScanKeyEncoded.length)
      // NOTE: We don't put the length of remainingEncoded as we can calculate later
      // on deserialization.
      Platform.copyMemory(remainingEncoded, Platform.BYTE_ARRAY_OFFSET,
        encodedBytes, startingOffset + 4 + rangeScanKeyEncoded.length,
        remainingEncoded.length)
      encodedBytes
    } else {
      // if the num of ordering cols is same as num of key schema cols, we don't need to
      // encode the remaining key as it's empty.
      val (encodedBytes, startingOffset) = encodeColumnFamilyPrefix(
        rangeScanKeyEncoded.length + 4
      )

      Platform.putInt(encodedBytes, startingOffset,
        rangeScanKeyEncoded.length)
      Platform.copyMemory(rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        encodedBytes, startingOffset + 4, rangeScanKeyEncoded.length)
      encodedBytes
    }
    result
  }

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    val prefixKeyEncodedLen = Platform.getInt(keyBytes, decodeKeyStartOffset)
    val prefixKeyEncoded = new Array[Byte](prefixKeyEncodedLen)
    Platform.copyMemory(keyBytes, decodeKeyStartOffset + 4,
      prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET, prefixKeyEncodedLen)

    val prefixKeyDecoded = dataEncoder.decodePrefixKeyForRangeScan(
      prefixKeyEncoded)

    if (orderingOrdinals.length < keySchema.length) {
      // Here we calculate the remainingKeyEncodedLen leveraging the length of keyBytes
      val remainingKeyEncodedLen = keyBytes.length - 4 -
        prefixKeyEncodedLen - offsetForColFamilyPrefix

      val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
      Platform.copyMemory(keyBytes, decodeKeyStartOffset + 4 + prefixKeyEncodedLen,
        remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        remainingKeyEncodedLen)

      val remainingKeyDecoded = dataEncoder.decodeRemainingKey(remainingKeyEncoded)

      val joined = joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded)
      val restored = restoreKeyProjection(joined)
      restored
    } else {
      // if the number of ordering cols is same as the number of key schema cols, we only
      // return the prefix key decoded unsafe row.
      prefixKeyDecoded
    }
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    val rangeScanKeyEncoded = dataEncoder.encodePrefixKeyForRangeScan(prefixKey)
    val (prefix, startingOffset) = encodeColumnFamilyPrefix(rangeScanKeyEncoded.length + 4)

    Platform.putInt(prefix, startingOffset, rangeScanKeyEncoded.length)
    Platform.copyMemory(rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      prefix, startingOffset + 4, rangeScanKeyEncoded.length)
    prefix
  }

  override def supportPrefixKeyScan: Boolean = true
}

/**
 * RocksDB Key Encoder for UnsafeRow that does not support prefix key scan.
 *
 * Encodes/decodes UnsafeRows to versioned byte arrays.
 * It uses the first byte of the generated byte array to store the version the describes how the
 * row is encoded in the rest of the byte array. Currently, the default version is 0,
 *
 * VERSION 0:  [ VERSION (1 byte) | ROW (N bytes) ]
 *    The bytes of a UnsafeRow is written unmodified to starting from offset 1
 *    (offset 0 is the version byte of value 0). That is, if the unsafe row has N bytes,
 *    then the generated array byte will be N+1 bytes.
 */
class NoPrefixKeyStateEncoder(
    dataEncoder: RocksDBDataEncoder,
    keySchema: StructType,
    useColumnFamilies: Boolean = false,
    virtualColFamilyId: Option[Short] = None)
  extends RocksDBKeyStateEncoderBase(useColumnFamilies, virtualColFamilyId) with Logging {

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    if (!useColumnFamilies) {
      dataEncoder.encodeKey(row)
    } else {
      val bytesToEncode = dataEncoder.encodeKey(row)
      val (encodedBytes, startingOffset) = encodeColumnFamilyPrefix(
        bytesToEncode.length +
          STATE_ENCODING_NUM_VERSION_BYTES
      )

      Platform.putByte(encodedBytes, startingOffset, STATE_ENCODING_VERSION)
      // Platform.BYTE_ARRAY_OFFSET is the recommended way to memcopy b/w byte arrays. See Platform.
      Platform.copyMemory(
        bytesToEncode, Platform.BYTE_ARRAY_OFFSET,
        encodedBytes, startingOffset + STATE_ENCODING_NUM_VERSION_BYTES, bytesToEncode.length)
      encodedBytes
    }
  }

  /**
   * Decode byte array for a key to a UnsafeRow.
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    if (useColumnFamilies) {
      if (keyBytes != null) {
        // Create new byte array without prefix
        val dataLength = keyBytes.length -
          STATE_ENCODING_NUM_VERSION_BYTES - VIRTUAL_COL_FAMILY_PREFIX_BYTES
        val dataBytes = new Array[Byte](dataLength)
        Platform.copyMemory(
          keyBytes,
          decodeKeyStartOffset + STATE_ENCODING_NUM_VERSION_BYTES,
          dataBytes,
          Platform.BYTE_ARRAY_OFFSET,
          dataLength)
        dataEncoder.decodeKey(dataBytes)
      } else {
        null
      }
    } else {
      dataEncoder.decodeKey(keyBytes)
    }
  }

  override def supportPrefixKeyScan: Boolean = false

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    throw new IllegalStateException("This encoder doesn't support prefix key!")
  }
}

/**
 * Supports encoding multiple values per key in RocksDB.
 * A single value is encoded in the format below, where first value is number of bytes
 * in actual encodedUnsafeRow followed by the encoded value itself.
 *
 * |---size(bytes)--|--unsafeRowEncodedBytes--|
 *
 * Multiple values are separated by a delimiter character.
 *
 * This encoder supports RocksDB StringAppendOperator merge operator. Values encoded can be
 * merged in RocksDB using merge operation, and all merged values can be read using decodeValues
 * operation.
 */
class MultiValuedStateEncoder(
    dataEncoder: RocksDBDataEncoder,
    valueSchema: StructType)
  extends RocksDBValueStateEncoder with Logging {

  override def encodeValue(row: UnsafeRow): Array[Byte] = {
    val bytes = dataEncoder.encodeValue(row)
    val numBytes = bytes.length

    val encodedBytes = new Array[Byte](java.lang.Integer.BYTES + bytes.length)
    Platform.putInt(encodedBytes, Platform.BYTE_ARRAY_OFFSET, numBytes)
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, java.lang.Integer.BYTES + Platform.BYTE_ARRAY_OFFSET, bytes.length)

    encodedBytes
  }

  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    if (valueBytes == null) {
      null
    } else {
      val numBytes = Platform.getInt(valueBytes, Platform.BYTE_ARRAY_OFFSET)
      val encodedValue = new Array[Byte](numBytes)
      Platform.copyMemory(valueBytes, java.lang.Integer.BYTES + Platform.BYTE_ARRAY_OFFSET,
        encodedValue, Platform.BYTE_ARRAY_OFFSET, numBytes)
      dataEncoder.decodeValue(encodedValue)
    }
  }

  override def decodeValues(valueBytes: Array[Byte]): Iterator[UnsafeRow] = {
    if (valueBytes == null) {
      Seq().iterator
    } else {
      new Iterator[UnsafeRow] {
        private var pos: Int = Platform.BYTE_ARRAY_OFFSET
        private val maxPos = Platform.BYTE_ARRAY_OFFSET + valueBytes.length

        override def hasNext: Boolean = {
          pos < maxPos
        }

        override def next(): UnsafeRow = {
          val numBytes = Platform.getInt(valueBytes, pos)

          pos += java.lang.Integer.BYTES
          val encodedValue = new Array[Byte](numBytes)
          Platform.copyMemory(valueBytes, pos,
            encodedValue, Platform.BYTE_ARRAY_OFFSET, numBytes)

          pos += numBytes
          pos += 1 // eat the delimiter character
          dataEncoder.decodeValue(encodedValue)
        }
      }
    }
  }

  override def supportsMultipleValuesPerKey: Boolean = true
}

/**
 * RocksDB Value Encoder for UnsafeRow that only supports single value.
 *
 * Encodes/decodes UnsafeRows to versioned byte arrays.
 * It uses the first byte of the generated byte array to store the version the describes how the
 * row is encoded in the rest of the byte array. Currently, the default version is 0,
 *
 * VERSION 0:  [ VERSION (1 byte) | ROW (N bytes) ]
 *    The bytes of a UnsafeRow is written unmodified to starting from offset 1
 *    (offset 0 is the version byte of value 0). That is, if the unsafe row has N bytes,
 *    then the generated array byte will be N+1 bytes.
 */
class SingleValueStateEncoder(
    dataEncoder: RocksDBDataEncoder,
    valueSchema: StructType)
  extends RocksDBValueStateEncoder {

  override def encodeValue(row: UnsafeRow): Array[Byte] = dataEncoder.encodeValue(row)

  /**
   * Decode byte array for a value to a UnsafeRow.
   *
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    dataEncoder.decodeValue(valueBytes)
  }

  override def supportsMultipleValuesPerKey: Boolean = false

  override def decodeValues(valueBytes: Array[Byte]): Iterator[UnsafeRow] = {
    throw new IllegalStateException("This encoder doesn't support multiple values!")
  }
}
