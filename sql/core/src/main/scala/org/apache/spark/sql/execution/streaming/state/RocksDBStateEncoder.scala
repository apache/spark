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

import scala.collection.mutable

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, StateStoreColumnFamilySchemaUtils}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider.{SCHEMA_ID_PREFIX_BYTES, STATE_ENCODING_NUM_VERSION_BYTES, STATE_ENCODING_VERSION}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

sealed trait RocksDBKeyStateEncoder {
  def supportPrefixKeyScan: Boolean
  def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte]
  def encodeKey(row: UnsafeRow): Array[Byte]
  def decodeKey(keyBytes: Array[Byte]): UnsafeRow
}

sealed trait RocksDBValueStateEncoder {
  def supportsMultipleValuesPerKey: Boolean
  def encodeValue(row: UnsafeRow): Array[Byte]
  def decodeValue(valueBytes: Array[Byte]): UnsafeRow
  def decodeValues(valueBytes: Array[Byte]): Iterator[UnsafeRow]
}

trait StateSchemaProvider extends Serializable {
  def getSchemaMetadataValue(key: StateSchemaMetadataKey): StateSchemaMetadataValue

  def getCurrentStateSchemaId(colFamilyName: String, isKey: Boolean): Short
}

// Test implementation that can be dynamically updated
class TestStateSchemaProvider extends StateSchemaProvider {
  private val schemas = mutable.Map.empty[StateSchemaMetadataKey, StateSchemaMetadataValue]

  /**
   * Captures a new schema pair (key schema and value schema) for a column family.
   * Each capture creates two entries - one for the key schema and one for the value schema.
   *
   * @param colFamilyName Name of the column family
   * @param keySchema Spark SQL schema for the key
   * @param valueSchema Spark SQL schema for the value
   * @param keySchemaId Schema ID for the key, defaults to 0
   * @param valueSchemaId Schema ID for the value, defaults to 0
   */
  def captureSchema(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keySchemaId: Short = 0,
      valueSchemaId: Short = 0): Unit = {
    schemas ++= Map(
      StateSchemaMetadataKey(colFamilyName, keySchemaId, isKey = true) ->
        StateSchemaMetadataValue(keySchema, SchemaConverters.toAvroTypeWithDefaults(keySchema)),
      StateSchemaMetadataKey(colFamilyName, valueSchemaId, isKey = false) ->
        StateSchemaMetadataValue(valueSchema, SchemaConverters.toAvroTypeWithDefaults(valueSchema))
    )
  }

  override def getSchemaMetadataValue(key: StateSchemaMetadataKey): StateSchemaMetadataValue = {
    schemas(key)
  }

  override def getCurrentStateSchemaId(colFamilyName: String, isKey: Boolean): Short = {
    schemas.keys
      .filter { key =>
        key.colFamilyName == colFamilyName &&
          key.isKey == isKey
      }
      .map(_.schemaId).max
  }
}

class InMemoryStateSchemaProvider(metadata: StateSchemaMetadata)
  extends StateSchemaProvider with Logging {

  override def getSchemaMetadataValue(key: StateSchemaMetadataKey): StateSchemaMetadataValue = {
    metadata.activeSchemas(key)
  }

  override def getCurrentStateSchemaId(colFamilyName: String, isKey: Boolean): Short = {
    metadata.activeSchemas
      .keys
      .filter { key =>
        key.colFamilyName == colFamilyName &&
          key.isKey == isKey
      }
      .map(_.schemaId).max
  }
}

/**
 * Broadcasts schema metadata information for stateful operators in a streaming query.
 *
 * This class provides a way to distribute schema evolution information to all executors
 * via Spark's broadcast mechanism. Each stateful operator in a streaming query maintains
 * its own instance of this class to track schema versions and evolution.
 *
 * @param broadcast Spark broadcast variable containing the schema metadata
 */
case class StateSchemaBroadcast(
    broadcast: Broadcast[StateSchemaMetadata]
) extends Logging with StateSchemaProvider {

  /**
   * Retrieves the schema information for a given column family and schema version
   *
   * @param key A combination of column family name and schema ID
   * @return The corresponding schema metadata value containing both SQL and Avro schemas
   */
  override def getSchemaMetadataValue(key: StateSchemaMetadataKey): StateSchemaMetadataValue = {
    broadcast.value.activeSchemas(key)
  }

  override def getCurrentStateSchemaId(colFamilyName: String, isKey: Boolean): Short = {
    broadcast.value.activeSchemas
      .keys
      .filter { key =>
        key.colFamilyName == colFamilyName &&
          key.isKey == isKey
      }
      .map(_.schemaId).max
  }
}

/**
 * Contains schema evolution metadata for a stateful operator.
 *
 * @param activeSchemas Map of all active schema versions, keyed by column family and schema ID.
 *                      This includes both the current schema and any previous schemas that
 *                      may still exist in the state store.
 */
case class StateSchemaMetadata(
    activeSchemas: Map[StateSchemaMetadataKey, StateSchemaMetadataValue]
)

object StateSchemaMetadata {

  def createStateSchemaMetadata(
      checkpointLocation: String,
      hadoopConf: Configuration,
      stateSchemaFiles: List[String]
  ): StateSchemaMetadata = {
    val fm = CheckpointFileManager.create(new Path(checkpointLocation), hadoopConf)

    // Build up our map of schema metadata
    val activeSchemas = stateSchemaFiles.zipWithIndex.foldLeft(
      Map.empty[StateSchemaMetadataKey, StateSchemaMetadataValue]) {
      case (schemas, (stateSchemaFile, _)) =>
        val fsDataInputStream = fm.open(new Path(stateSchemaFile))
        val colFamilySchemas = StateSchemaCompatibilityChecker.readSchemaFile(fsDataInputStream)

        // For each column family, create metadata entries for both key and value schemas
        val schemaEntries = colFamilySchemas.flatMap { colFamilySchema =>
          // Create key schema metadata
          val keyAvroSchema = SchemaConverters.toAvroTypeWithDefaults(
            colFamilySchema.keySchema)
          val keyEntry = StateSchemaMetadataKey(
            colFamilySchema.colFamilyName,
            colFamilySchema.keySchemaId,
            isKey = true
          ) -> StateSchemaMetadataValue(
            colFamilySchema.keySchema,
            keyAvroSchema
          )

          // Create value schema metadata
          val valueAvroSchema = SchemaConverters.toAvroTypeWithDefaults(
            colFamilySchema.valueSchema)
          val valueEntry = StateSchemaMetadataKey(
            colFamilySchema.colFamilyName,
            colFamilySchema.valueSchemaId,
            isKey = false
          ) -> StateSchemaMetadataValue(
            colFamilySchema.valueSchema,
            valueAvroSchema
          )

          Seq(keyEntry, valueEntry)
        }

        // Add new entries to our accumulated map
        schemas ++ schemaEntries.toMap
    }

    // Create the final metadata
    StateSchemaMetadata(activeSchemas)
  }
}

/**
 * Composite key for looking up schema metadata, combining column family and schema version.
 *
 * @param colFamilyName Name of the RocksDB column family this schema applies to
 * @param schemaId Version identifier for this schema
 */
case class StateSchemaMetadataKey(
    colFamilyName: String,
    schemaId: Short,
    isKey: Boolean
)

/**
 * Contains both SQL and Avro representations of a schema version.
 *
 * The SQL schema represents the logical structure while the Avro schema is used
 * for evolution compatibility checking and serialization.
 *
 * @param sqlSchema The Spark SQL schema definition
 * @param avroSchema The equivalent Avro schema used for compatibility checking
 */
case class StateSchemaMetadataValue(
    sqlSchema: StructType,
    avroSchema: Schema
)

/**
 * Contains schema version information for both key and value schemas in a state store.
 * This information is used to support schema evolution, allowing state schemas to be
 * modified over time while maintaining compatibility with existing state data.
 *
 * @param keySchemaId   A unique identifier for the version of the key schema.
 *                      Used to track and handle changes to the key schema structure.
 * @param valueSchemaId A unique identifier for the version of the value schema.
 *                      Used to track and handle changes to the value schema structure.
 */
case class StateSchemaInfo(
    keySchemaId: Short,
    valueSchemaId: Short
)

/**
 * Represents a row of state data along with its schema version.
 * Used during state storage operations to track which schema version was used
 * to encode the data, enabling proper decoding even when schemas have evolved.
 *
 * @param schemaId The version identifier for the schema that was used to encode this row.
 *                 This could be either a key schema ID or value schema ID depending on context.
 * @param bytes    The actual encoded data bytes for this row. When using Avro encoding,
 *                 these bytes contain the Avro-serialized data. For UnsafeRow encoding,
 *                 these contain the binary-encoded row data.
 */
case class StateSchemaIdRow(
    schemaId: Short,
    bytes: Array[Byte]
)

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

  def supportsSchemaEvolution: Boolean
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

  def encodeWithStateSchemaId(schemaIdRow: StateSchemaIdRow): Array[Byte] = {
    // Create result array big enough for all prefixes plus data
    val data = schemaIdRow.bytes
    val schemaId = schemaIdRow.schemaId
    val result = new Array[Byte](SCHEMA_ID_PREFIX_BYTES + data.length)
    var offset = Platform.BYTE_ARRAY_OFFSET

    Platform.putShort(result, offset, schemaId)
    offset += SCHEMA_ID_PREFIX_BYTES

    // Write the actual data
    Platform.copyMemory(
      data, Platform.BYTE_ARRAY_OFFSET,
      result, offset,
      data.length
    )
    result
  }

  def decodeStateSchemaIdRow(bytes: Array[Byte]): StateSchemaIdRow = {
    var offset = Platform.BYTE_ARRAY_OFFSET

    // Read column family ID if present
    val schemaId = Platform.getShort(bytes, offset)
    offset += SCHEMA_ID_PREFIX_BYTES

    // Extract the actual data
    val dataLength = bytes.length - SCHEMA_ID_PREFIX_BYTES
    val data = new Array[Byte](dataLength)
    Platform.copyMemory(
      bytes, offset,
      data, Platform.BYTE_ARRAY_OFFSET,
      dataLength
    )

    StateSchemaIdRow(schemaId, data)
  }

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
    valueSchema: StructType
) extends RocksDBDataEncoder(keyStateEncoderSpec, valueSchema) {

  override def supportsSchemaEvolution: Boolean = false

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
      // We must use idx here since we are already operating on the prefix which
      // already has the relevant range ordinals projected to the front.
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

/**
 * Encoder that uses Avro for serializing state store data with schema evolution support.
 * The encoded format varies depending on the key type and whether it's a key or value:
 *
 * For prefix and range scan keys:
 * |--prefix---|--schemaId (2 bytes)--|--remainingKeyBytes (avro-encoded)--|
 * where:
 * - prefix: Variable length prefix for scan operations
 * - schemaId: 2 byte short integer identifying the schema version
 * - remainingKeyBytes: Avro-encoded remaining key data
 *
 * For no-prefix keys and values:
 * |--schemaId (2 bytes)--|--avroEncodedBytes--|
 * where:
 * - schemaId: 2 byte short integer identifying the schema version
 * - avroEncodedBytes: Variable length Avro-encoded data
 *
 * The schema ID allows the state store to identify which schema version was used
 * to encode the data, enabling proper decoding even when schemas have evolved over time.
 *
 * @param keyStateEncoderSpec Specification for how to encode keys (prefix/range scan)
 * @param valueSchema Schema for the values to be encoded
 * @param stateSchemaProvider Optional state schema provider
 * @param columnFamilyName Column family name to be used
 */
class AvroStateEncoder(
    keyStateEncoderSpec: KeyStateEncoderSpec,
    valueSchema: StructType,
    stateSchemaProvider: Option[StateSchemaProvider],
    columnFamilyName: String
) extends RocksDBDataEncoder(keyStateEncoderSpec, valueSchema) with Logging {

  private val avroEncoder = createAvroEnc(keyStateEncoderSpec, valueSchema)

  // current schema IDs instantiated lazily
  // schema information
  private lazy val currentKeySchemaId: Short = getStateSchemaProvider.getCurrentStateSchemaId(
    columnFamilyName,
    isKey = true
  )

  private lazy val currentValSchemaId: Short = getStateSchemaProvider.getCurrentStateSchemaId(
    columnFamilyName,
    isKey = false
  )

  // Avro schema used by the avro encoders
  private lazy val keyAvroType: Schema = SchemaConverters.toAvroTypeWithDefaults(keySchema)
  private lazy val keyProj = UnsafeProjection.create(keySchema)

  private lazy val valueAvroType: Schema = SchemaConverters.toAvroTypeWithDefaults(valueSchema)
  private lazy val valueProj = UnsafeProjection.create(valueSchema)

  // Prefix Key schema and projection definitions used by the Avro Serializers
  // and Deserializers
  private lazy val prefixKeySchema = keyStateEncoderSpec match {
    case PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey) =>
      StructType(keySchema.take (numColsPrefixKey))
    case _ => throw unsupportedOperationForKeyStateEncoder("prefixKeySchema")
  }

  private lazy val prefixKeyAvroType = SchemaConverters.toAvroTypeWithDefaults(prefixKeySchema)
  private lazy val prefixKeyProj = UnsafeProjection.create(prefixKeySchema)

  // Range Key schema and projection definitions used by the Avro Serializers and
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

  private lazy val rangeScanAvroType = SchemaConverters.toAvroTypeWithDefaults(rangeScanAvroSchema)

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

  private lazy val remainingKeyAvroType = SchemaConverters.toAvroTypeWithDefaults(
    remainingKeySchema)

  private lazy val remainingKeyAvroProjection = UnsafeProjection.create(remainingKeySchema)

  private def getAvroSerializer(schema: StructType): AvroSerializer = {
    val avroType = SchemaConverters.toAvroTypeWithDefaults(schema)
    new AvroSerializer(schema, avroType, nullable = false)
  }

  private def getAvroDeserializer(schema: StructType): AvroDeserializer = {
    val avroType = SchemaConverters.toAvroTypeWithDefaults(schema)
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

  override def supportsSchemaEvolution: Boolean = true

  private def getStateSchemaProvider: StateSchemaProvider = {
    assert(stateSchemaProvider.isDefined, "StateSchemaProvider should always be" +
      " defined for the Avro encoder")
    stateSchemaProvider.get
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
   * Prepends a version byte to the beginning of a byte array.
   * This is used to maintain backward compatibility and version control of
   * the state encoding format.
   *
   * @param bytesToEncode The original byte array to prepend the version byte to
   * @return A new byte array with the version byte prepended at the beginning
   */
  private[sql] def prependVersionByte(bytesToEncode: Array[Byte]): Array[Byte] = {
    val encodedBytes = new Array[Byte](bytesToEncode.length + STATE_ENCODING_NUM_VERSION_BYTES)
    Platform.putByte(encodedBytes, Platform.BYTE_ARRAY_OFFSET, STATE_ENCODING_VERSION)
    Platform.copyMemory(
      bytesToEncode, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, Platform.BYTE_ARRAY_OFFSET + STATE_ENCODING_NUM_VERSION_BYTES,
      bytesToEncode.length)
    encodedBytes
  }

  /**
   * Removes the version byte from the beginning of a byte array.
   * This is used when decoding state data to get back to the original encoded format.
   *
   * @param bytes The byte array containing the version byte at the start
   * @return A new byte array with the version byte removed
   */
  private[sql] def removeVersionByte(bytes: Array[Byte]): Array[Byte] = {
    val resultBytes = new Array[Byte](bytes.length - STATE_ENCODING_NUM_VERSION_BYTES)
    Platform.copyMemory(
      bytes, STATE_ENCODING_NUM_VERSION_BYTES + Platform.BYTE_ARRAY_OFFSET,
      resultBytes, Platform.BYTE_ARRAY_OFFSET, resultBytes.length
    )
    resultBytes
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

  /**
   * This method takes a byte array written using Avro encoding, and
   * deserializes to an UnsafeRow using the Avro deserializer
   *
   * @param valueBytes The raw bytes containing Avro-encoded data
   * @param avroDeserializer Custom deserializer to convert Avro records to InternalRows
   * @param writerSchema The Avro schema used when writing the data
   * @param readerSchema The Avro schema to use for reading (may be different from writer schema)
   * @param valueProj Projection to convert InternalRow to UnsafeRow
   * @return The deserialized UnsafeRow, or null if input bytes are null
   */
  def decodeFromAvroToUnsafeRow(
      valueBytes: Array[Byte],
      avroDeserializer: AvroDeserializer,
      writerSchema: Schema,
      readerSchema: Schema,
      valueProj: UnsafeProjection): UnsafeRow = {
    if (valueBytes != null) {
      val reader = new GenericDatumReader[Any](writerSchema, readerSchema)
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
    val keyBytes = keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) =>
        val avroRow =
          encodeUnsafeRowToAvro(row, avroEncoder.keySerializer, keyAvroType, out)
        // prepend stateSchemaId to the Avro-encoded key portion for NoPrefixKeys
        encodeWithStateSchemaId(
          StateSchemaIdRow(currentKeySchemaId, avroRow))
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        encodeUnsafeRowToAvro(row, avroEncoder.keySerializer, prefixKeyAvroType, out)
      case _ => throw unsupportedOperationForKeyStateEncoder("encodeKey")
    }
    prependVersionByte(keyBytes)
  }

  override def encodeRemainingKey(row: UnsafeRow): Array[Byte] = {
    val avroRow = keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        encodeUnsafeRowToAvro(row, avroEncoder.suffixKeySerializer.get, remainingKeyAvroType, out)
      case RangeKeyScanStateEncoderSpec(_, _) =>
        encodeUnsafeRowToAvro(row, avroEncoder.keySerializer, remainingKeyAvroType, out)
      case _ => throw unsupportedOperationForKeyStateEncoder("encodeRemainingKey")
    }
    // prepend stateSchemaId to the remaining key portion
    prependVersionByte(encodeWithStateSchemaId(
      StateSchemaIdRow(currentKeySchemaId, avroRow)))
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
    prependVersionByte(out.toByteArray)
  }

  override def encodeValue(row: UnsafeRow): Array[Byte] = {
    val avroRow = encodeUnsafeRowToAvro(row, avroEncoder.valueSerializer, valueAvroType, out)
    // prepend stateSchemaId to the Avro-encoded value portion
    prependVersionByte(
      encodeWithStateSchemaId(StateSchemaIdRow(currentValSchemaId, avroRow)))
  }

  override def decodeKey(rowBytes: Array[Byte]): UnsafeRow = {
    val bytes = removeVersionByte(rowBytes)
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) =>
        val schemaIdRow = decodeStateSchemaIdRow(bytes)
        decodeFromAvroToUnsafeRow(
          schemaIdRow.bytes, avroEncoder.keyDeserializer, keyAvroType, keyProj)
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        decodeFromAvroToUnsafeRow(
          bytes, avroEncoder.keyDeserializer, prefixKeyAvroType, prefixKeyProj)
      case _ => throw unsupportedOperationForKeyStateEncoder("decodeKey")
    }
  }


  override def decodeRemainingKey(rowBytes: Array[Byte]): UnsafeRow = {
    val bytes = removeVersionByte(rowBytes)
    val schemaIdRow = decodeStateSchemaIdRow(bytes)
    keyStateEncoderSpec match {
      case PrefixKeyScanStateEncoderSpec(_, _) =>
        decodeFromAvroToUnsafeRow(schemaIdRow.bytes,
          avroEncoder.suffixKeyDeserializer.get, remainingKeyAvroType, remainingKeyAvroProjection)
      case RangeKeyScanStateEncoderSpec(_, _) =>
        decodeFromAvroToUnsafeRow(
          schemaIdRow.bytes,
          avroEncoder.keyDeserializer, remainingKeyAvroType, remainingKeyAvroProjection)
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
  override def decodePrefixKeyForRangeScan(rowBytes: Array[Byte]): UnsafeRow = {
    val bytes = removeVersionByte(rowBytes)
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

  override def decodeValue(rowBytes: Array[Byte]): UnsafeRow = {
    val bytes = removeVersionByte(rowBytes)
    val schemaIdRow = decodeStateSchemaIdRow(bytes)
    val writerSchema = getStateSchemaProvider.getSchemaMetadataValue(
      StateSchemaMetadataKey(
        columnFamilyName,
        schemaIdRow.schemaId,
        isKey = false
      )
    )
    decodeFromAvroToUnsafeRow(
      schemaIdRow.bytes,
      avroEncoder.valueDeserializer,
      writerSchema.avroSchema,
      valueAvroType, valueProj
    )
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
   * @return A configured RocksDBKeyStateEncoder instance
   */
  def getKeyEncoder(
      dataEncoder: RocksDBDataEncoder,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean): RocksDBKeyStateEncoder = {
    keyStateEncoderSpec.toEncoder(dataEncoder, useColumnFamilies)
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
    useColumnFamilies: Boolean = false)
  extends RocksDBKeyStateEncoder with Logging {

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
    // First encode prefix and remaining key parts
    val prefixKeyEncoded = dataEncoder.encodeKey(extractPrefixKey(row))
    val remainingEncoded = dataEncoder.encodeRemainingKey(remainingKeyProjection(row))

    // Combine prefix key and remaining key into single array
    val combinedData = new Array[Byte](4 + prefixKeyEncoded.length + remainingEncoded.length)
    Platform.putInt(combinedData, Platform.BYTE_ARRAY_OFFSET, prefixKeyEncoded.length)
    Platform.copyMemory(
      prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      combinedData, Platform.BYTE_ARRAY_OFFSET + 4,
      prefixKeyEncoded.length
    )
    Platform.copyMemory(
      remainingEncoded, Platform.BYTE_ARRAY_OFFSET,
      combinedData, Platform.BYTE_ARRAY_OFFSET + 4 + prefixKeyEncoded.length,
      remainingEncoded.length
    )

    combinedData
  }

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    val keyData = keyBytes

    // Get prefix key length from the start of the actual key data
    val prefixKeyEncodedLen = Platform.getInt(keyData, Platform.BYTE_ARRAY_OFFSET)
    val prefixKeyEncoded = new Array[Byte](prefixKeyEncodedLen)
    Platform.copyMemory(
      keyData, Platform.BYTE_ARRAY_OFFSET + 4,
      prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      prefixKeyEncodedLen
    )

    // Calculate remaining key length and extract it
    val remainingKeyEncodedLen = keyData.length - 4 - prefixKeyEncodedLen
    val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
    Platform.copyMemory(
      keyData, Platform.BYTE_ARRAY_OFFSET + 4 + prefixKeyEncodedLen,
      remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      remainingKeyEncodedLen
    )

    // Decode both parts and combine
    val prefixKeyDecoded = dataEncoder.decodeKey(prefixKeyEncoded)
    val remainingKeyDecoded = dataEncoder.decodeRemainingKey(remainingKeyEncoded)
    restoreKeyProjection(joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded))
  }

  private def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    prefixKeyProjection(key)
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    // First encode the prefix key part
    val prefixKeyEncoded = dataEncoder.encodeKey(prefixKey)

    // Create array with length prefix
    val dataWithLength = new Array[Byte](4 + prefixKeyEncoded.length)
    Platform.putInt(dataWithLength, Platform.BYTE_ARRAY_OFFSET, prefixKeyEncoded.length)
    Platform.copyMemory(
      prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      dataWithLength, Platform.BYTE_ARRAY_OFFSET + 4,
      prefixKeyEncoded.length
    )
    dataWithLength
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
    useColumnFamilies: Boolean = false)
  extends RocksDBKeyStateEncoder with Logging {

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
    // First encode the range scan ordered prefix
    val prefixKey = extractPrefixKey(row)
    val rangeScanKeyEncoded = dataEncoder.encodePrefixKeyForRangeScan(prefixKey)

    // We have remaining key parts to encode
    val remainingEncoded = dataEncoder.encodeRemainingKey(remainingKeyProjection(row))

    // Combine range scan key and remaining key with length prefix
    val combinedData = new Array[Byte](4 + rangeScanKeyEncoded.length + remainingEncoded.length)

    // Write length of range scan key
    Platform.putInt(combinedData, Platform.BYTE_ARRAY_OFFSET, rangeScanKeyEncoded.length)

    // Write range scan key
    Platform.copyMemory(
      rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      combinedData, Platform.BYTE_ARRAY_OFFSET + 4,
      rangeScanKeyEncoded.length
    )
    // Write remaining key
    Platform.copyMemory(
      remainingEncoded, Platform.BYTE_ARRAY_OFFSET,
      combinedData, Platform.BYTE_ARRAY_OFFSET + 4 + rangeScanKeyEncoded.length,
      remainingEncoded.length
    )

    combinedData
  }

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    val keyData = keyBytes

    // Get range scan key length and extract it
    val prefixKeyEncodedLen = Platform.getInt(keyData, Platform.BYTE_ARRAY_OFFSET)
    val prefixKeyEncoded = new Array[Byte](prefixKeyEncodedLen)
    Platform.copyMemory(
      keyData, Platform.BYTE_ARRAY_OFFSET + 4,
      prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      prefixKeyEncodedLen
    )

    // Decode the range scan prefix key
    val prefixKeyDecoded = dataEncoder.decodePrefixKeyForRangeScan(prefixKeyEncoded)

    if (orderingOrdinals.length < keySchema.length) {
      // We have remaining key parts to decode
      val remainingKeyEncodedLen = keyData.length - 4 - prefixKeyEncodedLen
      val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
      Platform.copyMemory(
        keyData, Platform.BYTE_ARRAY_OFFSET + 4 + prefixKeyEncodedLen,
        remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        remainingKeyEncodedLen
      )

      // Decode remaining key
      val remainingKeyDecoded = dataEncoder.decodeRemainingKey(remainingKeyEncoded)

      // Combine the parts and restore full key
      val joined = joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded)
      restoreKeyProjection(joined)
    } else {
      // No remaining key parts - return just the prefix key
      prefixKeyDecoded
    }
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    // First encode the range scan ordered prefix
    val rangeScanKeyEncoded = dataEncoder.encodePrefixKeyForRangeScan(prefixKey)

    // Add length prefix
    val dataWithLength = new Array[Byte](4 + rangeScanKeyEncoded.length)
    Platform.putInt(dataWithLength, Platform.BYTE_ARRAY_OFFSET, rangeScanKeyEncoded.length)
    Platform.copyMemory(
      rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      dataWithLength, Platform.BYTE_ARRAY_OFFSET + 4,
      rangeScanKeyEncoded.length
    )

    dataWithLength
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
    useColumnFamilies: Boolean = false)
  extends RocksDBKeyStateEncoder with Logging {

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    dataEncoder.encodeKey(row)
  }

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    dataEncoder.decodeKey(keyBytes)
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
    // First encode the row using either Avro or UnsafeRow encoding
    val rowBytes = dataEncoder.encodeValue(row)

    // Create data array with length prefix
    val dataWithLength = new Array[Byte](java.lang.Integer.BYTES + rowBytes.length)
    Platform.putInt(dataWithLength, Platform.BYTE_ARRAY_OFFSET, rowBytes.length)
    Platform.copyMemory(
      rowBytes, Platform.BYTE_ARRAY_OFFSET,
      dataWithLength, Platform.BYTE_ARRAY_OFFSET + java.lang.Integer.BYTES,
      rowBytes.length
    )

    dataWithLength
  }

  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    if (valueBytes == null) {
      null
    } else {
      // First decode the metadata prefixes
      val dataWithLength = valueBytes
      // Get the value length and extract value bytes
      val numBytes = Platform.getInt(dataWithLength, Platform.BYTE_ARRAY_OFFSET)
      val encodedValue = new Array[Byte](numBytes)
      Platform.copyMemory(
        dataWithLength, Platform.BYTE_ARRAY_OFFSET + java.lang.Integer.BYTES,
        encodedValue, Platform.BYTE_ARRAY_OFFSET,
        numBytes
      )
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

        override def hasNext: Boolean = pos < maxPos

        override def next(): UnsafeRow = {
          // Get value length
          val numBytes = Platform.getInt(valueBytes, pos)
          pos += java.lang.Integer.BYTES

          // Extract value bytes
          val encodedValue = new Array[Byte](numBytes)
          Platform.copyMemory(
            valueBytes, pos,
            encodedValue, Platform.BYTE_ARRAY_OFFSET,
            numBytes
          )
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
  extends RocksDBValueStateEncoder with Logging {

  override def encodeValue(row: UnsafeRow): Array[Byte] = {
    dataEncoder.encodeValue(row)
  }

  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    if (valueBytes == null) {
      return null
    }
    // First decode the metadata prefixes
    val data = valueBytes
    // Decode the actual value using either Avro or UnsafeRow
    dataEncoder.decodeValue(data)
  }

  override def supportsMultipleValuesPerKey: Boolean = false

  override def decodeValues(valueBytes: Array[Byte]): Iterator[UnsafeRow] = {
    throw new IllegalStateException("This encoder doesn't support multiple values!")
  }
}
