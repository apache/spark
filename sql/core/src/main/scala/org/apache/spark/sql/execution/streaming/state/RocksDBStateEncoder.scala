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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider.{STATE_ENCODING_NUM_VERSION_BYTES, STATE_ENCODING_VERSION}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.Platform

sealed trait RocksDBStateEncoder {
  def supportPrefixKeyScan: Boolean
  def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte]
  def extractPrefixKey(key: UnsafeRow): UnsafeRow

  def encodeKey(row: UnsafeRow): Array[Byte]
  def encodeValue(row: UnsafeRow): Array[Byte]

  def decodeKey(keyBytes: Array[Byte]): UnsafeRow
  def decodeValue(valueBytes: Array[Byte]): UnsafeRow
  def decode(byteArrayTuple: ByteArrayPair): UnsafeRowPair
}

object RocksDBStateEncoder {
  def getEncoder(
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int): RocksDBStateEncoder = {
    if (numColsPrefixKey > 0) {
      new PrefixKeyScanStateEncoder(keySchema, valueSchema, numColsPrefixKey)
    } else {
      new NoPrefixKeyStateEncoder(keySchema, valueSchema)
    }
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

class PrefixKeyScanStateEncoder(
    keySchema: StructType,
    valueSchema: StructType,
    numColsPrefixKey: Int) extends RocksDBStateEncoder {

  import RocksDBStateEncoder._

  require(keySchema.length > numColsPrefixKey, "The number of columns in the key must be " +
    "greater than the number of columns for prefix key!")

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
  private val valueRow = new UnsafeRow(valueSchema.size)
  private val rowTuple = new UnsafeRowPair()

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    val prefixKeyEncoded = encodeUnsafeRow(extractPrefixKey(row))
    val remainingEncoded = encodeUnsafeRow(remainingKeyProjection(row))

    val encodedBytes = new Array[Byte](prefixKeyEncoded.length + remainingEncoded.length + 4)
    Platform.putInt(encodedBytes, Platform.BYTE_ARRAY_OFFSET, prefixKeyEncoded.length)
    Platform.copyMemory(prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, Platform.BYTE_ARRAY_OFFSET + 4, prefixKeyEncoded.length)
    // NOTE: We don't put the length of remainingEncoded as we can calculate later
    // on deserialization.
    Platform.copyMemory(remainingEncoded, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, Platform.BYTE_ARRAY_OFFSET + 4 + prefixKeyEncoded.length,
      remainingEncoded.length)

    encodedBytes
  }

  override def encodeValue(row: UnsafeRow): Array[Byte] = encodeUnsafeRow(row)

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    val prefixKeyEncodedLen = Platform.getInt(keyBytes, Platform.BYTE_ARRAY_OFFSET)
    val prefixKeyEncoded = new Array[Byte](prefixKeyEncodedLen)
    Platform.copyMemory(keyBytes, Platform.BYTE_ARRAY_OFFSET + 4, prefixKeyEncoded,
      Platform.BYTE_ARRAY_OFFSET, prefixKeyEncodedLen)

    // Here we calculate the remainingKeyEncodedLen leveraging the length of keyBytes
    val remainingKeyEncodedLen = keyBytes.length - 4 - prefixKeyEncodedLen

    val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
    Platform.copyMemory(keyBytes, Platform.BYTE_ARRAY_OFFSET + 4 +
      prefixKeyEncodedLen, remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      remainingKeyEncodedLen)

    val prefixKeyDecoded = decodeToUnsafeRow(prefixKeyEncoded, numFields = numColsPrefixKey)
    val remainingKeyDecoded = decodeToUnsafeRow(remainingKeyEncoded,
      numFields = keySchema.length - numColsPrefixKey)

    restoreKeyProjection(joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded))
  }

  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    decodeToUnsafeRow(valueBytes, valueRow)
  }

  override def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    prefixKeyProjection(key)
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    val prefixKeyEncoded = encodeUnsafeRow(prefixKey)
    val prefix = new Array[Byte](prefixKeyEncoded.length + 4)
    Platform.putInt(prefix, Platform.BYTE_ARRAY_OFFSET, prefixKeyEncoded.length)
    Platform.copyMemory(prefixKeyEncoded, Platform.BYTE_ARRAY_OFFSET, prefix,
      Platform.BYTE_ARRAY_OFFSET + 4, prefixKeyEncoded.length)
    prefix
  }

  override def decode(byteArrayTuple: ByteArrayPair): UnsafeRowPair = {
    rowTuple.withRows(decodeKey(byteArrayTuple.key), decodeValue(byteArrayTuple.value))
  }

  override def supportPrefixKeyScan: Boolean = true
}

/**
 * Encodes/decodes UnsafeRows to versioned byte arrays.
 * It uses the first byte of the generated byte array to store the version the describes how the
 * row is encoded in the rest of the byte array. Currently, the default version is 0,
 *
 * VERSION 0:  [ VERSION (1 byte) | ROW (N bytes) ]
 *    The bytes of a UnsafeRow is written unmodified to starting from offset 1
 *    (offset 0 is the version byte of value 0). That is, if the unsafe row has N bytes,
 *    then the generated array byte will be N+1 bytes.
 */
class NoPrefixKeyStateEncoder(keySchema: StructType, valueSchema: StructType)
  extends RocksDBStateEncoder {

  import RocksDBStateEncoder._

  // Reusable objects
  private val keyRow = new UnsafeRow(keySchema.size)
  private val valueRow = new UnsafeRow(valueSchema.size)
  private val rowTuple = new UnsafeRowPair()

  override def encodeKey(row: UnsafeRow): Array[Byte] = encodeUnsafeRow(row)

  override def encodeValue(row: UnsafeRow): Array[Byte] = encodeUnsafeRow(row)

  /**
   * Decode byte array for a key to a UnsafeRow.
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    decodeToUnsafeRow(keyBytes, keyRow)
  }

  /**
   * Decode byte array for a value to a UnsafeRow.
   *
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    decodeToUnsafeRow(valueBytes, valueRow)
  }

  /**
   * Decode pair of key-value byte arrays in a pair of key-value UnsafeRows.
   *
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decode(byteArrayTuple: ByteArrayPair): UnsafeRowPair = {
    rowTuple.withRows(decodeKey(byteArrayTuple.key), decodeValue(byteArrayTuple.value))
  }

  override def supportPrefixKeyScan: Boolean = false

  override def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    throw new IllegalStateException("This encoder doesn't support prefix key!")
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    throw new IllegalStateException("This encoder doesn't support prefix key!")
  }
}
