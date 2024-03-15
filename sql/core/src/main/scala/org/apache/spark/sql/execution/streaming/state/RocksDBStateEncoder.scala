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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider.{STATE_ENCODING_NUM_VERSION_BYTES, STATE_ENCODING_VERSION}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructField, StructType}
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

object RocksDBStateEncoder {
  def getKeyEncoder(
      keySchema: StructType,
      keyStateEncoderType: KeyStateEncoderType = NoPrefixKeyStateEncoderType,
      numColsPrefixKey: Int = 0): RocksDBKeyStateEncoder = {
    keyStateEncoderType match {
      case NoPrefixKeyStateEncoderType =>
        new NoPrefixKeyStateEncoder(keySchema)

      case PrefixKeyScanStateEncoderType =>
        new PrefixKeyScanStateEncoder(keySchema, numColsPrefixKey)

      case RangeKeyScanStateEncoderType =>
        new RangeKeyScanStateEncoder(keySchema, numColsPrefixKey)
    }
  }

  def getValueEncoder(
      valueSchema: StructType,
      useMultipleValuesPerKey: Boolean): RocksDBValueStateEncoder = {
    if (useMultipleValuesPerKey) {
      new MultiValuedStateEncoder(valueSchema)
    } else {
      new SingleValueStateEncoder(valueSchema)
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

/**
 * RocksDB Key Encoder for UnsafeRow that supports prefix scan
 *
 * @param keySchema - schema of the key to be encoded
 * @param numColsPrefixKey - number of columns to be used for prefix key
 */
class PrefixKeyScanStateEncoder(
    keySchema: StructType,
    numColsPrefixKey: Int) extends RocksDBKeyStateEncoder {

  import RocksDBStateEncoder._

  if (numColsPrefixKey == 0 || numColsPrefixKey >= keySchema.length) {
    throw StateStoreErrors.incorrectNumOrderingColsNotSupported(numColsPrefixKey.toString)
  }

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

  private def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
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

  override def supportPrefixKeyScan: Boolean = true
}

/**
 * RocksDB Key Encoder for UnsafeRow that supports range scan for fixed size fields
 *
 * @param keySchema - schema of the key to be encoded
 * @param numColsPrefixKey - number of columns to be used for prefix key
 */
class RangeKeyScanStateEncoder(
    keySchema: StructType,
    numOrderingCols: Int) extends RocksDBKeyStateEncoder {

  import RocksDBStateEncoder._

  // Verify that num cols specified for ordering are valid
  if (numOrderingCols == 0 || numOrderingCols > keySchema.length) {
    throw StateStoreErrors.incorrectNumOrderingColsNotSupported(numOrderingCols.toString)
  }

  private val rangeScanKeyFieldsWithIdx: Seq[(StructField, Int)] = {
    keySchema.zipWithIndex.take(numOrderingCols)
  }

  private def isFixedSize(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: BooleanType | _: ShortType | _: IntegerType | _: LongType |
      _: FloatType | _: DoubleType => true
    case _ => false
  }

  // verify that only fixed sized columns are used for ordering
  rangeScanKeyFieldsWithIdx.foreach { case (field, idx) =>
    if (!isFixedSize(field.dataType)) {
      throw StateStoreErrors.variableSizeOrderingColsNotSupported(field.name, idx.toString)
    }
  }

  private val remainingKeyFieldsWithIdx: Seq[(StructField, Int)] = {
    keySchema.zipWithIndex.drop(numOrderingCols)
  }

  private val rangeScanKeyProjection: UnsafeProjection = {
    val refs = rangeScanKeyFieldsWithIdx.map(x =>
      BoundReference(x._2, x._1.dataType, x._1.nullable))
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

  private def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    rangeScanKeyProjection(key)
  }

  private def encodePrefixKeyForRangeScan(row: UnsafeRow): UnsafeRow = {
    val writer = new UnsafeRowWriter(numOrderingCols)
    writer.resetRowWriter()
    rangeScanKeyFieldsWithIdx.foreach { case (field, idx) =>
      val value = row.get(idx, field.dataType)
      field.dataType match {
        case BooleanType => writer.write(idx, value.asInstanceOf[Boolean])
        case ByteType => writer.write(idx, value.asInstanceOf[Byte])
        case ShortType =>
          val bbuf = ByteBuffer.allocate(2)
          bbuf.order(ByteOrder.BIG_ENDIAN)
          bbuf.putShort(value.asInstanceOf[Short])
          writer.write(idx, bbuf.array())

        case IntegerType =>
          val bbuf = ByteBuffer.allocate(4)
          bbuf.order(ByteOrder.BIG_ENDIAN)
          bbuf.putInt(value.asInstanceOf[Int])
          writer.write(idx, bbuf.array())

        case LongType =>
          val bbuf = ByteBuffer.allocate(8)
          bbuf.order(ByteOrder.BIG_ENDIAN)
          bbuf.putLong(value.asInstanceOf[Long])
          writer.write(idx, bbuf.array())

        case FloatType =>
          val bbuf = ByteBuffer.allocate(4)
          bbuf.order(ByteOrder.BIG_ENDIAN)
          bbuf.putFloat(value.asInstanceOf[Float])
          writer.write(idx, bbuf.array())

        case DoubleType =>
          val bbuf = ByteBuffer.allocate(8)
          bbuf.order(ByteOrder.BIG_ENDIAN)
          bbuf.putDouble(value.asInstanceOf[Double])
          writer.write(idx, bbuf.array())
      }
    }
    writer.getRow().copy()
  }

  private def decodePrefixKeyForRangeScan(row: UnsafeRow): UnsafeRow = {
    val writer = new UnsafeRowWriter(numOrderingCols)
    writer.resetRowWriter()
    rangeScanKeyFieldsWithIdx.foreach { case (field, idx) =>
      val value = if (field.dataType == BooleanType || field.dataType == ByteType) {
        row.get(idx, field.dataType)
      } else {
        row.getBinary(idx)
      }

      field.dataType match {
        case BooleanType => writer.write(idx, value.asInstanceOf[Boolean])
        case ByteType => writer.write(idx, value.asInstanceOf[Byte])
        case ShortType =>
          val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
          bbuf.order(ByteOrder.BIG_ENDIAN)
          writer.write(idx, bbuf.getShort)

        case IntegerType =>
          val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
          bbuf.order(ByteOrder.BIG_ENDIAN)
          writer.write(idx, bbuf.getInt)

        case LongType =>
          val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
          bbuf.order(ByteOrder.BIG_ENDIAN)
          writer.write(idx, bbuf.getLong)

        case FloatType =>
          val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
          bbuf.order(ByteOrder.BIG_ENDIAN)
          writer.write(idx, bbuf.getFloat)

        case DoubleType =>
          val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
          bbuf.order(ByteOrder.BIG_ENDIAN)
          writer.write(idx, bbuf.getDouble)
      }
    }
    writer.getRow().copy()
  }

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    val prefixKey = extractPrefixKey(row)
    val rangeScanKeyEncoded = encodeUnsafeRow(encodePrefixKeyForRangeScan(prefixKey))
    val remainingEncoded = encodeUnsafeRow(remainingKeyProjection(row))

    val encodedBytes = new Array[Byte](rangeScanKeyEncoded.length + remainingEncoded.length + 4)
    Platform.putInt(encodedBytes, Platform.BYTE_ARRAY_OFFSET, rangeScanKeyEncoded.length)
    Platform.copyMemory(rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, Platform.BYTE_ARRAY_OFFSET + 4, rangeScanKeyEncoded.length)
    // NOTE: We don't put the length of remainingEncoded as we can calculate later
    // on deserialization.
    Platform.copyMemory(remainingEncoded, Platform.BYTE_ARRAY_OFFSET,
      encodedBytes, Platform.BYTE_ARRAY_OFFSET + 4 + rangeScanKeyEncoded.length,
      remainingEncoded.length)

    encodedBytes
  }

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

    val prefixKeyDecodedForRangeScan = decodeToUnsafeRow(prefixKeyEncoded,
      numFields = numOrderingCols)
    val prefixKeyDecoded = decodePrefixKeyForRangeScan(prefixKeyDecodedForRangeScan)
    val remainingKeyDecoded = decodeToUnsafeRow(remainingKeyEncoded,
      numFields = keySchema.length - numOrderingCols)

    restoreKeyProjection(joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded))
  }

  override def encodePrefixKey(prefixKey: UnsafeRow): Array[Byte] = {
    val rangeScanKeyEncoded = encodeUnsafeRow(encodePrefixKeyForRangeScan(prefixKey))
    val prefix = new Array[Byte](rangeScanKeyEncoded.length + 4)
    Platform.putInt(prefix, Platform.BYTE_ARRAY_OFFSET, rangeScanKeyEncoded.length)
    Platform.copyMemory(rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET, prefix,
      Platform.BYTE_ARRAY_OFFSET + 4, rangeScanKeyEncoded.length)
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
class NoPrefixKeyStateEncoder(keySchema: StructType)
  extends RocksDBKeyStateEncoder {

  import RocksDBStateEncoder._

  // Reusable objects
  private val keyRow = new UnsafeRow(keySchema.size)

  override def encodeKey(row: UnsafeRow): Array[Byte] = encodeUnsafeRow(row)

  /**
   * Decode byte array for a key to a UnsafeRow.
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    decodeToUnsafeRow(keyBytes, keyRow)
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
class MultiValuedStateEncoder(valueSchema: StructType)
  extends RocksDBValueStateEncoder with Logging {

  import RocksDBStateEncoder._

  // Reusable objects
  private val valueRow = new UnsafeRow(valueSchema.size)

  override def encodeValue(row: UnsafeRow): Array[Byte] = {
    val bytes = encodeUnsafeRow(row)
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
      decodeToUnsafeRow(encodedValue, valueRow)
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
          decodeToUnsafeRow(encodedValue, valueRow)
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
class SingleValueStateEncoder(valueSchema: StructType)
  extends RocksDBValueStateEncoder {

  import RocksDBStateEncoder._

  // Reusable objects
  private val valueRow = new UnsafeRow(valueSchema.size)

  override def encodeValue(row: UnsafeRow): Array[Byte] = encodeUnsafeRow(row)

  /**
   * Decode byte array for a value to a UnsafeRow.
   *
   * @note The UnsafeRow returned is reused across calls, and the UnsafeRow just points to
   *       the given byte array.
   */
  override def decodeValue(valueBytes: Array[Byte]): UnsafeRow = {
    decodeToUnsafeRow(valueBytes, valueRow)
  }

  override def supportsMultipleValuesPerKey: Boolean = false

  override def decodeValues(valueBytes: Array[Byte]): Iterator[UnsafeRow] = {
    throw new IllegalStateException("This encoder doesn't support multiple values!")
  }
}
