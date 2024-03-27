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

object RocksDBStateEncoder {
  def getKeyEncoder(keyStateEncoderSpec: KeyStateEncoderSpec): RocksDBKeyStateEncoder = {
    // Return the key state encoder based on the requested type
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(keySchema) =>
        new NoPrefixKeyStateEncoder(keySchema)

      case PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey) =>
        new PrefixKeyScanStateEncoder(keySchema, numColsPrefixKey)

      case RangeKeyScanStateEncoderSpec(keySchema, numOrderingCols) =>
        new RangeKeyScanStateEncoder(keySchema, numOrderingCols)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported key state encoder spec: " +
          s"$keyStateEncoderSpec")
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
 * To encode a row for range scan, we first project the first numOrderingCols needed
 * for the range scan into an UnsafeRow; we then rewrite that UnsafeRow's fields in BIG_ENDIAN
 * to allow for scanning keys in sorted order using the byte-wise comparison method that
 * RocksDB uses.
 * Then, for the rest of the fields, we project those into another UnsafeRow.
 * We then effectively join these two UnsafeRows together, and finally take those bytes
 * to get the resulting row.
 * We cannot support variable sized fields given the UnsafeRow format which stores variable
 * sized fields as offset and length pointers to the actual values, thereby changing the required
 * ordering.
 * Note that we also support "null" values being passed for these fixed size fields. We prepend
 * a single byte to indicate whether the column value is null or not. We cannot change the
 * nullability on the UnsafeRow itself as the expected ordering would change if non-first
 * columns are marked as null. If the first col is null, those entries will appear last in
 * the iterator. If non-first columns are null, ordering based on the previous columns will
 * still be honored. For rows with null column values, ordering for subsequent columns
 * will also be maintained within those set of rows.
 *
 * @param keySchema - schema of the key to be encoded
 * @param numOrderingCols - number of columns to be used for range scan
 */
class RangeKeyScanStateEncoder(
    keySchema: StructType,
    numOrderingCols: Int) extends RocksDBKeyStateEncoder {

  import RocksDBStateEncoder._

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
      // NullType is technically fixed size, but not supported for ordering
      if (field.dataType == NullType) {
        throw StateStoreErrors.nullTypeOrderingColsNotSupported(field.name, idx.toString)
      } else {
        throw StateStoreErrors.variableSizeOrderingColsNotSupported(field.name, idx.toString)
      }
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

  private val restoreKeyProjection: UnsafeProjection = UnsafeProjection.create(keySchema)

  // Reusable objects
  private val joinedRowOnKey = new JoinedRow()

  private def extractPrefixKey(key: UnsafeRow): UnsafeRow = {
    rangeScanKeyProjection(key)
  }

  // Rewrite the unsafe row by replacing fixed size fields with BIG_ENDIAN encoding
  // using byte arrays.
  // To handle "null" values, we prepend a byte to the byte array indicating whether the value
  // is null or not. If the value is null, we write the null byte followed by a zero byte.
  // If the value is not null, we write the null byte followed by the value.
  // Note that setting null for the index on the unsafeRow is not feasible as it would change
  // the sorting order on iteration.
  private def encodePrefixKeyForRangeScan(row: UnsafeRow): UnsafeRow = {
    val writer = new UnsafeRowWriter(numOrderingCols)
    writer.resetRowWriter()
    rangeScanKeyFieldsWithIdx.foreach { case (field, idx) =>
      val value = row.get(idx, field.dataType)
      val isNullCol: Byte = if (value == null) 0x01.toByte else 0x00.toByte
      // Note that we cannot allocate a smaller buffer here even if the value is null
      // because the effective byte array is considered variable size and needs to have
      // the same size across all rows for the ordering to work as expected.
      val bbuf = ByteBuffer.allocate(field.dataType.defaultSize + 1)
      bbuf.order(ByteOrder.BIG_ENDIAN)
      bbuf.put(isNullCol)
      if (isNullCol == 0x01.toByte) {
        writer.write(idx, bbuf.array())
      } else {
        field.dataType match {
          case BooleanType =>
          case ByteType =>
            bbuf.put(value.asInstanceOf[Byte])
            writer.write(idx, bbuf.array())

          // for other multi-byte types, we need to convert to big-endian
          case ShortType =>
            bbuf.putShort(value.asInstanceOf[Short])
            writer.write(idx, bbuf.array())

          case IntegerType =>
            bbuf.putInt(value.asInstanceOf[Int])
            writer.write(idx, bbuf.array())

          case LongType =>
            bbuf.putLong(value.asInstanceOf[Long])
            writer.write(idx, bbuf.array())

          case FloatType =>
            bbuf.putFloat(value.asInstanceOf[Float])
            writer.write(idx, bbuf.array())

          case DoubleType =>
            bbuf.putDouble(value.asInstanceOf[Double])
            writer.write(idx, bbuf.array())
        }
      }
    }
    writer.getRow()
  }

  // Rewrite the unsafe row by converting back from BIG_ENDIAN byte arrays to the
  // original data types.
  // For decode, we extract the byte array from the UnsafeRow, and then read the first byte
  // to determine if the value is null or not. If the value is null, we set the ordinal on
  // the UnsafeRow to null. If the value is not null, we read the rest of the bytes to get the
  // actual value.
  private def decodePrefixKeyForRangeScan(row: UnsafeRow): UnsafeRow = {
    val writer = new UnsafeRowWriter(numOrderingCols)
    writer.resetRowWriter()
    rangeScanKeyFieldsWithIdx.foreach { case (field, idx) =>
      val value = row.getBinary(idx)
      val bbuf = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      bbuf.order(ByteOrder.BIG_ENDIAN)
      val isNullCol = bbuf.get()
      if (isNullCol == 0x01.toByte) {
        // set the column to null and skip reading the next byte
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
            writer.write(idx, bbuf.getFloat)

          case DoubleType =>
            writer.write(idx, bbuf.getDouble)
        }
      }
    }
    writer.getRow()
  }

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    val prefixKey = extractPrefixKey(row)
    val rangeScanKeyEncoded = encodeUnsafeRow(encodePrefixKeyForRangeScan(prefixKey))

    val result = if (numOrderingCols < keySchema.length) {
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
    } else {
      // if the num of ordering cols is same as num of key schema cols, we don't need to
      // encode the remaining key as it's empty.
      val encodedBytes = new Array[Byte](rangeScanKeyEncoded.length + 4)
      Platform.putInt(encodedBytes, Platform.BYTE_ARRAY_OFFSET, rangeScanKeyEncoded.length)
      Platform.copyMemory(rangeScanKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        encodedBytes, Platform.BYTE_ARRAY_OFFSET + 4, rangeScanKeyEncoded.length)
      encodedBytes
    }
    result
  }

  override def decodeKey(keyBytes: Array[Byte]): UnsafeRow = {
    val prefixKeyEncodedLen = Platform.getInt(keyBytes, Platform.BYTE_ARRAY_OFFSET)
    val prefixKeyEncoded = new Array[Byte](prefixKeyEncodedLen)
    Platform.copyMemory(keyBytes, Platform.BYTE_ARRAY_OFFSET + 4, prefixKeyEncoded,
      Platform.BYTE_ARRAY_OFFSET, prefixKeyEncodedLen)

    val prefixKeyDecodedForRangeScan = decodeToUnsafeRow(prefixKeyEncoded,
      numFields = numOrderingCols)
    val prefixKeyDecoded = decodePrefixKeyForRangeScan(prefixKeyDecodedForRangeScan)

    if (numOrderingCols < keySchema.length) {
      // Here we calculate the remainingKeyEncodedLen leveraging the length of keyBytes
      val remainingKeyEncodedLen = keyBytes.length - 4 - prefixKeyEncodedLen

      val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
      Platform.copyMemory(keyBytes, Platform.BYTE_ARRAY_OFFSET + 4 +
        prefixKeyEncodedLen, remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        remainingKeyEncodedLen)

      val remainingKeyDecoded = decodeToUnsafeRow(remainingKeyEncoded,
        numFields = keySchema.length - numOrderingCols)

      restoreKeyProjection(joinedRowOnKey.withLeft(prefixKeyDecoded).withRight(remainingKeyDecoded))
    } else {
      // if the number of ordering cols is same as the number of key schema cols, we only
      // return the prefix key decoded unsafe row.
      prefixKeyDecoded
    }
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
