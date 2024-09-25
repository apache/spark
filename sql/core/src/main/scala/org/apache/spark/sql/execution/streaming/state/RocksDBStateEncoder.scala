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

import java.lang.Double.{doubleToRawLongBits, longBitsToDouble}
import java.lang.Float.{floatToRawIntBits, intBitsToFloat}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.expressions.{BoundReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
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

abstract class RocksDBKeyStateEncoderBase(
    useColumnFamilies: Boolean,
    virtualColFamilyId: Option[Short] = None) extends RocksDBKeyStateEncoder {
  def offsetForColFamilyPrefix: Int =
    if (useColumnFamilies) VIRTUAL_COL_FAMILY_PREFIX_BYTES else 0

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

object RocksDBStateEncoder {
  def getKeyEncoder(
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      virtualColFamilyId: Option[Short] = None): RocksDBKeyStateEncoder = {
    // Return the key state encoder based on the requested type
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(keySchema) =>
        new NoPrefixKeyStateEncoder(keySchema, useColumnFamilies, virtualColFamilyId)

      case PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey) =>
        new PrefixKeyScanStateEncoder(keySchema, numColsPrefixKey,
          useColumnFamilies, virtualColFamilyId)

      case RangeKeyScanStateEncoderSpec(keySchema, orderingOrdinals) =>
        new RangeKeyScanStateEncoder(keySchema, orderingOrdinals,
          useColumnFamilies, virtualColFamilyId)

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

  def getColumnFamilyIdBytes(virtualColFamilyId: Short): Array[Byte] = {
    val encodedBytes = new Array[Byte](VIRTUAL_COL_FAMILY_PREFIX_BYTES)
    Platform.putShort(encodedBytes, Platform.BYTE_ARRAY_OFFSET, virtualColFamilyId)
    encodedBytes
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
 * @param useColumnFamilies - if column family is enabled for this encoder
 */
class PrefixKeyScanStateEncoder(
    keySchema: StructType,
    numColsPrefixKey: Int,
    useColumnFamilies: Boolean = false,
    virtualColFamilyId: Option[Short] = None)
  extends RocksDBKeyStateEncoderBase(useColumnFamilies, virtualColFamilyId) {

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
 * @param keySchema - schema of the key to be encoded
 * @param orderingOrdinals - the ordinals for which the range scan is constructed
 * @param useColumnFamilies - if column family is enabled for this encoder
 */
class RangeKeyScanStateEncoder(
    keySchema: StructType,
    orderingOrdinals: Seq[Int],
    useColumnFamilies: Boolean = false,
    virtualColFamilyId: Option[Short] = None)
  extends RocksDBKeyStateEncoderBase(useColumnFamilies, virtualColFamilyId) {

  import RocksDBStateEncoder._

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

  // bit masks used for checking sign or flipping all bits for negative float/double values
  private val floatFlipBitMask = 0xFFFFFFFF
  private val floatSignBitMask = 0x80000000

  private val doubleFlipBitMask = 0xFFFFFFFFFFFFFFFFL
  private val doubleSignBitMask = 0x8000000000000000L

  // Byte markers used to identify whether the value is null, negative or positive
  // To ensure sorted ordering, we use the lowest byte value for negative numbers followed by
  // positive numbers and then null values.
  private val negativeValMarker: Byte = 0x00.toByte
  private val positiveValMarker: Byte = 0x01.toByte
  private val nullValMarker: Byte = 0x02.toByte

  // Rewrite the unsafe row by replacing fixed size fields with BIG_ENDIAN encoding
  // using byte arrays.
  // To handle "null" values, we prepend a byte to the byte array indicating whether the value
  // is null or not. If the value is null, we write the null byte followed by zero bytes.
  // If the value is not null, we write the null byte followed by the value.
  // Note that setting null for the index on the unsafeRow is not feasible as it would change
  // the sorting order on iteration.
  // Also note that the same byte is used to indicate whether the value is negative or not.
  private def encodePrefixKeyForRangeScan(row: UnsafeRow): UnsafeRow = {
    val writer = new UnsafeRowWriter(orderingOrdinals.length)
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
    writer.getRow()
  }

  // Rewrite the unsafe row by converting back from BIG_ENDIAN byte arrays to the
  // original data types.
  // For decode, we extract the byte array from the UnsafeRow, and then read the first byte
  // to determine if the value is null or not. If the value is null, we set the ordinal on
  // the UnsafeRow to null. If the value is not null, we read the rest of the bytes to get the
  // actual value.
  // For negative float/double values, we need to flip all the bits back to get the original value.
  private def decodePrefixKeyForRangeScan(row: UnsafeRow): UnsafeRow = {
    val writer = new UnsafeRowWriter(orderingOrdinals.length)
    writer.resetRowWriter()
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

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    // This prefix key has the columns specified by orderingOrdinals
    val prefixKey = extractPrefixKey(row)
    val rangeScanKeyEncoded = encodeUnsafeRow(encodePrefixKeyForRangeScan(prefixKey))

    val result = if (orderingOrdinals.length < keySchema.length) {
      val remainingEncoded = encodeUnsafeRow(remainingKeyProjection(row))
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

    val prefixKeyDecodedForRangeScan = decodeToUnsafeRow(prefixKeyEncoded,
      numFields = orderingOrdinals.length)
    val prefixKeyDecoded = decodePrefixKeyForRangeScan(prefixKeyDecodedForRangeScan)

    if (orderingOrdinals.length < keySchema.length) {
      // Here we calculate the remainingKeyEncodedLen leveraging the length of keyBytes
      val remainingKeyEncodedLen = keyBytes.length - 4 -
        prefixKeyEncodedLen - offsetForColFamilyPrefix

      val remainingKeyEncoded = new Array[Byte](remainingKeyEncodedLen)
      Platform.copyMemory(keyBytes, decodeKeyStartOffset + 4 + prefixKeyEncodedLen,
        remainingKeyEncoded, Platform.BYTE_ARRAY_OFFSET,
        remainingKeyEncodedLen)

      val remainingKeyDecoded = decodeToUnsafeRow(remainingKeyEncoded,
        numFields = keySchema.length - orderingOrdinals.length)

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
    val rangeScanKeyEncoded = encodeUnsafeRow(encodePrefixKeyForRangeScan(prefixKey))
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
    keySchema: StructType,
    useColumnFamilies: Boolean = false,
    virtualColFamilyId: Option[Short] = None)
  extends RocksDBKeyStateEncoderBase(useColumnFamilies, virtualColFamilyId) {

  import RocksDBStateEncoder._

  // Reusable objects
  private val keyRow = new UnsafeRow(keySchema.size)

  override def encodeKey(row: UnsafeRow): Array[Byte] = {
    if (!useColumnFamilies) {
      encodeUnsafeRow(row)
    } else {
      val bytesToEncode = row.getBytes
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
        // Platform.BYTE_ARRAY_OFFSET is the recommended way refer to the 1st offset. See Platform.
        keyRow.pointTo(
          keyBytes,
          decodeKeyStartOffset + STATE_ENCODING_NUM_VERSION_BYTES,
          keyBytes.length - STATE_ENCODING_NUM_VERSION_BYTES - VIRTUAL_COL_FAMILY_PREFIX_BYTES)
        keyRow
      } else {
        null
      }
    } else decodeToUnsafeRow(keyBytes, keyRow)
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
  extends RocksDBValueStateEncoder with StateStoreThreadAwareLogging {

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
