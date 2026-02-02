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

import java.util.zip.CRC32C

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CHECKSUM, STATE_STORE_ID}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform

/**
 * A State store row and wrapper for [[UnsafeRow]] that includes the row checksum.
 *
 * @param row The UnsafeRow to be wrapped.
 * @param rowChecksum The checksum of the row.
 */
class StateStoreRowWithChecksum(row: UnsafeRow, rowChecksum: Int) extends StateStoreRow(row) {
  def checksum: Int = rowChecksum
}

object StateStoreRowWithChecksum {
  def apply(row: UnsafeRow, rowChecksum: Int): StateStoreRowWithChecksum = {
    new StateStoreRowWithChecksum(row, rowChecksum)
  }
}

/**
 * Integrity verifier for a State store key and value pair.
 * To ensure the key and value in the state store isn't corrupt.
 * */
trait KeyValueIntegrityVerifier {
  def verify(key: UnsafeRow, value: UnsafeRowWrapper): Unit
  def verify(keyBytes: Array[Byte], valueBytes: Option[Array[Byte]], expectedChecksum: Int): Unit
  def verify(
      keyBytes: ArrayIndexRange[Byte],
      valueBytes: Option[ArrayIndexRange[Byte]],
      expectedChecksum: Int): Unit
}

object KeyValueIntegrityVerifier {
  def create(
      storeId: String,
      rowChecksumEnabled: Boolean,
      verificationRatio: Long): Option[KeyValueIntegrityVerifier] = {
    assert(verificationRatio >= 0, "Verification ratio must be non-negative")
    if (rowChecksumEnabled && verificationRatio != 0) {
      Some(KeyValueChecksumVerifier(storeId, verificationRatio))
    } else {
      None
    }
  }
}

/**
 * Checksum based key value verifier. Computes the checksum of the key and value bytes
 * and compares it to the expected checksum. This also supports rate limiting the number of
 * verification performed via the [[verificationRatio]] parameter. Given that the verify method
 * can be called many times for different key and value pairs, this can be used to reduce the number
 * of key-value verified.
 *
 * NOTE: Not thread safe and expected to be accessed one thread at a time.
 *
 * @param storeId The id of the state store, used for logging purpose.
 * @param verificationRatio How often should verification occur.
 *                          Setting a value N means for every N verify call.
 * */
class KeyValueChecksumVerifier(
    storeId: String,
    verificationRatio: Long) extends KeyValueIntegrityVerifier with Logging {
  assert(verificationRatio > 0, "Verification ratio must be greater than 0")

  // Number of verification requests received via verify method call
  @volatile private var numRequests = 0L
  // Number of checksum verification performed
  @volatile private var numVerified = 0L
  def getNumRequests: Long = numRequests
  def getNumVerified: Long = numVerified

  /** [[verify]] methods call this to see if they should proceed */
  private def shouldVerify(): Boolean = {
    numRequests += 1
    if (numRequests % verificationRatio == 0) {
      numVerified += 1
      true
    } else {
      false
    }
  }

  /**
   * Computes the checksum of the key and value row, and compares it with the
   * expected checksum included in the [[UnsafeRowWrapper]]. Might not do any
   * checksum verification, depending on the [[verificationRatio]].
   *
   * @param key The key row.
   * @param value The value row.
   * */
  override def verify(key: UnsafeRow, value: UnsafeRowWrapper): Unit = {
    assert(value.isInstanceOf[StateStoreRowWithChecksum],
      s"Expected StateStoreRowWithChecksum, but got ${value.getClass.getName}")

    if (shouldVerify()) {
      val valWithChecksum = value.asInstanceOf[StateStoreRowWithChecksum]
      val expected = valWithChecksum.checksum
      val computed = KeyValueChecksum.create(key, Some(valWithChecksum.unsafeRow()))
      verifyChecksum(expected, computed)
    }
  }

  /**
   * Computes the checksum of the key and value bytes (if present), and compares it with the
   * expected checksum specified. Might not do any checksum verification,
   * depending on the [[verificationRatio]].
   *
   * @param keyBytes The key bytes.
   * @param valueBytes Optional value bytes.
   * @param expectedChecksum The expected checksum value.
   * */
  override def verify(
      keyBytes: Array[Byte],
      valueBytes: Option[Array[Byte]],
      expectedChecksum: Int): Unit = {
    if (shouldVerify()) {
      val computed = KeyValueChecksum.create(keyBytes, valueBytes)
      verifyChecksum(expectedChecksum, computed)
    }
  }

  /**
   * Computes the checksum of the key and value bytes (if present), and compares it with the
   * expected checksum specified. Might not do any checksum verification,
   * depending on the [[verificationRatio]].
   *
   * @param keyBytes Specifies the index range of the key bytes in the underlying array.
   * @param valueBytes Optional, specifies the index range of the value bytes
   *                   in the underlying array.
   * @param expectedChecksum The expected checksum value.
   * */
  override def verify(
      keyBytes: ArrayIndexRange[Byte],
      valueBytes: Option[ArrayIndexRange[Byte]],
      expectedChecksum: Int): Unit = {
    if (shouldVerify()) {
      val computed = KeyValueChecksum.create(keyBytes, valueBytes)
      verifyChecksum(expectedChecksum, computed)
    }
  }

  private def verifyChecksum(expected: Int, computed: Int): Unit = {
    logDebug(s"Verifying row checksum, expected: $expected, computed: $computed")
    if (expected != computed) {
      logError(log"Row checksum verification failed for store ${MDC(STATE_STORE_ID, storeId)}, " +
        log"Expected checksum: ${MDC(CHECKSUM, expected)}, " +
        log"Computed checksum: ${MDC(CHECKSUM, computed)}")
      throw StateStoreErrors.rowChecksumVerificationFailed(storeId, expected, computed)
    }
  }
}

object KeyValueChecksumVerifier {
  def apply(storeId: String, verificationRatio: Long): KeyValueChecksumVerifier = {
    new KeyValueChecksumVerifier(storeId, verificationRatio)
  }
}

/** For Key Value checksum creation */
object KeyValueChecksum {
  /**
   * Creates a checksum value using the bytes of the key and value row.
   * If value row isn't specified, will create using key row bytes only.
   * */
  def create(keyRow: UnsafeRow, valueRow: Option[UnsafeRow]): Int = {
    create(keyRow.getBytes, valueRow.map(_.getBytes))
  }

  /**
   * Creates a checksum value using the key and value bytes.
   * If value bytes isn't specified, will create using key bytes only.
   * */
  def create(keyBytes: Array[Byte], valueBytes: Option[Array[Byte]]): Int = {
    create(
      ArrayIndexRange(keyBytes, 0, keyBytes.length),
      valueBytes.map(v => ArrayIndexRange(v, 0, v.length)))
  }

  /**
   * Creates a checksum value using key bytes array index range and value bytes array index range.
   * If value bytes index range isn't specified, will create using key bytes only.
   *
   * @param keyBytes Specifies the index range of bytes to use in the underlying array.
   * @param valueBytes Optional, specifies the index range of bytes to use in the underlying array.
   * */
  def create(keyBytes: ArrayIndexRange[Byte], valueBytes: Option[ArrayIndexRange[Byte]]): Int = {
    // We can later make the checksum algorithm configurable
    val crc32c = new CRC32C()

    crc32c.update(keyBytes.array, keyBytes.fromIndex, keyBytes.length)
    valueBytes.foreach { value =>
      crc32c.update(value.array, value.fromIndex, value.length)
    }

    crc32c.getValue.toInt
  }
}

/**
 * Used to encode and decode checksum value with/from the row bytes.
 * */
object KeyValueChecksumEncoder {
  /**
   * Encodes the value row bytes with a checksum value. This encodes the bytes in a way that
   * supports additional values to be later merged to it. If the value would only ever have a
   * single value (no merge), then you should use [[encodeSingleValueRowWithChecksum]] instead.
   *
   * It is encoded as: checksum (4 bytes) + rowBytes.length (4 bytes) + rowBytes
   *
   * @param rowBytes Value row bytes.
   * @param checksum Checksum value to encode with the value row bytes.
   * @return The encoded value row bytes that includes the checksum.
   * */
  def encodeValueRowWithChecksum(rowBytes: Array[Byte], checksum: Int): Array[Byte] = {
    val result = new Array[Byte](java.lang.Integer.BYTES * 2 + rowBytes.length)
    Platform.putInt(result, Platform.BYTE_ARRAY_OFFSET, checksum)
    Platform.putInt(result, Platform.BYTE_ARRAY_OFFSET + java.lang.Integer.BYTES, rowBytes.length)

    // Write the actual data
    Platform.copyMemory(
      rowBytes, Platform.BYTE_ARRAY_OFFSET,
      result, Platform.BYTE_ARRAY_OFFSET + java.lang.Integer.BYTES * 2,
      rowBytes.length
    )
    result
  }

  /**
   * Decode and verify a value row bytes encoded with checksum via [[encodeValueRowWithChecksum]]
   * back to the original value row bytes. Supports decoding both one or more values bytes
   * (i.e. merged values). This copies each individual value and removes their encoded checksum to
   * form the original value bytes. Because it does copy, it is more expensive than
   * [[decodeAndVerifyMultiValueRowWithChecksum]] method which returns
   * the index range instead of copy (preferred for multi-value bytes). This method is just used
   * to support calling store.get for a key that has merged values.
   *
   * @param verifier used for checksum verification.
   * @param keyBytes Key bytes for the value to decode, only used for checksum verification.
   * @param valueBytes The value bytes to decode.
   * @return The original value row bytes, without the checksum.
   * */
  def decodeAndVerifyValueRowWithChecksum(
      verifier: Option[KeyValueIntegrityVerifier],
      keyBytes: Array[Byte],
      valueBytes: Array[Byte]): Array[Byte] = {
    // First get the total size of the original values
    // Doing this to also support decoding merged values (via merge) e.g. val1,val2,val3
    val valuesEnd = Platform.BYTE_ARRAY_OFFSET + valueBytes.length
    var currentPosition = Platform.BYTE_ARRAY_OFFSET
    var resultSize = 0
    var numValues = 0
    while (currentPosition < valuesEnd) {
      // skip the checksum (first 4 bytes)
      currentPosition += java.lang.Integer.BYTES
      val valueRowSize = Platform.getInt(valueBytes, currentPosition)
      // move to the next value and skip the delimiter character used for rocksdb merge
      currentPosition += java.lang.Integer.BYTES + valueRowSize + 1
      resultSize += valueRowSize
      numValues += 1
    }

    // include the number of delimiters used for merge
    resultSize += numValues - 1

    // now verify and decode to original merged values
    val result = new Array[Byte](resultSize)
    var resultPosition = Platform.BYTE_ARRAY_OFFSET
    val keyRowIndex = ArrayIndexRange(keyBytes, 0, keyBytes.length)
    currentPosition = Platform.BYTE_ARRAY_OFFSET // reset to beginning of values
    var currentValueCount = 0 // count of values iterated

    while (currentPosition < valuesEnd) {
      currentValueCount += 1
      val checksum = Platform.getInt(valueBytes, currentPosition)
      currentPosition += java.lang.Integer.BYTES
      val valueRowSize = Platform.getInt(valueBytes, currentPosition)
      currentPosition += java.lang.Integer.BYTES

      // verify the current value using the index range to avoid copying
      // convert position to array index
      val from = byteOffsetToIndex(currentPosition)
      val until = from + valueRowSize
      val valueRowIndex = ArrayIndexRange(valueBytes, from, until)
      verifier.foreach(_.verify(keyRowIndex, Some(valueRowIndex), checksum))

      // No delimiter is needed if single value or the last value in multi-value
      val copyLength = if (currentValueCount < numValues) {
        valueRowSize + 1 // copy the delimiter
      } else {
        valueRowSize
      }
      Platform.copyMemory(
        valueBytes, currentPosition,
        result, resultPosition,
        copyLength
      )

      // move to the next value
      currentPosition += copyLength
      resultPosition += copyLength
    }

    result
  }

  /**
   * Decode and verify a value row bytes, that might contain one or more values (merged)
   * encoded with checksum via [[encodeValueRowWithChecksum]] back to the original value
   * row bytes index.
   * This returns an iterator of index range of the original individual value row bytes
   * and verifies their checksum. Cheaper since it does not copy the value bytes unlike
   * [[decodeAndVerifyValueRowWithChecksum]].
   *
   * @param verifier Used for checksum verification.
   * @param keyBytes Key bytes for the value to decode, only used for checksum verification.
   * @param valueBytes The value bytes to decode.
   * @return Iterator of index range representing the original value row bytes, without checksum.
   */
  def decodeAndVerifyMultiValueRowWithChecksum(
      verifier: Option[KeyValueIntegrityVerifier],
      keyBytes: Array[Byte],
      valueBytes: Array[Byte]): Iterator[ArrayIndexRange[Byte]] = {
    if (valueBytes == null) {
      Seq().iterator
    } else {
      new Iterator[ArrayIndexRange[Byte]] {
        private val keyRowIndex = ArrayIndexRange(keyBytes, 0, keyBytes.length)
        private var position: Int = Platform.BYTE_ARRAY_OFFSET
        private val valuesEnd = Platform.BYTE_ARRAY_OFFSET + valueBytes.length

        override def hasNext: Boolean = position < valuesEnd

        override def next(): ArrayIndexRange[Byte] = {
          val (valueRowIndex, checksum) =
            getValueRowIndexAndChecksum(valueBytes, startingPosition = position)
          verifier.foreach(_.verify(keyRowIndex, Some(valueRowIndex), checksum))
          // move to the next value and skip the delimiter character used for rocksdb merge
          position = byteIndexToOffset(valueRowIndex.untilIndex) + 1
          valueRowIndex
        }
      }
    }
  }

  /**
   * Decodes one value row bytes encoded with checksum via [[encodeValueRowWithChecksum]]
   * back to the original value row bytes. This is used for an encoded value row that
   * currently only have one value. This returns the index range of the original value row bytes
   * and the encoded checksum value. Cheaper since it does not copy the value bytes.
   *
   * @param bytes The value bytes to decode.
   * @return A tuple containing the index range of the original value row bytes and the checksum.
   */
  def decodeOneValueRowIndexWithChecksum(bytes: Array[Byte]): (ArrayIndexRange[Byte], Int) = {
    getValueRowIndexAndChecksum(bytes, startingPosition = Platform.BYTE_ARRAY_OFFSET)
  }

  /** Get the original value row index and checksum for a row encoded via
   * [[encodeValueRowWithChecksum]]
   * */
  private def getValueRowIndexAndChecksum(
      bytes: Array[Byte],
      startingPosition: Int): (ArrayIndexRange[Byte], Int) = {
    var position = startingPosition
    val checksum = Platform.getInt(bytes, position)
    position += java.lang.Integer.BYTES
    val rowSize = Platform.getInt(bytes, position)
    position += java.lang.Integer.BYTES

    // convert position to array index
    val fromIndex = byteOffsetToIndex(position)
    (ArrayIndexRange(bytes, fromIndex, fromIndex + rowSize), checksum)
  }

  /**
   * Encodes the key row bytes with a checksum value.
   * It is encoded as: checksum (4 bytes) + rowBytes
   *
   * @param rowBytes Key row bytes.
   * @param checksum Checksum value to encode with the key row bytes.
   * @return The encoded key row bytes that includes the checksum.
   * */
  def encodeKeyRowWithChecksum(rowBytes: Array[Byte], checksum: Int): Array[Byte] = {
    encodeSingleValueRowWithChecksum(rowBytes, checksum)
  }

  /**
   * Decodes the key row encoded with [[encodeKeyRowWithChecksum]] and
   * returns the original key bytes and the checksum value.
   *
   * @param bytes The encoded key bytes with checksum.
   * @return Tuple of the original key bytes and the checksum value.
   * */
  def decodeKeyRowWithChecksum(bytes: Array[Byte]): (Array[Byte], Int) = {
    decodeSingleValueRowWithChecksum(bytes)
  }

  /**
   * Decode and verify the key row encoded with [[encodeKeyRowWithChecksum]] and
   * returns the original key bytes.
   *
   * @param verifier Used for checksum verification.
   * @param bytes The encoded key bytes with checksum.
   * @return The original key bytes.
   * */
  def decodeAndVerifyKeyRowWithChecksum(
      verifier: Option[KeyValueIntegrityVerifier],
      bytes: Array[Byte]): Array[Byte] = {
    val (originalBytes, checksum) = decodeKeyRowWithChecksum(bytes)
    verifier.foreach(_.verify(originalBytes, None, checksum))
    originalBytes
  }

  /**
   * Decodes a key row encoded with [[encodeKeyRowWithChecksum]] and
   * returns the index range of the original key bytes and checksum value. This is cheaper
   * than [[decodeKeyRowWithChecksum]], since it doesn't copy the key bytes.
   *
   * @param keyBytes The encoded key bytes with checksum.
   * @return A tuple containing the index range of the original key row bytes and the checksum.
   * */
  def decodeKeyRowIndexWithChecksum(keyBytes: Array[Byte]): (ArrayIndexRange[Byte], Int) = {
    decodeSingleValueRowIndexWithChecksum(keyBytes)
  }

  /**
   * Encodes a value row bytes that will only ever have a single value (no multi-value)
   * with a checksum value.
   * It is encoded as: checksum (4 bytes) + rowBytes.
   * Since it will only ever have a single value, no need to encode the rowBytes length.
   *
   * @param rowBytes Value row bytes.
   * @param checksum Checksum value to encode with the value row bytes.
   * @return The encoded value row bytes that includes the checksum.
   * */
  def encodeSingleValueRowWithChecksum(rowBytes: Array[Byte], checksum: Int): Array[Byte] = {
    val result = new Array[Byte](java.lang.Integer.BYTES + rowBytes.length)
    Platform.putInt(result, Platform.BYTE_ARRAY_OFFSET, checksum)

    // Write the actual data
    Platform.copyMemory(
      rowBytes, Platform.BYTE_ARRAY_OFFSET,
      result, Platform.BYTE_ARRAY_OFFSET + java.lang.Integer.BYTES,
      rowBytes.length
    )
    result
  }

  /**
   * Decodes a single value row encoded with [[encodeSingleValueRowWithChecksum]] and
   * returns the original value bytes and checksum value.
   *
   * @param bytes The encoded value bytes with checksum.
   * @return Tuple of the original value bytes and the checksum value.
   * */
  def decodeSingleValueRowWithChecksum(bytes: Array[Byte]): (Array[Byte], Int) = {
    val checksum = Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET)
    val row = new Array[Byte](bytes.length - java.lang.Integer.BYTES)
    Platform.copyMemory(
      bytes, Platform.BYTE_ARRAY_OFFSET + java.lang.Integer.BYTES,
      row, Platform.BYTE_ARRAY_OFFSET,
      row.length
    )

    (row, checksum)
  }

  /**
   * Decode and verify a single value row encoded with [[encodeSingleValueRowWithChecksum]] and
   * returns the original value bytes.
   *
   * @param verifier used for checksum verification.
   * @param keyBytes Key bytes for the value to decode, only used for checksum verification.
   * @param valueBytes The value bytes to decode.
   * @return The original value row bytes, without the checksum.
   * */
  def decodeAndVerifySingleValueRowWithChecksum(
      verifier: Option[KeyValueIntegrityVerifier],
      keyBytes: Array[Byte],
      valueBytes: Array[Byte]): Array[Byte] = {
    val (originalValueBytes, checksum) = decodeSingleValueRowWithChecksum(valueBytes)
    verifier.foreach(_.verify(keyBytes, Some(originalValueBytes), checksum))
    originalValueBytes
  }

  /**
   * Decodes a single value row encoded with [[encodeSingleValueRowWithChecksum]] and
   * returns the index range of the original value bytes and checksum value. This is cheaper
   * than [[decodeSingleValueRowWithChecksum]], since it doesn't copy the value bytes.
   *
   * @param bytes The encoded value bytes with checksum.
   * @return A tuple containing the index range of the original value row bytes and the checksum.
   * */
  def decodeSingleValueRowIndexWithChecksum(bytes: Array[Byte]): (ArrayIndexRange[Byte], Int) = {
    var position = Platform.BYTE_ARRAY_OFFSET
    val checksum = Platform.getInt(bytes, position)
    position += java.lang.Integer.BYTES

    // convert position to array index
    val fromIndex = byteOffsetToIndex(position)
    (ArrayIndexRange(bytes, fromIndex, bytes.length), checksum)
  }

  /** Convert byte array address offset to array index */
  private def byteOffsetToIndex(offset: Int): Int = {
    offset - Platform.BYTE_ARRAY_OFFSET
  }

  /** Convert byte array index to array address offset */
  private def byteIndexToOffset(index: Int): Int = {
    index + Platform.BYTE_ARRAY_OFFSET
  }
}
