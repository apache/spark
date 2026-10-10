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

package org.apache.spark.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.UUID

/** Additional codecs not supported by Apache Commons Codecs. */
object Codec {

  def uuidToBytes(id: UUID): Array[Byte] = uuidToByteBuffer(id).array()

  def uuidFromBytes(bytes: Array[Byte]): UUID = {
    require(bytes.length == 16)
    uuidFromByteBuffer(ByteBuffer.wrap(bytes))
  }

  def uuidToByteBuffer(id: UUID): ByteBuffer = {
    val buffer = ByteBuffer.allocate(16)
    buffer.putLong(id.getMostSignificantBits)
    buffer.putLong(id.getLeastSignificantBits)
    buffer.rewind()
    buffer
  }

  def uuidFromByteBuffer(buffer: ByteBuffer): UUID = {
    require(buffer.remaining() >= 16)
    val highBits = buffer.getLong
    val lowBits = buffer.getLong
    new UUID(highBits, lowBits)
  }

  /**
   * This implements Base85 using the 4 byte block aligned encoding and character set from Z85.
   *
   * @see https://rfc.zeromq.org/spec/32/
   */
  object Base85Codec {

    final val ENCODE_MAP: Array[Byte] = {
      val chars = ('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z') ++ ".-:+=^!/*?&<>()[]{}@%$#"
      chars.map(_.toByte).toArray
    }

    lazy val DECODE_MAP: Array[Byte] = {
      require(ENCODE_MAP.length - 1 <= Byte.MaxValue)
      // The bitmask is the same as largest possible value, so the length of the array must
      // be one greater.
      val map: Array[Byte] = Array.fill(ASCII_BITMASK + 1)(-1)
      for ((b, i) <- ENCODE_MAP.zipWithIndex) {
        map(b) = i.toByte
      }
      map
    }

    final val BASE: Long = 85L
    final val BASE_2ND_POWER: Long = 7225L // 85^2
    final val BASE_3RD_POWER: Long = 614125L // 85^3
    final val BASE_4TH_POWER: Long = 52200625L // 85^4
    final val ASCII_BITMASK: Int = 0x7F

    // UUIDs always encode into 20 characters.
    final val ENCODED_UUID_LENGTH: Int = 20

    /** Encode a 16 byte UUID. */
    def encodeUUID(id: UUID): String = {
      val buffer = uuidToByteBuffer(id)
      encodeBlocks(buffer)
    }

    /**
     * Decode a 16 byte UUID. */
    def decodeUUID(encoded: String): UUID = {
      val buffer = decodeBlocks(encoded)
      uuidFromByteBuffer(buffer)
    }

    /**
     * Encode an arbitrary byte array.
     *
     * Unaligned input will be padded to a multiple of 4 bytes.
     */
    def encodeBytes(input: Array[Byte]): String = {
      if (input.length % 4 == 0) {
        encodeBlocks(ByteBuffer.wrap(input))
      } else {
        val alignedLength = ((input.length + 4) / 4) * 4
        val buffer = ByteBuffer.allocate(alignedLength)
        buffer.put(input)
        while (buffer.hasRemaining) {
          buffer.put(0.asInstanceOf[Byte])
        }
        buffer.rewind()
        encodeBlocks(buffer)
      }
    }

    /**
     * Encode an arbitrary byte array using 4 byte blocks.
     *
     * Expects the input to be 4 byte aligned.
     */
    private def encodeBlocks(buffer: ByteBuffer): String = {
      require(buffer.remaining() % 4 == 0)
      val numBlocks = buffer.remaining() / 4
      // Every 4 byte block gets encoded into 5 bytes/chars
      val outputLength = numBlocks * 5
      val output: Array[Byte] = Array.ofDim(outputLength)
      var outputIndex = 0

      while (buffer.hasRemaining) {
        var sum: Long = buffer.getInt & 0x00000000ffffffffL
        output(outputIndex) = ENCODE_MAP((sum / BASE_4TH_POWER).toInt)
        sum %= BASE_4TH_POWER
        output(outputIndex + 1) = ENCODE_MAP((sum / BASE_3RD_POWER).toInt)
        sum %= BASE_3RD_POWER
        output(outputIndex + 2) = ENCODE_MAP((sum / BASE_2ND_POWER).toInt)
        sum %= BASE_2ND_POWER
        output(outputIndex + 3) = ENCODE_MAP((sum / BASE).toInt)
        output(outputIndex + 4) = ENCODE_MAP((sum % BASE).toInt)
        outputIndex += 5
      }

      new String(output, US_ASCII)
    }

    /**
     * Decode an arbitrary byte array.
     *
     * Only `outputLength` bytes will be returned.
     * Any extra bytes, such as padding added because the input was unaligned, will be dropped.
     */
    def decodeBytes(encoded: String, outputLength: Int): Array[Byte] = {
      val result = decodeBlocks(encoded)
      if (result.remaining() > outputLength) {
        // Only read the expected number of bytes.
        val output: Array[Byte] = Array.ofDim(outputLength)
        result.get(output)
        output
      } else {
        result.array()
      }
    }

    /**
     * Decode an arbitrary byte array.
     *
     * Output may contain padding bytes, if the input was not 4 byte aligned.
     * Use [[decodeBytes]] in that case and specify the expected number of output bytes
     * without padding.
     */
    def decodeAlignedBytes(encoded: String): Array[Byte] = decodeBlocks(encoded).array()

    /**
     * Decode an arbitrary byte array.
     *
     * Output may contain padding bytes, if the input was not 4 byte aligned.
     */
    private def decodeBlocks(encoded: String): ByteBuffer = {
      val input = encoded.toCharArray
      require(input.length % 5 == 0, "Input should be 5 character aligned.")
      val buffer = ByteBuffer.allocate(input.length / 5 * 4)

      // A mechanism to detect invalid characters in the input while decoding, that only has a
      // single conditional at the very end, instead of branching for every character.
      var canary: Int = 0
      def decodeInputChar(i: Int): Long = {
        val c = input(i)
        canary |= c // non-ascii char has bits outside of ASCII_BITMASK
        val b = DECODE_MAP(c & ASCII_BITMASK)
        canary |= b // invalid char maps to -1, which has bits outside ASCII_BITMASK
        b.toLong
      }

      var inputIndex = 0
      while (buffer.hasRemaining) {
        var sum = 0L
        sum += decodeInputChar(inputIndex) * BASE_4TH_POWER
        sum += decodeInputChar(inputIndex + 1) * BASE_3RD_POWER
        sum += decodeInputChar(inputIndex + 2) * BASE_2ND_POWER
        sum += decodeInputChar(inputIndex + 3) * BASE
        sum += decodeInputChar(inputIndex + 4)
        buffer.putInt(sum.toInt)
        inputIndex += 5
      }
      require((canary & ~ASCII_BITMASK) == 0, s"Input is not valid Z85: $encoded")
      buffer.rewind()
      buffer
    }
  }
}
