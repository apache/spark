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

package org.apache.spark.unsafe.types;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ibm.icu.lang.UCharacter;

import org.apache.spark.sql.catalyst.collation.CollationFactory;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UTF8StringBuilder;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

import static org.apache.spark.unsafe.Platform.*;
import org.apache.spark.util.SparkEnvUtils$;


/**
 * A UTF-8 String for internal Spark use.
 * <p>
 * A String encoded in UTF-8 as an Array[Byte], which can be used for comparison,
 * search, see http://en.wikipedia.org/wiki/UTF-8 for details.
 * <p>
 * Note: This is not designed for general use cases, should not be used outside SQL.
 */
public final class UTF8String implements Comparable<UTF8String>, Externalizable, KryoSerializable,
  Cloneable {

  // These are only updated by readExternal() or read()
  @Nonnull
  private Object base;
  private long offset;
  private int numBytes;
  private volatile int numChars = -1;

  /**
   * The validity of the UTF8Strings can be cached to avoid repeated validation checks, because
   * that operation requires full string scan. Valid strings have no illegal UTF-8 byte sequences.
   */
  private enum UTF8StringValidity {
    UNKNOWN, IS_VALID, NOT_VALID
  }

  /**
   * Internal flag to indicate whether the UTF-8 string is valid or not. Initially, the validity
   * is UNKNOWN, and will be set to either IS_VALID or NOT_VALID after the first validation check.
   */
  private volatile UTF8StringValidity isValid = UTF8StringValidity.UNKNOWN;

  /**
   * In case the current UTF-8 string is not valid, the number of bytes of the validated version
   * of the current string (after possible replacement) will be stored in this field. This value
   * will be equal to `numBytes` if the current string is valid. However, note that this doesn't
   * GUARANTEE that the string is valid - only the `isValid` field can provide that information.
   */
  private volatile int numBytesValid = -1;

  /**
   * The ASCII-ness of the UTF8Strings can be cached to avoid repeated checks, because that
   * operation requires full string scan. Full ASCII strings contain only ASCII characters.
   */
  private enum IsFullAscii {
    UNKNOWN, FULL_ASCII, NOT_ASCII
  }

  /**
   * Internal flag to indicate whether the string is full ASCII or not. Initially, the ASCII-ness
   * is UNKNOWN, and will be set to either FULL_ASCII or NOT_ASCII after the first check.
   */
  private volatile IsFullAscii isFullAscii = IsFullAscii.UNKNOWN;

  public Object getBaseObject() { return base; }
  public long getBaseOffset() { return offset; }

  /**
   * A char in UTF-8 encoding can take 1-4 bytes depending on the first byte which
   * indicates the size of the char. See Unicode standard in page 126, Table 3-6:
   * http://www.unicode.org/versions/Unicode10.0.0/UnicodeStandard-10.0.pdf
   *
   * Binary    Hex          Comments
   * 0xxxxxxx  0x00..0x7F   Only byte of a 1-byte character encoding
   * 10xxxxxx  0x80..0xBF   Continuation bytes (1-3 continuation bytes)
   * 110xxxxx  0xC0..0xDF   First byte of a 2-byte character encoding
   * 1110xxxx  0xE0..0xEF   First byte of a 3-byte character encoding
   * 11110xxx  0xF0..0xF7   First byte of a 4-byte character encoding
   *
   * As a consequence of the well-formedness conditions specified in
   * Table 3-7 (page 126), the following byte values are disallowed in UTF-8:
   *   C0–C1, F5–FF.
   */
  private static byte[] bytesOfCodePointInUTF8 = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x00..0x0F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x10..0x1F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x20..0x2F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x30..0x3F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x40..0x4F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x50..0x5F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x60..0x6F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x70..0x7F
    // Continuation bytes cannot appear as the first byte
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x80..0x8F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x90..0x9F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xA0..0xAF
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xB0..0xBF
    0, 0, // 0xC0..0xC1 - disallowed in UTF-8
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xC2..0xCF
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xD0..0xDF
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 0xE0..0xEF
    4, 4, 4, 4, 4, // 0xF0..0xF4
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 // 0xF5..0xFF - disallowed in UTF-8
  };

  private static final UTF8String COMMA_UTF8 = UTF8String.fromString(",");
  public static final UTF8String EMPTY_UTF8 = UTF8String.fromString("");
  public static final UTF8String ZERO_UTF8 = UTF8String.fromString("0");


  /**
   * Creates an UTF8String from byte array, which should be encoded in UTF-8.
   *
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET, bytes.length);
    } else {
      return null;
    }
  }

  /**
   * Creates an UTF8String from byte array, which should be encoded in UTF-8.
   *
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes, int offset, int numBytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET + offset, numBytes);
    } else {
      return null;
    }
  }

  /**
   * Creates an UTF8String from given address (base and offset) and length.
   */
  public static UTF8String fromAddress(Object base, long offset, int numBytes) {
    return new UTF8String(base, offset, numBytes);
  }

  /**
   * Creates an UTF8String from String.
   */
  public static UTF8String fromString(String str) {
    return str == null ? null : fromBytes(str.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Creates an UTF8String that contains `length` spaces.
   */
  public static UTF8String blankString(int length) {
    byte[] spaces = new byte[length];
    Arrays.fill(spaces, (byte) ' ');
    return fromBytes(spaces);
  }

  /**
   * Determines if the specified character (Unicode code point) is white space or an ISO control
   * character according to Java.
   */
  public static boolean isWhitespaceOrISOControl(int codePoint) {
    return Character.isWhitespace(codePoint) || Character.isISOControl(codePoint);
  }

  private UTF8String(Object base, long offset, int numBytes) {
    this.base = base;
    this.offset = offset;
    this.numBytes = numBytes;
  }

  // for serialization
  public UTF8String() {
    this(null, 0, 0);
  }

  /**
   * Writes the content of this string into a memory address, identified by an object and an offset.
   * The target memory address must already been allocated, and have enough space to hold all the
   * bytes in this string.
   */
  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(base, offset, target, targetOffset, numBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + numBytes);
  }

  /**
   * Returns a {@link ByteBuffer} wrapping the base object if it is a byte array
   * or a copy of the data if the base object is not a byte array.
   *
   * Unlike getBytes this will not create a copy the array if this is a slice.
   */
  @Nonnull
  public ByteBuffer getByteBuffer() {
    if (base instanceof byte[] bytes && offset >= BYTE_ARRAY_OFFSET) {

      // the offset includes an object header... this is only needed for unsafe copies
      final long arrayOffset = offset - BYTE_ARRAY_OFFSET;

      // verify that the offset and length points somewhere inside the byte array
      // and that the offset can safely be truncated to a 32-bit integer
      if ((long) bytes.length < arrayOffset + numBytes) {
        throw new ArrayIndexOutOfBoundsException();
      }

      return ByteBuffer.wrap(bytes, (int) arrayOffset, numBytes);
    } else {
      return ByteBuffer.wrap(getBytes());
    }
  }

  public void writeTo(OutputStream out) throws IOException {
    final ByteBuffer bb = this.getByteBuffer();
    assert(bb.hasArray());

    // similar to Utils.writeByteBuffer but without the spark-core dependency
    out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
  }

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   * @param b The first byte of a code point
   */
  public static int numBytesForFirstByte(final byte b) {
    final int offset = b & 0xFF;
    byte numBytes = bytesOfCodePointInUTF8[offset];
    return (numBytes == 0) ? 1: numBytes; // Skip the first byte disallowed in UTF-8
  }

  /**
   * Returns the number of bytes
   */
  public int numBytes() {
    return numBytes;
  }

  /**
   * Returns the number of code points in it.
   */
  public int numChars() {
    if (numChars == -1) numChars = getNumChars();
    return numChars;
  }

  /**
   * Private helper method to calculate the number of code points in the UTF-8 string. Counting
   * the code points is a linear time operation, as we need to scan the entire UTF-8 string.
   * Hence, this method should generally only be called once for non-empty UTF-8 strings.
   */
  private int getNumChars() {
    int len = 0;
    for (int i = 0; i < numBytes; i += numBytesForFirstByte(getByte(i))) {
      len += 1;
    }
    return len;
  }

  /**
   * Returns a 64-bit integer that can be used as the prefix used in sorting.
   */
  public long getPrefix() {
    return ByteArray.getPrefix(base, offset, numBytes);
  }

  /**
   * Returns the underline bytes, will be a copy of it if it's part of another array.
   */
  public byte[] getBytes() {
    // avoid copy if `base` is `byte[]`
    if (offset == BYTE_ARRAY_OFFSET && base instanceof byte[] bytes
      && bytes.length == numBytes) {
      return bytes;
    } else {
      byte[] bytes = new byte[numBytes];
      copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, numBytes);
      return bytes;
    }
  }

  /**
   * Utility methods and constants for UTF-8 string validation.
   */

  private static boolean isValidContinuationByte(byte b) {
     return b >= (byte) 0x80 && b <= (byte) 0xBF;
  }

  private static boolean isValidSecondByte(byte b, byte firstByte) {
    return switch (firstByte) {
      case (byte) 0xE0 -> b >= (byte) 0xA0 && b <= (byte) 0xBF;
      case (byte) 0xED -> b >= (byte) 0x80 && b <= (byte) 0x9F;
      case (byte) 0xF0 -> b >= (byte) 0x90 && b <= (byte) 0xBF;
      case (byte) 0xF4 -> b >= (byte) 0x80 && b <= (byte) 0x8F;
      default -> isValidContinuationByte(b);
    };
  }

  /**
   * The Unicode replacement character (U+FFFD) is used to replace invalid code points.
   */
  private static final byte[] UNICODE_REPLACEMENT_CHARACTER =
    new byte[] { (byte) 0xEF, (byte) 0xBF, (byte) 0xBD };

  /**
   * Private helper method to insert the Unicode replacement character (U+FFFD) to a byte array.
   */
  private static void insertReplacementCharacter(byte[] bytes, int byteIndex) {
    for (byte b : UTF8String.UNICODE_REPLACEMENT_CHARACTER) {
      bytes[byteIndex++] = b;
    }
  }

  /**
   * Returns a validated version of the current UTF-8 string by replacing invalid UTF-8 sequences
   * with the Unicode replacement character (U+FFFD), as per the rules defined in the Unicode
   * standard 15, Section 3.9, Paragraph D86, Table 3-7. This behaviour is consistent with the
   * behaviour of `UnicodeString` in ICU4C.
   *
   * @return A new UTF8String that is a valid UTF8 string.
   */
  public UTF8String makeValid() {
    if (isValid()) return this;
    return UTF8String.fromBytes(makeValidBytes());
  }

  /**
   * Private helper method to create a valid UTF-8 byte sequence from the current UTF8String.
   * In order to use this method, the number of bytes of the validated version of the current
   * string (after possible replacement) must be evaluated first by calling `getIsValid`.
   */
  private byte[] makeValidBytes() {
    assert(numBytesValid > 0);
    byte[] bytes = new byte[numBytesValid];
    int byteIndex = 0, byteIndexValid = 0;
    while (byteIndex < numBytes) {
      // Read the first byte.
      byte firstByte = getByte(byteIndex);
      int expectedLen = bytesOfCodePointInUTF8[firstByte & 0xFF];
      int codePointLen = Math.min(expectedLen, numBytes - byteIndex);
      // 0B UTF-8 sequence (invalid first byte).
      if (codePointLen == 0) {
        insertReplacementCharacter(bytes, byteIndexValid);
        byteIndexValid += UNICODE_REPLACEMENT_CHARACTER.length;
        ++byteIndex;
        continue;
      }
      // 1B UTF-8 sequence (ASCII or truncated).
      if (codePointLen == 1) {
        if (firstByte >= 0) {
          bytes[byteIndexValid++] = firstByte;
        }
        else {
          insertReplacementCharacter(bytes, byteIndexValid);
          byteIndexValid += UNICODE_REPLACEMENT_CHARACTER.length;
        }
        ++byteIndex;
        continue;
      }
      // Read the second byte.
      byte secondByte = getByte(byteIndex + 1);
      if (!isValidSecondByte(secondByte, firstByte)) {
        insertReplacementCharacter(bytes, byteIndexValid);
        byteIndexValid += UNICODE_REPLACEMENT_CHARACTER.length;
        ++byteIndex;
        continue;
      }
      // Read remaining continuation bytes.
      int continuationBytes = 2;
      for (; continuationBytes < codePointLen; ++continuationBytes) {
        byte nextByte = getByte(byteIndex + continuationBytes);
        if (!isValidContinuationByte(nextByte)) {
          break;
        }
      }
      // Invalid UTF-8 sequence (not enough continuation bytes).
      if (continuationBytes < expectedLen) {
        insertReplacementCharacter(bytes, byteIndexValid);
        byteIndexValid += UNICODE_REPLACEMENT_CHARACTER.length;
        byteIndex += continuationBytes;
        continue;
      }
      // Valid UTF-8 sequence.
      for (int i = 0; i < codePointLen; ++i) {
        bytes[byteIndexValid++] = getByte(byteIndex + i);
      }
      byteIndex += codePointLen;
    }
    return bytes;
  }

  /**
   * Checks if the current UTF8String is valid.
   *
   * @return If string represents a valid UTF8 string.
   */
  public boolean isValid() {
    if (isValid == UTF8StringValidity.UNKNOWN) {
      isValid = getIsValid();
    }
    return isValid == UTF8StringValidity.IS_VALID;
  }

  /**
   * Private helper method to calculate whether the current UTF-8 string is valid. Checking
   * all code points is a linear time operation, as we need to scan the entire UTF-8 string.
   * Hence, this method should generally only be called only once during UTF8String lifetime.
   * Unlike `getNumBytesValid`, this method performs early exit as soon as an invalid byte
   * sequence is found, and returns a boolean indicating the validity of the current string.
   */
  private UTF8StringValidity getIsValid() {
    boolean isValid = true;
    int byteIndex = 0, byteCount = 0;
    while (byteIndex < numBytes) {
      // Read the first byte.
      byte firstByte = getByte(byteIndex);
      int expectedLen = bytesOfCodePointInUTF8[firstByte & 0xFF];
      int codePointLen = Math.min(expectedLen, numBytes - byteIndex);
      // 0B UTF-8 sequence (invalid first byte).
      if (codePointLen == 0) {
        byteCount += UNICODE_REPLACEMENT_CHARACTER.length;
        isValid = false;
        ++byteIndex;
        continue;
      }
      // 1B UTF-8 sequence (ASCII or truncated).
      if (codePointLen == 1) {
        if (firstByte >= 0) {
          ++byteCount;
        }
        else {
          byteCount += UNICODE_REPLACEMENT_CHARACTER.length;
          isValid = false;
        }
        ++byteIndex;
        continue;
      }
      // Read the second byte.
      byte secondByte = getByte(byteIndex + 1);
      if (!isValidSecondByte(secondByte, firstByte)) {
        byteCount += UNICODE_REPLACEMENT_CHARACTER.length;
        isValid = false;
        ++byteIndex;
        continue;
      }
      // Read remaining continuation bytes.
      int continuationBytes = 2;
      for (; continuationBytes < codePointLen; ++continuationBytes) {
        byte nextByte = getByte(byteIndex + continuationBytes);
        if (!isValidContinuationByte(nextByte)) {
          break;
        }
      }
      // Invalid UTF-8 sequence (not enough continuation bytes).
      if (continuationBytes < expectedLen) {
        byteCount += UNICODE_REPLACEMENT_CHARACTER.length;
        isValid = false;
        byteIndex += continuationBytes;
        continue;
      }
      // Valid UTF-8 sequence.
      for (int i = 0; i < codePointLen; ++i) {
        ++byteCount;
      }
      byteIndex += codePointLen;
    }
    setNumBytesValid(byteCount);
    return isValid ? UTF8StringValidity.IS_VALID : UTF8StringValidity.NOT_VALID;
  }

  /**
   * The method sets the total number of bytes of the validated version of the current string
   * (after possible replacement), which will be equal to `numBytes` if the UTF8String is valid.
   * This method should generally only be called once, from the `getIsValid` method.
   */
  private void setNumBytesValid(int byteCount) {
    if (byteCount < 0) {
      throw new IllegalStateException("Error in UTF-8 byte count");
    }
    numBytesValid = byteCount;
  }

  /**
   * Code point iteration over a UTF8String can be done using one of two modes:
   * 1. CODE_POINT_ITERATOR_ASSUME_VALID: The caller ensures that the UTF8String is valid and does
   *    not contain any invalid UTF-8 byte sequences. In this case, the code point iterator will
   *    return the code points in the current string one by one, as integers. If an invalid code
   *    point is found within the string during iteration, an exception will be thrown. This mode
   *    is more dangerous, but faster - since no scan is needed prior to beginning iteration.
   * 2. CODE_POINT_ITERATOR_MAKE_VALID: The caller does not ensure that the UTF8String is valid,
   *    but instead expects the code point iterator to first check whether the current UTF8String
   *    is valid, then perform the invalid byte sequence replacement using `makeValid`, and finally
   *    begin the code point iteration over the resulting valid UTF8String. However, the original
   *    UTF8String stays unchanged. This mode is safer, but slower - due to initial validation.
   * The default mode is CODE_POINT_ITERATOR_ASSUME_VALID.
   */
  public enum CodePointIteratorType {
    CODE_POINT_ITERATOR_ASSUME_VALID, // USE ONLY WITH VALID STRINGS
    CODE_POINT_ITERATOR_MAKE_VALID
  }

  /**
   * Returns a code point iterator for this UTF8String.
   */
  public Iterator<Integer> codePointIterator() {
    return codePointIterator(CodePointIteratorType.CODE_POINT_ITERATOR_ASSUME_VALID);
  }

  public Iterator<Integer> codePointIterator(CodePointIteratorType iteratorMode) {
    if (iteratorMode == CodePointIteratorType.CODE_POINT_ITERATOR_MAKE_VALID) {
      return makeValid().codePointIterator();
    }
    return new CodePointIterator();
  }

  /**
   * Code point iterator implementation for the UTF8String class. The iterator will return code
   * points in the current string one by one, as integers. However, the code point iterator is only
   * guaranteed to work if the current UTF8String does not contain any invalid UTF-8 byte sequences.
   * If the current string contains any invalid UTF-8 byte sequences, exceptions will be thrown.
   */
  private class CodePointIterator implements Iterator<Integer> {
    // Byte index used to iterate over the current UTF8String.
    private int byteIndex = 0;

    @Override
    public boolean hasNext() {
      return byteIndex < numBytes;
    }

    @Override
    public Integer next() {
      if (!hasNext()) {
        throw new IndexOutOfBoundsException();
      }
      int codePoint = codePointFrom(byteIndex);
      byteIndex += numBytesForFirstByte(getByte(byteIndex));
      return codePoint;
    }
  }

  /**
   * Reverse version of the code point iterator for this UTF8String, returns code points in the
   * current string one by one, as integers, in reverse order. The logic is similar to the above.
   */

  public Iterator<Integer> reverseCodePointIterator() {
    return reverseCodePointIterator(CodePointIteratorType.CODE_POINT_ITERATOR_ASSUME_VALID);
  }

  public Iterator<Integer> reverseCodePointIterator(CodePointIteratorType iteratorMode) {
    if (iteratorMode == CodePointIteratorType.CODE_POINT_ITERATOR_MAKE_VALID) {
      return makeValid().reverseCodePointIterator();
    }
    return new ReverseCodePointIterator();
  }

  private class ReverseCodePointIterator implements Iterator<Integer> {
    private int byteIndex = numBytes - 1;

    @Override
    public boolean hasNext() {
      return byteIndex >= 0;
    }

    @Override
    public Integer next() {
      if (!hasNext()) {
        throw new IndexOutOfBoundsException();
      }
      while (byteIndex > 0 && isContinuationByte(getByte(byteIndex))) {
        --byteIndex;
      }
      return codePointFrom(byteIndex--);
    }

    private boolean isContinuationByte(byte b) {
      return (b & 0xC0) == 0x80;
    }
  }

  /**
   * Returns a substring of this.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  public UTF8String substring(final int start, final int until) {
    if (until <= start || start >= numBytes) {
      return EMPTY_UTF8;
    }

    int i = 0;
    int c = 0;
    while (i < numBytes && c < start) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    int j = i;
    while (i < numBytes && c < until) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    if (i > j) {
      byte[] bytes = new byte[i - j];
      copyMemory(base, offset + j, bytes, BYTE_ARRAY_OFFSET, i - j);
      return fromBytes(bytes);
    } else {
      return EMPTY_UTF8;
    }
  }

  public UTF8String substringSQL(int pos, int length) {
    // Information regarding the pos calculation:
    // Hive and SQL use one-based indexing for SUBSTR arguments but also accept zero and
    // negative indices for start positions. If a start index i is greater than 0, it
    // refers to element i-1 in the sequence. If a start index i is less than 0, it refers
    // to the -ith element before the end of the sequence. If a start index i is 0, it
    // refers to the first element.
    int len = numChars();
    // `len + pos` does not overflow as `len >= 0`.
    int start = (pos > 0) ? pos -1 : ((pos < 0) ? len + pos : 0);

    int end;
    if ((long) start + length > Integer.MAX_VALUE) {
      end = Integer.MAX_VALUE;
    } else if ((long) start + length < Integer.MIN_VALUE) {
      end = Integer.MIN_VALUE;
    } else {
      end = start + length;
    }
    return substring(start, end);
  }

  /**
   * Returns whether this contains `substring` or not.
   */
  public boolean contains(final UTF8String substring) {
    if (substring.numBytes == 0) {
      return true;
    }

    byte first = substring.getByte(0);
    for (int i = 0; i <= numBytes - substring.numBytes; i++) {
      if (getByte(i) == first && matchAt(substring, i)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the byte at (byte) position `byteIndex`. If byte index is invalid, returns 0.
   */
  public byte getByte(int byteIndex) {
    return Platform.getByte(base, offset + byteIndex);
  }

  /**
   * Returns the code point at (char) position `charIndex`. If char index is invalid, throws
   * exception. Note that this method is not efficient as it needs to traverse the UTF-8 string.
   * If `byteIndex` of the first byte in the code point is known, use `codePointFrom` instead.
   */
  public int getChar(int charIndex) {
    if (charIndex < 0 || charIndex >= numChars()) {
      throw new IndexOutOfBoundsException();
    }
    int charCount = 0, byteCount = 0;
    while (charCount < charIndex) {
      byteCount += numBytesForFirstByte(getByte(byteCount));
      charCount += 1;
    }
    return codePointFrom(byteCount);
  }

  /**
   * Returns the code point starting from the byte at position `byteIndex`.
   * If byte index is invalid, throws exception.
   */
  public int codePointFrom(int byteIndex) {
    if (byteIndex < 0 || byteIndex >= numBytes) {
      throw new IndexOutOfBoundsException();
    }
    byte b = getByte(byteIndex);
    int numBytes = numBytesForFirstByte(b);
    return switch (numBytes) {
      case 1 ->
        b & 0x7F;
      case 2 ->
        ((b & 0x1F) << 6) | (getByte(byteIndex + 1) & 0x3F);
      case 3 ->
        ((b & 0x0F) << 12) | ((getByte(byteIndex + 1) & 0x3F) << 6) |
        (getByte(byteIndex + 2) & 0x3F);
      case 4 ->
        ((b & 0x07) << 18) | ((getByte(byteIndex + 1) & 0x3F) << 12) |
        ((getByte(byteIndex + 2) & 0x3F) << 6) | (getByte(byteIndex + 3) & 0x3F);
      default ->
        throw new IllegalStateException("Error in UTF-8 code point");
    };
  }

  public boolean matchAt(final UTF8String s, int pos) {
    if (s.numBytes + pos > numBytes || pos < 0) {
      return false;
    }
    return ByteArrayMethods.arrayEquals(base, offset + pos, s.base, s.offset, s.numBytes);
  }

  public boolean startsWith(final UTF8String prefix) {
    return matchAt(prefix, 0);
  }

  public boolean endsWith(final UTF8String suffix) {
    return matchAt(suffix, numBytes - suffix.numBytes);
  }

  /**
   * Method for ASCII character conversion using a functional interface for chars.
   */

  private UTF8String convertAscii(Function<Character, Character> charConverter) {
    byte[] bytes = new byte[numBytes];
    for (int i = 0; i < numBytes; i++) {
        bytes[i] = (byte) charConverter.apply((char) getByte(i)).charValue();
    }
    return fromBytes(bytes);
  }

  /**
   * Returns the upper case of this string
   */
  public UTF8String toUpperCase() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    return isFullAscii() ? toUpperCaseAscii() : toUpperCaseSlow();
  }

  public UTF8String toUpperCaseAscii() {
    return convertAscii(Character::toUpperCase);
  }

  private UTF8String toUpperCaseSlow() {
    return fromString(toString().toUpperCase());
  }

  /**
   * Returns the lower case of this string
   */
  public UTF8String toLowerCase() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    return isFullAscii() ? toLowerCaseAscii() : toLowerCaseSlow();
  }

  public boolean isFullAscii() {
    if (isFullAscii == IsFullAscii.UNKNOWN) {
      isFullAscii = getIsFullAscii();
    }
    return isFullAscii == IsFullAscii.FULL_ASCII;
  }

  private IsFullAscii getIsFullAscii() {
    for (var i = 0; i < numBytes; i++) {
      if (getByte(i) < 0) {
        return IsFullAscii.NOT_ASCII;
      }
    }
    return IsFullAscii.FULL_ASCII;
  }

  private UTF8String toLowerCaseSlow() {
    return fromString(toString().toLowerCase());
  }

  public UTF8String toLowerCaseAscii() {
    return convertAscii(Character::toLowerCase);
  }

  /**
   * Returns the title case of this string, that could be used as title. There are essentially two
   * different version of this method - one using the JVM case mapping rules, and the other using
   * the ICU case mapping rules. ASCII implementation is the same for both, but please refer to the
   * respective methods for the slow (non-ASCII) implementation for more details on the differences.
   */
  public UTF8String toTitleCase() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    return isFullAscii() ? toTitleCaseAscii() : toTitleCaseSlow();
  }

  public UTF8String toTitleCaseICU() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    return isFullAscii() ? toTitleCaseAscii() : toTitleCaseSlowICU();
  }

  /*
   * Fast path to return the title case of this string, given that all characters are ASCII.
   * This implementation essentially works for all collations currently supported in Spark.
   * This method is more efficient, because it skips copying the data to and from StringBuilder.
   */
  private UTF8String toTitleCaseAscii() {
    byte[] bytes = new byte[numBytes];
    byte prev = ' ', curr;
    for (int i = 0; i < numBytes; i++) {
      curr = getByte(i);
      if (prev == ' ') {
        bytes[i] = (byte) Character.toTitleCase(curr);
      } else {
        bytes[i] = curr;
      }
      prev = curr;
    }
    return fromBytes(bytes);
  }

  /*
   * Slow path to return the title case of this string, according to JVM case mapping rules.
   * This is considered the "old" behaviour for UTF8_BINARY collation, and is not recommended.
   * To use this, set the spark.sql.ICU_CASE_MAPPINGS_ENABLED configuration to `false`.
   */
  private UTF8String toTitleCaseSlow() {
    StringBuilder sb = new StringBuilder();
    String s = toString();
    sb.append(s);
    sb.setCharAt(0, Character.toTitleCase(sb.charAt(0)));
    for (int i = 1; i < s.length(); i++) {
      if (sb.charAt(i - 1) == ' ') {
        sb.setCharAt(i, Character.toTitleCase(sb.charAt(i)));
      }
    }
    return fromString(sb.toString());
  }

  /*
   * Slow path to return the title case of this string, according to ICU case mapping rules.
   * This is considered the "new" behaviour for UTF8_BINARY collation, and is recommended.
   * This is used by default, since spark.sql.ICU_CASE_MAPPINGS_ENABLED is set to `true`.
   */
  private UTF8String toTitleCaseSlowICU() {
    StringBuilder sb = new StringBuilder();
    String s = toString();
    sb.append(s);
    sb.setCharAt(0, (char) UCharacter.toTitleCase(sb.charAt(0)));
    for (int i = 1; i < s.length(); i++) {
      if (sb.charAt(i - 1) == ' ') {
        sb.setCharAt(i, (char) UCharacter.toTitleCase(sb.charAt(i)));
      }
    }
    return fromString(sb.toString());
  }

  /*
   * Returns the index of the string `match` in this String. This string has to be a comma separated
   * list. If `match` contains a comma 0 will be returned. If the `match` isn't part of this String,
   * 0 will be returned, else the index of match (1-based index)
   */
  public int findInSet(UTF8String match) {
    if (match.contains(COMMA_UTF8)) {
      return 0;
    }

    int n = 1, lastComma = -1;
    for (int i = 0; i < numBytes; i++) {
      if (getByte(i) == (byte) ',') {
        if (i - (lastComma + 1) == match.numBytes &&
          ByteArrayMethods.arrayEquals(base, offset + (lastComma + 1), match.base, match.offset,
            match.numBytes)) {
          return n;
        }
        lastComma = i;
        n++;
      }
    }
    if (numBytes - (lastComma + 1) == match.numBytes &&
      ByteArrayMethods.arrayEquals(base, offset + (lastComma + 1), match.base, match.offset,
        match.numBytes)) {
      return n;
    }
    return 0;
  }

  /**
   * Copy the bytes from the current UTF8String, and make a new UTF8String.
   * @param start the start position of the current UTF8String in bytes.
   * @param end the end position of the current UTF8String in bytes.
   * @return a new UTF8String in the position of [start, end] of current UTF8String bytes.
   */
  public UTF8String copyUTF8String(int start, int end) {
    int len = end - start + 1;
    byte[] newBytes = new byte[len];
    copyMemory(base, offset + start, newBytes, BYTE_ARRAY_OFFSET, len);
    return UTF8String.fromBytes(newBytes);
  }

  /**
   * Trims space characters (ASCII 32) from both ends of this string.
   *
   * @return this string with no spaces at the start or end
   */
  public UTF8String trim() {
    int s = 0;
    // skip all of the space (0x20) in the left side
    while (s < this.numBytes && getByte(s) == ' ') s++;
    if (s == this.numBytes) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    // skip all of the space (0x20) in the right side
    int e = this.numBytes - 1;
    while (e > s && getByte(e) == ' ') e--;
    if (s == 0 && e == numBytes - 1) {
      // Nothing trimmed
      return this;
    }
    return copyUTF8String(s, e);
  }

  /**
   * Trims whitespace ASCII characters from both ends of this string.
   *
   * Note that, this method is different from {@link UTF8String#trim()} which removes
   * only spaces(= ASCII 32) from both ends.
   *
   * @return A UTF8String whose value is this UTF8String, with any leading and trailing white
   * space removed, or this UTF8String if it has no leading or trailing whitespace.
   *
   */
  public UTF8String trimAll() {
    int s = 0;
    // skip all of the whitespaces in the left side
    while (s < this.numBytes && isWhitespaceOrISOControl(getByte(s))) s++;
    if (s == this.numBytes) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    // skip all of the whitespaces in the right side
    int e = this.numBytes - 1;
    while (e > s && isWhitespaceOrISOControl(getByte(e))) e--;
    if (s == 0 && e == numBytes - 1) {
      // Nothing trimmed
      return this;
    }
    return copyUTF8String(s, e);
  }

  /**
   * Trims instances of the given trim string from both ends of this string.
   *
   * @param trimString the trim character string
   * @return this string with no occurrences of the trim string at the start or end, or `null`
   *  if `trimString` is `null`
   */
  public UTF8String trim(UTF8String trimString) {
    if (trimString != null) {
      return trimLeft(trimString).trimRight(trimString);
    } else {
      return null;
    }
  }

  /**
   * Trims space characters (ASCII 32) from the start of this string.
   *
   * @return this string with no spaces at the start
   */
  public UTF8String trimLeft() {
    int s = 0;
    // skip all of the space (0x20) in the left side
    while (s < this.numBytes && getByte(s) == 0x20) s++;
    if (s == 0) {
      // Nothing trimmed
      return this;
    }
    if (s == this.numBytes) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    return copyUTF8String(s, this.numBytes - 1);
  }

  /**
   * Trims instances of the given trim string from the start of this string.
   *
   * @param trimString the trim character string
   * @return this string with no occurrences of the trim string at the start, or `null`
   *  if `trimString` is `null`
   */
  public UTF8String trimLeft(UTF8String trimString) {
    if (trimString == null) return null;
    // the searching byte position in the source string
    int searchIdx = 0;
    // the first beginning byte position of a non-matching character
    int trimIdx = 0;

    while (searchIdx < numBytes) {
      UTF8String searchChar = copyUTF8String(
          searchIdx, searchIdx + numBytesForFirstByte(this.getByte(searchIdx)) - 1);
      int searchCharBytes = searchChar.numBytes;
      // try to find the matching for the searchChar in the trimString set
      if (trimString.find(searchChar, 0) >= 0) {
        trimIdx += searchCharBytes;
      } else {
        // no matching, exit the search
        break;
      }
      searchIdx += searchCharBytes;
    }
    if (searchIdx == 0) {
      // Nothing trimmed
      return this;
    }
    if (trimIdx >= numBytes) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    return copyUTF8String(trimIdx, numBytes - 1);
  }

  /**
   * Trims space characters (ASCII 32) from the end of this string.
   *
   * @return this string with no spaces at the end
   */
  public UTF8String trimRight() {
    int e = numBytes - 1;
    // skip all of the space (0x20) in the right side
    while (e >= 0 && getByte(e) == 0x20) e--;
    if (e == numBytes - 1) {
      // Nothing trimmed
      return this;
    }
    if (e < 0) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    return copyUTF8String(0, e);
  }

  /**
   * Trims at most `numSpaces` space characters (ASCII 32) from the end of this string.
   */
  public UTF8String trimTrailingSpaces(int numSpaces) {
    assert numSpaces > 0;
    int endIdx = numBytes - 1;
    int trimTo = numBytes - numSpaces;
    while (endIdx >= trimTo && getByte(endIdx) == 0x20) endIdx--;
    return copyUTF8String(0, endIdx);
  }

  /**
   * Trims instances of the given trim string from the end of this string.
   *
   * @param trimString the trim character string
   * @return this string with no occurrences of the trim string at the end, or `null`
   *  if `trimString` is `null`
   */
  public UTF8String trimRight(UTF8String trimString) {
    if (trimString == null) return null;
    int charIdx = 0;
    // number of characters from the source string
    int numChars = 0;
    // array of character length for the source string
    int[] stringCharLen = new int[numBytes];
    // array of the first byte position for each character in the source string
    int[] stringCharPos = new int[numBytes];
    // build the position and length array
    while (charIdx < numBytes) {
      stringCharPos[numChars] = charIdx;
      stringCharLen[numChars] = numBytesForFirstByte(getByte(charIdx));
      charIdx += stringCharLen[numChars];
      numChars ++;
    }

    // index trimEnd points to the first no matching byte position from the right side of
    // the source string.
    int trimEnd = numBytes - 1;
    while (numChars > 0) {
      UTF8String searchChar = copyUTF8String(
          stringCharPos[numChars - 1],
          stringCharPos[numChars - 1] + stringCharLen[numChars - 1] - 1);
      if (trimString.find(searchChar, 0) >= 0) {
        trimEnd -= stringCharLen[numChars - 1];
      } else {
        break;
      }
      numChars --;
    }

    if (trimEnd == numBytes - 1) {
      // Nothing trimmed
      return this;
    }
    if (trimEnd < 0) {
      // Everything trimmed
      return EMPTY_UTF8;
    }
    return copyUTF8String(0, trimEnd);
  }

  public UTF8String reverse() {
    byte[] result = new byte[this.numBytes];

    int i = 0; // position in byte
    while (i < numBytes) {
      int len = numBytesForFirstByte(getByte(i));
      copyMemory(this.base, this.offset + i, result,
        BYTE_ARRAY_OFFSET + result.length - i - len, len);

      i += len;
    }

    return UTF8String.fromBytes(result);
  }

  public UTF8String repeat(int times) {
    if (times <= 0) {
      return EMPTY_UTF8;
    }

    byte[] newBytes = new byte[Math.multiplyExact(numBytes, times)];
    copyMemory(this.base, this.offset, newBytes, BYTE_ARRAY_OFFSET, numBytes);

    int copied = 1;
    while (copied < times) {
      int toCopy = Math.min(copied, times - copied);
      System.arraycopy(newBytes, 0, newBytes, copied * numBytes, numBytes * toCopy);
      copied += toCopy;
    }

    return UTF8String.fromBytes(newBytes);
  }

  /**
   * Returns the (default) position of the first occurrence of an empty substr in the current
   * string from the specified position (0-based index).
   *
   * @param start the start position of the current string for searching
   * @return the position of the first occurrence of the empty substr (now, always 0)
   */
  public int indexOfEmpty(int start) {
    return 0; // TODO: Fix this behaviour (SPARK-48284)
  }

  /**
   * Returns the position of the first occurrence of substr in
   * current string from the specified position (0-based index).
   *
   * @param v the string to be searched
   * @param start the start position of the current string for searching
   * @return the position of the first occurrence of substr, if not found, -1 returned.
   */
  public int indexOf(UTF8String v, int start) {
    if (v.numBytes() == 0) {
      return indexOfEmpty(start);
    }

    // locate to the start position.
    int i = 0; // position in byte
    int c = 0; // position in character
    while (i < numBytes && c < start) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    do {
      if (i + v.numBytes > numBytes) {
        return -1;
      }
      if (ByteArrayMethods.arrayEquals(base, offset + i, v.base, v.offset, v.numBytes)) {
        return c;
      }
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    } while (i < numBytes);

    return -1;
  }

  public int charPosToByte(int charPos) {
    if (charPos < 0) {
      return -1;
    }

    int i = 0;
    int c = 0;
    while (i < numBytes && c < charPos) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }
    return i;
  }

  public int bytePosToChar(int bytePos) {
    int i = 0;
    int c = 0;
    while (i < numBytes && i < bytePos) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }
    return c;
  }

  /**
   * Find the `str` from left to right.
   */
  public int find(UTF8String str, int start) {
    assert (str.numBytes > 0);
    while (start <= numBytes - str.numBytes) {
      if (ByteArrayMethods.arrayEquals(base, offset + start, str.base, str.offset, str.numBytes)) {
        return start;
      }
      start += 1;
    }
    return -1;
  }

  /**
   * Find the `str` from right to left.
   */
  public int rfind(UTF8String str, int start) {
    assert (str.numBytes > 0);
    while (start >= 0) {
      if (ByteArrayMethods.arrayEquals(base, offset + start, str.base, str.offset, str.numBytes)) {
        return start;
      }
      start -= 1;
    }
    return -1;
  }

  /**
   * Returns the substring from string str before count occurrences of the delimiter delim.
   * If count is positive, everything the left of the final delimiter (counting from left) is
   * returned. If count is negative, every to the right of the final delimiter (counting from the
   * right) is returned. subStringIndex performs a case-sensitive match when searching for delim.
   */
  public UTF8String subStringIndex(UTF8String delim, int count) {
    if (delim.numBytes == 0 || count == 0) {
      return EMPTY_UTF8;
    }
    if (count > 0) {
      int idx = -1;
      while (count > 0) {
        idx = find(delim, idx + 1);
        if (idx >= 0) {
          count --;
        } else {
          // can not find enough delim
          return this;
        }
      }
      if (idx == 0) {
        return EMPTY_UTF8;
      }
      byte[] bytes = new byte[idx];
      copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, idx);
      return fromBytes(bytes);

    } else {
      int idx = numBytes - delim.numBytes + 1;
      count = -count;
      while (count > 0) {
        idx = rfind(delim, idx - 1);
        if (idx >= 0) {
          count --;
        } else {
          // can not find enough delim
          return this;
        }
      }
      if (idx + delim.numBytes == numBytes) {
        return EMPTY_UTF8;
      }
      int size = numBytes - delim.numBytes - idx;
      byte[] bytes = new byte[size];
      copyMemory(base, offset + idx + delim.numBytes, bytes, BYTE_ARRAY_OFFSET, size);
      return fromBytes(bytes);
    }
  }

  /**
   * Returns str, right-padded with pad to a length of len
   * For example:
   *   ('hi', 5, '??') =&gt; 'hi???'
   *   ('hi', 1, '??') =&gt; 'h'
   */
  public UTF8String rpad(int len, UTF8String pad) {
    int spaces = len - this.numChars(); // number of char need to pad
    if (spaces <= 0 || pad.numBytes() == 0) {
      // no padding at all, return the substring of the current string
      return substring(0, len);
    } else {
      int padChars = pad.numChars();
      int count = spaces / padChars; // how many padding string needed
      // the partial string of the padding
      UTF8String remain = pad.substring(0, spaces - padChars * count);

      int resultSize =
        Math.toIntExact((long) numBytes + (long) pad.numBytes * count + remain.numBytes);
      byte[] data = new byte[resultSize];
      copyMemory(this.base, this.offset, data, BYTE_ARRAY_OFFSET, this.numBytes);
      int offset = this.numBytes;
      int idx = 0;
      while (idx < count) {
        copyMemory(pad.base, pad.offset, data, BYTE_ARRAY_OFFSET + offset, pad.numBytes);
        ++ idx;
        offset += pad.numBytes;
      }
      copyMemory(remain.base, remain.offset, data, BYTE_ARRAY_OFFSET + offset, remain.numBytes);

      return UTF8String.fromBytes(data);
    }
  }

  /**
   * Returns str, left-padded with pad to a length of len.
   * For example:
   *   ('hi', 5, '??') =&gt; '???hi'
   *   ('hi', 1, '??') =&gt; 'h'
   */
  public UTF8String lpad(int len, UTF8String pad) {
    int spaces = len - this.numChars(); // number of char need to pad
    if (spaces <= 0 || pad.numBytes() == 0) {
      // no padding at all, return the substring of the current string
      return substring(0, len);
    } else {
      int padChars = pad.numChars();
      int count = spaces / padChars; // how many padding string needed
      // the partial string of the padding
      UTF8String remain = pad.substring(0, spaces - padChars * count);

      int resultSize =
        Math.toIntExact((long) numBytes + (long) pad.numBytes * count + remain.numBytes);
      byte[] data = new byte[resultSize];

      int offset = 0;
      int idx = 0;
      while (idx < count) {
        copyMemory(pad.base, pad.offset, data, BYTE_ARRAY_OFFSET + offset, pad.numBytes);
        ++ idx;
        offset += pad.numBytes;
      }
      copyMemory(remain.base, remain.offset, data, BYTE_ARRAY_OFFSET + offset, remain.numBytes);
      offset += remain.numBytes;
      copyMemory(this.base, this.offset, data, BYTE_ARRAY_OFFSET + offset, numBytes());

      return UTF8String.fromBytes(data);
    }
  }

  /**
   * Concatenates input strings together into a single string. Returns null if any input is null.
   */
  public static UTF8String concat(UTF8String... inputs) {
    // Compute the total length of the result.
    long totalLength = 0;
    for (UTF8String input : inputs) {
      if (input == null) {
        return null;
      }
      totalLength += input.numBytes;
    }

    // Allocate a new byte array, and copy the inputs one by one into it.
    final byte[] result = new byte[Math.toIntExact(totalLength)];
    int offset = 0;
    for (UTF8String input : inputs) {
      int len = input.numBytes;
      copyMemory(
        input.base, input.offset,
        result, BYTE_ARRAY_OFFSET + offset,
        len);
      offset += len;
    }
    return fromBytes(result);
  }

  /**
   * Concatenates input strings together into a single string using the separator.
   * A null input is skipped. For example, concat(",", "a", null, "c") would yield "a,c".
   */
  public static UTF8String concatWs(UTF8String separator, UTF8String... inputs) {
    if (separator == null) {
      return null;
    }

    long numInputBytes = 0L;  // total number of bytes from the inputs
    int numInputs = 0;      // number of non-null inputs
    for (UTF8String input : inputs) {
      if (input != null) {
        numInputBytes += input.numBytes;
        numInputs++;
      }
    }

    if (numInputs == 0) {
      // Return an empty string if there is no input, or all the inputs are null.
      return EMPTY_UTF8;
    }

    // Allocate a new byte array, and copy the inputs one by one into it.
    // The size of the new array is the size of all inputs, plus the separators.
    int resultSize = Math.toIntExact(numInputBytes + (numInputs - 1) * (long)separator.numBytes);
    final byte[] result = new byte[resultSize];
    int offset = 0;

    for (int i = 0, j = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        int len = inputs[i].numBytes;
        copyMemory(
          inputs[i].base, inputs[i].offset,
          result, BYTE_ARRAY_OFFSET + offset,
          len);
        offset += len;

        j++;
        // Add separator if this is not the last input.
        if (j < numInputs) {
          copyMemory(
            separator.base, separator.offset,
            result, BYTE_ARRAY_OFFSET + offset,
            separator.numBytes);
          offset += separator.numBytes;
        }
      }
    }
    return fromBytes(result);
  }

  public UTF8String[] split(UTF8String pattern, int limit) {
    // For the empty `pattern` a `split` function ignores trailing empty strings unless original
    // string is empty.
    if (numBytes() != 0 && pattern.numBytes() == 0) {
      int newLimit = limit > numChars() || limit <= 0 ? numChars() : limit;
      byte[] input = getBytes();
      int byteIndex = 0;
      int charIndex = 0;
      UTF8String[] result = new UTF8String[newLimit];
      while (charIndex < newLimit) {
        int currCharNumBytes = numBytesForFirstByte(input[byteIndex]);
        result[charIndex++] = UTF8String.fromBytes(input, byteIndex, currCharNumBytes);
        byteIndex += currCharNumBytes;
      }
      return result;
    }
    return split(pattern.toString(), limit);
  }

  public UTF8String[] splitSQL(UTF8String delimiter, int limit) {
    // if delimiter is empty string, skip the regex based splitting directly as regex
    // treats empty string as matching anything, thus use the input directly.
    if (delimiter.numBytes() == 0) {
      return new UTF8String[]{this};
    } else {
      // we do not treat delimiter as a regex but consider the whole string of delimiter
      // as the separator to split string. Java String's split, however, only accept
      // regex as the pattern to split, thus we can quote the delimiter to escape special
      // characters in the string.
      return split(Pattern.quote(delimiter.toString()), limit);
    }
  }

  private UTF8String[] split(String delimiter, int limit) {
    // Java String's split method supports "ignore empty string" behavior when the limit is 0
    // whereas other languages do not. To avoid this java specific behavior, we fall back to
    // -1 when the limit is 0.
    if (limit == 0) {
      limit = -1;
    }
    String[] splits = toString().split(delimiter, limit);
    UTF8String[] res = new UTF8String[splits.length];
    for (int i = 0; i < res.length; i++) {
      res[i] = fromString(splits[i]);
    }
    return res;
  }

  public UTF8String replace(UTF8String search, UTF8String replace) {
    // This implementation is loosely based on commons-lang3's StringUtils.replace().
    if (numBytes == 0 || search.numBytes == 0) {
      return this;
    }
    // Find the first occurrence of the search string.
    int start = 0;
    int end = this.find(search, start);
    if (end == -1) {
      // Search string was not found, so string is unchanged.
      return this;
    }
    // At least one match was found. Estimate space needed for result.
    // The 16x multiplier here is chosen to match commons-lang3's implementation.
    int increase = Math.max(0, replace.numBytes - search.numBytes) * 16;
    final UTF8StringBuilder buf = new UTF8StringBuilder(numBytes + increase);
    while (end != -1) {
      buf.appendBytes(this.base, this.offset + start, end - start);
      buf.append(replace);
      start = end + search.numBytes;
      end = this.find(search, start);
    }
    buf.appendBytes(this.base, this.offset + start, numBytes - start);
    return buf.build();
  }

  public UTF8String translate(Map<String, String> dict) {
    String srcStr = this.toString();

    StringBuilder sb = new StringBuilder();
    int charCount = 0;
    for (int k = 0; k < srcStr.length(); k += charCount) {
      int codePoint = srcStr.codePointAt(k);
      charCount = Character.charCount(codePoint);
      String subStr = srcStr.substring(k, k + charCount);
      String translated = dict.get(subStr);
      if (null == translated) {
        sb.append(subStr);
      } else if (!"\0".equals(translated)) {
        sb.append(translated);
      }
    }
    return fromString(sb.toString());
  }

  /**
   * Wrapper over `long` to allow result of parsing long from string to be accessed via reference.
   * This is done solely for better performance and is not expected to be used by end users.
   */
  public static class LongWrapper implements Serializable {
    public transient long value = 0;
  }

  /**
   * Wrapper over `int` to allow result of parsing integer from string to be accessed via reference.
   * This is done solely for better performance and is not expected to be used by end users.
   *
   * {@link LongWrapper} could have been used here but using `int` directly save the extra cost of
   * conversion from `long` to `int`
   */
  public static class IntWrapper implements Serializable {
    public transient int value = 0;
  }

  /**
   * Parses this UTF8String(trimmed if needed) to long.
   *
   * Note that, in this method we accumulate the result in negative format, and convert it to
   * positive format at the end, if this string is not started with '-'. This is because min value
   * is bigger than max value in digits, e.g. Long.MAX_VALUE is '9223372036854775807' and
   * Long.MIN_VALUE is '-9223372036854775808'.
   *
   * This code is mostly copied from LazyLong.parseLong in Hive.
   *
   * @param toLongResult If a valid `long` was parsed from this UTF8String, then its value would
   *                     be set in `toLongResult`
   * @return true if the parsing was successful else false
   */
  public boolean toLong(LongWrapper toLongResult) {
    return toLong(toLongResult, true);
  }

  private boolean toLong(LongWrapper toLongResult, boolean allowDecimal) {
    int offset = 0;
    while (offset < this.numBytes && isWhitespaceOrISOControl(getByte(offset))) offset++;
    if (offset == this.numBytes) return false;

    int end = this.numBytes - 1;
    while (end > offset && isWhitespaceOrISOControl(getByte(end))) end--;

    byte b = getByte(offset);
    final boolean negative = b == '-';
    if (negative || b == '+') {
      if (end - offset == 0) {
        return false;
      }
      offset++;
    }

    final byte separator = '.';
    final int radix = 10;
    final long stopValue = Long.MIN_VALUE / radix;
    long result = 0;

    while (offset <= end) {
      b = getByte(offset);
      offset++;
      if (b == separator && allowDecimal) {
        // We allow decimals and will return a truncated integral in that case.
        // Therefore we won't throw an exception here (checking the fractional
        // part happens below.)
        break;
      }

      int digit;
      if (b >= '0' && b <= '9') {
        digit = b - '0';
      } else {
        return false;
      }

      // We are going to process the new digit and accumulate the result. However, before doing
      // this, if the result is already smaller than the stopValue(Long.MIN_VALUE / radix), then
      // result * 10 will definitely be smaller than minValue, and we can stop.
      if (result < stopValue) {
        return false;
      }

      result = result * radix - digit;
      // Since the previous result is less than or equal to stopValue(Long.MIN_VALUE / radix), we
      // can just use `result > 0` to check overflow. If result overflows, we should stop.
      if (result > 0) {
        return false;
      }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset <= end) {
      byte currentByte = getByte(offset);
      if (currentByte < '0' || currentByte > '9') {
        return false;
      }
      offset++;
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        return false;
      }
    }

    toLongResult.value = result;
    return true;
  }

  /**
   * Parses this UTF8String(trimmed if needed) to int.
   *
   * Note that, in this method we accumulate the result in negative format, and convert it to
   * positive format at the end, if this string is not started with '-'. This is because min value
   * is bigger than max value in digits, e.g. Integer.MAX_VALUE is '2147483647' and
   * Integer.MIN_VALUE is '-2147483648'.
   *
   * This code is mostly copied from LazyInt.parseInt in Hive.
   *
   * Note that, this method is almost same as `toLong`, but we leave it duplicated for performance
   * reasons, like Hive does.
   *
   * @param intWrapper If a valid `int` was parsed from this UTF8String, then its value would
   *                    be set in `intWrapper`
   * @return true if the parsing was successful else false
   */
  public boolean toInt(IntWrapper intWrapper) {
    return toInt(intWrapper, true);
  }

  private boolean toInt(IntWrapper intWrapper, boolean allowDecimal) {
    int offset = 0;
    while (offset < this.numBytes && isWhitespaceOrISOControl(getByte(offset))) offset++;
    if (offset == this.numBytes) return false;

    int end = this.numBytes - 1;
    while (end > offset && isWhitespaceOrISOControl(getByte(end))) end--;

    byte b = getByte(offset);
    final boolean negative = b == '-';
    if (negative || b == '+') {
      if (end - offset == 0) {
        return false;
      }
      offset++;
    }

    final byte separator = '.';
    final int radix = 10;
    final int stopValue = Integer.MIN_VALUE / radix;
    int result = 0;

    while (offset <= end) {
      b = getByte(offset);
      offset++;
      if (b == separator && allowDecimal) {
        // We allow decimals and will return a truncated integral in that case.
        // Therefore we won't throw an exception here (checking the fractional
        // part happens below.)
        break;
      }

      int digit;
      if (b >= '0' && b <= '9') {
        digit = b - '0';
      } else {
        return false;
      }

      // We are going to process the new digit and accumulate the result. However, before doing
      // this, if the result is already smaller than the stopValue(Integer.MIN_VALUE / radix), then
      // result * 10 will definitely be smaller than minValue, and we can stop
      if (result < stopValue) {
        return false;
      }

      result = result * radix - digit;
      // Since the previous result is less than or equal to stopValue(Integer.MIN_VALUE / radix),
      // we can just use `result > 0` to check overflow. If result overflows, we should stop
      if (result > 0) {
        return false;
      }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well formed.
    while (offset <= end) {
      byte currentByte = getByte(offset);
      if (currentByte < '0' || currentByte > '9') {
        return false;
      }
      offset++;
    }

    if (!negative) {
      result = -result;
      if (result < 0) {
        return false;
      }
    }
    intWrapper.value = result;
    return true;
  }

  public boolean toShort(IntWrapper intWrapper) {
    if (toInt(intWrapper)) {
      int intValue = intWrapper.value;
      short result = (short) intValue;
      return result == intValue;
    }
    return false;
  }

  public boolean toByte(IntWrapper intWrapper) {
    if (toInt(intWrapper)) {
      int intValue = intWrapper.value;
      byte result = (byte) intValue;
      return result == intValue;
    }
    return false;
  }

  /**
   * Parses UTF8String(trimmed if needed) to long. This method is used when ANSI is enabled.
   *
   * @return If string contains valid numeric value then it returns the long value otherwise a
   * NumberFormatException  is thrown.
   */
  public long toLongExact() {
    LongWrapper result = new LongWrapper();
    if (toLong(result, false)) {
      return result.value;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: '" + this + "'");
  }

  /**
   * Parses UTF8String(trimmed if needed) to int. This method is used when ANSI is enabled.
   *
   * @return If string contains valid numeric value then it returns the int value otherwise a
   * NumberFormatException  is thrown.
   */
  public int toIntExact() {
    IntWrapper result = new IntWrapper();
    if (toInt(result, false)) {
      return result.value;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: '" + this + "'");
  }

  public short toShortExact() {
    int value = this.toIntExact();
    short result = (short) value;
    if (result == value) {
      return result;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: '" + this + "'");
  }

  public byte toByteExact() {
    int value = this.toIntExact();
    byte result = (byte) value;
    if (result == value) {
      return result;
    }
    throw new NumberFormatException("invalid input syntax for type numeric: '" + this + "'");
  }

  /**
   * Returns a string representation of this UTF8String object. The string representation consists
   * of the string's characters encoded in UTF-8 and the result of this method is always a valid
   * UTF-8 string. However, if the current UTF8String contains illegal UTF-8 byte sequences, the
   * method will replace the illegal byte sequences with the Unicode replacement character U+FFFD,
   * according to Java specification. Using this method with invalid UTF8Strings is NOT RECOMMENDED.
   */
  @Override
  public String toString() {
    return new String(getBytes(), StandardCharsets.UTF_8);
  }

  /**
   * Returns a string representation of this UTF8String object, but uses our custom implementation
   * for invalid UTF-8 byte sequence replacement, as per the specification defined in the Unicode
   * standard 15, Section 3.9, Paragraph D86, Table 3-7. Hence, the result of this method is
   * always a valid UTF-8 string. This is the recommended method to use with invalid UTF8Strings.
   */
  public String toValidString() {
    if (isValid()) return toString();
    return new String(makeValidBytes(), StandardCharsets.UTF_8);
  }

  @Override
  public UTF8String clone() {
    return fromBytes(getBytes());
  }

  public UTF8String copy() {
    byte[] bytes = new byte[numBytes];
    copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, numBytes);
    return fromBytes(bytes);
  }

  /**
   * Implementation of Comparable interface. This method is kept for backwards compatibility.
   * It should not be used in spark code base, given that string comparison requires passing
   * collation id. Either explicitly use `binaryCompare` or use `semanticCompare`.
   */
  @Override
  public int compareTo(@Nonnull final UTF8String other) {
    if (SparkEnvUtils$.MODULE$.isTesting()) {
      throw new UnsupportedOperationException(
        "compareTo should not be used in spark code base. Use binaryCompare or semanticCompare.");
    } else {
      return binaryCompare(other);
    }
  }

  /**
   * Binary comparison of two UTF8String. Can only be used for default UTF8_BINARY collation.
   */
  public int binaryCompare(final UTF8String other) {
    return ByteArray.compareBinary(
      base, offset, numBytes, other.base, other.offset, other.numBytes);
  }

  /**
   * Collation-aware comparison of two UTF8String. The collation to use is specified by the
   * `collationId` parameter.
   */
  public int semanticCompare(final UTF8String other, int collationId) {
    return CollationFactory.fetchCollation(collationId).comparator.compare(this, other);
  }

  /**
   * Binary equality check of two UTF8String. Note that binary equality is not the same as
   * equality under given collation. E.g. if string is collated in case-insensitive two strings
   * are considered equal even if they are different in binary comparison.
   */
  @Override
  public boolean equals(final Object other) {
    if (other instanceof UTF8String o) {
      return binaryEquals(o);
    } else {
      return false;
    }
  }

  /**
   * Binary equality check of two UTF8String. Note that binary equality is not the same as
   * equality under given collation. E.g. if string is collated in case-insensitive two strings
   * are considered equal even if they are different in binary comparison.
   */
  public boolean binaryEquals(final UTF8String other) {
    if (numBytes != other.numBytes) {
      return false;
    }

    return ByteArrayMethods.arrayEquals(base, offset, other.base, other.offset, numBytes);
  }

  /**
   * Collation-aware equality comparison of two UTF8String.
   */
  public boolean semanticEquals(final UTF8String other, int collationId) {
    return CollationFactory.fetchCollation(collationId).equalsFunction.apply(this, other);
  }

  /**
   * Levenshtein distance is a metric for measuring the distance of two strings. The distance is
   * defined by the minimum number of single-character edits (i.e. insertions, deletions or
   * substitutions) that are required to change one of the strings into the other.
   */
  public int levenshteinDistance(UTF8String other) {
    // Implementation adopted from
    // org.apache.commons.text.similarity.LevenshteinDistance.unlimitedCompare

    int n = numChars();
    int m = other.numChars();

    if (n == 0) {
      return m;
    } else if (m == 0) {
      return n;
    }

    UTF8String s, t;

    if (n <= m) {
      s = this;
      t = other;
    } else {
      s = other;
      t = this;
      int swap;
      swap = n;
      n = m;
      m = swap;
    }

    int[] p = new int[n + 1];
    int[] d = new int[n + 1];
    int[] swap;

    int i, i_bytes, j, j_bytes, num_bytes_j, cost;

    for (i = 0; i <= n; i++) {
      p[i] = i;
    }

    for (j = 0, j_bytes = 0; j < m; j_bytes += num_bytes_j, j++) {
      num_bytes_j = numBytesForFirstByte(t.getByte(j_bytes));
      d[0] = j + 1;

      for (i = 0, i_bytes = 0; i < n; i_bytes += numBytesForFirstByte(s.getByte(i_bytes)), i++) {
        if (s.getByte(i_bytes) != t.getByte(j_bytes) ||
              num_bytes_j != numBytesForFirstByte(s.getByte(i_bytes))) {
          cost = 1;
        } else {
          cost = (ByteArrayMethods.arrayEquals(t.base, t.offset + j_bytes, s.base,
              s.offset + i_bytes, num_bytes_j)) ? 0 : 1;
        }
        d[i + 1] = Math.min(Math.min(d[i] + 1, p[i + 1] + 1), p[i] + cost);
      }

      swap = p;
      p = d;
      d = swap;
    }

    return p[n];
  }

  public int levenshteinDistance(UTF8String other, int threshold) {
    // Implementation adopted from
    // org.apache.commons.text.similarity.LevenshteinDistance.limitedCompare

    int n = numChars();
    int m = other.numChars();

    if (n == 0) {
      return m <= threshold ? m : -1;
    }
    if (m == 0) {
      return n <= threshold ? n : -1;
    }

    UTF8String s, t;

    if (n <= m) {
      s = this;
      t = other;
    } else {
      s = other;
      t = this;
      int swap;
      swap = n;
      n = m;
      m = swap;
    }

    if (m - n > threshold) {
      return -1;
    }

    int[] p = new int[n + 1];
    int[] d = new int[n + 1];
    int[] swap;

    int i, i_bytes, num_bytes_i, j, j_bytes, num_bytes_j;

    final int boundary = Math.min(n, threshold) + 1;
    for (i = 0; i < boundary; i++) { p[i] = i; }
    Arrays.fill(p, boundary, p.length, Integer.MAX_VALUE);
    Arrays.fill(d, Integer.MAX_VALUE);

    for (j = 0, j_bytes = 0; j < m; j_bytes += num_bytes_j, j++) {
      num_bytes_j = numBytesForFirstByte(t.getByte(j_bytes));

      d[0] = j + 1;

      final int min = Math.max(1, j + 1 - threshold);
      final int max = j + 1 > Integer.MAX_VALUE - threshold ? n : Math.min(n, j + 1 + threshold);
      if (min > 1) {
        d[min - 1] = Integer.MAX_VALUE;
      }

      int lowerBound = Integer.MAX_VALUE;

      for (i = 0, i_bytes = 0; i <= max; i_bytes += num_bytes_i, i++) {
        if (i < min - 1) {
          num_bytes_i = numBytesForFirstByte(s.getByte(i_bytes));
        } else if (i == min - 1) {
          num_bytes_i = 0;
        } else {
          if (ByteArrayMethods.arrayEquals(t.base, t.offset + j_bytes,
                  s.base, s.offset + i_bytes, num_bytes_j)) {
            d[i] = p[i - 1];
          } else {
            d[i] = 1 + Math.min(Math.min(d[i - 1], p[i]), p[i - 1]);
          }
          lowerBound = Math.min(lowerBound, d[i]);
          num_bytes_i = numBytesForFirstByte(s.getByte(i_bytes));
        }
      }

      if (lowerBound > threshold) {
        return -1;
      }

      swap = p;
      p = d;
      d = swap;
    }

    // if p[n] is greater than the threshold, there's no guarantee on it
    // being the correct distance
    if (p[n] <= threshold) {
      return p[n];
    }
    return -1;
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeBytes(base, offset, numBytes, 42);
  }

  /**
   * Soundex mapping table
   */
  private static final byte[] US_ENGLISH_MAPPING = {'0', '1', '2', '3', '0', '1', '2', '7',
    '0', '2', '2', '4', '5', '5', '0', '1', '2', '6', '2', '3', '0', '1', '7', '2', '0', '2'};

  /**
   * Encodes a string into a Soundex value. Soundex is an encoding used to relate similar names,
   * but can also be used as a general purpose scheme to find word with similar phonemes.
   * https://en.wikipedia.org/wiki/Soundex
   */
  public UTF8String soundex() {
    if (numBytes == 0) {
      return EMPTY_UTF8;
    }

    byte b = getByte(0);
    if ('a' <= b && b <= 'z') {
      b -= 32;
    } else if (b < 'A' || 'Z' < b) {
      // first character must be a letter
      return this;
    }
    byte[] sx = {'0', '0', '0', '0'};
    sx[0] = b;
    int sxi = 1;
    int idx = b - 'A';
    byte lastCode = US_ENGLISH_MAPPING[idx];

    for (int i = 1; i < numBytes; i++) {
      b = getByte(i);
      if ('a' <= b && b <= 'z') {
        b -= 32;
      } else if (b < 'A' || 'Z' < b) {
        // not a letter, skip it
        lastCode = '0';
        continue;
      }
      idx = b - 'A';
      byte code = US_ENGLISH_MAPPING[idx];
      if (code == '7') {
        // ignore it
      } else {
        if (code != '0' && code != lastCode) {
          sx[sxi++] = code;
          if (sxi > 3) break;
        }
        lastCode = code;
      }
    }
    return UTF8String.fromBytes(sx);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    byte[] bytes = getBytes();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    offset = BYTE_ARRAY_OFFSET;
    numBytes = in.readInt();
    base = new byte[numBytes];
    in.readFully((byte[]) base);
  }

  @Override
  public void write(Kryo kryo, Output out) {
    byte[] bytes = getBytes();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void read(Kryo kryo, Input in) {
    this.offset = BYTE_ARRAY_OFFSET;
    this.numBytes = in.readInt();
    this.base = new byte[numBytes];
    in.read((byte[]) base);
  }

  /**
   * Convert a long value to its binary format stripping leading zeros.
   */
  public static UTF8String toBinaryString(long val) {
    int zeros = Long.numberOfLeadingZeros(val);
    if (zeros == Long.SIZE) {
      return UTF8String.ZERO_UTF8;
    } else {
      int length = Long.SIZE - zeros;
      byte[] bytes = new byte[length];
      do {
        bytes[--length] = (byte) ((val & 0x1) == 1 ? '1': '0');
        val >>>= 1;
      } while (length > 0);
      return fromBytes(bytes);
    }
  }
}
