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

package org.apache.spark.types.variant;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkRuntimeException;
import scala.collection.immutable.Map$;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This class defines constants related to the variant format and provides functions for
 * manipulating variant binaries.

 * A variant is made up of 2 binaries: value and metadata. A variant value consists of a one-byte
 * header and a number of content bytes (can be zero). The header byte is divided into upper 6 bits
 * (called "type info") and lower 2 bits (called "basic type"). The content format is explained in
 * the below constants for all possible basic type and type info values.

 * The variant metadata includes a version id and a dictionary of distinct strings (case-sensitive).
 * Its binary format is:
 * - Version: 1-byte unsigned integer. The only acceptable value is 1 currently.
 * - Dictionary size: 4-byte little-endian unsigned integer. The number of keys in the
 * dictionary.
 * - Offsets: (size + 1) * 4-byte little-endian unsigned integers. `offsets[i]` represents the
 * starting position of string i, counting starting from the address of `offsets[0]`. Strings
 * must be stored contiguously, so we don’t need to store the string size, instead, we compute it
 * with `offset[i + 1] - offset[i]`.
 * - UTF-8 string data.
 */
public class VariantUtil {
  public static final int BASIC_TYPE_BITS = 2;
  public static final int BASIC_TYPE_MASK = 0x3;
  public static final int TYPE_INFO_MASK = 0x3F;
  // The inclusive maximum value of the type info value. It is the size limit of `SHORT_STR`.
  public static final int MAX_SHORT_STR_SIZE = 0x3F;

  // Below is all possible basic type values.
  // Primitive value. The type info value must be one of the values in the below section.
  public static final int PRIMITIVE = 0;
  // Short string value. The type info value is the string size, which must be in `[0,
  // kMaxShortStrSize]`.
  // The string content bytes directly follow the header byte.
  public static final int SHORT_STR = 1;
  // Object value. The content contains a size, a list of field ids, a list of field offsets, and
  // the actual field data. The length of the id list is `size`, while the length of the offset
  // list is `size + 1`, where the last offset represent the total size of the field data. The
  // fields in an object must be sorted by the field name in alphabetical order. Duplicate field
  // names in one object are not allowed.
  // We use 5 bits in the type info to specify the integer type of the object header: it should
  // be 0_b4_b3b2_b1b0 (MSB is 0), where:
  // - b4 specifies the type of size. When it is 0/1, `size` is a little-endian 1/4-byte
  // unsigned integer.
  // - b3b2/b1b0 specifies the integer type of id and offset. When the 2 bits are  0/1/2, the
  // list contains 1/2/3-byte little-endian unsigned integers.
  public static final int OBJECT = 2;
  // Array value. The content contains a size, a list of field offsets, and the actual element
  // data. It is similar to an object without the id list. The length of the offset list
  // is `size + 1`, where the last offset represent the total size of the element data.
  // Its type info should be: 000_b2_b1b0:
  // - b2 specifies the type of size.
  // - b1b0 specifies the integer type of offset.
  public static final int ARRAY = 3;

  // Below is all possible type info values for `PRIMITIVE`.
  // JSON Null value. Empty content.
  public static final int NULL = 0;
  // True value. Empty content.
  public static final int TRUE = 1;
  // False value. Empty content.
  public static final int FALSE = 2;
  // 1-byte little-endian signed integer.
  public static final int INT1 = 3;
  // 2-byte little-endian signed integer.
  public static final int INT2 = 4;
  // 4-byte little-endian signed integer.
  public static final int INT4 = 5;
  // 4-byte little-endian signed integer.
  public static final int INT8 = 6;
  // 8-byte IEEE double.
  public static final int DOUBLE = 7;
  // 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer.
  public static final int DECIMAL4 = 8;
  // 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer.
  public static final int DECIMAL8 = 9;
  // 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer.
  public static final int DECIMAL16 = 10;
  // Long string value. The content is (4-byte little-endian unsigned integer representing the
  // string size) + (size bytes of string content).
  public static final int LONG_STR = 16;

  public static final byte VERSION = 1;
  // The lower 4 bits of the first metadata byte contain the version.
  public static final byte VERSION_MASK = 0x0F;

  public static final int U8_MAX = 0xFF;
  public static final int U16_MAX = 0xFFFF;
  public static final int U24_MAX = 0xFFFFFF;
  public static final int U24_SIZE = 3;
  public static final int U32_SIZE = 4;

  // Both variant value and variant metadata need to be no longer than 16MiB.
  public static final int SIZE_LIMIT = U24_MAX + 1;

  public static final int MAX_DECIMAL4_PRECISION = 9;
  public static final int MAX_DECIMAL8_PRECISION = 18;
  public static final int MAX_DECIMAL16_PRECISION = 38;

  // Write the least significant `numBytes` bytes in `value` into `bytes[pos, pos + numBytes)` in
  // little endian.
  public static void writeLong(byte[] bytes, int pos, long value, int numBytes) {
    for (int i = 0; i < numBytes; ++i) {
      bytes[pos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
    }
  }

  public static byte primitiveHeader(int type) {
    return (byte) (type << 2 | PRIMITIVE);
  }

  public static byte shortStrHeader(int size) {
    return (byte) (size << 2 | SHORT_STR);
  }

  public static byte objectHeader(boolean largeSize, int idSize, int offsetSize) {
    return (byte) (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 4)) |
        ((idSize - 1) << (BASIC_TYPE_BITS + 2)) |
        ((offsetSize - 1) << BASIC_TYPE_BITS) | OBJECT);
  }

  public static byte arrayHeader(boolean largeSize, int offsetSize) {
    return (byte) (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 2)) |
        ((offsetSize - 1) << BASIC_TYPE_BITS) | ARRAY);
  }

  // An exception indicating that the variant value or metadata doesn't
  static SparkRuntimeException malformedVariant() {
    return new SparkRuntimeException("MALFORMED_VARIANT",
        Map$.MODULE$.<String, String>empty(), null, new QueryContext[]{}, "");
  }

  // An exception indicating that an external caller tried to call the Variant constructor with
  // value or metadata exceeding the 16MiB size limit. We will never construct a Variant this large,
  // so it should only be possible to encounter this exception when reading a Variant produced by
  // another tool.
  static SparkRuntimeException variantConstructorSizeLimit() {
    return new SparkRuntimeException("VARIANT_CONSTRUCTOR_SIZE_LIMIT",
        Map$.MODULE$.<String, String>empty(), null, new QueryContext[]{}, "");
  }

  // Check the validity of an array index `pos`. Throw `MALFORMED_VARIANT` if it is out of bound,
  // meaning that the variant is malformed.
  static void checkIndex(int pos, int length) {
    if (pos < 0 || pos >= length) throw malformedVariant();
  }

  // Read a little-endian signed long value from `bytes[pos, pos + numBytes)`.
  static long readLong(byte[] bytes, int pos, int numBytes) {
    checkIndex(pos, bytes.length);
    checkIndex(pos + numBytes - 1, bytes.length);
    long result = 0;
    // All bytes except the most significant byte should be unsign-extended and shifted (so we need
    // `& 0xFF`). The most significant byte should be sign-extended and is handled after the loop.
    for (int i = 0; i < numBytes - 1; ++i) {
      long unsignedByteValue = bytes[pos + i] & 0xFF;
      result |= unsignedByteValue << (8 * i);
    }
    long signedByteValue = bytes[pos + numBytes - 1];
    result |= signedByteValue << (8 * (numBytes - 1));
    return result;
  }

  // Read a little-endian unsigned int value from `bytes[pos, pos + numBytes)`. The value must fit
  // into a non-negative int (`[0, Integer.MAX_VALUE]`).
  static int readUnsigned(byte[] bytes, int pos, int numBytes) {
    checkIndex(pos, bytes.length);
    checkIndex(pos + numBytes - 1, bytes.length);
    int result = 0;
    // Similar to the `readLong` loop, but all bytes should be unsign-extended.
    for (int i = 0; i < numBytes; ++i) {
      int unsignedByteValue = bytes[pos + i] & 0xFF;
      result |= unsignedByteValue << (8 * i);
    }
    if (result < 0) throw malformedVariant();
    return result;
  }

  // The value type of variant value. It is determined by the header byte but not a 1:1 mapping
  // (for example, INT1/2/4/8 all maps to `Type.LONG`).
  public enum Type {
    OBJECT,
    ARRAY,
    NULL,
    BOOLEAN,
    LONG,
    STRING,
    DOUBLE,
    DECIMAL,
  }

  // Get the value type of variant value `value[pos...]`. It is only legal to call `get*` if
  // `getType` returns this type (for example, it is only legal to call `getLong` if `getType`
  // returns `Type.Long`).
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static Type getType(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    switch (basicType) {
      case SHORT_STR:
        return Type.STRING;
      case OBJECT:
        return Type.OBJECT;
      case ARRAY:
        return Type.ARRAY;
      default:
        switch (typeInfo) {
          case NULL:
            return Type.NULL;
          case TRUE:
          case FALSE:
            return Type.BOOLEAN;
          case INT1:
          case INT2:
          case INT4:
          case INT8:
            return Type.LONG;
          case DOUBLE:
            return Type.DOUBLE;
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            return Type.DECIMAL;
          case LONG_STR:
            return Type.STRING;
          default:
            throw malformedVariant();
        }
    }
  }

  // Compute the size in bytes of the variant value `value[pos...]`. `value.length - pos` is an
  // upper bound of the size, but the actual size can be smaller.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static int valueSize(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    switch (basicType) {
      case SHORT_STR:
        return 1 + typeInfo;
      case OBJECT:
        return handleObject(value, pos,
            (size, idSize, offsetSize, idStart, offsetStart, dataStart) ->
                dataStart - pos + readUnsigned(value, offsetStart + size * offsetSize, offsetSize));
      case ARRAY:
        return handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) ->
            dataStart - pos + readUnsigned(value, offsetStart + size * offsetSize, offsetSize));
      default:
        switch (typeInfo) {
          case NULL:
          case TRUE:
          case FALSE:
            return 1;
          case INT1:
            return 2;
          case INT2:
            return 3;
          case INT4:
            return 5;
          case INT8:
          case DOUBLE:
            return 9;
          case DECIMAL4:
            return 6;
          case DECIMAL8:
            return 10;
          case DECIMAL16:
            return 18;
          case LONG_STR:
            return 1 + U32_SIZE + readUnsigned(value, pos + 1, U32_SIZE);
          default:
            throw malformedVariant();
        }
    }
  }

  static IllegalStateException unexpectedType(Type type) {
    return new IllegalStateException("Expect type to be " + type);
  }

  // Get a boolean value from variant value `value[pos...]`.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static boolean getBoolean(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType != PRIMITIVE || (typeInfo != TRUE && typeInfo != FALSE)) {
      throw unexpectedType(Type.BOOLEAN);
    }
    return typeInfo == TRUE;
  }

  // Get a long value from variant value `value[pos...]`.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static long getLong(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType != PRIMITIVE) throw unexpectedType(Type.LONG);
    switch (typeInfo) {
      case INT1:
        return readLong(value, pos + 1, 1);
      case INT2:
        return readLong(value, pos + 1, 2);
      case INT4:
        return readLong(value, pos + 1, 4);
      case INT8:
        return readLong(value, pos + 1, 8);
      default:
        throw unexpectedType(Type.LONG);
    }
  }

  // Get a double value from variant value `value[pos...]`.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static double getDouble(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType != PRIMITIVE || typeInfo != DOUBLE) throw unexpectedType(Type.DOUBLE);
    return Double.longBitsToDouble(readLong(value, pos + 1, 8));
  }

  // Get a decimal value from variant value `value[pos...]`.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static BigDecimal getDecimal(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType != PRIMITIVE) throw unexpectedType(Type.DECIMAL);
    int scale = value[pos + 1];
    BigDecimal result;
    switch (typeInfo) {
      case DECIMAL4:
        result = BigDecimal.valueOf(readLong(value, pos + 2, 4), scale);
        break;
      case DECIMAL8:
        result = BigDecimal.valueOf(readLong(value, pos + 2, 8), scale);
        break;
      case DECIMAL16:
        checkIndex(pos + 17, value.length);
        byte[] bytes = new byte[16];
        // Copy the bytes reversely because the `BigInteger` constructor expects a big-endian
        // representation.
        for (int i = 0; i < 16; ++i) {
          bytes[i] = value[pos + 17 - i];
        }
        result = new BigDecimal(new BigInteger(bytes), scale);
        break;
      default:
        throw unexpectedType(Type.DECIMAL);
    }
    return result.stripTrailingZeros();
  }

  // Get a string value from variant value `value[pos...]`.
  // Throw `MALFORMED_VARIANT` if the variant is malformed.
  public static String getString(byte[] value, int pos) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType == SHORT_STR || (basicType == PRIMITIVE && typeInfo == LONG_STR)) {
      int start;
      int length;
      if (basicType == SHORT_STR) {
        start = pos + 1;
        length = typeInfo;
      } else {
        start = pos + 1 + U32_SIZE;
        length = readUnsigned(value, pos + 1, U32_SIZE);
      }
      checkIndex(start + length - 1, value.length);
      return new String(value, start, length);
    }
    throw unexpectedType(Type.STRING);
  }

  public interface ObjectHandler<T> {
    /**
     * @param size Number of object fields.
     * @param idSize The integer size of the field id list.
     * @param offsetSize The integer size of the offset list.
     * @param idStart The starting index of the field id list in the variant value array.
     * @param offsetStart The starting index of the offset list in the variant value array.
     * @param dataStart The starting index of field data in the variant value array.
     */
    T apply(int size, int idSize, int offsetSize, int idStart, int offsetStart, int dataStart);
  }

  // A helper function to access a variant object. It provides `handler` with its required
  // parameters and returns what it returns.
  public static <T> T handleObject(byte[] value, int pos, ObjectHandler<T> handler) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType != OBJECT) throw unexpectedType(Type.OBJECT);
    // Refer to the comment of the `OBJECT` constant for the details of the object header encoding.
    // Suppose `typeInfo` has a bit representation of 0_b4_b3b2_b1b0, the following line extracts
    // b4 to determine whether the object uses a 1/4-byte size.
    boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
    int sizeBytes = (largeSize ? U32_SIZE : 1);
    int size = readUnsigned(value, pos + 1, sizeBytes);
    // Extracts b3b2 to determine the integer size of the field id list.
    int idSize = ((typeInfo >> 2) & 0x3) + 1;
    // Extracts b1b0 to determine the integer size of the offset list.
    int offsetSize = (typeInfo & 0x3) + 1;
    int idStart = pos + 1 + sizeBytes;
    int offsetStart = idStart + size * idSize;
    int dataStart = offsetStart + (size + 1) * offsetSize;
    return handler.apply(size, idSize, offsetSize, idStart, offsetStart, dataStart);
  }

  public interface ArrayHandler<T> {
    /**
     * @param size Number of array elements.
     * @param offsetSize The integer size of the offset list.
     * @param offsetStart The starting index of the offset list in the variant value array.
     * @param dataStart The starting index of element data in the variant value array.
     */
    T apply(int size, int offsetSize, int offsetStart, int dataStart);
  }

  // A helper function to access a variant array.
  public static <T> T handleArray(byte[] value, int pos, ArrayHandler<T> handler) {
    checkIndex(pos, value.length);
    int basicType = value[pos] & BASIC_TYPE_MASK;
    int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if (basicType != ARRAY) throw unexpectedType(Type.ARRAY);
    // Refer to the comment of the `ARRAY` constant for the details of the object header encoding.
    // Suppose `typeInfo` has a bit representation of 000_b2_b1b0, the following line extracts
    // b2 to determine whether the object uses a 1/4-byte size.
    boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
    int sizeBytes = (largeSize ? U32_SIZE : 1);
    int size = readUnsigned(value, pos + 1, sizeBytes);
    // Extracts b1b0 to determine the integer size of the offset list.
    int offsetSize = (typeInfo & 0x3) + 1;
    int offsetStart = pos + 1 + sizeBytes;
    int dataStart = offsetStart + (size + 1) * offsetSize;
    return handler.apply(size, offsetSize, offsetStart, dataStart);
  }

  // Get a key at `id` in the variant metadata.
  // Throw `MALFORMED_VARIANT` if the variant is malformed. An out-of-bound `id` is also considered
  // a malformed variant because it is read from the corresponding variant value.
  public static String getMetadataKey(byte[] metadata, int id) {
    checkIndex(0, metadata.length);
    // Extracts the highest 2 bits in the metadata header to determine the integer size of the
    // offset list.
    int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
    int dictSize = readUnsigned(metadata, 1, offsetSize);
    if (id >= dictSize) throw malformedVariant();
    // There are a header byte, a `dictSize` with `offsetSize` bytes, and `(dictSize + 1)` offsets
    // before the string data.
    int stringStart = 1 + (dictSize + 2) * offsetSize;
    int offset = readUnsigned(metadata, 1 + (id + 1) * offsetSize, offsetSize);
    int nextOffset = readUnsigned(metadata, 1 + (id + 2) * offsetSize, offsetSize);
    if (offset > nextOffset) throw malformedVariant();
    checkIndex(stringStart + nextOffset - 1, metadata.length);
    return new String(metadata, stringStart + offset, nextOffset - offset);
  }
}
