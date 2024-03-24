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

package org.apache.spark.variant;

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
}
