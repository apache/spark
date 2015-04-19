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

package org.apache.spark.unsafe.array;

import org.apache.spark.unsafe.PlatformDependent;

import java.lang.Object;

public class ByteArrayMethods {

  // TODO: there are substantial opportunities for optimization here and we should investigate them.
  // See the fast comparisions in Guava's UnsignedBytes, for instance:
  // https://code.google.com/p/guava-libraries/source/browse/guava/src/com/google/common/primitives/UnsignedBytes.java

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    int remainder = numBytes % 8;
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
    }
  }

  public static void zeroBytes(
      Object baseObject,
      long baseOffset,
      long lengthInBytes) {
    for (int i = 0; i < lengthInBytes; i++) {
      PlatformDependent.UNSAFE.putByte(baseObject, baseOffset + i, (byte) 0);
    }
  }

  /**
   * Optimized  equality check for equal-length byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean arrayEquals(
      Object leftBaseObject,
      long leftBaseOffset,
      Object rightBaseObject,
      long rightBaseOffset,
      long arrayLengthInBytes) {
    for (int i = 0; i < arrayLengthInBytes; i++) {
      final byte left =
        PlatformDependent.UNSAFE.getByte(leftBaseObject, leftBaseOffset + i);
      final byte right =
        PlatformDependent.UNSAFE.getByte(rightBaseObject, rightBaseOffset + i);
      if (left != right) return false;
    }
    return true;
  }

  /**
   * Optimized byte array equality check for 8-byte-word-aligned byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean wordAlignedArrayEquals(
      Object leftBaseObject,
      long leftBaseOffset,
      Object rightBaseObject,
      long rightBaseOffset,
      long arrayLengthInBytes) {
    for (int i = 0; i < arrayLengthInBytes; i += 8) {
      final long left =
        PlatformDependent.UNSAFE.getLong(leftBaseObject, leftBaseOffset + i);
      final long right =
        PlatformDependent.UNSAFE.getLong(rightBaseObject, rightBaseOffset + i);
      if (left != right) return false;
    }
    return true;
  }
}
