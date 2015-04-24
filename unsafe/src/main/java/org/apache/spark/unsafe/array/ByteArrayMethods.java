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

public class ByteArrayMethods {

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
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
    // TODO: this can be optimized by comparing words and falling back to individual byte
    // comparison only at the end of the array (Guava's UnsignedBytes has an implementation of this)
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
