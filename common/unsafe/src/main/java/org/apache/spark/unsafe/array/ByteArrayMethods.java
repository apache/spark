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

import org.apache.spark.unsafe.Platform;

public class ByteArrayMethods {

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  /** Returns the next number greater or equal num that is power of 2. */
  public static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    return (int)roundNumberOfBytesToNearestWord((long)numBytes);
  }

  public static long roundNumberOfBytesToNearestWord(long numBytes) {
    long remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
    }
  }

  // Some JVMs can't allocate arrays of length Integer.MAX_VALUE; actual max is somewhat smaller.
  // Be conservative and lower the cap a little.
  // Refer to "http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/tip/src/share/classes/java/util/ArrayList.java#l229"
  // This value is word rounded. Use this value if the allocated byte arrays are used to store other
  // types rather than bytes.
  public static final int MAX_ROUNDED_ARRAY_LENGTH = Integer.MAX_VALUE - 15;

  private static final boolean unaligned = Platform.unaligned();
  /**
   * Optimized byte array equality check for byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean arrayEquals(
      Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
    int i = 0;

    // check if stars align and we can get both offsets to be aligned
    if (!unaligned && ((leftOffset % 8) == (rightOffset % 8))) {
      while ((leftOffset + i) % 8 != 0 && i < length) {
        if (Platform.getByte(leftBase, leftOffset + i) !=
            Platform.getByte(rightBase, rightOffset + i)) {
              return false;
        }
        i += 1;
      }
    }
    // for architectures that support unaligned accesses, chew it up 8 bytes at a time
    if (unaligned || (((leftOffset + i) % 8 == 0) && ((rightOffset + i) % 8 == 0))) {
      while (i <= length - 8) {
        if (Platform.getLong(leftBase, leftOffset + i) !=
            Platform.getLong(rightBase, rightOffset + i)) {
              return false;
        }
        i += 8;
      }
    }
    // this will finish off the unaligned comparisons, or do the entire aligned
    // comparison whichever is needed.
    while (i < length) {
      if (Platform.getByte(leftBase, leftOffset + i) !=
          Platform.getByte(rightBase, rightOffset + i)) {
            return false;
      }
      i += 1;
    }
    return true;
  }
}
