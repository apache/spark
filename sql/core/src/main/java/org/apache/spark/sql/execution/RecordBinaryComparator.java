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

package org.apache.spark.sql.execution;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;

import java.nio.ByteOrder;

public final class RecordBinaryComparator extends RecordComparator {

  private static final boolean UNALIGNED = Platform.unaligned();
  private static final boolean LITTLE_ENDIAN =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  @Override
  public int compare(
      Object leftObj, long leftOff, int leftLen, Object rightObj, long rightOff, int rightLen) {
    int i = 0;

    // If the arrays have different length, the longer one is larger.
    if (leftLen != rightLen) {
      return leftLen - rightLen;
    }

    // The following logic uses `leftLen` as the length for both `leftObj` and `rightObj`, since
    // we have guaranteed `leftLen` == `rightLen`.

    // check if stars align and we can get both offsets to be aligned
    if (!UNALIGNED && ((leftOff % 8) == (rightOff % 8))) {
      while ((leftOff + i) % 8 != 0 && i < leftLen) {
        final int v1 = Platform.getByte(leftObj, leftOff + i);
        final int v2 = Platform.getByte(rightObj, rightOff + i);
        if (v1 != v2) {
          return (v1 & 0xff) > (v2 & 0xff) ? 1 : -1;
        }
        i += 1;
      }
    }
    // for architectures that support unaligned accesses, chew it up 8 bytes at a time
    if (UNALIGNED || (((leftOff + i) % 8 == 0) && ((rightOff + i) % 8 == 0))) {
      while (i <= leftLen - 8) {
        long v1 = Platform.getLong(leftObj, leftOff + i);
        long v2 = Platform.getLong(rightObj, rightOff + i);
        if (v1 != v2) {
          if (LITTLE_ENDIAN) {
            // if read as little-endian, we have to reverse bytes so that the long comparison result
            // is equivalent to byte-by-byte comparison result.
            // See discussion in https://github.com/apache/spark/pull/26548#issuecomment-554645859
            v1 = Long.reverseBytes(v1);
            v2 = Long.reverseBytes(v2);
          }
          return Long.compareUnsigned(v1, v2);
        }
        i += 8;
      }
    }
    // this will finish off the unaligned comparisons, or do the entire aligned comparison
    // whichever is needed.
    while (i < leftLen) {
      final int v1 = Platform.getByte(leftObj, leftOff + i);
      final int v2 = Platform.getByte(rightObj, rightOff + i);
      if (v1 != v2) {
        return (v1 & 0xff) > (v2 & 0xff) ? 1 : -1;
      }
      i += 1;
    }

    // The two arrays are equal.
    return 0;
  }
}
