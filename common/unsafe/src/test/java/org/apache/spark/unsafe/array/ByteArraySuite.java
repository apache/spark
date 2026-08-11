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
import org.apache.spark.unsafe.types.ByteArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ByteArraySuite {
  private long getPrefixByByte(byte[] bytes) {
    final int minLen = Math.min(bytes.length, 8);
    long p = 0;
    for (int i = 0; i < minLen; ++i) {
      p |= ((long) Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + i) & 0xff)
              << (56 - 8 * i);
    }
    return p;
  }

  @Test
  public void testGetPrefix() {
    for (int i = 0; i <= 9; i++) {
      byte[] bytes = new byte[i];
      int prefix = i - 1;
      while (prefix >= 0) {
        bytes[prefix] = (byte) prefix;
        prefix -= 1;
      }

      long result = ByteArray.getPrefix(bytes);
      long expected = getPrefixByByte(bytes);
      Assertions.assertEquals(result, expected);
    }
  }

  @Test
  public void testCompareBinary() {
    byte[] x1 = new byte[0];
    byte[] y1 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    Assertions.assertTrue(ByteArray.compareBinary(x1, y1) < 0);

    byte[] x2 = new byte[]{(byte) 200, (byte) 100};
    byte[] y2 = new byte[]{(byte) 100, (byte) 100};
    Assertions.assertTrue(ByteArray.compareBinary(x2, y2) > 0);

    byte[] x3 = new byte[]{(byte) 100, (byte) 200, (byte) 12};
    byte[] y3 = new byte[]{(byte) 100, (byte) 200};
    Assertions.assertTrue(ByteArray.compareBinary(x3, y3) > 0);

    byte[] x4 = new byte[]{(byte) 100, (byte) 200};
    byte[] y4 = new byte[]{(byte) 100, (byte) 200};
    Assertions.assertEquals(0, ByteArray.compareBinary(x4, y4));
  }

  @Test
  public void testConcat() {
    byte[] x1 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    byte[] y1 = new byte[]{(byte) 4, (byte) 5, (byte) 6};
    byte[] result1 = ByteArray.concat(x1, y1);
    byte[] expected1 = new byte[]{(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6};
    Assertions.assertArrayEquals(expected1, result1);

    byte[] x2 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    byte[] y2 = new byte[0];
    byte[] result2 = ByteArray.concat(x2, y2);
    byte[] expected2 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    Assertions.assertArrayEquals(expected2, result2);

    byte[] x3 = new byte[0];
    byte[] y3 = new byte[]{(byte) 4, (byte) 5, (byte) 6};
    byte[] result3 = ByteArray.concat(x3, y3);
    byte[] expected3 = new byte[]{(byte) 4, (byte) 5, (byte) 6};
    Assertions.assertArrayEquals(expected3, result3);

    byte[] x4 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    byte[] y4 = null;
    byte[] result4 = ByteArray.concat(x4, y4);
    Assertions.assertArrayEquals(null, result4);
  }

  @Test
  public void testConcatWS() {
    byte[] separator = new byte[]{(byte) 42};

    byte[] x1 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    byte[] y1 = new byte[]{(byte) 4, (byte) 5, (byte) 6};
    byte[] result1 = ByteArray.concatWS(separator, x1, y1);
    byte[] expected1 = new byte[]{(byte) 1, (byte) 2, (byte) 3, (byte) 42,
            (byte) 4, (byte) 5, (byte) 6};
    Assertions.assertArrayEquals(expected1, result1);

    byte[] x2 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    byte[] y2 = new byte[0];
    byte[] result2 = ByteArray.concatWS(separator, x2, y2);
    byte[] expected2 = new byte[]{(byte) 1, (byte) 2, (byte) 3, (byte) 42};
    Assertions.assertArrayEquals(expected2, result2);

    byte[] x3 = new byte[0];
    byte[] y3 = new byte[]{(byte) 4, (byte) 5, (byte) 6};
    byte[] result3 = ByteArray.concatWS(separator, x3, y3);
    byte[] expected3 = new byte[]{(byte) 42, (byte) 4, (byte) 5, (byte) 6};
    Assertions.assertArrayEquals(expected3, result3);

    byte[] x4 = new byte[]{(byte) 1, (byte) 2, (byte) 3};
    byte[] y4 = null;
    byte[] result4 = ByteArray.concatWS(separator, x4, y4);
    Assertions.assertArrayEquals(null, result4);
  }

  @Test
  public void testSubStringSQL() {
    byte[] bytes = new byte[]{(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
    byte[] empty = new byte[0];

    // Positive, zero and negative positions.
    Assertions.assertArrayEquals(new byte[]{(byte) 2, (byte) 3},
            ByteArray.subStringSQL(bytes, 2, 2));
    Assertions.assertArrayEquals(new byte[]{(byte) 1, (byte) 2},
            ByteArray.subStringSQL(bytes, 0, 2));
    Assertions.assertArrayEquals(new byte[]{(byte) 4, (byte) 5},
            ByteArray.subStringSQL(bytes, -2, 2));

    // A position past the end of the input, and a length that runs past it.
    Assertions.assertArrayEquals(empty, ByteArray.subStringSQL(bytes, 6, 2));
    Assertions.assertArrayEquals(new byte[]{(byte) 4, (byte) 5},
            ByteArray.subStringSQL(bytes, 4, 100));

    // A non-positive length yields the empty byte sequence.
    Assertions.assertArrayEquals(empty, ByteArray.subStringSQL(bytes, 2, 0));
    Assertions.assertArrayEquals(empty, ByteArray.subStringSQL(bytes, 2, -1));

    // SPARK-58708: `start + len` must be computed without overflowing the `int` range.
    // Before the fix these returned a byte sequence longer than the input, zero-padded by
    // `Arrays.copyOfRange`, instead of the empty byte sequence.
    Assertions.assertArrayEquals(empty,
            ByteArray.subStringSQL(bytes, -1207959552, -1207959552));
    Assertions.assertArrayEquals(empty,
            ByteArray.subStringSQL(bytes, -2147483647, Integer.MIN_VALUE));
    Assertions.assertArrayEquals(empty,
            ByteArray.subStringSQL(bytes, Integer.MIN_VALUE, 5));

    // The same offsets, in combinations that do have a non-empty result.
    Assertions.assertArrayEquals(bytes, ByteArray.subStringSQL(bytes, 1, Integer.MAX_VALUE));
    Assertions.assertArrayEquals(new byte[]{(byte) 1, (byte) 2, (byte) 3, (byte) 4},
            ByteArray.subStringSQL(bytes, Integer.MIN_VALUE, Integer.MAX_VALUE));
  }

  @Test
  public void testPad() {
    byte[] bytes = new byte[]{(byte) 1, (byte) 2};
    byte[] pad = new byte[]{(byte) 3, (byte) 4};
    byte[] emptyPad = new byte[0];
    byte[] empty = new byte[0];

    // Sanity: positive lengths are unaffected.
    Assertions.assertArrayEquals(new byte[]{(byte) 3, (byte) 4, (byte) 3, (byte) 1, (byte) 2},
            ByteArray.lpad(bytes, 5, pad));
    Assertions.assertArrayEquals(new byte[]{(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 3},
            ByteArray.rpad(bytes, 5, pad));
    Assertions.assertArrayEquals(new byte[]{(byte) 1}, ByteArray.lpad(bytes, 1, pad));
    Assertions.assertArrayEquals(new byte[]{(byte) 1}, ByteArray.rpad(bytes, 1, pad));

    // SPARK-58708: a non-positive length yields the empty byte sequence, matching
    // `UTF8String.lpad` and `UTF8String.rpad`. Before the fix a negative length reached
    // `new byte[len]` and threw `NegativeArraySizeException`.
    for (int len : new int[]{0, -1, -100, Integer.MIN_VALUE}) {
      Assertions.assertArrayEquals(empty, ByteArray.lpad(bytes, len, pad));
      Assertions.assertArrayEquals(empty, ByteArray.rpad(bytes, len, pad));
      // The empty-padding-pattern path allocates separately and must be guarded too.
      Assertions.assertArrayEquals(empty, ByteArray.lpad(bytes, len, emptyPad));
      Assertions.assertArrayEquals(empty, ByteArray.rpad(bytes, len, emptyPad));
    }

    // A null input or padding pattern still yields null.
    Assertions.assertArrayEquals(null, ByteArray.lpad(null, -1, pad));
    Assertions.assertArrayEquals(null, ByteArray.rpad(bytes, -1, null));
  }
}
