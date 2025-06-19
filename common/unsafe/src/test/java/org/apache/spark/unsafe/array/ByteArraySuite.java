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
}
