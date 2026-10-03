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

import java.util.Arrays;
import java.util.Random;

import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

public class ByteArrayMethodsSuite {

  @Test
  public void containsMatchesNaiveSearch() {
    // contains() scans 8 positions per word read, so cover a match in every lane of a word and in
    // the final (overlapping) word, near misses whose first and last bytes match, and bytes that
    // a zero-byte test could confuse (0x00, 0x7F, 0x80, 0xFF).
    long address = Platform.allocateMemory(128);
    try {
      for (int n = 0; n <= 40; n++) {
        for (int m = 1; m <= n; m++) {
          byte[] needle = new byte[m];
          Arrays.fill(needle, (byte) 'b');
          needle[0] = 'a';
          needle[m - 1] = 'c';
          for (int p = 0; p + m <= n; p++) {
            byte[] haystack = new byte[n];
            Arrays.fill(haystack, (byte) 'x');
            System.arraycopy(needle, 0, haystack, p, m);
            checkContains(address, haystack, needle);
            if (m >= 3) {
              haystack[p + 1] = 'x';
              checkContains(address, haystack, needle);
            }
          }
        }
      }

      byte[] alphabet = {'a', (byte) 0x80, 0x00, 'b', 0x7F, (byte) 0xFF, 0x01};
      Random random = new Random(42);
      for (int iter = 0; iter < 20000; iter++) {
        int alphabetSize = 1 + random.nextInt(alphabet.length);
        byte[] haystack = new byte[random.nextInt(41)];
        for (int i = 0; i < haystack.length; i++) {
          haystack[i] = alphabet[random.nextInt(alphabetSize)];
        }
        byte[] needle;
        if (haystack.length > 0 && random.nextBoolean()) {
          int start = random.nextInt(haystack.length);
          needle = Arrays.copyOfRange(
            haystack, start, start + 1 + random.nextInt(haystack.length - start));
        } else {
          needle = new byte[1 + random.nextInt(12)];
        }
        // Randomize some needle bytes, turning a slice of the haystack into a near miss.
        for (int i = random.nextInt(3); i > 0; i--) {
          needle[random.nextInt(needle.length)] = alphabet[random.nextInt(alphabetSize)];
        }
        checkContains(address, haystack, needle);
      }

      // The shorter (x - 0x01..01) & ~x & 0x80..80 zero-byte test also flags a 0x01 byte right
      // above a zero byte; here that would turn the near miss at 0 into a false match at 1.
      checkContains(address, new byte[] {0, 1, 0, 0, 'x', 'x', 'x', 'x', 'x', 'x'},
        new byte[] {0, 0, 0});

      Assertions.assertTrue(ByteArrayMethods.contains(new byte[0], new byte[0]));
      Assertions.assertTrue(ByteArrayMethods.contains(new byte[] {1}, new byte[0]));
    } finally {
      Platform.freeMemory(address);
    }
  }

  /**
   * Checks contains() against a naive search, for byte arrays and for the haystack both on heap
   * at a non-zero offset and off heap at `address`. Copies of the needle are placed right before
   * and after the haystack, so reading past either end of it would report a false match; the
   * copy after it is also the needle that is searched for, so it has a non-zero offset too.
   */
  private static void checkContains(long address, byte[] haystack, byte[] needle) {
    boolean expected = false;
    for (int i = 0; i + needle.length <= haystack.length && !expected; i++) {
      expected = Arrays.equals(haystack, i, i + needle.length, needle, 0, needle.length);
    }
    String message = Arrays.toString(haystack) + " contains " + Arrays.toString(needle);
    Assertions.assertEquals(expected, ByteArrayMethods.contains(haystack, needle), message);

    int n = haystack.length;
    int m = needle.length;
    byte[] padded = new byte[m + n + m];
    System.arraycopy(needle, 0, padded, 0, m);
    System.arraycopy(haystack, 0, padded, m, n);
    System.arraycopy(needle, 0, padded, m + n, m);
    Assertions.assertEquals(expected, ByteArrayMethods.contains(
      padded, BYTE_ARRAY_OFFSET + m, n, padded, BYTE_ARRAY_OFFSET + m + n, m), message);
    Platform.copyMemory(padded, BYTE_ARRAY_OFFSET, null, address, padded.length);
    Assertions.assertEquals(expected,
      ByteArrayMethods.contains(null, address + m, n, null, address + m + n, m), message);
  }
}
