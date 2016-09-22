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

package org.apache.spark.sql.catalyst.expressions;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.spark.unsafe.Platform;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the XXH64 function.
 * <p/>
 * Test constants were taken from the original implementation and the airlift/slice implementation.
 */
public class XXH64Suite {

  private static final XXH64 hasher = new XXH64(0);

  private static final int SIZE = 101;
  private static final long PRIME = 2654435761L;
  private static final byte[] BUFFER = new byte[SIZE];
  private static final int TEST_INT = 0x4B1FFF9E; // First 4 bytes in the buffer
  private static final long TEST_LONG = 0xDD2F535E4B1FFF9EL; // First 8 bytes in the buffer

  /* Create the test data. */
  static {
    long seed = PRIME;
    for (int i = 0; i < SIZE; i++) {
      BUFFER[i] = (byte) (seed >> 24);
      seed *= seed;
    }
  }

  @Test
  public void testKnownIntegerInputs() {
    Assert.assertEquals(0x9256E58AA397AEF1L, hasher.hashInt(TEST_INT));
    Assert.assertEquals(0x9D5FFDFB928AB4BL, XXH64.hashInt(TEST_INT, PRIME));
  }

  @Test
  public void testKnownLongInputs() {
    Assert.assertEquals(0xF74CB1451B32B8CFL, hasher.hashLong(TEST_LONG));
    Assert.assertEquals(0x9C44B77FBCC302C5L, XXH64.hashLong(TEST_LONG, PRIME));
  }

  @Test
  public void testKnownByteArrayInputs() {
    Assert.assertEquals(0xEF46DB3751D8E999L,
            hasher.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 0));
    Assert.assertEquals(0xAC75FDA2929B17EFL,
            XXH64.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 0, PRIME));
    Assert.assertEquals(0x4FCE394CC88952D8L,
            hasher.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 1));
    Assert.assertEquals(0x739840CB819FA723L,
            XXH64.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 1, PRIME));

    // These tests currently fail in a big endian environment because the test data and expected
    // answers are generated with little endian the assumptions. We could revisit this when Platform
    // becomes endian aware.
    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      Assert.assertEquals(0x9256E58AA397AEF1L,
              hasher.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 4));
      Assert.assertEquals(0x9D5FFDFB928AB4BL,
              XXH64.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 4, PRIME));
      Assert.assertEquals(0xF74CB1451B32B8CFL,
              hasher.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 8));
      Assert.assertEquals(0x9C44B77FBCC302C5L,
              XXH64.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 8, PRIME));
      Assert.assertEquals(0xCFFA8DB881BC3A3DL,
              hasher.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 14));
      Assert.assertEquals(0x5B9611585EFCC9CBL,
              XXH64.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, 14, PRIME));
      Assert.assertEquals(0x0EAB543384F878ADL,
              hasher.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, SIZE));
      Assert.assertEquals(0xCAA65939306F1E21L,
              XXH64.hashUnsafeBytes(BUFFER, Platform.BYTE_ARRAY_OFFSET, SIZE, PRIME));
    }
  }

  @Test
  public void randomizedStressTest() {
    int size = 65536;
    Random rand = new Random();

    // A set used to track collision rate.
    Set<Long> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int vint = rand.nextInt();
      long lint = rand.nextLong();
      Assert.assertEquals(hasher.hashInt(vint), hasher.hashInt(vint));
      Assert.assertEquals(hasher.hashLong(lint), hasher.hashLong(lint));

      hashcodes.add(hasher.hashLong(lint));
    }

    // A very loose bound.
    Assert.assertTrue(hashcodes.size() > size * 0.95d);
  }

  @Test
  public void randomizedStressTestBytes() {
    int size = 65536;
    Random rand = new Random();

    // A set used to track collision rate.
    Set<Long> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int byteArrSize = rand.nextInt(100) * 8;
      byte[] bytes = new byte[byteArrSize];
      rand.nextBytes(bytes);

      Assert.assertEquals(
              hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
              hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(hasher.hashUnsafeWords(
              bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assert.assertTrue(hashcodes.size() > size * 0.95d);
  }

  @Test
  public void randomizedStressTestPaddedStrings() {
    int size = 64000;
    // A set used to track collision rate.
    Set<Long> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int byteArrSize = 8;
      byte[] strBytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
      byte[] paddedBytes = new byte[byteArrSize];
      System.arraycopy(strBytes, 0, paddedBytes, 0, strBytes.length);

      Assert.assertEquals(
              hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
              hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(hasher.hashUnsafeWords(
              paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assert.assertTrue(hashcodes.size() > size * 0.95d);
  }
}
