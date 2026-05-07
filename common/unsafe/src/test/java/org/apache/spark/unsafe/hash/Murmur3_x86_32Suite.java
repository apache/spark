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

package org.apache.spark.unsafe.hash;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import scala.util.hashing.MurmurHash3$;

import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test file based on Guava's Murmur3Hash32Test.
 */
public class Murmur3_x86_32Suite {

  private static final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);

  @Test
  public void testKnownIntegerInputs() {
    Assertions.assertEquals(593689054, hasher.hashInt(0));
    Assertions.assertEquals(-189366624, hasher.hashInt(-42));
    Assertions.assertEquals(-1134849565, hasher.hashInt(42));
    Assertions.assertEquals(-1718298732, hasher.hashInt(Integer.MIN_VALUE));
    Assertions.assertEquals(-1653689534, hasher.hashInt(Integer.MAX_VALUE));
  }

  @Test
  public void testKnownLongInputs() {
    Assertions.assertEquals(1669671676, hasher.hashLong(0L));
    Assertions.assertEquals(-846261623, hasher.hashLong(-42L));
    Assertions.assertEquals(1871679806, hasher.hashLong(42L));
    Assertions.assertEquals(1366273829, hasher.hashLong(Long.MIN_VALUE));
    Assertions.assertEquals(-2106506049, hasher.hashLong(Long.MAX_VALUE));
  }

  // SPARK-23381 Check whether the hash of the byte array is the same as another implementations
  @Test
  public void testKnownBytesInputs() {
    byte[] test = "test".getBytes(StandardCharsets.UTF_8);
    Assertions.assertEquals(MurmurHash3$.MODULE$.bytesHash(test, 0),
      Murmur3_x86_32.hashUnsafeBytes2(test, Platform.BYTE_ARRAY_OFFSET, test.length, 0));
    byte[] test1 = "test1".getBytes(StandardCharsets.UTF_8);
    Assertions.assertEquals(MurmurHash3$.MODULE$.bytesHash(test1, 0),
      Murmur3_x86_32.hashUnsafeBytes2(test1, Platform.BYTE_ARRAY_OFFSET, test1.length, 0));
    byte[] te = "te".getBytes(StandardCharsets.UTF_8);
    Assertions.assertEquals(MurmurHash3$.MODULE$.bytesHash(te, 0),
      Murmur3_x86_32.hashUnsafeBytes2(te, Platform.BYTE_ARRAY_OFFSET, te.length, 0));
    byte[] tes = "tes".getBytes(StandardCharsets.UTF_8);
    Assertions.assertEquals(MurmurHash3$.MODULE$.bytesHash(tes, 0),
      Murmur3_x86_32.hashUnsafeBytes2(tes, Platform.BYTE_ARRAY_OFFSET, tes.length, 0));
  }

  @Test
  public void randomizedStressTest() {
    int size = 65536;
    Random rand = new Random();

    // A set used to track collision rate.
    Set<Integer> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int vint = rand.nextInt();
      long lint = rand.nextLong();
      Assertions.assertEquals(hasher.hashInt(vint), hasher.hashInt(vint));
      Assertions.assertEquals(hasher.hashLong(lint), hasher.hashLong(lint));

      hashcodes.add(hasher.hashLong(lint));
    }

    // A very loose bound.
    Assertions.assertTrue(hashcodes.size() > size * 0.95);
  }

  @Test
  public void randomizedStressTestBytes() {
    int size = 65536;
    Random rand = new Random();

    // A set used to track collision rate.
    Set<Integer> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int byteArrSize = rand.nextInt(100) * 8;
      byte[] bytes = new byte[byteArrSize];
      rand.nextBytes(bytes);

      Assertions.assertEquals(
        hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
        hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(hasher.hashUnsafeWords(
        bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assertions.assertTrue(hashcodes.size() > size * 0.95);
  }

  @Test
  public void randomizedStressTestPaddedStrings() {
    int size = 64000;
    // A set used to track collision rate.
    Set<Integer> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int byteArrSize = 8;
      byte[] strBytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
      byte[] paddedBytes = new byte[byteArrSize];
      System.arraycopy(strBytes, 0, paddedBytes, 0, strBytes.length);

      Assertions.assertEquals(
        hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
        hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(hasher.hashUnsafeWords(
        paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assertions.assertTrue(hashcodes.size() > size * 0.95);
  }
}
