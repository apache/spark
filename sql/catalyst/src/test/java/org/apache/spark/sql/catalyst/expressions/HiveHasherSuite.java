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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class HiveHasherSuite {

  @Test
  public void testKnownIntegerInputs() {
    int[] inputs = {0, Integer.MIN_VALUE, Integer.MAX_VALUE, 593689054, -189366624};
    for (int input : inputs) {
      Assertions.assertEquals(input, HiveHasher.hashInt(input));
    }
  }

  @Test
  public void testKnownLongInputs() {
    Assertions.assertEquals(0, HiveHasher.hashLong(0L));
    Assertions.assertEquals(41, HiveHasher.hashLong(-42L));
    Assertions.assertEquals(42, HiveHasher.hashLong(42L));
    Assertions.assertEquals(-2147483648, HiveHasher.hashLong(Long.MIN_VALUE));
    Assertions.assertEquals(-2147483648, HiveHasher.hashLong(Long.MAX_VALUE));
  }

  @Test
  public void testKnownStringAndIntInputs() {
    int[] inputs = {84, 19, 8};
    int[] expected = {-823832826, -823835053, 111972242};

    for (int i = 0; i < inputs.length; i++) {
      UTF8String s = UTF8String.fromString("val_" + inputs[i]);
      int hash = HiveHasher.hashUnsafeBytes(s.getBaseObject(), s.getBaseOffset(), s.numBytes());
      Assertions.assertEquals(expected[i], ((31 * inputs[i]) + hash));
    }
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
      Assertions.assertEquals(HiveHasher.hashInt(vint), HiveHasher.hashInt(vint));
      Assertions.assertEquals(HiveHasher.hashLong(lint), HiveHasher.hashLong(lint));

      hashcodes.add(HiveHasher.hashLong(lint));
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
          HiveHasher.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
          HiveHasher.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(HiveHasher.hashUnsafeBytes(
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
          HiveHasher.hashUnsafeBytes(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
          HiveHasher.hashUnsafeBytes(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(HiveHasher.hashUnsafeBytes(
          paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assertions.assertTrue(hashcodes.size() > size * 0.95);
  }
}
