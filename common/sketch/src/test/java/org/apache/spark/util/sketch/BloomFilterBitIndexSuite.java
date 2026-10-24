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

package org.apache.spark.util.sketch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Checks that reducing a hash with {@link BloomFilterBase#bitIndex} always agrees with the modulo
 * it replaces, so that the bit positions of a bloom filter are unchanged by the masking fast path.
 */
public class BloomFilterBitIndexSuite {

  private static final int NUM_SAMPLES = 10000;

  @Test
  public void testBitSizeMaskIsSetOnlyForPowerOfTwoBitSizes() {
    // A BitArray rounds up to whole 64 bit words, so its bit size is a power of two exactly when
    // its word count is.
    assertEquals((1L << 20) - 1, new BitArray(1L << 20).bitSizeMask());
    assertEquals(64 - 1, new BitArray(1).bitSizeMask());
    // 65 bits are rounded up to two whole words, which is 128 bits.
    assertEquals(128 - 1, new BitArray(65).bitSizeMask());
    assertEquals(0, new BitArray(192).bitSizeMask());
    assertEquals(0, new BitArray(1000000).bitSizeMask());
  }

  @Test
  public void testBitSizeMaskSurvivesSerDe() throws IOException {
    BitArray bits = new BitArray(1L << 20);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bits.writeTo(new DataOutputStream(out));
    BitArray deserialized =
      BitArray.readFrom(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(bits.bitSize(), deserialized.bitSize());
    assertEquals(bits.bitSizeMask(), deserialized.bitSizeMask());
  }

  @Test
  public void testLongBitIndexMatchesModulo() {
    Random r = new Random(37);
    for (long numBits : new long[] {1L << 10, 1L << 23, 1L << 26, 64, 1024 + 64, 12345 * 64L}) {
      BitArray bits = new BitArray(numBits);
      long bitSize = bits.bitSize();
      long mask = bits.bitSizeMask();

      for (long hash : new long[] {0, 1, 63, 64, bitSize - 1, bitSize, Long.MAX_VALUE}) {
        assertEquals(hash % bitSize, BloomFilterBase.bitIndex(hash, bitSize, mask));
      }
      for (int i = 0; i < NUM_SAMPLES; i++) {
        // The scatter loops flip negative hashes before reducing them, so only non-negative
        // values ever reach `bitIndex`.
        long hash = r.nextLong() & Long.MAX_VALUE;
        assertEquals(hash % bitSize, BloomFilterBase.bitIndex(hash, bitSize, mask));
      }
    }
  }

  @Test
  public void testIntBitIndexMatchesModulo() {
    Random r = new Random(37);
    for (long numBits : new long[] {1L << 10, 1L << 23, 64, 1024 + 64, 12345 * 64L}) {
      BitArray bits = new BitArray(numBits);
      long bitSize = bits.bitSize();
      long mask = bits.bitSizeMask();

      for (int hash : new int[] {0, 1, 63, 64, Integer.MAX_VALUE}) {
        assertEquals(hash % bitSize, BloomFilterBase.bitIndex(hash, bitSize, mask));
      }
      for (int i = 0; i < NUM_SAMPLES; i++) {
        int hash = r.nextInt() & Integer.MAX_VALUE;
        assertEquals(hash % bitSize, BloomFilterBase.bitIndex(hash, bitSize, mask));
      }
    }
  }

  @Test
  public void testIntBitIndexMatchesModuloForBitSizesBeyondIntRange() {
    // Allocating such a bit array is not possible, but the reduction still has to be correct for
    // a bit size that does not fit in an int.
    long bitSize = Integer.MAX_VALUE + 64L;
    Random r = new Random(37);
    for (int i = 0; i < NUM_SAMPLES; i++) {
      int hash = r.nextInt() & Integer.MAX_VALUE;
      assertEquals(hash % bitSize, BloomFilterBase.bitIndex(hash, bitSize, 0));
    }
  }
}
