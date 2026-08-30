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
package org.apache.spark.network.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ByteUnitSuite {

  @Test
  void toBytesConvertsEachUnit() {
    assertEquals(0L, ByteUnit.PiB.toBytes(0));
    assertEquals(10L, ByteUnit.BYTE.toBytes(10));
    assertEquals(1L << 10, ByteUnit.KiB.toBytes(1));
    assertEquals(1L << 20, ByteUnit.MiB.toBytes(1));
    assertEquals(1L << 30, ByteUnit.GiB.toBytes(1));
    assertEquals(1L << 40, ByteUnit.TiB.toBytes(1));
    assertEquals(1L << 50, ByteUnit.PiB.toBytes(1));
  }

  @Test
  void toBytesRejectsNegativeValues() {
    assertThrows(IllegalArgumentException.class, () -> ByteUnit.KiB.toBytes(-1));
    assertThrows(IllegalArgumentException.class, () -> ByteUnit.PiB.toBytes(Long.MIN_VALUE));
  }

  @Test
  void toBytesRejectsOverflow() {
    // PiB has multiplier 2^50, so any value above Long.MAX_VALUE / 2^50 (= 8191) overflows.
    assertThrows(IllegalArgumentException.class, () -> ByteUnit.PiB.toBytes(8192));
    assertThrows(IllegalArgumentException.class, () -> ByteUnit.PiB.toBytes(Long.MAX_VALUE));
    // GiB has multiplier 2^30, so Long.MAX_VALUE / 2^30 (= 8589934591) is the last safe value.
    assertThrows(IllegalArgumentException.class, () -> ByteUnit.GiB.toBytes(8589934592L));
  }

  @Test
  void toBytesAcceptsLargestNonOverflowingValue() {
    // Long.MAX_VALUE / 2^50 == 8191 is the largest PiB value that still fits in a long; the
    // result is pinned to an independent constant so a wrong product or a misfiring guard is
    // caught rather than recomputed on both sides.
    long bytes = ByteUnit.PiB.toBytes(8191);
    assertEquals(9222246136947933184L, bytes);
    assertTrue(bytes > 0, "conversion at the boundary must stay positive");
  }
}
