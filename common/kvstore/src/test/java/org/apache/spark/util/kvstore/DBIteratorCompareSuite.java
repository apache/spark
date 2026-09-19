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

package org.apache.spark.util.kvstore;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the static byte-array comparator that LevelDBIterator and RocksDBIterator
 * each define (the two backends are kept independent, so the method is duplicated). The
 * comparator is pure and needs no database, so these tests run on every platform.
 */
public class DBIteratorCompareSuite {

  @FunctionalInterface
  private interface ByteArrayComparator {
    int compare(byte[] a, byte[] b);
  }

  @Test
  public void testLevelDBIteratorCompare() {
    checkUnsignedByteOrdering(LevelDBIterator::compare);
  }

  @Test
  public void testRocksDBIteratorCompare() {
    checkUnsignedByteOrdering(RocksDBIterator::compare);
  }

  private static void checkUnsignedByteOrdering(ByteArrayComparator cmp) {
    // Equal arrays compare equal.
    assertEquals(0, cmp.compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }));

    // Empty arrays compare equal, and an empty array sorts before any non-empty one.
    assertEquals(0, cmp.compare(new byte[] {}, new byte[] {}));
    assertTrue(cmp.compare(new byte[] {}, new byte[] { 1 }) < 0);
    assertTrue(cmp.compare(new byte[] { 1 }, new byte[] {}) > 0);

    // A prefix sorts before the longer array that extends it.
    assertTrue(cmp.compare(new byte[] { 1, 2 }, new byte[] { 1, 2, 3 }) < 0);
    assertTrue(cmp.compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }) > 0);

    // Bytes must be ordered as unsigned, matching the underlying key ordering: 0x80 (128) is
    // greater than 0x7f (127). A signed comparison would wrongly treat 0x80 as -128.
    byte[] highBit = new byte[] { (byte) 0x80 };
    byte[] lowBit = new byte[] { 0x7f };
    assertTrue(cmp.compare(highBit, lowBit) > 0);
    assertTrue(cmp.compare(lowBit, highBit) < 0);

    // 0xff (255) is the largest byte value when unsigned, so it sorts after 0x00.
    assertTrue(cmp.compare(new byte[] { (byte) 0xff }, new byte[] { 0x00 }) > 0);

    // The first differing byte decides the order, regardless of later bytes.
    assertTrue(cmp.compare(new byte[] { 0x01, (byte) 0xff }, new byte[] { 0x02, 0x00 }) < 0);
  }

}
