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

package org.apache.spark.unsafe.bitset;

import junit.framework.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.MemoryBlock;

public class BitSetSuite {

  private static BitSet createBitSet(int capacity) {
    assert capacity % 64 == 0;
    return new BitSet(MemoryBlock.fromLongArray(new long[capacity / 64]));
  }

  @Test
  public void basicOps() {
    BitSet bs = createBitSet(64);
    Assert.assertEquals(64, bs.capacity());

    // Make sure the bit set starts empty.
    for (int i = 0; i < bs.capacity(); i++) {
      Assert.assertFalse(bs.isSet(i));
    }
    // another form of asserting that the bit set is empty
    Assert.assertFalse(bs.anySet());

    // Set every bit and check it.
    for (int i = 0; i < bs.capacity(); i++) {
      bs.set(i);
      Assert.assertTrue(bs.isSet(i));
    }

    // Unset every bit and check it.
    for (int i = 0; i < bs.capacity(); i++) {
      Assert.assertTrue(bs.isSet(i));
      bs.unset(i);
      Assert.assertFalse(bs.isSet(i));
    }

    // Make sure anySet() can detect any set bit
    bs = createBitSet(256);
    bs.set(64);
    Assert.assertTrue(bs.anySet());
  }

  @Test
  public void traversal() {
    BitSet bs = createBitSet(256);

    Assert.assertEquals(-1, bs.nextSetBit(0));
    Assert.assertEquals(-1, bs.nextSetBit(10));
    Assert.assertEquals(-1, bs.nextSetBit(64));

    bs.set(10);
    Assert.assertEquals(10, bs.nextSetBit(0));
    Assert.assertEquals(10, bs.nextSetBit(1));
    Assert.assertEquals(10, bs.nextSetBit(10));
    Assert.assertEquals(-1, bs.nextSetBit(11));

    bs.set(11);
    Assert.assertEquals(10, bs.nextSetBit(10));
    Assert.assertEquals(11, bs.nextSetBit(11));

    // Skip a whole word and find it
    bs.set(190);
    Assert.assertEquals(190, bs.nextSetBit(12));

    Assert.assertEquals(-1, bs.nextSetBit(191));
    Assert.assertEquals(-1, bs.nextSetBit(256));
  }
}
