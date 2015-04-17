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

package org.apache.spark.unsafe.map;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryLocation;
import static org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET;
import org.junit.Assert;
import org.junit.Test;

import java.lang.Exception;
import java.lang.IllegalStateException;
import java.nio.ByteBuffer;
import java.util.*;

public abstract class AbstractTestBytesToBytesMap {

  protected final Random rand = new Random(42);

  protected final MemoryAllocator allocator = getMemoryAllocator();

  protected abstract MemoryAllocator getMemoryAllocator();

  protected byte[] getByteArray(MemoryLocation loc, int size) {
    final byte[] arr = new byte[size];
    PlatformDependent.UNSAFE.copyMemory(
      loc.getBaseObject(),
      loc.getBaseOffset(),
      arr,
      BYTE_ARRAY_OFFSET,
      size
    );
    return arr;
  }

  protected byte[] getRandomByteArray(int numWords) {
    Assert.assertTrue(numWords > 0);
    final int lengthInBytes = numWords * 8;
    final byte[] bytes = new byte[lengthInBytes];
    rand.nextBytes(bytes);
    return bytes;
  }

  /**
   * Fast equality checking for byte arrays, since these comparisons are a bottleneck
   * in our stress tests.
   */
  protected boolean arrayEquals(
      byte[] expected,
      MemoryLocation actualAddr,
      long actualLengthBytes) {
    return (actualLengthBytes == expected.length) && ByteArrayMethods.wordAlignedArrayEquals(
      expected,
      BYTE_ARRAY_OFFSET,
      actualAddr.getBaseObject(),
      actualAddr.getBaseOffset(),
      expected.length
    );
  }

  @Test
  public void emptyMap() {
    BytesToBytesMap map = new BytesToBytesMap(allocator, 64);
    Assert.assertEquals(0, map.size());
    final int keyLengthInWords = 10;
    final int keyLengthInBytes = keyLengthInWords * 8;
    final byte[] key = getRandomByteArray(keyLengthInWords);
    Assert.assertFalse(map.lookup(key, BYTE_ARRAY_OFFSET, keyLengthInBytes).isDefined());
  }

  @Test
  public void setAndRetrieveAKey() {
    BytesToBytesMap map = new BytesToBytesMap(allocator, 64);
    final int recordLengthWords = 10;
    final int recordLengthBytes = recordLengthWords * 8;
    final byte[] keyData = getRandomByteArray(recordLengthWords);
    final byte[] valueData = getRandomByteArray(recordLengthWords);
    try {
      final BytesToBytesMap.Location loc =
        map.lookup(keyData, BYTE_ARRAY_OFFSET, recordLengthBytes);
      Assert.assertFalse(loc.isDefined());
      loc.storeKeyAndValue(
        keyData,
        BYTE_ARRAY_OFFSET,
        recordLengthBytes,
        valueData,
        BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      Assert.assertTrue(map.lookup(keyData, BYTE_ARRAY_OFFSET, recordLengthBytes).isDefined());
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());

      Assert.assertArrayEquals(keyData, getByteArray(loc.getKeyAddress(), recordLengthBytes));
      Assert.assertArrayEquals(valueData, getByteArray(loc.getValueAddress(), recordLengthBytes));

      try {
        loc.storeKeyAndValue(
          keyData,
          BYTE_ARRAY_OFFSET,
          recordLengthBytes,
          valueData,
          BYTE_ARRAY_OFFSET,
          recordLengthBytes
        );
        Assert.fail("Should not be able to set a new value for a key");
      } catch (IllegalStateException e) {
        // Expected exception; do nothing.
      }
    } finally {
      map.free();
    }
  }

  @Test
  public void iteratorTest() throws Exception {
    final int size = 128;
    BytesToBytesMap map = new BytesToBytesMap(allocator, size / 2);
    try {
      for (long i = 0; i < size; i++) {
        final long[] value = new long[] { i };
        final BytesToBytesMap.Location loc =
          map.lookup(value, PlatformDependent.LONG_ARRAY_OFFSET, 8);
        Assert.assertFalse(loc.isDefined());
        loc.storeKeyAndValue(
          value,
          PlatformDependent.LONG_ARRAY_OFFSET,
          8,
          value,
          PlatformDependent.LONG_ARRAY_OFFSET,
          8
        );
      }
      final java.util.BitSet valuesSeen = new java.util.BitSet(size);
      final Iterator<BytesToBytesMap.Location> iter = map.iterator();
      while (iter.hasNext()) {
        final BytesToBytesMap.Location loc = iter.next();
        Assert.assertTrue(loc.isDefined());
        final MemoryLocation keyAddress = loc.getKeyAddress();
        final MemoryLocation valueAddress = loc.getValueAddress();
        final long key =  PlatformDependent.UNSAFE.getLong(
          keyAddress.getBaseObject(), keyAddress.getBaseOffset());
        final long value = PlatformDependent.UNSAFE.getLong(
          valueAddress.getBaseObject(), valueAddress.getBaseOffset());
        Assert.assertEquals(key, value);
        valuesSeen.set((int) value);
      }
      Assert.assertEquals(size, valuesSeen.cardinality());
    } finally {
      map.free();
    }
  }

  @Test
  public void randomizedStressTest() {
    final long size = 65536;
    // Java arrays' hashCodes() aren't based on the arrays' contents, so we need to wrap arrays
    // into ByteBuffers in order to use them as keys here.
    final Map<ByteBuffer, byte[]> expected = new HashMap<ByteBuffer, byte[]>();
    final BytesToBytesMap map = new BytesToBytesMap(allocator, size);

    try {
      // Fill the map to 90% full so that we can trigger probing
      for (int i = 0; i < size * 0.9; i++) {
        final byte[] key = getRandomByteArray(rand.nextInt(256) + 1);
        final byte[] value = getRandomByteArray(rand.nextInt(512) + 1);
        if (!expected.containsKey(ByteBuffer.wrap(key))) {
          expected.put(ByteBuffer.wrap(key), value);
          final BytesToBytesMap.Location loc = map.lookup(
            key,
            BYTE_ARRAY_OFFSET,
            key.length
          );
          Assert.assertFalse(loc.isDefined());
          loc.storeKeyAndValue(
            key,
            BYTE_ARRAY_OFFSET,
            key.length,
            value,
            BYTE_ARRAY_OFFSET,
            value.length
          );
          Assert.assertTrue(loc.isDefined());
        }
      }

      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        final byte[] key = entry.getKey().array();
        final byte[] value = entry.getValue();
        final BytesToBytesMap.Location loc = map.lookup(key, BYTE_ARRAY_OFFSET, key.length);
        Assert.assertTrue(loc.isDefined());
        Assert.assertTrue(arrayEquals(key, loc.getKeyAddress(), loc.getKeyLength()));
        Assert.assertTrue(arrayEquals(value, loc.getValueAddress(), loc.getValueLength()));
      }
    } finally {
      map.free();
    }
  }
}
