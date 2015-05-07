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

import java.lang.Exception;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.PlatformDependent;
import static org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

public abstract class AbstractBytesToBytesMapSuite {

  private final Random rand = new Random(42);

  private TaskMemoryManager memoryManager;

  @Before
  public void setup() {
    memoryManager = new TaskMemoryManager(new ExecutorMemoryManager(getMemoryAllocator()));
  }

  @After
  public void tearDown() {
    if (memoryManager != null) {
      memoryManager.cleanUpAllAllocatedMemory();
      memoryManager = null;
    }
  }

  protected abstract MemoryAllocator getMemoryAllocator();

  private static byte[] getByteArray(MemoryLocation loc, int size) {
    final byte[] arr = new byte[size];
    PlatformDependent.copyMemory(
      loc.getBaseObject(),
      loc.getBaseOffset(),
      arr,
      BYTE_ARRAY_OFFSET,
      size
    );
    return arr;
  }

  private byte[] getRandomByteArray(int numWords) {
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
  private static boolean arrayEquals(
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
    BytesToBytesMap map = new BytesToBytesMap(memoryManager, 64);
    try {
      Assert.assertEquals(0, map.size());
      final int keyLengthInWords = 10;
      final int keyLengthInBytes = keyLengthInWords * 8;
      final byte[] key = getRandomByteArray(keyLengthInWords);
      Assert.assertFalse(map.lookup(key, BYTE_ARRAY_OFFSET, keyLengthInBytes).isDefined());
    } finally {
      map.free();
    }
  }

  @Test
  public void setAndRetrieveAKey() {
    BytesToBytesMap map = new BytesToBytesMap(memoryManager, 64);
    final int recordLengthWords = 10;
    final int recordLengthBytes = recordLengthWords * 8;
    final byte[] keyData = getRandomByteArray(recordLengthWords);
    final byte[] valueData = getRandomByteArray(recordLengthWords);
    try {
      final BytesToBytesMap.Location loc =
        map.lookup(keyData, BYTE_ARRAY_OFFSET, recordLengthBytes);
      Assert.assertFalse(loc.isDefined());
      loc.putNewKey(
        keyData,
        BYTE_ARRAY_OFFSET,
        recordLengthBytes,
        valueData,
        BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      // After storing the key and value, the other location methods should return results that
      // reflect the result of this store without us having to call lookup() again on the same key.
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());
      Assert.assertArrayEquals(keyData, getByteArray(loc.getKeyAddress(), recordLengthBytes));
      Assert.assertArrayEquals(valueData, getByteArray(loc.getValueAddress(), recordLengthBytes));

      // After calling lookup() the location should still point to the correct data.
      Assert.assertTrue(map.lookup(keyData, BYTE_ARRAY_OFFSET, recordLengthBytes).isDefined());
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());
      Assert.assertArrayEquals(keyData, getByteArray(loc.getKeyAddress(), recordLengthBytes));
      Assert.assertArrayEquals(valueData, getByteArray(loc.getValueAddress(), recordLengthBytes));

      try {
        loc.putNewKey(
          keyData,
          BYTE_ARRAY_OFFSET,
          recordLengthBytes,
          valueData,
          BYTE_ARRAY_OFFSET,
          recordLengthBytes
        );
        Assert.fail("Should not be able to set a new value for a key");
      } catch (AssertionError e) {
        // Expected exception; do nothing.
      }
    } finally {
      map.free();
    }
  }

  @Test
  public void iteratorTest() throws Exception {
    final int size = 128;
    BytesToBytesMap map = new BytesToBytesMap(memoryManager, size / 2);
    try {
      for (long i = 0; i < size; i++) {
        final long[] value = new long[] { i };
        final BytesToBytesMap.Location loc =
          map.lookup(value, PlatformDependent.LONG_ARRAY_OFFSET, 8);
        Assert.assertFalse(loc.isDefined());
        loc.putNewKey(
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
    final int size = 65536;
    // Java arrays' hashCodes() aren't based on the arrays' contents, so we need to wrap arrays
    // into ByteBuffers in order to use them as keys here.
    final Map<ByteBuffer, byte[]> expected = new HashMap<ByteBuffer, byte[]>();
    final BytesToBytesMap map = new BytesToBytesMap(memoryManager, size);

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
          loc.putNewKey(
            key,
            BYTE_ARRAY_OFFSET,
            key.length,
            value,
            BYTE_ARRAY_OFFSET,
            value.length
          );
          // After calling putNewKey, the following should be true, even before calling
          // lookup():
          Assert.assertTrue(loc.isDefined());
          Assert.assertEquals(key.length, loc.getKeyLength());
          Assert.assertEquals(value.length, loc.getValueLength());
          Assert.assertTrue(arrayEquals(key, loc.getKeyAddress(), key.length));
          Assert.assertTrue(arrayEquals(value, loc.getValueAddress(), value.length));
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
