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

package org.apache.spark.sql.execution;

import java.util.Comparator;
import java.util.Iterator;

import scala.math.Ordering;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRowLocation;
import org.apache.spark.sql.catalyst.util.ObjectPool;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSet;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.map.HashMapGrowthStrategy;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.collection.SortDataFormat;
import org.apache.spark.util.collection.Sorter;

/**
 * An append-only hash map where keys and values are contiguous regions of bytes.
 * <p>
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 * <p>
 * The map can support up to 2^29 keys. If the key cardinality is higher than this, you should
 * probably be using sorting instead of hashing for better cache locality.
 * <p>
 * This class is not thread safe.
 */
public class UnsafeAppendOnlyMap {

  /**
   * The maximum number of keys that BytesToBytesMap supports. The hash table has to be
   * power-of-2-sized and its backing Java array can contain at most (1 << 30) elements, since
   * that's the largest power-of-2 that's less than Integer.MAX_VALUE. We need two long array
   * entries per key, giving us a maximum capacity of (1 << 29).
   */
  @VisibleForTesting
  static final int MAX_CAPACITY = (1 << 29);

  private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  private final TaskMemoryManager memoryManager;

  /**
   * A single array to store the key and value.
   * <p/>
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
   */
  private long[] longArray;

  /**
   * A {@link org.apache.spark.unsafe.bitset.BitSet} used to track location of the map where the key is set.
   * Size of the bitset should be half of the size of the long array.
   */
  private BitSet bitset;

  private final double loadFactor;

  /**
   * Number of keys defined in the map.
   */
  private int size;

  /**
   * The map will be expanded once the number of keys exceeds this threshold.
   */
  private int growthThreshold;

  private int capacity;

  /**
   * Mask for truncating hashcodes so that they do not exceed the long array's size.
   * This is a strength reduction optimization; we're essentially performing a modulus operation,
   * but doing so with a bitmask because this is a power-of-2-sized hash map.
   */
  private int mask;

  /**
   * Return value of {@link UnsafeAppendOnlyMap#lookup(Object, long, int)}.
   */
  private final UnsafeRowLocation loc;

  private final boolean enablePerfMetrics;
  private long timeSpentResizingNs = 0;

  private long numKeyLookups = 0;
  private long numProbes = 0;
  private long numHashCollisions = 0;

  public UnsafeAppendOnlyMap(
      TaskMemoryManager memoryManager,
      int initialCapacity,
      double loadFactor,
      boolean enablePerfMetrics) {
    this.memoryManager = memoryManager;
    this.loadFactor = loadFactor;
    this.loc = new UnsafeRowLocation(memoryManager);
    this.enablePerfMetrics = enablePerfMetrics;

    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("Initial capacity must be greater than 0");
    }
    if (initialCapacity > MAX_CAPACITY) {
      throw new IllegalArgumentException("Initial capacity " + initialCapacity
              + " exceeds maximum capacity of " + MAX_CAPACITY);
    }

    this.capacity = initialCapacity;
    allocate(initialCapacity);
  }

  public UnsafeAppendOnlyMap(
      TaskMemoryManager memoryManager,
      int initialCapacity,
      boolean enablePerfMetrics) {
    this(memoryManager, initialCapacity, 0.70, enablePerfMetrics);
  }

  private static final class SortComparator implements Comparator<Long> {

    private final TaskMemoryManager memoryManager;
    private final Ordering<InternalRow> ordering;
    private final int numFields;
    private final ObjectPool objPool;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();

    SortComparator(TaskMemoryManager memoryManager,
        Ordering<InternalRow> ordering, int numFields, ObjectPool objPool) {
      this.memoryManager = memoryManager;
      this.numFields = numFields;
      this.ordering = ordering;
      this.objPool = objPool;
    }

    @Override
    public int compare(Long fullKeyAddress1, Long fullKeyAddress2) {
      final Object baseObject1 = memoryManager.getPage(fullKeyAddress1);
      final long baseOffset1 = memoryManager.getOffsetInPage(fullKeyAddress1) + 8;

      final Object baseObject2 = memoryManager.getPage(fullKeyAddress2);
      final long baseOffset2 = memoryManager.getOffsetInPage(fullKeyAddress2) + 8;

      row1.pointTo(baseObject1, baseOffset1, numFields, -1, objPool);
      row2.pointTo(baseObject2, baseOffset2, numFields, -1, objPool);
      return ordering.compare(row1, row2);
    }
  }

  private static final class KVArraySortDataFormat extends SortDataFormat<Long, long[]> {

    @Override
    public Long getKey(long[] data, int pos) {
      return data[2 * pos];
    }

    @Override
    public Long newKey() {
      return 0L;
    }

    @Override
    public void swap(long[] data,int pos0, int pos1) {
      long tmpKey = data[2 * pos0];
      long tmpVal = data[2 * pos0 + 1];
      data[2 * pos0] = data[2 * pos1];
      data[2 * pos0 + 1] = data[2 * pos1 + 1];
      data[2 * pos1] = tmpKey;
      data[2 * pos1 + 1] = tmpVal;
    }

    @Override
    public void copyElement(long[] src, int srcPos, long[] dst, int dstPos) {
      dst[2 * dstPos] = src[2 * srcPos];
      dst[2 * dstPos + 1] = src[2 * srcPos + 1];
    }

    @Override
    public void copyRange(long[] src, int srcPos, long[] dst, int dstPos, int length) {
      System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length);
    }

    @Override
    public long[] allocate(int length) {
        return new long[2 * length];
    }
  }

  public Iterator<UnsafeRowLocation> getSortedIterator(Ordering<InternalRow> ordering,
      int numFields, ObjectPool objPool) {
    // Pack KV pairs into the front of the underlying array
    int keyIndex = 0;
    int newIndex = 0;
    while (keyIndex < capacity) {
      if (bitset.isSet(keyIndex)) {
        longArray[2 * newIndex] = longArray[2 * keyIndex];
        longArray[2 * newIndex + 1] = longArray[2 * keyIndex + 1];
        newIndex += 1;
      }
      keyIndex += 1;
    }
    Comparator<Long> sortComparator = new SortComparator(this.memoryManager,
        ordering, numFields, objPool);
    Sorter<Long, long[]> sorter = new Sorter<>(new KVArraySortDataFormat());
    sorter.sort(longArray, 0, newIndex, sortComparator);
    return new UnsafeMapSorterIterator(newIndex, longArray, this.loc);
  }

  /**
   * Iterate through the data and memory location of records are returned in order of the key.
   */
  public static final class UnsafeMapSorterIterator implements Iterator<UnsafeRowLocation> {

    private final long[] pointerArray;
    private final int numRecords;
    private int currentRecordNumber = 0;
    private final UnsafeRowLocation loc;

    public UnsafeMapSorterIterator(int numRecords, long[] pointerArray,
        UnsafeRowLocation loc) {
      this.numRecords = numRecords;
      this.pointerArray = pointerArray;
      this.loc = loc;
    }

    @Override
    public boolean hasNext() {
      return currentRecordNumber != numRecords;
    }

    @Override
    public UnsafeRowLocation next() {
      loc.with(pointerArray[currentRecordNumber * 2]);
      currentRecordNumber++;
      return loc;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Allocate new data structures for this map. When calling this outside of the constructor,
   * make sure to keep references to the old data structures so that you can free them.
   *
   * @param capacity the new map capacity
   */
  private void allocate(int capacity) {
    assert (capacity >= 0);
    assert (capacity <= MAX_CAPACITY);

    longArray = new long[capacity * 2];
    bitset = new BitSet(MemoryBlock.fromLongArray(new long[capacity / 64]));

    this.growthThreshold = (int) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  public boolean hasSpaceForAnotherRecord() {
    return size < growthThreshold;
  }

  /**
   * Returns the total amount of memory, in bytes, consumed by this map's managed structures.
   */
  public long getMemoryUsage() {
    return capacity * 8L * 2 + capacity / 8;
  }

  public static int getCapacity(int initialCapacity) {
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    return Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(initialCapacity)), 64);
  }

  public static long getMemoryUsage(int capacity) {
    return capacity * 8L * 2 + capacity / 8;
  }

  public int getNextCapacity() {
    int nextCapacity =
        Math.min(growthStrategy.nextCapacity(capacity), MAX_CAPACITY);
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    return Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(nextCapacity)), 64);
  }

  public int numRecords() {
    return size;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   * <p/>
   * This method is idempotent.
   */
  public void free() {
    if (longArray != null) {
      longArray = null;
    }
    if (bitset != null) {
      // The bitset's heap memory isn't managed by a memory manager, so no need to free it here.
      bitset = null;
    }
  }

  /**
   * Returns the total amount of time spent resizing this map (in nanoseconds).
   */
  public long getTimeSpentResizingNs() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return timeSpentResizingNs;
  }

  /**
   * Returns the average number of probes per key lookup.
   */
  public double getAverageProbesPerLookup() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return (1.0 * numProbes) / numKeyLookups;
  }

  public long getNumHashCollisions() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return numHashCollisions;
  }

  /**
   * Looks up a key, and return a UnsafeRowLocation handle that can be used to test existence
   * and read/write values.
   * <p/>
   * This function always return the same UnsafeRowLocation instance to avoid object allocation.
   */
  public UnsafeRowLocation lookup(
      Object keyBaseObject,
      long keyBaseOffset,
      int keyRowLengthBytes) {
    if (enablePerfMetrics) {
      numKeyLookups++;
    }
    final int hashcode = HASHER.hashUnsafeWords(keyBaseObject, keyBaseOffset, keyRowLengthBytes);
    int pos = hashcode & mask;
    int step = 1;
    while (true) {
      if (enablePerfMetrics) {
        numProbes++;
      }
      if (!bitset.isSet(pos)) {
        // This is a new key.
        return loc.with(pos, hashcode, false, longArray[pos * 2]);
      } else {
        long stored = longArray[pos * 2 + 1];
        if ((int) (stored) == hashcode) {
          // Full hash code matches.  Let's compare the keys for equality.
          loc.with(pos, hashcode, true, longArray[pos * 2]);
          if (loc.getKeyLength() == keyRowLengthBytes) {
            final MemoryLocation keyAddress = loc.getKeyAddress();
            final Object storedKeyBaseObject = keyAddress.getBaseObject();
            final long storedKeyBaseOffset = keyAddress.getBaseOffset();
            final boolean areEqual = ByteArrayMethods.arrayEquals(
                keyBaseObject,
                keyBaseOffset,
                storedKeyBaseObject,
                storedKeyBaseOffset,
                keyRowLengthBytes
            );
            if (areEqual) {
              return loc;
            } else {
              if (enablePerfMetrics) {
                numHashCollisions++;
              }
            }
          }
        }
      }
      pos = (pos + step) & mask;
      step++;
    }
  }

  /**
   * Store a new key and value. This method may only be called once for a given key; if you want
   * to update the value associated with a key, then you can directly manipulate the bytes stored
   * at the value address.
   * <p/>
   * It is only valid to call this method immediately after calling `lookup()` using the same key.
   * <p/>
   * The key and value must be word-aligned (that is, their sizes must multiples of 8).
   * <p/>
   * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
   * will return information on the data stored by this `putNewKey` call.
   * <p/>
   * As an example usage, here's the proper way to store a new key:
   * <p/>
   * <pre>
   *   Location loc = map.lookup(keyBaseObject, keyBaseOffset, keyLengthInBytes);
   *   if (!loc.isDefined()) {
   *     loc.putNewKey(keyBaseObject, keyBaseOffset, keyLengthInBytes, ...)
   *   }
   * </pre>
   * <p/>
   * Unspecified behavior if the key is not defined.
   */
  public void putNewKey(
      long storedKeyAddress,
      UnsafeRowLocation location) {
    if (size == MAX_CAPACITY) {
      throw new IllegalStateException("BytesToBytesMap has reached maximum capacity");
    }
    size++;
    bitset.set(location.getPos());

    longArray[location.getPos() * 2] = storedKeyAddress;
    longArray[location.getPos() * 2 + 1] = location.getKeyHashcode();
    location.updateAddressesAndSizes(storedKeyAddress);
    location.setDefined(true);
    if (size > growthThreshold && longArray.length < MAX_CAPACITY) {
      growAndRehash();
    }
  }

  /**
   * Grows the size of the hash table and re-hash everything.
   */
  @VisibleForTesting
  public void growAndRehash() {
    long resizeStartTime = -1;
    if (enablePerfMetrics) {
      resizeStartTime = System.nanoTime();
    }
    // Store references to the old data structures to be used when we re-hash
    final long[] oldLongArray = longArray;
    final BitSet oldBitSet = bitset;
    final int oldCapacity = (int) oldBitSet.capacity();

    int nextCapacity =
        Math.min(growthStrategy.nextCapacity(oldCapacity), MAX_CAPACITY);
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    capacity =
        Math.max((int) Math.min(MAX_CAPACITY, nextPowerOf2(nextCapacity)), 64);
    // Allocate the new data structures
    allocate(capacity);

    // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
    for (int pos = oldBitSet.nextSetBit(0);
         pos >= 0; pos = oldBitSet.nextSetBit(pos + 1)) {
      final long keyPointer = oldLongArray[pos * 2];
      final int hashcode = (int) oldLongArray[pos * 2 + 1];
      int newPos = hashcode & mask;
      int step = 1;
      boolean keepGoing = true;

      // No need to check for equality here when we insert so this has one less if branch than
      // the similar code path in addWithoutResize.
      while (keepGoing) {
        if (!bitset.isSet(newPos)) {
          bitset.set(newPos);
          longArray[newPos * 2] = keyPointer;
          longArray[newPos * 2 + 1] = hashcode;
          keepGoing = false;
        } else {
          newPos = (newPos + step) & mask;
          step++;
        }
      }
    }

    // Deallocate the old data structures.
    //memoryManager.free(oldLongArray.memoryBlock());
    if (enablePerfMetrics) {
      timeSpentResizingNs += System.nanoTime() - resizeStartTime;
    }
  }

  /**
   * Returns the next number greater or equal num that is power of 2.
   */
  private static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }
}
