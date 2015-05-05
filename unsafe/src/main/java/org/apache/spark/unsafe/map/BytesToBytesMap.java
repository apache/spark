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

import java.lang.Override;
import java.lang.UnsupportedOperationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.unsafe.*;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.bitset.BitSet;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.memory.*;

/**
 * An append-only hash map where keys and values are contiguous regions of bytes.
 * <p>
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 * <p>
 * The map can support up to 2^31 keys because we use 32 bit MurmurHash. If the key cardinality is
 * higher than this, you should probably be using sorting instead of hashing for better cache
 * locality.
 * <p>
 * This class is not thread safe.
 */
public final class BytesToBytesMap {

  private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  private final TaskMemoryManager memoryManager;

  /**
   * A linked list for tracking all allocated data pages so that we can free all of our memory.
   */
  private final List<MemoryBlock> dataPages = new LinkedList<MemoryBlock>();

  /**
   * The data page that will be used to store keys and values for new hashtable entries. When this
   * page becomes full, a new page will be allocated and this pointer will change to point to that
   * new page.
   */
  private MemoryBlock currentDataPage = null;

  /**
   * Offset into `currentDataPage` that points to the location where new data can be inserted into
   * the page.
   */
  private long pageCursor = 0;

  /**
   * The size of the data pages that hold key and value data. Map entries cannot span multiple
   * pages, so this limits the maximum entry size.
   */
  private static final long PAGE_SIZE_BYTES = 1L << 26; // 64 megabytes

  // This choice of page table size and page size means that we can address up to 500 gigabytes
  // of memory.

  /**
   * A single array to store the key and value.
   *
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
   */
  private LongArray longArray;
  // TODO: we're wasting 32 bits of space here; we can probably store fewer bits of the hashcode
  // and exploit word-alignment to use fewer bits to hold the address.  This might let us store
  // only one long per map entry, increasing the chance that this array will fit in cache at the
  // expense of maybe performing more lookups if we have hash collisions.  Say that we stored only
  // 27 bits of the hashcode and 37 bits of the address.  37 bits is enough to address 1 terabyte
  // of RAM given word-alignment.  If we use 13 bits of this for our page table, that gives us a
  // maximum page size of 2^24 * 8 = ~134 megabytes per page. This change will require us to store
  // full base addresses in the page table for off-heap mode so that we can reconstruct the full
  // absolute memory addresses.

  /**
   * A {@link BitSet} used to track location of the map where the key is set.
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

  /**
   * Mask for truncating hashcodes so that they do not exceed the long array's size.
   * This is a strength reduction optimization; we're essentially performing a modulus operation,
   * but doing so with a bitmask because this is a power-of-2-sized hash map.
   */
  private int mask;

  /**
   * Return value of {@link BytesToBytesMap#lookup(Object, long, int)}.
   */
  private final Location loc;

  private final boolean enablePerfMetrics;

  private long timeSpentResizingNs = 0;

  private long numProbes = 0;

  private long numKeyLookups = 0;

  private long numHashCollisions = 0;

  public BytesToBytesMap(
      TaskMemoryManager memoryManager,
      int initialCapacity,
      double loadFactor,
      boolean enablePerfMetrics) {
    this.memoryManager = memoryManager;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    this.enablePerfMetrics = enablePerfMetrics;
    allocate(initialCapacity);
  }

  public BytesToBytesMap(TaskMemoryManager memoryManager, int initialCapacity) {
    this(memoryManager, initialCapacity, 0.70, false);
  }

  public BytesToBytesMap(
      TaskMemoryManager memoryManager,
      int initialCapacity,
      boolean enablePerfMetrics) {
    this(memoryManager, initialCapacity, 0.70, enablePerfMetrics);
  }

  /**
   * Returns the number of keys defined in the map.
   */
  public int size() { return size; }

  /**
   * Returns an iterator for iterating over the entries of this map.
   *
   * For efficiency, all calls to `next()` will return the same {@link Location} object.
   *
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public Iterator<Location> iterator() {
    return new Iterator<Location>() {

      private int nextPos = bitset.nextSetBit(0);

      @Override
      public boolean hasNext() {
        return nextPos != -1;
      }

      @Override
      public Location next() {
        final int pos = nextPos;
        nextPos = bitset.nextSetBit(nextPos + 1);
        return loc.with(pos, 0, true);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Looks up a key, and return a {@link Location} handle that can be used to test existence
   * and read/write values.
   *
   * This function always return the same {@link Location} instance to avoid object allocation.
   */
  public Location lookup(
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
        return loc.with(pos, hashcode, false);
      } else {
        long stored = longArray.get(pos * 2 + 1);
        if ((int) (stored) == hashcode) {
          // Full hash code matches.  Let's compare the keys for equality.
          loc.with(pos, hashcode, true);
          if (loc.getKeyLength() == keyRowLengthBytes) {
            final MemoryLocation keyAddress = loc.getKeyAddress();
            final Object storedKeyBaseObject = keyAddress.getBaseObject();
            final long storedKeyBaseOffset = keyAddress.getBaseOffset();
            final boolean areEqual = ByteArrayMethods.wordAlignedArrayEquals(
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
   * Handle returned by {@link BytesToBytesMap#lookup(Object, long, int)} function.
   */
  public final class Location {
    /** An index into the hash map's Long array */
    private int pos;
    /** True if this location points to a position where a key is defined, false otherwise */
    private boolean isDefined;
    /**
     * The hashcode of the most recent key passed to
     * {@link BytesToBytesMap#lookup(Object, long, int)}. Caching this hashcode here allows us to
     * avoid re-hashing the key when storing a value for that key.
     */
    private int keyHashcode;
    private final MemoryLocation keyMemoryLocation = new MemoryLocation();
    private final MemoryLocation valueMemoryLocation = new MemoryLocation();
    private int keyLength;
    private int valueLength;

    private void updateAddressesAndSizes(long fullKeyAddress) {
        final Object page = memoryManager.getPage(fullKeyAddress);
        final long keyOffsetInPage = memoryManager.getOffsetInPage(fullKeyAddress);
        long position = keyOffsetInPage;
        keyLength = (int) PlatformDependent.UNSAFE.getLong(page, position);
        position += 8; // word used to store the key size
        keyMemoryLocation.setObjAndOffset(page, position);
        position += keyLength;
        valueLength = (int) PlatformDependent.UNSAFE.getLong(page, position);
        position += 8; // word used to store the key size
        valueMemoryLocation.setObjAndOffset(page, position);
    }

    Location with(int pos, int keyHashcode, boolean isDefined) {
      this.pos = pos;
      this.isDefined = isDefined;
      this.keyHashcode = keyHashcode;
      if (isDefined) {
        final long fullKeyAddress = longArray.get(pos * 2);
        updateAddressesAndSizes(fullKeyAddress);
      }
      return this;
    }

    /**
     * Returns true if the key is defined at this position, and false otherwise.
     */
    public boolean isDefined() {
      return isDefined;
    }

    /**
     * Returns the address of the key defined at this position.
     * This points to the first byte of the key data.
     * Unspecified behavior if the key is not defined.
     * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
     */
    public MemoryLocation getKeyAddress() {
      assert (isDefined);
      return keyMemoryLocation;
    }

    /**
     * Returns the length of the key defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public int getKeyLength() {
      assert (isDefined);
      return keyLength;
    }

    /**
     * Returns the address of the value defined at this position.
     * This points to the first byte of the value data.
     * Unspecified behavior if the key is not defined.
     * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
     */
    public MemoryLocation getValueAddress() {
      assert (isDefined);
      return valueMemoryLocation;
    }

    /**
     * Returns the length of the value defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public int getValueLength() {
      assert (isDefined);
      return valueLength;
    }

    /**
     * Store a new key and value. This method may only be called once for a given key; if you want
     * to update the value associated with a key, then you can directly manipulate the bytes stored
     * at the value address.
     * <p>
     * It is only valid to call this method immediately after calling `lookup()` using the same key.
     * <p>
     * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
     * will return information on the data stored by this `putNewKey` call.
     * <p>
     * As an example usage, here's the proper way to store a new key:
     * <p>
     * <pre>
     *   Location loc = map.lookup(keyBaseObject, keyBaseOffset, keyLengthInBytes);
     *   if (!loc.isDefined()) {
     *     loc.putNewKey(keyBaseObject, keyBaseOffset, keyLengthInBytes, ...)
     *   }
     * </pre>
     * <p>
     * Unspecified behavior if the key is not defined.
     */
    public void putNewKey(
        Object keyBaseObject,
        long keyBaseOffset,
        int keyLengthBytes,
        Object valueBaseObject,
        long valueBaseOffset,
        int valueLengthBytes) {
      assert (!isDefined) : "Can only set value once for a key";
      isDefined = true;
      assert (keyLengthBytes % 8 == 0);
      assert (valueLengthBytes % 8 == 0);
      // Here, we'll copy the data into our data pages. Because we only store a relative offset from
      // the key address instead of storing the absolute address of the value, the key and value
      // must be stored in the same memory page.
      // (8 byte key length) (key) (8 byte value length) (value)
      final long requiredSize = 8 + keyLengthBytes + 8 + valueLengthBytes;
      assert(requiredSize <= PAGE_SIZE_BYTES);
      size++;
      bitset.set(pos);

      // If there's not enough space in the current page, allocate a new page:
      if (currentDataPage == null || PAGE_SIZE_BYTES - pageCursor < requiredSize) {
        MemoryBlock newPage = memoryManager.allocatePage(PAGE_SIZE_BYTES);
        dataPages.add(newPage);
        pageCursor = 0;
        currentDataPage = newPage;
      }

      // Compute all of our offsets up-front:
      final Object pageBaseObject = currentDataPage.getBaseObject();
      final long pageBaseOffset = currentDataPage.getBaseOffset();
      final long keySizeOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += 8; // word used to store the key size
      final long keyDataOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += keyLengthBytes;
      final long valueSizeOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += 8; // word used to store the value size
      final long valueDataOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += valueLengthBytes;

      // Copy the key
      PlatformDependent.UNSAFE.putLong(pageBaseObject, keySizeOffsetInPage, keyLengthBytes);
      PlatformDependent.copyMemory(
        keyBaseObject, keyBaseOffset, pageBaseObject, keyDataOffsetInPage, keyLengthBytes);
      // Copy the value
      PlatformDependent.UNSAFE.putLong(pageBaseObject, valueSizeOffsetInPage, valueLengthBytes);
      PlatformDependent.copyMemory(
        valueBaseObject, valueBaseOffset, pageBaseObject, valueDataOffsetInPage, valueLengthBytes);

      final long storedKeyAddress = memoryManager.encodePageNumberAndOffset(
        currentDataPage, keySizeOffsetInPage);
      longArray.set(pos * 2, storedKeyAddress);
      longArray.set(pos * 2 + 1, keyHashcode);
      updateAddressesAndSizes(storedKeyAddress);
      isDefined = true;
      if (size > growthThreshold) {
        growAndRehash();
      }
    }
  }

  /**
   * Allocate new data structures for this map. When calling this outside of the constructor,
   * make sure to keep references to the old data structures so that you can free them.
   *
   * @param capacity the new map capacity
   */
  private void allocate(int capacity) {
    capacity = Math.max((int) Math.min(Integer.MAX_VALUE, nextPowerOf2(capacity)), 64);
    longArray = new LongArray(memoryManager.allocate(capacity * 8 * 2));
    bitset = new BitSet(MemoryBlock.fromLongArray(new long[capacity / 64]));

    this.growthThreshold = (int) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   *
   * This method is idempotent.
   */
  public void free() {
    if (longArray != null) {
      memoryManager.free(longArray.memoryBlock());
      longArray = null;
    }
    if (bitset != null) {
      // The bitset's heap memory isn't managed by a memory manager, so no need to free it here.
      bitset = null;
    }
    Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
    while (dataPagesIterator.hasNext()) {
      memoryManager.freePage(dataPagesIterator.next());
      dataPagesIterator.remove();
    }
    assert(dataPages.isEmpty());
  }

  /** Returns the total amount of memory, in bytes, consumed by this map's managed structures. */
  public long getTotalMemoryConsumption() {
    return (
      dataPages.size() * PAGE_SIZE_BYTES +
      bitset.memoryBlock().size() +
      longArray.memoryBlock().size());
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
   * Grows the size of the hash table and re-hash everything.
   */
  private void growAndRehash() {
    long resizeStartTime = -1;
    if (enablePerfMetrics) {
      resizeStartTime = System.nanoTime();
    }
    // Store references to the old data structures to be used when we re-hash
    final LongArray oldLongArray = longArray;
    final BitSet oldBitSet = bitset;
    final int oldCapacity = (int) oldBitSet.capacity();

    // Allocate the new data structures
    allocate(Math.min(Integer.MAX_VALUE, growthStrategy.nextCapacity(oldCapacity)));

    // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
    for (int pos = oldBitSet.nextSetBit(0); pos >= 0; pos = oldBitSet.nextSetBit(pos + 1)) {
      final long keyPointer = oldLongArray.get(pos * 2);
      final int hashcode = (int) oldLongArray.get(pos * 2 + 1);
      int newPos = hashcode & mask;
      int step = 1;
      boolean keepGoing = true;

      // No need to check for equality here when we insert so this has one less if branch than
      // the similar code path in addWithoutResize.
      while (keepGoing) {
        if (!bitset.isSet(newPos)) {
          bitset.set(newPos);
          longArray.set(newPos * 2, keyPointer);
          longArray.set(newPos * 2 + 1, hashcode);
          keepGoing = false;
        } else {
          newPos = (newPos + step) & mask;
          step++;
        }
      }
    }

    // Deallocate the old data structures.
    memoryManager.free(oldLongArray.memoryBlock());
    if (enablePerfMetrics) {
      timeSpentResizingNs += System.nanoTime() - resizeStartTime;
    }
  }

  /** Returns the next number greater or equal num that is power of 2. */
  private static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }
}
