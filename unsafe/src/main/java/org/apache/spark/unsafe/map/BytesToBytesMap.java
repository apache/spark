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

import org.apache.spark.unsafe.*;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.bitset.BitSet;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.memory.HeapMemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;

import java.lang.IllegalStateException;import java.lang.Long;import java.lang.Object;import java.lang.Override;import java.lang.UnsupportedOperationException;import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A bytes to bytes hash map where keys and values are contiguous regions of bytes.
 *
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 *
 * Note that even though we use long for indexing, the map can support up to 2^31 keys because
 * we use 32 bit MurmurHash. In either case, if the key cardinality is so high, you should probably
 * be using sorting instead of hashing for better cache locality.
 */
public final class BytesToBytesMap {

  private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /** Bit mask for the upper 13 bits of a long */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /** Bit mask for the lower 32 bits of a long */
  private static final long MASK_LONG_LOWER_32_BITS = 0xFFFFFFFFL;

  private final MemoryAllocator allocator;

  /**
   * Tracks whether we're using in-heap or off-heap addresses.
   */
  private final boolean inHeap;

  /**
   * A linked list for tracking all allocated data pages so that we can free all of our memory.
   */
  private final List<MemoryBlock> dataPages = new LinkedList<MemoryBlock>();

  private static final long PAGE_SIZE_BYTES = 64000000;

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
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   */
  private final Object[] pageTable = new Object[PAGE_TABLE_SIZE];

  /**
   * When using an in-heap allocator, this holds the current page number.
   */
  private int currentPageNumber = -1;

  /**
   * The number of entries in the page table.
   */
  private static final int PAGE_TABLE_SIZE = 8096;  // Use the upper 13 bits to address the table.

  // TODO: This page table size places a limit on the maximum page size. We should account for this
  // somewhere as part of final cleanup in this file.


  /**
   * A single array to store the key and value.
   *
   * // TODO this comment may be out of date; fix it:
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds the upper bits of the key's hashcode plus
   * the relative offset from the key pointer to the value at index {@code i}.
   */
  private LongArray longArray;

  /**
   * A {@link BitSet} used to track location of the map where the key is set.
   * Size of the bitset should be half of the size of the long array.
   */
  private BitSet bitset;

  private final double loadFactor;

  /**
   * Number of keys defined in the map.
   */
  private long size;

  private long growthThreshold;

  private long mask;

  private final Location loc;


  public BytesToBytesMap(MemoryAllocator allocator, long initialCapacity, double loadFactor) {
    this.inHeap = allocator instanceof HeapMemoryAllocator;
    this.allocator = allocator;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    allocate(initialCapacity);
  }

  public BytesToBytesMap(MemoryAllocator allocator, long initialCapacity) {
    this(allocator, initialCapacity, 0.70);
  }

  // TODO: consider finalizer.

  /**
   * Returns the number of keys defined in the map.
   */
  public long size() { return size; }

  /**
   * Returns an iterator for iterating over the entries of this map.
   *
   * For efficiency, all calls to `next()` will return the same `Location` object.
   *
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public Iterator<Location> iterator() {
    return new Iterator<Location>() {

      private long nextPos = bitset.nextSetBit(0);

      @Override
      public boolean hasNext() {
        return nextPos != -1;
      }

      @Override
      public Location next() {
        final long pos = nextPos;
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

    final int hashcode = HASHER.hashUnsafeWords(keyBaseObject, keyBaseOffset, keyRowLengthBytes);
    long pos = ((long) hashcode) & mask;
    long step = 1;
    while (true) {
      if (!bitset.isSet(pos)) {
        // This is a new key.
        return loc.with(pos, hashcode, false);
      } else {
        long stored = longArray.get(pos * 2 + 1);
        if (((int) (stored & MASK_LONG_LOWER_32_BITS)) == hashcode) {
          // Full hash code matches.  Let's compare the keys for equality.
          loc.with(pos, hashcode, false);
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
              return loc.with(pos, hashcode, true);
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
    private long pos;
    /** True if this location points to a position where a key is defined, felase otherwise */
    private boolean isDefined;
    /**
     * The hashcode of the most recent key passed to
     * {@link BytesToBytesMap#lookup(Object, long, int)}. Caching this hashcode here allows us to
     * avoid re-hashing the key when storing a value for that key.
     */
    private int keyHashcode;
    private final MemoryLocation keyMemoryLocation = new MemoryLocation();
    private final MemoryLocation valueMemoryLocation = new MemoryLocation();

    Location with(long pos, int keyHashcode, boolean isDefined) {
      this.pos = pos;
      this.isDefined = isDefined;
      this.keyHashcode = keyHashcode;
      return this;
    }

    /**
     * Returns true if the key is defined at this position, and false otherwise.
     */
    public boolean isDefined() {
      return isDefined;
    }

    private Object getPage(long fullKeyAddress) {
      assert (inHeap);
      final int keyPageNumber = (int) ((fullKeyAddress & MASK_LONG_UPPER_13_BITS) >>> 51);
      assert (keyPageNumber >= 0 && keyPageNumber < PAGE_TABLE_SIZE);
      assert (keyPageNumber <= currentPageNumber);
      final Object page = pageTable[keyPageNumber];
      assert (page != null);
      return page;
    }

    private long getOffsetInPage(long fullKeyAddress) {
      return (fullKeyAddress & MASK_LONG_LOWER_51_BITS);
    }

    /**
     * Returns the address of the key defined at this position.
     * This points to the first byte of the key data.
     * Unspecified behavior if the key is not defined.
     * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
     */
    public MemoryLocation getKeyAddress() {
      final long fullKeyAddress = longArray.get(pos * 2);
      if (inHeap) {
        keyMemoryLocation.setObjAndOffset(
          getPage(fullKeyAddress), getOffsetInPage(fullKeyAddress) + 8);
      } else {
        keyMemoryLocation.setObjAndOffset(null, fullKeyAddress + 8);
      }
      return keyMemoryLocation;
    }

    /**
     * Returns the length of the key defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public long getKeyLength() {
      final long fullKeyAddress = longArray.get(pos * 2);
      if (inHeap) {
        return PlatformDependent.UNSAFE.getLong(
          getPage(fullKeyAddress), getOffsetInPage(fullKeyAddress));
      } else {
        return PlatformDependent.UNSAFE.getLong(fullKeyAddress);
      }
    }

    /**
     * Returns the address of the value defined at this position.
     * This points to the first byte of the value data.
     * Unspecified behavior if the key is not defined.
     * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
     */
    public MemoryLocation getValueAddress() {
      // The relative offset from the key position to the value position was stored in the upper 32
      // bits of the value long:
      final long offsetFromKeyToValue = (longArray.get(pos * 2 + 1) & ~MASK_LONG_LOWER_32_BITS) >>> 32;
      final long fullKeyAddress = longArray.get(pos * 2);
      if (inHeap) {
        valueMemoryLocation.setObjAndOffset(
          getPage(fullKeyAddress),
          getOffsetInPage(fullKeyAddress) + 8 + offsetFromKeyToValue
        );
      } else {
        valueMemoryLocation.setObjAndOffset(null, fullKeyAddress + 8 + offsetFromKeyToValue);
      }
      return valueMemoryLocation;
    }

    /**
     * Returns the length of the value defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public long getValueLength() {
      // The relative offset from the key position to the value position was stored in the upper 32
      // bits of the value long:
        final long offsetFromKeyToValue = (longArray.get(pos * 2 + 1) & ~MASK_LONG_LOWER_32_BITS) >>> 32;
      final long fullKeyAddress = longArray.get(pos * 2);
      if (inHeap) {
        return PlatformDependent.UNSAFE.getLong(
          getPage(fullKeyAddress),
          getOffsetInPage(fullKeyAddress) + offsetFromKeyToValue
        );
      } else {
        return PlatformDependent.UNSAFE.getLong(fullKeyAddress + offsetFromKeyToValue);
      }
    }

    /**
     * Sets the value defined at this position. This method may only be called once for a given
     * key; if you want to update the value associated with a key, then you can directly manipulate
     * the bytes stored at the value address.
     *
     * It is only valid to call this method after having first called `lookup()` using the same key.
     *
     * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
     * will return information on the data stored by this `storeKeyAndValue` call.
     *
     * As an example usage, here's the proper way to store a new key:
     *
     * <code>
     *   Location loc = map.lookup(keyBaseOffset, keyBaseObject, keyLengthInBytes);
     *   if (!loc.isDefined()) {
     *     loc.storeKeyAndValue(keyBaseOffset, keyBaseObject, keyLengthInBytes, ...)
     *   }
     * </code>
     *
     * Unspecified behavior if the key is not defined.
     */
    public void storeKeyAndValue(
        Object keyBaseObject,
        long keyBaseOffset,
        int keyLengthBytes,  // TODO(josh): words?  bytes? eventually, we'll want to be more consistent about this
        Object valueBaseObject,
        long valueBaseOffset,
        long valueLengthBytes) {
      if (isDefined) {
        throw new IllegalStateException("Can only set value once for a key");
      }
      isDefined = true;
      assert (keyLengthBytes % 8 == 0);
      assert (valueLengthBytes % 8 == 0);
      // Here, we'll copy the data into our data pages. Because we only store a relative offset from
      // the key address instead of storing the absolute address of the value, the key and value
      // must be stored in the same memory page.
      final long requiredSize = 8 + 8 + keyLengthBytes + valueLengthBytes;
      assert(requiredSize <= PAGE_SIZE_BYTES);
      // Bookeeping
      size++;
      bitset.set(pos);

      // If there's not enough space in the current page, allocate a new page:
      if (currentDataPage == null || PAGE_SIZE_BYTES - pageCursor < requiredSize) {
        MemoryBlock newPage = allocator.allocate(PAGE_SIZE_BYTES);
        dataPages.add(newPage);
        pageCursor = 0;
        currentPageNumber++;
        pageTable[currentPageNumber] = newPage.getBaseObject();
        currentDataPage = newPage;
      }

      // Compute all of our offsets up-front:
      final Object pageBaseObject = currentDataPage.getBaseObject();
      final long pageBaseOffset = currentDataPage.getBaseOffset();
      final long keySizeOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += 8;
      final long keyDataOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += keyLengthBytes;
      final long valueSizeOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += 8;
      final long valueDataOffsetInPage = pageBaseOffset + pageCursor;
      pageCursor += valueLengthBytes;
      final long relativeOffsetFromKeyToValue = valueSizeOffsetInPage - keySizeOffsetInPage;
      assert(relativeOffsetFromKeyToValue > 0);

      // Copy the key
      PlatformDependent.UNSAFE.putLong(pageBaseObject, keySizeOffsetInPage, keyLengthBytes);
      PlatformDependent.UNSAFE.copyMemory(
        keyBaseObject, keyBaseOffset, pageBaseObject, keyDataOffsetInPage, keyLengthBytes);
      // Copy the value
      PlatformDependent.UNSAFE.putLong(pageBaseObject, valueSizeOffsetInPage, valueLengthBytes);
      PlatformDependent.UNSAFE.copyMemory(
        valueBaseObject, valueBaseOffset, pageBaseObject, valueDataOffsetInPage, valueLengthBytes);

      final long storedKeyAddress;
      if (inHeap) {
        // If we're in-heap, then we need to store the page number in the upper 13 bits of the
        // address
        storedKeyAddress = (((long) currentPageNumber) << 51) | (keySizeOffsetInPage & MASK_LONG_LOWER_51_BITS);
      } else {
        // Otherwise, just store the raw memory address
        storedKeyAddress = keySizeOffsetInPage;
      }
      longArray.set(pos * 2, storedKeyAddress);
      final long storedValueOffsetAndKeyHashcode =
        (relativeOffsetFromKeyToValue << 32) | (keyHashcode & MASK_LONG_LOWER_32_BITS);
      longArray.set(pos * 2 + 1, storedValueOffsetAndKeyHashcode);
      if (size > growthThreshold) {
        growAndRehash();
      }
    }
  }

  private void allocate(long capacity) {
    capacity = java.lang.Math.max(nextPowerOf2(capacity), 64);
    longArray = new LongArray(allocator.allocate(capacity * 8 * 2));
    bitset = new BitSet(allocator.allocate(capacity / 8));

    this.growthThreshold = (long) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   */
  public void free() {
    allocator.free(longArray.memoryBlock());
    longArray = null;
    allocator.free(bitset.memoryBlock());
    bitset = null;
    Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
    while (dataPagesIterator.hasNext()) {
      allocator.free(dataPagesIterator.next());
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
   * Grows the size of the hash table and re-hash everything.
   */
  private void growAndRehash() {
    // Store references to the old data structures to be used when we re-hash
    final LongArray oldLongArray = longArray;
    final BitSet oldBitSet = bitset;
    final long oldCapacity = oldBitSet.capacity();

    // Allocate the new data structures
    allocate(growthStrategy.nextCapacity(oldCapacity));

    // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
    for (long pos = oldBitSet.nextSetBit(0); pos >= 0; pos = oldBitSet.nextSetBit(pos + 1)) {
      final long keyPointer = oldLongArray.get(pos * 2);
      final long valueOffsetPlusHashcode = oldLongArray.get(pos * 2 + 1);
      final int hashcode = (int) (valueOffsetPlusHashcode & MASK_LONG_LOWER_32_BITS);
      long newPos = ((long) hashcode) & mask;
      long step = 1;
      boolean keepGoing = true;

      // No need to check for equality here when we insert so this has one less if branch than
      // the similar code path in addWithoutResize.
      while (keepGoing) {
        if (!bitset.isSet(newPos)) {
          longArray.set(newPos * 2, keyPointer);
          longArray.set(newPos * 2 + 1, valueOffsetPlusHashcode);
          bitset.set(newPos);
          keepGoing = false;
        } else {
          newPos = (newPos + step) & mask;
          step++;
        }
      }
    }

    // Deallocate the old data structures.
    allocator.free(oldLongArray.memoryBlock());
    allocator.free(oldBitSet.memoryBlock());
  }

  /** Returns the next number greater or equal num that is power of 2. */
  private static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }
}
