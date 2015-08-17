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

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.bitset.BitSet;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

/**
 * An append-only hash map where keys and values are contiguous regions of bytes.
 *
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 *
 * The map can support up to 2^29 keys. If the key cardinality is higher than this, you should
 * probably be using sorting instead of hashing for better cache locality.
 *
 * The key and values under the hood are stored together, in the following format:
 *   Bytes 0 to 4: len(k) (key length in bytes) + len(v) (value length in bytes) + 4
 *   Bytes 4 to 8: len(k)
 *   Bytes 8 to 8 + len(k): key data
 *   Bytes 8 + len(k) to 8 + len(k) + len(v): value data
 *
 * This means that the first four bytes store the entire record (key + value) length. This format
 * is consistent with {@link org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter},
 * so we can pass records from this map directly into the sorter to sort records in place.
 */
public final class BytesToBytesMap {

  private final Logger logger = LoggerFactory.getLogger(BytesToBytesMap.class);

  private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  /**
   * Special record length that is placed after the last record in a data page.
   */
  private static final int END_OF_PAGE_MARKER = -1;

  private final TaskMemoryManager taskMemoryManager;

  private final ShuffleMemoryManager shuffleMemoryManager;

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
   * the page. This does not incorporate the page's base offset.
   */
  private long pageCursor = 0;

  /**
   * The maximum number of keys that BytesToBytesMap supports. The hash table has to be
   * power-of-2-sized and its backing Java array can contain at most (1 &lt;&lt; 30) elements,
   * since that's the largest power-of-2 that's less than Integer.MAX_VALUE. We need two long array
   * entries per key, giving us a maximum capacity of (1 &lt;&lt; 29).
   */
  @VisibleForTesting
  static final int MAX_CAPACITY = (1 << 29);

  // This choice of page table size and page size means that we can address up to 500 gigabytes
  // of memory.

  /**
   * A single array to store the key and value.
   *
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
   */
  @Nullable private LongArray longArray;
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
  @Nullable private BitSet bitset;

  private final double loadFactor;

  /**
   * The size of the data pages that hold key and value data. Map entries cannot span multiple
   * pages, so this limits the maximum entry size.
   */
  private final long pageSizeBytes;

  /**
   * Number of keys defined in the map.
   */
  private int numElements;

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

  private long peakMemoryUsedBytes = 0L;

  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      int initialCapacity,
      double loadFactor,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this.taskMemoryManager = taskMemoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    this.pageSizeBytes = pageSizeBytes;
    this.enablePerfMetrics = enablePerfMetrics;
    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("Initial capacity must be greater than 0");
    }
    if (initialCapacity > MAX_CAPACITY) {
      throw new IllegalArgumentException(
        "Initial capacity " + initialCapacity + " exceeds maximum capacity of " + MAX_CAPACITY);
    }
    if (pageSizeBytes > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) {
      throw new IllegalArgumentException("Page size " + pageSizeBytes + " cannot exceed " +
        TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES);
    }
    allocate(initialCapacity);

    // Acquire a new page as soon as we construct the map to ensure that we have at least
    // one page to work with. Otherwise, other operators in the same task may starve this
    // map (SPARK-9747).
    acquireNewPage();
  }

  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      int initialCapacity,
      long pageSizeBytes) {
    this(taskMemoryManager, shuffleMemoryManager, initialCapacity, 0.70, pageSizeBytes, false);
  }

  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this(
      taskMemoryManager,
      shuffleMemoryManager,
      initialCapacity,
      0.70,
      pageSizeBytes,
      enablePerfMetrics);
  }

  /**
   * Returns the number of keys defined in the map.
   */
  public int numElements() { return numElements; }

  public static final class BytesToBytesMapIterator implements Iterator<Location> {

    private final int numRecords;
    private final Iterator<MemoryBlock> dataPagesIterator;
    private final Location loc;

    private MemoryBlock currentPage = null;
    private int currentRecordNumber = 0;
    private Object pageBaseObject;
    private long offsetInPage;

    // If this iterator destructive or not. When it is true, it frees each page as it moves onto
    // next one.
    private boolean destructive = false;
    private BytesToBytesMap bmap;

    private BytesToBytesMapIterator(
        int numRecords, Iterator<MemoryBlock> dataPagesIterator, Location loc,
        boolean destructive, BytesToBytesMap bmap) {
      this.numRecords = numRecords;
      this.dataPagesIterator = dataPagesIterator;
      this.loc = loc;
      this.destructive = destructive;
      this.bmap = bmap;
      if (dataPagesIterator.hasNext()) {
        advanceToNextPage();
      }
    }

    private void advanceToNextPage() {
      if (destructive && currentPage != null) {
        dataPagesIterator.remove();
        this.bmap.taskMemoryManager.freePage(currentPage);
        this.bmap.shuffleMemoryManager.release(currentPage.size());
      }
      currentPage = dataPagesIterator.next();
      pageBaseObject = currentPage.getBaseObject();
      offsetInPage = currentPage.getBaseOffset();
    }

    @Override
    public boolean hasNext() {
      return currentRecordNumber != numRecords;
    }

    @Override
    public Location next() {
      int totalLength = Platform.getInt(pageBaseObject, offsetInPage);
      if (totalLength == END_OF_PAGE_MARKER) {
        advanceToNextPage();
        totalLength = Platform.getInt(pageBaseObject, offsetInPage);
      }
      loc.with(currentPage, offsetInPage);
      offsetInPage += 4 + totalLength;
      currentRecordNumber++;
      return loc;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns an iterator for iterating over the entries of this map.
   *
   * For efficiency, all calls to `next()` will return the same {@link Location} object.
   *
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public BytesToBytesMapIterator iterator() {
    return new BytesToBytesMapIterator(numElements, dataPages.iterator(), loc, false, this);
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map. It frees each page
   * as it moves onto next one. Notice: it is illegal to call any method on the map after
   * `destructiveIterator()` has been called.
   *
   * For efficiency, all calls to `next()` will return the same {@link Location} object.
   *
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public BytesToBytesMapIterator destructiveIterator() {
    return new BytesToBytesMapIterator(numElements, dataPages.iterator(), loc, true, this);
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
    safeLookup(keyBaseObject, keyBaseOffset, keyRowLengthBytes, loc);
    return loc;
  }

  /**
   * Looks up a key, and saves the result in provided `loc`.
   *
   * This is a thread-safe version of `lookup`, could be used by multiple threads.
   */
  public void safeLookup(
      Object keyBaseObject,
      long keyBaseOffset,
      int keyRowLengthBytes,
      Location loc) {
    assert(bitset != null);
    assert(longArray != null);

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
        loc.with(pos, hashcode, false);
        return;
      } else {
        long stored = longArray.get(pos * 2 + 1);
        if ((int) (stored) == hashcode) {
          // Full hash code matches.  Let's compare the keys for equality.
          loc.with(pos, hashcode, true);
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
              return;
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

    /**
     * Memory page containing the record. Only set if created by {@link BytesToBytesMap#iterator()}.
     */
    @Nullable private MemoryBlock memoryPage;

    private void updateAddressesAndSizes(long fullKeyAddress) {
      updateAddressesAndSizes(
        taskMemoryManager.getPage(fullKeyAddress),
        taskMemoryManager.getOffsetInPage(fullKeyAddress));
    }

    private void updateAddressesAndSizes(final Object page, final long offsetInPage) {
      long position = offsetInPage;
      final int totalLength = Platform.getInt(page, position);
      position += 4;
      keyLength = Platform.getInt(page, position);
      position += 4;
      valueLength = totalLength - keyLength - 4;

      keyMemoryLocation.setObjAndOffset(page, position);

      position += keyLength;
      valueMemoryLocation.setObjAndOffset(page, position);
    }

    private Location with(int pos, int keyHashcode, boolean isDefined) {
      assert(longArray != null);
      this.pos = pos;
      this.isDefined = isDefined;
      this.keyHashcode = keyHashcode;
      if (isDefined) {
        final long fullKeyAddress = longArray.get(pos * 2);
        updateAddressesAndSizes(fullKeyAddress);
      }
      return this;
    }

    private Location with(MemoryBlock page, long offsetInPage) {
      this.isDefined = true;
      this.memoryPage = page;
      updateAddressesAndSizes(page.getBaseObject(), offsetInPage);
      return this;
    }

    /**
     * Returns the memory page that contains the current record.
     * This is only valid if this is returned by {@link BytesToBytesMap#iterator()}.
     */
    public MemoryBlock getMemoryPage() {
      return this.memoryPage;
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
     * at the value address. The return value indicates whether the put succeeded or whether it
     * failed because additional memory could not be acquired.
     * <p>
     * It is only valid to call this method immediately after calling `lookup()` using the same key.
     * </p>
     * <p>
     * The key and value must be word-aligned (that is, their sizes must multiples of 8).
     * </p>
     * <p>
     * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
     * will return information on the data stored by this `putNewKey` call.
     * </p>
     * <p>
     * As an example usage, here's the proper way to store a new key:
     * </p>
     * <pre>
     *   Location loc = map.lookup(keyBaseObject, keyBaseOffset, keyLengthInBytes);
     *   if (!loc.isDefined()) {
     *     if (!loc.putNewKey(keyBaseObject, keyBaseOffset, keyLengthInBytes, ...)) {
     *       // handle failure to grow map (by spilling, for example)
     *     }
     *   }
     * </pre>
     * <p>
     * Unspecified behavior if the key is not defined.
     * </p>
     *
     * @return true if the put() was successful and false if the put() failed because memory could
     *         not be acquired.
     */
    public boolean putNewKey(
        Object keyBaseObject,
        long keyBaseOffset,
        int keyLengthBytes,
        Object valueBaseObject,
        long valueBaseOffset,
        int valueLengthBytes) {
      assert (!isDefined) : "Can only set value once for a key";
      assert (keyLengthBytes % 8 == 0);
      assert (valueLengthBytes % 8 == 0);
      assert(bitset != null);
      assert(longArray != null);

      if (numElements == MAX_CAPACITY) {
        throw new IllegalStateException("BytesToBytesMap has reached maximum capacity");
      }

      // Here, we'll copy the data into our data pages. Because we only store a relative offset from
      // the key address instead of storing the absolute address of the value, the key and value
      // must be stored in the same memory page.
      // (8 byte key length) (key) (value)
      final long requiredSize = 8 + keyLengthBytes + valueLengthBytes;

      // --- Figure out where to insert the new record ---------------------------------------------

      final MemoryBlock dataPage;
      final Object dataPageBaseObject;
      final long dataPageInsertOffset;
      boolean useOverflowPage = requiredSize > pageSizeBytes - 8;
      if (useOverflowPage) {
        // The record is larger than the page size, so allocate a special overflow page just to hold
        // that record.
        final long memoryRequested = requiredSize + 8;
        final long memoryGranted = shuffleMemoryManager.tryToAcquire(memoryRequested);
        if (memoryGranted != memoryRequested) {
          shuffleMemoryManager.release(memoryGranted);
          logger.debug("Failed to acquire {} bytes of memory", memoryRequested);
          return false;
        }
        MemoryBlock overflowPage = taskMemoryManager.allocatePage(memoryRequested);
        dataPages.add(overflowPage);
        dataPage = overflowPage;
        dataPageBaseObject = overflowPage.getBaseObject();
        dataPageInsertOffset = overflowPage.getBaseOffset();
      } else if (currentDataPage == null || pageSizeBytes - 8 - pageCursor < requiredSize) {
        // The record can fit in a data page, but either we have not allocated any pages yet or
        // the current page does not have enough space.
        if (currentDataPage != null) {
          // There wasn't enough space in the current page, so write an end-of-page marker:
          final Object pageBaseObject = currentDataPage.getBaseObject();
          final long lengthOffsetInPage = currentDataPage.getBaseOffset() + pageCursor;
          Platform.putInt(pageBaseObject, lengthOffsetInPage, END_OF_PAGE_MARKER);
        }
        if (!acquireNewPage()) {
          return false;
        }
        dataPage = currentDataPage;
        dataPageBaseObject = currentDataPage.getBaseObject();
        dataPageInsertOffset = currentDataPage.getBaseOffset();
      } else {
        // There is enough space in the current data page.
        dataPage = currentDataPage;
        dataPageBaseObject = currentDataPage.getBaseObject();
        dataPageInsertOffset = currentDataPage.getBaseOffset() + pageCursor;
      }

      // --- Append the key and value data to the current data page --------------------------------

      long insertCursor = dataPageInsertOffset;

      // Compute all of our offsets up-front:
      final long recordOffset = insertCursor;
      insertCursor += 4;
      final long keyLengthOffset = insertCursor;
      insertCursor += 4;
      final long keyDataOffsetInPage = insertCursor;
      insertCursor += keyLengthBytes;
      final long valueDataOffsetInPage = insertCursor;
      insertCursor += valueLengthBytes; // word used to store the value size

      Platform.putInt(dataPageBaseObject, recordOffset,
        keyLengthBytes + valueLengthBytes + 4);
      Platform.putInt(dataPageBaseObject, keyLengthOffset, keyLengthBytes);
      // Copy the key
      Platform.copyMemory(
        keyBaseObject, keyBaseOffset, dataPageBaseObject, keyDataOffsetInPage, keyLengthBytes);
      // Copy the value
      Platform.copyMemory(valueBaseObject, valueBaseOffset, dataPageBaseObject,
        valueDataOffsetInPage, valueLengthBytes);

      // --- Update bookeeping data structures -----------------------------------------------------

      if (useOverflowPage) {
        // Store the end-of-page marker at the end of the data page
        Platform.putInt(dataPageBaseObject, insertCursor, END_OF_PAGE_MARKER);
      } else {
        pageCursor += requiredSize;
      }

      numElements++;
      bitset.set(pos);
      final long storedKeyAddress = taskMemoryManager.encodePageNumberAndOffset(
        dataPage, recordOffset);
      longArray.set(pos * 2, storedKeyAddress);
      longArray.set(pos * 2 + 1, keyHashcode);
      updateAddressesAndSizes(storedKeyAddress);
      isDefined = true;
      if (numElements > growthThreshold && longArray.size() < MAX_CAPACITY) {
        growAndRehash();
      }
      return true;
    }
  }

  /**
   * Acquire a new page from the {@link ShuffleMemoryManager}.
   * @return whether there is enough space to allocate the new page.
   */
  private boolean acquireNewPage() {
    final long memoryGranted = shuffleMemoryManager.tryToAcquire(pageSizeBytes);
    if (memoryGranted != pageSizeBytes) {
      shuffleMemoryManager.release(memoryGranted);
      logger.debug("Failed to acquire {} bytes of memory", pageSizeBytes);
      return false;
    }
    MemoryBlock newPage = taskMemoryManager.allocatePage(pageSizeBytes);
    dataPages.add(newPage);
    pageCursor = 0;
    currentDataPage = newPage;
    return true;
  }

  /**
   * Allocate new data structures for this map. When calling this outside of the constructor,
   * make sure to keep references to the old data structures so that you can free them.
   *
   * @param capacity the new map capacity
   */
  private void allocate(int capacity) {
    assert (capacity >= 0);
    // The capacity needs to be divisible by 64 so that our bit set can be sized properly
    capacity = Math.max((int) Math.min(MAX_CAPACITY, ByteArrayMethods.nextPowerOf2(capacity)), 64);
    assert (capacity <= MAX_CAPACITY);
    longArray = new LongArray(MemoryBlock.fromLongArray(new long[capacity * 2]));
    bitset = new BitSet(MemoryBlock.fromLongArray(new long[capacity / 64]));

    this.growthThreshold = (int) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   *
   * This method is idempotent and can be called multiple times.
   */
  public void free() {
    updatePeakMemoryUsed();
    longArray = null;
    bitset = null;
    Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
    while (dataPagesIterator.hasNext()) {
      MemoryBlock dataPage = dataPagesIterator.next();
      dataPagesIterator.remove();
      taskMemoryManager.freePage(dataPage);
      shuffleMemoryManager.release(dataPage.size());
    }
    assert(dataPages.isEmpty());
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }

  public ShuffleMemoryManager getShuffleMemoryManager() {
    return shuffleMemoryManager;
  }

  public long getPageSizeBytes() {
    return pageSizeBytes;
  }

  /**
   * Returns the total amount of memory, in bytes, consumed by this map's managed structures.
   */
  public long getTotalMemoryConsumption() {
    long totalDataPagesSize = 0L;
    for (MemoryBlock dataPage : dataPages) {
      totalDataPagesSize += dataPage.size();
    }
    return totalDataPagesSize +
      ((bitset != null) ? bitset.memoryBlock().size() : 0L) +
      ((longArray != null) ? longArray.memoryBlock().size() : 0L);
  }

  private void updatePeakMemoryUsed() {
    long mem = getTotalMemoryConsumption();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
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

  @VisibleForTesting
  public int getNumDataPages() {
    return dataPages.size();
  }

  /**
   * Grows the size of the hash table and re-hash everything.
   */
  @VisibleForTesting
  void growAndRehash() {
    assert(bitset != null);
    assert(longArray != null);

    long resizeStartTime = -1;
    if (enablePerfMetrics) {
      resizeStartTime = System.nanoTime();
    }
    // Store references to the old data structures to be used when we re-hash
    final LongArray oldLongArray = longArray;
    final BitSet oldBitSet = bitset;
    final int oldCapacity = (int) oldBitSet.capacity();

    // Allocate the new data structures
    allocate(Math.min(growthStrategy.nextCapacity(oldCapacity), MAX_CAPACITY));

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

    if (enablePerfMetrics) {
      timeSpentResizingNs += System.nanoTime() - resizeStartTime;
    }
  }
}
