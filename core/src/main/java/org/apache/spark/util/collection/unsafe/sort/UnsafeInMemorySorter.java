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

package org.apache.spark.util.collection.unsafe.sort;

import java.util.Comparator;
import java.io.IOException;

import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

/**
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 */
public final class UnsafeInMemorySorter {

  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    private final RecordComparator recordComparator;
    private final PrefixComparator prefixComparator;
    private final TaskMemoryManager memoryManager;

    SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.memoryManager = memoryManager;
    }

    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix);
      if (prefixComparisonResult == 0) {
        final Object baseObject1 = memoryManager.getPage(r1.recordPointer);
        final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + 4; // skip length
        final Object baseObject2 = memoryManager.getPage(r2.recordPointer);
        final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + 4; // skip length
        return recordComparator.compare(baseObject1, baseOffset1, baseObject2, baseOffset2);
      } else {
        return prefixComparisonResult;
      }
    }
  }

  private final TaskMemoryManager memoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
  private final Sorter<RecordPointerAndKeyPrefix, LongArray> sorter;
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   */
  private LongArray pointerArray;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pointerArrayInsertPosition = 0;

  public UnsafeInMemorySorter(
      final TaskMemoryManager memoryManager,
      final ShuffleMemoryManager shuffleMemoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      int initialSize) throws IOException {
    assert (initialSize > 0);
    this.memoryManager = memoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.pointerArray = allocateLongArray(initialSize);
    this.sorter = new Sorter<>(new UnsafeSortDataFormat(memoryManager, shuffleMemoryManager));
    this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
  }

  private LongArray allocateLongArray(int size) throws IOException {
    MemoryBlock page = allocateMemoryBlock(size * 2);
    return new LongArray(page);
  }

  private MemoryBlock allocateMemoryBlock(int size) throws IOException {
    long memoryToAcquire = size * LongArray.WIDTH;
    final long memoryGranted = shuffleMemoryManager.tryToAcquire(memoryToAcquire);
    if (memoryGranted != memoryToAcquire) {
      shuffleMemoryManager.release(memoryGranted);
      throw new IOException("Unable to acquire " + memoryToAcquire + " bytes of memory");
    }
    MemoryBlock page = memoryManager.allocatePage(memoryToAcquire);
    return page;
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pointerArrayInsertPosition / 2;
  }

  public long getMemoryUsage() {
    return pointerArray.memoryBlock().size();
  }

  static long getMemoryRequirementsForPointerArray(long numEntries) {
    return numEntries * 2L * 8L;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pointerArrayInsertPosition + 2 < pointerArray.size();
  }

  public void releaseMemory() {
    releasedPointerArray(pointerArray);
  }

  private void releasedPointerArray(LongArray array) {
    if (array != null) {
      memoryManager.freePage(array.memoryBlock());
      shuffleMemoryManager.release(array.memoryBlock().size());
      array = null;
    }
  }

  public void expandPointerArray() throws IOException {
    final LongArray oldArray = pointerArray;
    // Guard against overflow:
    final int newSize = oldArray.size() * 2 > 0 ? (int)(oldArray.size() * 2) : Integer.MAX_VALUE;
    pointerArray = allocateLongArray(newSize / 2);
    pointerArray.copyFrom(oldArray);
    releasedPointerArray(oldArray);
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a 4-byte integer, followed by the record's bytes.
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix a user-defined key prefix
   */
  public void insertRecord(long recordPointer, long keyPrefix) throws IOException {
    if (!hasSpaceForAnotherRecord()) {
      expandPointerArray();
    }
    pointerArray.set(pointerArrayInsertPosition, recordPointer);
    pointerArrayInsertPosition++;
    pointerArray.set(pointerArrayInsertPosition, keyPrefix);
    pointerArrayInsertPosition++;
  }

  public static final class SortedIterator extends UnsafeSorterIterator {

    private final TaskMemoryManager memoryManager;
    private final int sortBufferInsertPosition;
    private final LongArray sortBuffer;
    private int position = 0;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;

    private SortedIterator(
        TaskMemoryManager memoryManager,
        int sortBufferInsertPosition,
        LongArray sortBuffer) {
      this.memoryManager = memoryManager;
      this.sortBufferInsertPosition = sortBufferInsertPosition;
      this.sortBuffer = sortBuffer;
    }

    @Override
    public boolean hasNext() {
      return position < sortBufferInsertPosition;
    }

    @Override
    public void loadNext() {
      // This pointer points to a 4-byte record length, followed by the record's bytes
      final long recordPointer = sortBuffer.get(position);
      baseObject = memoryManager.getPage(recordPointer);
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + 4;  // Skip over record length
      recordLength = Platform.getInt(baseObject, baseOffset - 4);
      keyPrefix = sortBuffer.get(position + 1);
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
  public SortedIterator getSortedIterator() {
    sorter.sort(pointerArray, 0, pointerArrayInsertPosition / 2, sortComparator);
    return new SortedIterator(memoryManager, pointerArrayInsertPosition, pointerArray);
  }
}
