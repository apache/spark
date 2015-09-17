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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.Sorter;
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
  private final Sorter<RecordPointerAndKeyPrefix, long[]> sorter;
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   */
  private long[] pointerArray;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pointerArrayInsertPosition = 0;

  public UnsafeInMemorySorter(
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      int initialSize) {
    assert (initialSize > 0);
    this.pointerArray = new long[initialSize * 2];
    this.memoryManager = memoryManager;
    this.sorter = new Sorter<>(UnsafeSortDataFormat.INSTANCE);
    this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pointerArrayInsertPosition / 2;
  }

  public long getMemoryUsage() {
    return pointerArray.length * 8L;
  }

  static long getMemoryRequirementsForPointerArray(long numEntries) {
    return numEntries * 2L * 8L;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pointerArrayInsertPosition + 2 < pointerArray.length;
  }

  public void expandPointerArray() {
    final long[] oldArray = pointerArray;
    // Guard against overflow:
    final int newLength = oldArray.length * 2 > 0 ? (oldArray.length * 2) : Integer.MAX_VALUE;
    pointerArray = new long[newLength];
    System.arraycopy(oldArray, 0, pointerArray, 0, oldArray.length);
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a 4-byte integer, followed by the record's bytes.
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix a user-defined key prefix
   */
  public void insertRecord(long recordPointer, long keyPrefix) {
    if (!hasSpaceForAnotherRecord()) {
      expandPointerArray();
    }
    pointerArray[pointerArrayInsertPosition] = recordPointer;
    pointerArrayInsertPosition++;
    pointerArray[pointerArrayInsertPosition] = keyPrefix;
    pointerArrayInsertPosition++;
  }

  public static final class SortedIterator extends UnsafeSorterIterator {

    private final TaskMemoryManager memoryManager;
    private final int sortBufferInsertPosition;
    private final long[] sortBuffer;
    private int position = 0;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;

    private SortedIterator(
        TaskMemoryManager memoryManager,
        int sortBufferInsertPosition,
        long[] sortBuffer) {
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
      final long recordPointer = sortBuffer[position];
      baseObject = memoryManager.getPage(recordPointer);
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + 4;  // Skip over record length
      recordLength = Platform.getInt(baseObject, baseOffset - 4);
      keyPrefix = sortBuffer[position + 1];
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
