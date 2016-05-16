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

import org.apache.avro.reflect.Nullable;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.Sorter;

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

  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;
  @Nullable
  private final Sorter<RecordPointerAndKeyPrefix, LongArray> sorter;
  @Nullable
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * If non-null, specifies the radix sort parameters and that radix sort will be used.
   */
  @Nullable
  private final PrefixComparators.RadixSortSupport radixSortSupport;

  /**
   * Set to 2x for radix sort to reserve extra memory for sorting, otherwise 1x.
   */
  private final int memoryAllocationFactor;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   */
  private LongArray array;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pos = 0;

  private long initialSize;

  private long totalSortTimeNanos = 0L;

  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator,
    int initialSize,
    boolean canUseRadixSort) {
    this(consumer, memoryManager, recordComparator, prefixComparator,
      consumer.allocateArray(initialSize * 2), canUseRadixSort);
  }

  public UnsafeInMemorySorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array,
      boolean canUseRadixSort) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.initialSize = array.size();
    if (recordComparator != null) {
      this.sorter = new Sorter<>(UnsafeSortDataFormat.INSTANCE);
      this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
      if (canUseRadixSort && prefixComparator instanceof PrefixComparators.RadixSortSupport) {
        this.radixSortSupport = (PrefixComparators.RadixSortSupport)prefixComparator;
      } else {
        this.radixSortSupport = null;
      }
    } else {
      this.sorter = null;
      this.sortComparator = null;
      this.radixSortSupport = null;
    }
    this.memoryAllocationFactor = this.radixSortSupport != null ? 2 : 1;
    this.array = array;
  }

  /**
   * Free the memory used by pointer array.
   */
  public void free() {
    if (consumer != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  public void reset() {
    if (consumer != null) {
      consumer.freeArray(array);
      this.array = consumer.allocateArray(initialSize);
    }
    pos = 0;
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 2;
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }

  public long getMemoryUsage() {
    return array.size() * 8;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos + 1 < (array.size() / memoryAllocationFactor);
  }

  public void expandPointerArray(LongArray newArray) {
    if (newArray.size() < array.size()) {
      throw new OutOfMemoryError("Not enough memory to grow pointer array");
    }
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      array.size() * (8 / memoryAllocationFactor));
    consumer.freeArray(array);
    array = newArray;
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
      throw new IllegalStateException("There is no space for new record");
    }
    array.set(pos, recordPointer);
    pos++;
    array.set(pos, keyPrefix);
    pos++;
  }

  public final class SortedIterator extends UnsafeSorterIterator implements Cloneable {

    private final int numRecords;
    private int position;
    private int offset;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;

    private SortedIterator(int numRecords, int offset) {
      this.numRecords = numRecords;
      this.position = 0;
      this.offset = offset;
    }

    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords, offset);
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      return iter;
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      return position / 2 < numRecords;
    }

    @Override
    public void loadNext() {
      // This pointer points to a 4-byte record length, followed by the record's bytes
      final long recordPointer = array.get(offset + position);
      baseObject = memoryManager.getPage(recordPointer);
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + 4;  // Skip over record length
      recordLength = Platform.getInt(baseObject, baseOffset - 4);
      keyPrefix = array.get(offset + position + 1);
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
    int offset = 0;
    long start = System.nanoTime();
    if (sorter != null) {
      if (this.radixSortSupport != null) {
        // TODO(ekl) we should handle NULL values before radix sort for efficiency, since they
        // force a full-width sort (and we cannot radix-sort nullable long fields at all).
        offset = RadixSort.sortKeyPrefixArray(
          array, pos / 2, 0, 7, radixSortSupport.sortDescending(), radixSortSupport.sortSigned());
      } else {
        sorter.sort(array, 0, pos / 2, sortComparator);
      }
    }
    totalSortTimeNanos += System.nanoTime() - start;
    return new SortedIterator(pos / 2, offset);
  }
}
