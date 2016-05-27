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

package org.apache.spark.shuffle.sort;

import java.util.Comparator;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.RadixSort;

final class ShuffleInMemorySorter {

  private final Sorter<PackedRecordPointer, LongArray> sorter;
  private static final class SortComparator implements Comparator<PackedRecordPointer> {
    @Override
    public int compare(PackedRecordPointer left, PackedRecordPointer right) {
      int leftId = left.getPartitionId();
      int rightId = right.getPartitionId();
      return leftId < rightId ? -1 : (leftId > rightId ? 1 : 0);
    }
  }
  private static final SortComparator SORT_COMPARATOR = new SortComparator();

  private final MemoryConsumer consumer;

  /**
   * An array of record pointers and partition ids that have been encoded by
   * {@link PackedRecordPointer}. The sort operates on this array instead of directly manipulating
   * records.
   */
  private LongArray array;

  /**
   * Whether to use radix sort for sorting in-memory partition ids. Radix sort is much faster
   * but requires additional memory to be reserved memory as pointers are added.
   */
  private final boolean useRadixSort;

  /**
   * Set to 2x for radix sort to reserve extra memory for sorting, otherwise 1x.
   */
  private final int memoryAllocationFactor;

  /**
   * The position in the pointer array where new records can be inserted.
   */
  private int pos = 0;

  private int initialSize;

  ShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort) {
    this.consumer = consumer;
    assert (initialSize > 0);
    this.initialSize = initialSize;
    this.useRadixSort = useRadixSort;
    this.memoryAllocationFactor = useRadixSort ? 2 : 1;
    this.array = consumer.allocateArray(initialSize);
    this.sorter = new Sorter<>(ShuffleSortDataFormat.INSTANCE);
  }

  public void free() {
    if (array != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  public int numRecords() {
    return pos;
  }

  public void reset() {
    if (consumer != null) {
      consumer.freeArray(array);
      this.array = consumer.allocateArray(initialSize);
    }
    pos = 0;
  }

  public void expandPointerArray(LongArray newArray) {
    assert(newArray.size() > array.size());
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      array.size() * (8 / memoryAllocationFactor)
    );
    consumer.freeArray(array);
    array = newArray;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos < array.size() / memoryAllocationFactor;
  }

  public long getMemoryUsage() {
    return array.size() * 8;
  }

  /**
   * Inserts a record to be sorted.
   *
   * @param recordPointer a pointer to the record, encoded by the task memory manager. Due to
   *                      certain pointer compression techniques used by the sorter, the sort can
   *                      only operate on pointers that point to locations in the first
   *                      {@link PackedRecordPointer#MAXIMUM_PAGE_SIZE_BYTES} bytes of a data page.
   * @param partitionId the partition id, which must be less than or equal to
   *                    {@link PackedRecordPointer#MAXIMUM_PARTITION_ID}.
   */
  public void insertRecord(long recordPointer, int partitionId) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
    pos++;
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   */
  public static final class ShuffleSorterIterator {

    private final LongArray pointerArray;
    private final int limit;
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    ShuffleSorterIterator(int numRecords, LongArray pointerArray, int startingPosition) {
      this.limit = numRecords + startingPosition;
      this.pointerArray = pointerArray;
      this.position = startingPosition;
    }

    public boolean hasNext() {
      return position < limit;
    }

    public void loadNext() {
      packedRecordPointer.set(pointerArray.get(position));
      position++;
    }
  }

  /**
   * Return an iterator over record pointers in sorted order.
   */
  public ShuffleSorterIterator getSortedIterator() {
    int offset = 0;
    if (useRadixSort) {
      offset = RadixSort.sort(
        array, pos,
        PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
        PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
    } else {
      sorter.sort(array, 0, pos, SORT_COMPARATOR);
    }
    return new ShuffleSorterIterator(pos, array, offset);
  }
}
