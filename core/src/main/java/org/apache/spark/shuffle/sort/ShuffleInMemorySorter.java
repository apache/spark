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

import org.apache.spark.util.collection.Sorter;

final class ShuffleInMemorySorter {

  private final Sorter<PackedRecordPointer, long[]> sorter;
  private static final class SortComparator implements Comparator<PackedRecordPointer> {
    @Override
    public int compare(PackedRecordPointer left, PackedRecordPointer right) {
      return left.getPartitionId() - right.getPartitionId();
    }
  }
  private static final SortComparator SORT_COMPARATOR = new SortComparator();

  /**
   * An array of record pointers and partition ids that have been encoded by
   * {@link PackedRecordPointer}. The sort operates on this array instead of directly manipulating
   * records.
   */
  private long[] array;

  /**
   * The position in the pointer array where new records can be inserted.
   */
  private int pos = 0;

  public ShuffleInMemorySorter(int initialSize) {
    assert (initialSize > 0);
    this.array = new long[initialSize];
    this.sorter = new Sorter<>(ShuffleSortDataFormat.INSTANCE);
  }

  public int numRecords() {
    return pos;
  }

  public void reset() {
    pos = 0;
  }

  private int newLength() {
    // Guard against overflow:
    return array.length <= Integer.MAX_VALUE / 2 ? (array.length * 2) : Integer.MAX_VALUE;
  }

  /**
   * Returns the memory needed to expand
   */
  public long getMemoryToExpand() {
    return ((long) (newLength() - array.length)) * 8;
  }

  public void expandPointerArray() {
    final long[] oldArray = array;
    array = new long[newLength()];
    System.arraycopy(oldArray, 0, array, 0, oldArray.length);
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos < array.length;
  }

  public long getMemoryUsage() {
    return array.length * 8L;
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
      if (array.length == Integer.MAX_VALUE) {
        throw new IllegalStateException("Sort pointer array has reached maximum size");
      } else {
        expandPointerArray();
      }
    }
    array[pos] =
        PackedRecordPointer.packPointer(recordPointer, partitionId);
    pos++;
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   */
  public static final class ShuffleSorterIterator {

    private final long[] pointerArray;
    private final int numRecords;
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    public ShuffleSorterIterator(int numRecords, long[] pointerArray) {
      this.numRecords = numRecords;
      this.pointerArray = pointerArray;
    }

    public boolean hasNext() {
      return position < numRecords;
    }

    public void loadNext() {
      packedRecordPointer.set(pointerArray[position]);
      position++;
    }
  }

  /**
   * Return an iterator over record pointers in sorted order.
   */
  public ShuffleSorterIterator getSortedIterator() {
    sorter.sort(array, 0, pos, SORT_COMPARATOR);
    return new ShuffleSorterIterator(pos, array);
  }
}
