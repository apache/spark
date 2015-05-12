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

package org.apache.spark.shuffle.unsafe;

import java.util.Comparator;

import org.apache.spark.util.collection.Sorter;

final class UnsafeShuffleInMemorySorter {

  private final Sorter<PackedRecordPointer, long[]> sorter;
  private static final class SortComparator implements Comparator<PackedRecordPointer> {
    @Override
    public int compare(PackedRecordPointer left, PackedRecordPointer right) {
      return left.getPartitionId() - right.getPartitionId();
    }
  }
  private static final SortComparator SORT_COMPARATOR = new SortComparator();

  private long[] sortBuffer;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int sortBufferInsertPosition = 0;

  public UnsafeShuffleInMemorySorter(int initialSize) {
    assert (initialSize > 0);
    this.sortBuffer = new long[initialSize];
    this.sorter = new Sorter<PackedRecordPointer, long[]>(UnsafeShuffleSortDataFormat.INSTANCE);
  }

  public void expandSortBuffer() {
    final long[] oldBuffer = sortBuffer;
    // Guard against overflow:
    final int newLength = oldBuffer.length * 2 > 0 ? (oldBuffer.length * 2) : Integer.MAX_VALUE;
    sortBuffer = new long[newLength];
    System.arraycopy(oldBuffer, 0, sortBuffer, 0, oldBuffer.length);
  }

  public boolean hasSpaceForAnotherRecord() {
    return sortBufferInsertPosition + 1 < sortBuffer.length;
  }

  public long getMemoryUsage() {
    return sortBuffer.length * 8L;
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
      if (sortBuffer.length == Integer.MAX_VALUE) {
        throw new IllegalStateException("Sort buffer has reached maximum size");
      } else {
        expandSortBuffer();
      }
    }
    sortBuffer[sortBufferInsertPosition] =
        PackedRecordPointer.packPointer(recordPointer, partitionId);
    sortBufferInsertPosition++;
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   */
  public static final class UnsafeShuffleSorterIterator {

    private final long[] sortBuffer;
    private final int numRecords;
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    public UnsafeShuffleSorterIterator(int numRecords, long[] sortBuffer) {
      this.numRecords = numRecords;
      this.sortBuffer = sortBuffer;
    }

    public boolean hasNext() {
      return position < numRecords;
    }

    public void loadNext() {
      packedRecordPointer.set(sortBuffer[position]);
      position++;
    }
  }

  /**
   * Return an iterator over record pointers in sorted order.
   */
  public UnsafeShuffleSorterIterator getSortedIterator() {
    sorter.sort(sortBuffer, 0, sortBufferInsertPosition, SORT_COMPARATOR);
    return new UnsafeShuffleSorterIterator(sortBufferInsertPosition, sortBuffer);
  }
}
