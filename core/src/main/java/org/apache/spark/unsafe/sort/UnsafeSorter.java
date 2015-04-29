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

package org.apache.spark.unsafe.sort;

import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import static org.apache.spark.unsafe.sort.UnsafeSortDataFormat.KeyPointerAndPrefix;

public final class UnsafeSorter {

  public static abstract class RecordComparator {
    public abstract int compare(
      Object leftBaseObject,
      long leftBaseOffset,
      Object rightBaseObject,
      long rightBaseOffset);
  }

  public static abstract class PrefixComputer {
    public abstract long computePrefix(Object baseObject, long baseOffset);
  }

  /**
   * Compares 8-byte key prefixes in prefix sort. Subclasses may implement type-specific comparisons,
   * such as lexicographic comparison for strings.
   */
  public static abstract class PrefixComparator {
    public abstract int compare(long prefix1, long prefix2);
  }

  private final TaskMemoryManager memoryManager;
  private final PrefixComputer prefixComputer;
  private final Sorter<KeyPointerAndPrefix, long[]> sorter;
  private final Comparator<KeyPointerAndPrefix> sortComparator;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   */
  private long[] sortBuffer = new long[1024];

  private int sortBufferInsertPosition = 0;

  private void expandSortBuffer(int newSize) {
    assert (newSize > sortBuffer.length);
    final long[] oldBuffer = sortBuffer;
    sortBuffer = new long[newSize];
    System.arraycopy(oldBuffer, 0, sortBuffer, 0, oldBuffer.length);
  }

  public UnsafeSorter(
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      PrefixComputer prefixComputer,
      final PrefixComparator prefixComparator) {
    this.memoryManager = memoryManager;
    this.prefixComputer = prefixComputer;
    this.sorter =
      new Sorter<KeyPointerAndPrefix, long[]>(UnsafeSortDataFormat.INSTANCE);
    this.sortComparator = new Comparator<KeyPointerAndPrefix>() {
      @Override
      public int compare(KeyPointerAndPrefix left, KeyPointerAndPrefix right) {
        if (left.keyPrefix == right.keyPrefix) {
          final Object leftBaseObject = memoryManager.getPage(left.recordPointer);
          final long leftBaseOffset = memoryManager.getOffsetInPage(left.recordPointer);
          final Object rightBaseObject = memoryManager.getPage(right.recordPointer);
          final long rightBaseOffset = memoryManager.getOffsetInPage(right.recordPointer);
          return recordComparator.compare(
            leftBaseObject, leftBaseOffset, rightBaseObject, rightBaseOffset);
        } else {
          return prefixComparator.compare(left.keyPrefix, right.keyPrefix);
        }
      }
    };
  }

  public void insertRecord(long objectAddress) {
    if (sortBufferInsertPosition + 2 == sortBuffer.length) {
      expandSortBuffer(sortBuffer.length * 2);
    }
    final Object baseObject = memoryManager.getPage(objectAddress);
    final long baseOffset = memoryManager.getOffsetInPage(objectAddress);
    final long keyPrefix = prefixComputer.computePrefix(baseObject, baseOffset);
    sortBuffer[sortBufferInsertPosition] = objectAddress;
    sortBuffer[sortBufferInsertPosition + 1] = keyPrefix;
    sortBufferInsertPosition += 2;
  }

  public Iterator<MemoryLocation> getSortedIterator() {
    final MemoryLocation memoryLocation = new MemoryLocation();
    sorter.sort(sortBuffer, 0, sortBufferInsertPosition, sortComparator);
    return new Iterator<MemoryLocation>() {
      int position = 0;

      @Override
      public boolean hasNext() {
        return position < sortBufferInsertPosition;
      }

      @Override
      public MemoryLocation next() {
        final long address = sortBuffer[position];
        position += 2;
        final Object baseObject = memoryManager.getPage(address);
        final long baseOffset = memoryManager.getOffsetInPage(address);
        memoryLocation.setObjAndOffset(baseObject, baseOffset);
        return memoryLocation;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

}
