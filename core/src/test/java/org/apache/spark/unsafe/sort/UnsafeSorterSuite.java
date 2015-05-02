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

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.*;

import org.apache.spark.HashPartitioner;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

public class UnsafeSorterSuite {

  private static String getStringFromDataPage(Object baseObject, long baseOffset) {
    final int strLength = (int) PlatformDependent.UNSAFE.getLong(baseObject, baseOffset);
    final byte[] strBytes = new byte[strLength];
    PlatformDependent.copyMemory(
      baseObject,
      baseOffset + 8,
      strBytes,
      PlatformDependent.BYTE_ARRAY_OFFSET, strLength);
    return new String(strBytes);
  }

  @Test
  public void testSortingEmptyInput() {
    final UnsafeSorter sorter = new UnsafeSorter(
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP)),
      mock(UnsafeSorter.RecordComparator.class),
      mock(UnsafeSorter.PrefixComparator.class),
      100);
    final Iterator<UnsafeSorter.RecordPointerAndKeyPrefix> iter = sorter.getSortedIterator();
    assert(!iter.hasNext());
  }

  /**
   * Tests the type of sorting that's used in the non-combiner path of sort-based shuffle.
   */
  @Test
  public void testSortingOnlyByPartitionId() throws Exception {
    final String[] dataToSort = new String[] {
      "Boba",
      "Pearls",
      "Tapioca",
      "Taho",
      "Condensed Milk",
      "Jasmine",
      "Milk Tea",
      "Lychee",
      "Mango"
    };
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
    final MemoryBlock dataPage = memoryManager.allocatePage(2048);
    final Object baseObject = dataPage.getBaseObject();
    // Write the records into the data page:
    long position = dataPage.getBaseOffset();
    for (String str : dataToSort) {
      final byte[] strBytes = str.getBytes("utf-8");
      PlatformDependent.UNSAFE.putLong(baseObject, position, strBytes.length);
      position += 8;
      PlatformDependent.copyMemory(
        strBytes,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        baseObject,
        position,
        strBytes.length);
      position += strBytes.length;
    }
    // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
    // use a dummy comparator
    final UnsafeSorter.RecordComparator recordComparator = new UnsafeSorter.RecordComparator() {
      @Override
      public int compare(
          Object leftBaseObject,
          long leftBaseOffset,
          Object rightBaseObject,
          long rightBaseOffset) {
        return 0;
      }
    };
    // Compute key prefixes based on the records' partition ids
    final HashPartitioner hashPartitioner = new HashPartitioner(4);
    // Use integer comparison for comparing prefixes (which are partition ids, in this case)
    final UnsafeSorter.PrefixComparator prefixComparator = new UnsafeSorter.PrefixComparator() {
      @Override
      public int compare(long prefix1, long prefix2) {
        return (int) prefix1 - (int) prefix2;
      }
    };
    final UnsafeSorter sorter = new UnsafeSorter(memoryManager, recordComparator, prefixComparator,
      dataToSort.length);
    // Given a page of records, insert those records into the sorter one-by-one:
    position = dataPage.getBaseOffset();
    for (int i = 0; i < dataToSort.length; i++) {
      // position now points to the start of a record (which holds its length).
      final long recordLength = PlatformDependent.UNSAFE.getLong(baseObject, position);
      final long address = memoryManager.encodePageNumberAndOffset(dataPage, position);
      final String str = getStringFromDataPage(baseObject, position);
      final int partitionId = hashPartitioner.getPartition(str);
      sorter.insertRecord(address, partitionId);
      position += 8 + recordLength;
    }
    final Iterator<UnsafeSorter.RecordPointerAndKeyPrefix> iter = sorter.getSortedIterator();
    int iterLength = 0;
    long prevPrefix = -1;
    Arrays.sort(dataToSort);
    while (iter.hasNext()) {
      final UnsafeSorter.RecordPointerAndKeyPrefix pointerAndPrefix = iter.next();
      final Object recordBaseObject = memoryManager.getPage(pointerAndPrefix.recordPointer);
      final long recordBaseOffset = memoryManager.getOffsetInPage(pointerAndPrefix.recordPointer);
      final String str = getStringFromDataPage(recordBaseObject, recordBaseOffset);
      Assert.assertTrue("String should be valid", Arrays.binarySearch(dataToSort, str) != -1);
      Assert.assertTrue("Prefix " + pointerAndPrefix.keyPrefix + " should be >=  previous prefix " +
        prevPrefix, pointerAndPrefix.keyPrefix >= prevPrefix);
      prevPrefix = pointerAndPrefix.keyPrefix;
      iterLength++;
    }
    Assert.assertEquals(dataToSort.length, iterLength);
  }
}
