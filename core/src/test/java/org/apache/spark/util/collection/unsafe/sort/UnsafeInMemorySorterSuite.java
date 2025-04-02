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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.spark.unsafe.array.LongArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.memory.TestMemoryConsumer;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.internal.config.package$;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class UnsafeInMemorySorterSuite {

  protected boolean shouldUseRadixSort() { return false; }

  private static String getStringFromDataPage(Object baseObject, long baseOffset, int length) {
    final byte[] strBytes = new byte[length];
    Platform.copyMemory(baseObject, baseOffset, strBytes, Platform.BYTE_ARRAY_OFFSET, length);
    return new String(strBytes, StandardCharsets.UTF_8);
  }

  @Test
  public void testSortingEmptyInput() {
    final TaskMemoryManager memoryManager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
    final TestMemoryConsumer consumer = new TestMemoryConsumer(memoryManager);
    final UnsafeInMemorySorter sorter = new UnsafeInMemorySorter(consumer,
      memoryManager,
      mock(RecordComparator.class),
      mock(PrefixComparator.class),
      100,
      shouldUseRadixSort());
    final UnsafeSorterIterator iter = sorter.getSortedIterator();
    Assertions.assertFalse(iter.hasNext());
  }

  @Test
  public void testSortingOnlyByIntegerPrefix() throws Exception {
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
    final TaskMemoryManager memoryManager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
    final TestMemoryConsumer consumer = new TestMemoryConsumer(memoryManager);
    final MemoryBlock dataPage = memoryManager.allocatePage(2048, consumer);
    final Object baseObject = dataPage.getBaseObject();
    // Write the records into the data page:
    long position = dataPage.getBaseOffset();
    for (String str : dataToSort) {
      final byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
      Platform.putInt(baseObject, position, strBytes.length);
      position += 4;
      Platform.copyMemory(
        strBytes, Platform.BYTE_ARRAY_OFFSET, baseObject, position, strBytes.length);
      position += strBytes.length;
    }
    // Since the key fits within the 8-byte prefix, we don't need to do any record comparison, so
    // use a dummy comparator
    final RecordComparator recordComparator = new RecordComparator() {
      @Override
      public int compare(
        Object leftBaseObject,
        long leftBaseOffset,
        int leftBaseLength,
        Object rightBaseObject,
        long rightBaseOffset,
        int rightBaseLength) {
        return 0;
      }
    };
    // Compute key prefixes based on the records' partition ids
    final HashPartitioner hashPartitioner = new HashPartitioner(4);
    // Use integer comparison for comparing prefixes (which are partition ids, in this case)
    final PrefixComparator prefixComparator = PrefixComparators.LONG;
    UnsafeInMemorySorter sorter = new UnsafeInMemorySorter(consumer, memoryManager,
      recordComparator, prefixComparator, dataToSort.length, shouldUseRadixSort());
    // Given a page of records, insert those records into the sorter one-by-one:
    position = dataPage.getBaseOffset();
    for (int i = 0; i < dataToSort.length; i++) {
      if (!sorter.hasSpaceForAnotherRecord()) {
        sorter.expandPointerArray(
          consumer.allocateArray(sorter.getMemoryUsage() / 8 * 2));
      }
      // position now points to the start of a record (which holds its length).
      final int recordLength = Platform.getInt(baseObject, position);
      final long address = memoryManager.encodePageNumberAndOffset(dataPage, position);
      final String str = getStringFromDataPage(baseObject, position + 4, recordLength);
      final int partitionId = hashPartitioner.getPartition(str);
      sorter.insertRecord(address, partitionId, false);
      position += 4 + recordLength;
    }
    final UnsafeSorterIterator iter = sorter.getSortedIterator();
    int iterLength = 0;
    long prevPrefix = -1;
    while (iter.hasNext()) {
      iter.loadNext();
      final String str =
        getStringFromDataPage(iter.getBaseObject(), iter.getBaseOffset(), iter.getRecordLength());
      final long keyPrefix = iter.getKeyPrefix();
      assertTrue(Arrays.asList(dataToSort).contains(str));
      assertTrue(keyPrefix >= prevPrefix);
      prevPrefix = keyPrefix;
      iterLength++;
    }
    assertEquals(dataToSort.length, iterLength);
  }

  @Test
  public void testNoOOMDuringReset() {
    final SparkConf sparkConf = new SparkConf();
    sparkConf.set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false);

    final TestMemoryManager testMemoryManager =
            new TestMemoryManager(sparkConf);
    final TaskMemoryManager memoryManager = new TaskMemoryManager(
            testMemoryManager, 0);
    final TestMemoryConsumer consumer = new TestMemoryConsumer(memoryManager);

    // Use integer comparison for comparing prefixes (which are partition ids, in this case)
    final PrefixComparator prefixComparator = PrefixComparators.LONG;
    final RecordComparator recordComparator = new RecordComparator() {
      @Override
      public int compare(
              Object leftBaseObject,
              long leftBaseOffset,
              int leftBaseLength,
              Object rightBaseObject,
              long rightBaseOffset,
              int rightBaseLength) {
        return 0;
      }
    };
    UnsafeInMemorySorter sorter = new UnsafeInMemorySorter(consumer, memoryManager,
            recordComparator, prefixComparator, 100, shouldUseRadixSort());

    // Ensure that the sorter does not OOM while freeing its memory.
    testMemoryManager.markconsequentOOM(Integer.MAX_VALUE);
    sorter.freeMemory();
    testMemoryManager.resetConsequentOOM();
    Assertions.assertFalse(sorter.hasSpaceForAnotherRecord());

    // Get the sorter in an usable state again by allocating a new pointer array.
    LongArray array = consumer.allocateArray(1000);
    sorter.expandPointerArray(array);

    // Ensure that it is safe to call freeMemory() multiple times.
    testMemoryManager.markconsequentOOM(Integer.MAX_VALUE);
    sorter.freeMemory();
    sorter.freeMemory();
    testMemoryManager.resetConsequentOOM();
    Assertions.assertFalse(sorter.hasSpaceForAnotherRecord());

    assertEquals(0L, memoryManager.cleanUpAllAllocatedMemory());
  }

}
