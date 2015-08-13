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

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

public class UnsafeShuffleInMemorySorterSuite {

  private static String getStringFromDataPage(Object baseObject, long baseOffset, int strLength) {
    final byte[] strBytes = new byte[strLength];
    Platform.copyMemory(baseObject, baseOffset, strBytes, Platform.BYTE_ARRAY_OFFSET, strLength);
    return new String(strBytes);
  }

  @Test
  public void testSortingEmptyInput() {
    final UnsafeShuffleInMemorySorter sorter = new UnsafeShuffleInMemorySorter(100);
    final UnsafeShuffleInMemorySorter.UnsafeShuffleSorterIterator iter = sorter.getSortedIterator();
    assert(!iter.hasNext());
  }

  @Test
  public void testBasicSorting() throws Exception {
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
    final UnsafeShuffleInMemorySorter sorter = new UnsafeShuffleInMemorySorter(4);
    final HashPartitioner hashPartitioner = new HashPartitioner(4);

    // Write the records into the data page and store pointers into the sorter
    long position = dataPage.getBaseOffset();
    for (String str : dataToSort) {
      final long recordAddress = memoryManager.encodePageNumberAndOffset(dataPage, position);
      final byte[] strBytes = str.getBytes("utf-8");
      Platform.putInt(baseObject, position, strBytes.length);
      position += 4;
      Platform.copyMemory(
        strBytes, Platform.BYTE_ARRAY_OFFSET, baseObject, position, strBytes.length);
      position += strBytes.length;
      sorter.insertRecord(recordAddress, hashPartitioner.getPartition(str));
    }

    // Sort the records
    final UnsafeShuffleInMemorySorter.UnsafeShuffleSorterIterator iter = sorter.getSortedIterator();
    int prevPartitionId = -1;
    Arrays.sort(dataToSort);
    for (int i = 0; i < dataToSort.length; i++) {
      Assert.assertTrue(iter.hasNext());
      iter.loadNext();
      final int partitionId = iter.packedRecordPointer.getPartitionId();
      Assert.assertTrue(partitionId >= 0 && partitionId <= 3);
      Assert.assertTrue("Partition id " + partitionId + " should be >= prev id " + prevPartitionId,
        partitionId >= prevPartitionId);
      final long recordAddress = iter.packedRecordPointer.getRecordPointer();
      final int recordLength = Platform.getInt(
        memoryManager.getPage(recordAddress), memoryManager.getOffsetInPage(recordAddress));
      final String str = getStringFromDataPage(
        memoryManager.getPage(recordAddress),
        memoryManager.getOffsetInPage(recordAddress) + 4, // skip over record length
        recordLength);
      Assert.assertTrue(Arrays.binarySearch(dataToSort, str) != -1);
    }
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testSortingManyNumbers() throws Exception {
    UnsafeShuffleInMemorySorter sorter = new UnsafeShuffleInMemorySorter(4);
    int[] numbersToSort = new int[128000];
    Random random = new Random(16);
    for (int i = 0; i < numbersToSort.length; i++) {
      numbersToSort[i] = random.nextInt(PackedRecordPointer.MAXIMUM_PARTITION_ID + 1);
      sorter.insertRecord(0, numbersToSort[i]);
    }
    Arrays.sort(numbersToSort);
    int[] sorterResult = new int[numbersToSort.length];
    UnsafeShuffleInMemorySorter.UnsafeShuffleSorterIterator iter = sorter.getSortedIterator();
    int j = 0;
    while (iter.hasNext()) {
      iter.loadNext();
      sorterResult[j] = iter.packedRecordPointer.getPartitionId();
      j += 1;
    }
    Assert.assertArrayEquals(numbersToSort, sorterResult);
  }
}
