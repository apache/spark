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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TestMemoryConsumer;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;

public class ShuffleInMemorySorterSuite {

  protected boolean shouldUseRadixSort() { return false; }

  final TestMemoryManager memoryManager =
    new TestMemoryManager(new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false));
  final TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 0);
  final TestMemoryConsumer consumer = new TestMemoryConsumer(taskMemoryManager);

  private static String getStringFromDataPage(Object baseObject, long baseOffset, int strLength) {
    final byte[] strBytes = new byte[strLength];
    Platform.copyMemory(baseObject, baseOffset, strBytes, Platform.BYTE_ARRAY_OFFSET, strLength);
    return new String(strBytes, StandardCharsets.UTF_8);
  }

  @Test
  public void testSortingEmptyInput() {
    final ShuffleInMemorySorter sorter = new ShuffleInMemorySorter(
      consumer, 100, shouldUseRadixSort());
    final ShuffleInMemorySorter.ShuffleSorterIterator iter = sorter.getSortedIterator();
    Assertions.assertFalse(iter.hasNext());
  }

  @Test
  public void testBasicSorting() {
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
    final SparkConf conf = new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false);
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryConsumer c = new TestMemoryConsumer(memoryManager);
    final MemoryBlock dataPage = memoryManager.allocatePage(2048, c);
    final Object baseObject = dataPage.getBaseObject();
    final ShuffleInMemorySorter sorter = new ShuffleInMemorySorter(
      consumer, 4, shouldUseRadixSort());
    final HashPartitioner hashPartitioner = new HashPartitioner(4);

    // Write the records into the data page and store pointers into the sorter
    long position = dataPage.getBaseOffset();
    for (String str : dataToSort) {
      if (!sorter.hasSpaceForAnotherRecord()) {
        sorter.expandPointerArray(
          consumer.allocateArray(sorter.getMemoryUsage() / 8 * 2));
      }
      final long recordAddress = memoryManager.encodePageNumberAndOffset(dataPage, position);
      final byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
      Platform.putInt(baseObject, position, strBytes.length);
      position += 4;
      Platform.copyMemory(
        strBytes, Platform.BYTE_ARRAY_OFFSET, baseObject, position, strBytes.length);
      position += strBytes.length;
      sorter.insertRecord(recordAddress, hashPartitioner.getPartition(str));
    }

    // Sort the records
    final ShuffleInMemorySorter.ShuffleSorterIterator iter = sorter.getSortedIterator();
    int prevPartitionId = -1;
    Arrays.sort(dataToSort);
    for (int i = 0; i < dataToSort.length; i++) {
      Assertions.assertTrue(iter.hasNext());
      iter.loadNext();
      final int partitionId = iter.packedRecordPointer.getPartitionId();
      Assertions.assertTrue(partitionId >= 0 && partitionId <= 3);
      Assertions.assertTrue(partitionId >= prevPartitionId,
        "Partition id " + partitionId + " should be >= prev id " + prevPartitionId);
      final long recordAddress = iter.packedRecordPointer.getRecordPointer();
      final int recordLength = Platform.getInt(
        memoryManager.getPage(recordAddress), memoryManager.getOffsetInPage(recordAddress));
      final String str = getStringFromDataPage(
        memoryManager.getPage(recordAddress),
        memoryManager.getOffsetInPage(recordAddress) + 4, // skip over record length
        recordLength);
      Assertions.assertTrue(Arrays.binarySearch(dataToSort, str) != -1);
    }
    Assertions.assertFalse(iter.hasNext());
  }

  @Test
  public void testSortingManyNumbers() {
    ShuffleInMemorySorter sorter = new ShuffleInMemorySorter(consumer, 4, shouldUseRadixSort());
    int[] numbersToSort = new int[128000];
    Random random = new Random(16);
    for (int i = 0; i < numbersToSort.length; i++) {
      if (!sorter.hasSpaceForAnotherRecord()) {
        sorter.expandPointerArray(consumer.allocateArray(sorter.getMemoryUsage() / 8 * 2));
      }
      numbersToSort[i] = random.nextInt(PackedRecordPointer.MAXIMUM_PARTITION_ID + 1);
      sorter.insertRecord(0, numbersToSort[i]);
    }
    Arrays.sort(numbersToSort);
    int[] sorterResult = new int[numbersToSort.length];
    ShuffleInMemorySorter.ShuffleSorterIterator iter = sorter.getSortedIterator();
    int j = 0;
    while (iter.hasNext()) {
      iter.loadNext();
      sorterResult[j] = iter.packedRecordPointer.getPartitionId();
      j += 1;
    }
    Assertions.assertArrayEquals(numbersToSort, sorterResult);
  }
}
