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

import java.io.IOException;

import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;

import static org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES;
import static org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PARTITION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PackedRecordPointerSuite {

  @Test
  public void heap() throws IOException {
    final SparkConf conf = new SparkConf().set("spark.memory.offHeap.enabled", "false");
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryBlock page0 = memoryManager.allocatePage(128, null);
    final MemoryBlock page1 = memoryManager.allocatePage(128, null);
    final long addressInPage1 = memoryManager.encodePageNumberAndOffset(page1,
      page1.getBaseOffset() + 42);
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(addressInPage1, 360));
    assertEquals(360, packedPointer.getPartitionId());
    final long recordPointer = packedPointer.getRecordPointer();
    assertEquals(1, TaskMemoryManager.decodePageNumber(recordPointer));
    assertEquals(page1.getBaseOffset() + 42, memoryManager.getOffsetInPage(recordPointer));
    assertEquals(addressInPage1, recordPointer);
    memoryManager.cleanUpAllAllocatedMemory();
  }

  @Test
  public void offHeap() throws IOException {
    final SparkConf conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "10000");
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryBlock page0 = memoryManager.allocatePage(128, null);
    final MemoryBlock page1 = memoryManager.allocatePage(128, null);
    final long addressInPage1 = memoryManager.encodePageNumberAndOffset(page1,
      page1.getBaseOffset() + 42);
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(addressInPage1, 360));
    assertEquals(360, packedPointer.getPartitionId());
    final long recordPointer = packedPointer.getRecordPointer();
    assertEquals(1, TaskMemoryManager.decodePageNumber(recordPointer));
    assertEquals(page1.getBaseOffset() + 42, memoryManager.getOffsetInPage(recordPointer));
    assertEquals(addressInPage1, recordPointer);
    memoryManager.cleanUpAllAllocatedMemory();
  }

  @Test
  public void maximumPartitionIdCanBeEncoded() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(0, MAXIMUM_PARTITION_ID));
    assertEquals(MAXIMUM_PARTITION_ID, packedPointer.getPartitionId());
  }

  @Test
  public void partitionIdsGreaterThanMaximumPartitionIdWillOverflowOrTriggerError() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    try {
      // Pointers greater than the maximum partition ID will overflow or trigger an assertion error
      packedPointer.set(PackedRecordPointer.packPointer(0, MAXIMUM_PARTITION_ID + 1));
      assertFalse(MAXIMUM_PARTITION_ID  + 1 == packedPointer.getPartitionId());
    } catch (AssertionError e ) {
      // pass
    }
  }

  @Test
  public void maximumOffsetInPageCanBeEncoded() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    long address = TaskMemoryManager.encodePageNumberAndOffset(0, MAXIMUM_PAGE_SIZE_BYTES - 1);
    packedPointer.set(PackedRecordPointer.packPointer(address, 0));
    assertEquals(address, packedPointer.getRecordPointer());
  }

  @Test
  public void offsetsPastMaxOffsetInPageWillOverflow() {
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    long address = TaskMemoryManager.encodePageNumberAndOffset(0, MAXIMUM_PAGE_SIZE_BYTES);
    packedPointer.set(PackedRecordPointer.packPointer(address, 0));
    assertEquals(0, packedPointer.getRecordPointer());
  }
}
