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

package org.apache.spark.memory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.internal.config.package$;

public class TaskMemoryManagerSuite {

  @Test
  public void leakedPageMemoryIsDetected() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new UnifiedMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false),
        Long.MAX_VALUE,
        Long.MAX_VALUE / 2,
        1),
      0);
    final MemoryConsumer c = new TestMemoryConsumer(manager);
    manager.allocatePage(4096, c);  // leak memory
    Assertions.assertEquals(4096, manager.getMemoryConsumptionForThisTask());
    Assertions.assertEquals(4096, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void encodePageNumberAndOffsetOffHeap() {
    final SparkConf conf = new SparkConf()
      .set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), true)
      .set(package$.MODULE$.MEMORY_OFFHEAP_SIZE(), 1000L);
    final TaskMemoryManager manager = new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.OFF_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    // In off-heap mode, an offset is an absolute address that may require more than 51 bits to
    // encode. This test exercises that corner-case:
    final long offset = ((1L << TaskMemoryManager.OFFSET_BITS) + 10);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, offset);
    Assertions.assertNull(manager.getPage(encodedAddress));
    Assertions.assertEquals(offset, manager.getOffsetInPage(encodedAddress));
    manager.freePage(dataPage, c);
  }

  @Test
  public void encodePageNumberAndOffsetOnHeap() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, 64);
    Assertions.assertEquals(dataPage.getBaseObject(), manager.getPage(encodedAddress));
    Assertions.assertEquals(64, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void freeingPageSetsPageNumberToSpecialConstant() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    c.freePage(dataPage);
    Assertions.assertEquals(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER, dataPage.pageNumber);
  }

  @Test
  public void freeingPageDirectlyInAllocatorTriggersAssertionError() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = manager.allocatePage(256, c);
    Assertions.assertThrows(AssertionError.class, () -> MemoryAllocator.HEAP.free(dataPage));
  }

  @Test
  public void callingFreePageOnDirectlyAllocatedPageTriggersAssertionError() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(
        new SparkConf().set(package$.MODULE$.MEMORY_OFFHEAP_ENABLED(), false)), 0);
    final MemoryConsumer c = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    final MemoryBlock dataPage = MemoryAllocator.HEAP.allocate(256);
    Assertions.assertThrows(AssertionError.class, () -> manager.freePage(dataPage, c));
  }

  @Test
  public void cooperativeSpilling() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    c1.use(100);
    Assertions.assertEquals(100, c1.getUsed());
    c2.use(100);
    Assertions.assertEquals(100, c2.getUsed());
    Assertions.assertEquals(0, c1.getUsed());  // spilled
    c1.use(100);
    Assertions.assertEquals(100, c1.getUsed());
    Assertions.assertEquals(0, c2.getUsed());  // spilled

    c1.use(50);
    Assertions.assertEquals(50, c1.getUsed());  // spilled
    Assertions.assertEquals(0, c2.getUsed());
    c2.use(50);
    Assertions.assertEquals(50, c1.getUsed());
    Assertions.assertEquals(50, c2.getUsed());

    c1.use(100);
    Assertions.assertEquals(100, c1.getUsed());
    Assertions.assertEquals(0, c2.getUsed());  // spilled

    c1.free(20);
    Assertions.assertEquals(80, c1.getUsed());
    c2.use(10);
    Assertions.assertEquals(80, c1.getUsed());
    Assertions.assertEquals(10, c2.getUsed());
    c2.use(100);
    Assertions.assertEquals(100, c2.getUsed());
    Assertions.assertEquals(0, c1.getUsed());  // spilled

    c1.free(0);
    c2.free(100);
    Assertions.assertEquals(0, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void cooperativeSpilling2() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c3 = new TestMemoryConsumer(manager);

    c1.use(20);
    Assertions.assertEquals(20, c1.getUsed());
    c2.use(80);
    Assertions.assertEquals(80, c2.getUsed());
    c3.use(80);
    Assertions.assertEquals(20, c1.getUsed());  // c1: not spilled
    Assertions.assertEquals(0, c2.getUsed());   // c2: spilled as it has required size of memory
    Assertions.assertEquals(80, c3.getUsed());

    c2.use(80);
    Assertions.assertEquals(20, c1.getUsed());  // c1: not spilled
    Assertions.assertEquals(0, c3.getUsed());   // c3: spilled as it has required size of memory
    Assertions.assertEquals(80, c2.getUsed());

    c3.use(10);
    Assertions.assertEquals(0, c1.getUsed());   // c1: spilled as it has required size of memory
    Assertions
      .assertEquals(80, c2.getUsed());  // c2: not spilled as spilling c1 already satisfies c3
    Assertions.assertEquals(10, c3.getUsed());

    c1.free(0);
    c2.free(80);
    c3.free(10);
    Assertions.assertEquals(0, manager.cleanUpAllAllocatedMemory());
  }


  @Test
  public void selfSpillIsLowestPriorities() {
    // Test that requesting memory consumer (a "self-spill") is chosen last to spill.
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c3 = new TestMemoryConsumer(manager);

    // Self-spill is the lowest priority: c2 and c3 are spilled first even though they have less
    // memory.
    c1.use(50);
    c2.use(40);
    c3.use(10);
    c1.use(50);
    Assertions.assertEquals(100, c1.getUsed());
    Assertions.assertEquals(0, c2.getUsed());
    Assertions.assertEquals(0, c3.getUsed());
    // Force a self-spill.
    c1.use(50);
    Assertions.assertEquals(50, c1.getUsed());
    // Force a self-spill after c2 is spilled.
    c2.use(10);
    c1.use(60);
    Assertions.assertEquals(60, c1.getUsed());
    Assertions.assertEquals(0, c2.getUsed());

    c1.free(c1.getUsed());

    // Redo a similar scenario but with a different memory requester.
    c1.use(50);
    c2.use(40);
    c3.use(10);
    c3.use(50);
    Assertions.assertEquals(0, c1.getUsed());
    Assertions.assertEquals(40, c2.getUsed());
    Assertions.assertEquals(60, c3.getUsed());
  }

  @Test
  public void prefersSmallestBigEnoughAllocation() {
    // Test that the smallest consumer with at least the requested size is chosen to spill.
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c3 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c4 = new TestMemoryConsumer(manager);


    c1.use(50);
    c2.use(40);
    c3.use(10);
    c4.use(5);
    Assertions.assertEquals(50, c1.getUsed());
    Assertions.assertEquals(40, c2.getUsed());
    Assertions.assertEquals(0, c3.getUsed());
    Assertions.assertEquals(5, c4.getUsed());

    // Allocate 45. 5 is unused and 40 will come from c2.
    c3.use(45);
    Assertions.assertEquals(50, c1.getUsed());
    Assertions.assertEquals(0, c2.getUsed());
    Assertions.assertEquals(45, c3.getUsed());
    Assertions.assertEquals(5, c4.getUsed());

    // Allocate 51. 50 is taken from c1, then c4 is the best fit to get 1 more byte.
    c2.use(51);
    Assertions.assertEquals(0, c1.getUsed());
    Assertions.assertEquals(51, c2.getUsed());
    Assertions.assertEquals(45, c3.getUsed());
    Assertions.assertEquals(0, c4.getUsed());
  }

  @Test
  public void shouldNotForceSpillingInDifferentModes() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager, MemoryMode.ON_HEAP);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager, MemoryMode.OFF_HEAP);
    c1.use(80);
    Assertions.assertEquals(80, c1.getUsed());
    c2.use(80);
    Assertions.assertEquals(20, c2.getUsed());  // not enough memory
    Assertions.assertEquals(80, c1.getUsed());  // not spilled

    c2.use(10);
    Assertions.assertEquals(10, c2.getUsed());  // spilled
    Assertions.assertEquals(80, c1.getUsed());  // not spilled
  }

  @Test
  public void offHeapConfigurationBackwardsCompatibility() {
    // Tests backwards-compatibility with the old `spark.unsafe.offHeap` configuration, which
    // was deprecated in Spark 1.6 and replaced by `spark.memory.offHeap.enabled` (see SPARK-12251).
    final SparkConf conf = new SparkConf()
      .set("spark.unsafe.offHeap", "true")
      .set(package$.MODULE$.MEMORY_OFFHEAP_SIZE(), 1000L);
    final TaskMemoryManager manager = new TaskMemoryManager(new TestMemoryManager(conf), 0);
    Assertions.assertSame(MemoryMode.OFF_HEAP, manager.tungstenMemoryMode);
  }

}
