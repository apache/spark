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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.unsafe.memory.MemoryBlock;

public class TaskMemoryManagerSuite {

  @Test
  public void leakedPageMemoryIsDetected() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MAX_VALUE,
        Long.MAX_VALUE,
        1),
      0);
    manager.allocatePage(4096, null);  // leak memory
    Assert.assertEquals(4096, manager.getMemoryConsumptionForThisTask());
    Assert.assertEquals(4096, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void encodePageNumberAndOffsetOffHeap() {
    final SparkConf conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1000");
    final TaskMemoryManager manager = new TaskMemoryManager(new TestMemoryManager(conf), 0);
    final MemoryBlock dataPage = manager.allocatePage(256, null);
    // In off-heap mode, an offset is an absolute address that may require more than 51 bits to
    // encode. This test exercises that corner-case:
    final long offset = ((1L << TaskMemoryManager.OFFSET_BITS) + 10);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, offset);
    Assert.assertEquals(null, manager.getPage(encodedAddress));
    Assert.assertEquals(offset, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void encodePageNumberAndOffsetOnHeap() {
    final TaskMemoryManager manager = new TaskMemoryManager(
      new TestMemoryManager(new SparkConf().set("spark.memory.offHeap.enabled", "false")), 0);
    final MemoryBlock dataPage = manager.allocatePage(256, null);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, 64);
    Assert.assertEquals(dataPage.getBaseObject(), manager.getPage(encodedAddress));
    Assert.assertEquals(64, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void cooperativeSpilling() {
    final TestMemoryManager memoryManager = new TestMemoryManager(new SparkConf());
    memoryManager.limit(100);
    final TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);

    TestMemoryConsumer c1 = new TestMemoryConsumer(manager);
    TestMemoryConsumer c2 = new TestMemoryConsumer(manager);
    c1.use(100);
    assert(c1.getUsed() == 100);
    c2.use(100);
    assert(c2.getUsed() == 100);
    assert(c1.getUsed() == 0);  // spilled
    c1.use(100);
    assert(c1.getUsed() == 100);
    assert(c2.getUsed() == 0);  // spilled

    c1.use(50);
    assert(c1.getUsed() == 50);  // spilled
    assert(c2.getUsed() == 0);
    c2.use(50);
    assert(c1.getUsed() == 50);
    assert(c2.getUsed() == 50);

    c1.use(100);
    assert(c1.getUsed() == 100);
    assert(c2.getUsed() == 0);  // spilled

    c1.free(20);
    assert(c1.getUsed() == 80);
    c2.use(10);
    assert(c1.getUsed() == 80);
    assert(c2.getUsed() == 10);
    c2.use(100);
    assert(c2.getUsed() == 100);
    assert(c1.getUsed() == 0);  // spilled

    c1.free(0);
    c2.free(100);
    assert(manager.cleanUpAllAllocatedMemory() == 0);
  }

  @Test
  public void offHeapConfigurationBackwardsCompatibility() {
    // Tests backwards-compatibility with the old `spark.unsafe.offHeap` configuration, which
    // was deprecated in Spark 1.6 and replaced by `spark.memory.offHeap.enabled` (see SPARK-12251).
    final SparkConf conf = new SparkConf()
      .set("spark.unsafe.offHeap", "true")
      .set("spark.memory.offHeap.size", "1000");
    final TaskMemoryManager manager = new TaskMemoryManager(new TestMemoryManager(conf), 0);
    assert(manager.tungstenMemoryMode == MemoryMode.OFF_HEAP);
  }

}
