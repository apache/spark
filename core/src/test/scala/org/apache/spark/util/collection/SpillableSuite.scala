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

package org.apache.spark.util.collection

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.internal.config.SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}

class SpillableSuite extends SparkFunSuite {

  private val initialMemoryThreshold = 100L

  private var memoryManager: TestMemoryManager = _
  private var taskMemoryManager: TaskMemoryManager = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf().set(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD, initialMemoryThreshold)
    val env = mock(classOf[SparkEnv])
    when(env.conf).thenReturn(conf)
    SparkEnv.set(env)
    memoryManager = new TestMemoryManager(conf)
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
  }

  override protected def afterEach(): Unit = {
    try {
      SparkEnv.set(null)
      assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0)
    } finally {
      super.afterEach()
    }
  }

  test("spill is not skipped when triggered by a distinct but equal consumer") {
    memoryManager.limit(1000)
    val spillable = new ValueEqualSpillable(taskMemoryManager)
    val other = new ValueEqualSpillable(taskMemoryManager)
    assert(spillable == other)

    // Growing past the initial threshold acquires 2 * 150 - 100 = 200 bytes without spilling.
    assert(!spillable.grow(150))
    assert(spillable.getUsed === 200)

    // `other` requests 2 * 500 - 100 = 900 bytes but only 800 are free, so TaskMemoryManager asks
    // `spillable` to spill on behalf of `other`, which must release its memory.
    assert(!other.grow(500))
    assert(spillable.forceSpillCount === 1)
    assert(spillable.getUsed === 0)
    assert(spillable.memoryBytesSpilled === 200)
    assert(other.getUsed === 900)

    // A spill triggered by the consumer itself is still skipped.
    assert(other.spill(Long.MaxValue, other) === 0)
    assert(other.forceSpillCount === 0)

    other.releaseMemory()
  }
}

/** A [[Spillable]] whose instances are all equal to each other. */
private class ValueEqualSpillable(taskMemoryManager: TaskMemoryManager)
  extends Spillable[Unit](taskMemoryManager) {

  var forceSpillCount = 0

  def grow(currentMemory: Long): Boolean = maybeSpill((), currentMemory)

  override protected def spill(collection: Unit): Unit = {}

  override protected def forceSpill(): Boolean = {
    forceSpillCount += 1
    true
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[ValueEqualSpillable]

  override def hashCode(): Int = 0
}
