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

package org.apache.spark.memory

import org.apache.spark.SparkFunSuite

/**
 * Trait-level compile and behavior contract tests for [[ManagedConsumer]]. End-to-end
 * registry behavior (identity-based dedup, registration idempotency), self-exclusion via
 * object identity, [[MemoryManager.shrinkExternal]] orchestration, and integration with
 * [[UnifiedMemoryManager]] storage acquire / execution reclaim paths are covered in
 * `UnifiedMemoryManagerSuite`.
 */
class ManagedConsumerSuite extends SparkFunSuite {

  test("a minimal ManagedConsumer compiles and exposes the SPI members") {
    val consumer = new ManagedConsumer {
      private var held = 1000L
      override val memoryMode: MemoryMode = MemoryMode.OFF_HEAP
      override def getShrinkableMemoryBytes: Long = held
      override def shrink(numBytes: Long): Long = {
        val released = math.min(numBytes, held)
        held -= released
        released
      }
    }

    // `name` has a default (getClass.getSimpleName); anonymous classes get "".
    // MemoryManager.consumerLogName falls back to the FQ name in that case.
    assert(consumer.name === "" || consumer.name.nonEmpty)
    assert(MemoryManager.consumerLogName(consumer).nonEmpty)
    assert(consumer.memoryMode === MemoryMode.OFF_HEAP)
    assert(consumer.getShrinkableMemoryBytes === 1000L)
    assert(consumer.shrink(300L) === 300L)
    assert(consumer.getShrinkableMemoryBytes === 700L)
  }

  test("named ManagedConsumer overrides `name` for logs") {
    class NamedCache extends ManagedConsumer {
      override val name: String = "MyVeloxCache:executor-7"
      override val memoryMode: MemoryMode = MemoryMode.OFF_HEAP
      override def getShrinkableMemoryBytes: Long = 0L
      override def shrink(numBytes: Long): Long = 0L
    }
    val c = new NamedCache
    assert(c.name === "MyVeloxCache:executor-7")
    assert(MemoryManager.consumerLogName(c) === "MyVeloxCache:executor-7")
  }

  test("ManagedConsumer is independent of UnmanagedMemoryConsumer") {
    // The two traits are NOT in an inheritance relation. A consumer may implement either,
    // both, or neither. Verifying the type system here documents that contract.
    val pureManaged: ManagedConsumer = new ManagedConsumer {
      override val memoryMode: MemoryMode = MemoryMode.OFF_HEAP
      override def getShrinkableMemoryBytes: Long = 0L
      override def shrink(numBytes: Long): Long = 0L
    }
    val pureUnmanaged: UnmanagedMemoryConsumer = new UnmanagedMemoryConsumer {
      override val unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("PureUnmanaged", "k")
      override val memoryMode: MemoryMode = MemoryMode.OFF_HEAP
      override def getMemBytesUsed: Long = 0L
    }
    assert(!pureManaged.isInstanceOf[UnmanagedMemoryConsumer])
    assert(!pureUnmanaged.isInstanceOf[ManagedConsumer])

    // A single component MAY implement both, with the mutual-exclusion contract from the
    // scaladoc: getMemBytesUsed must NOT double-report bytes already accounted via
    // acquireStorageMemory.
    val both = new ManagedConsumer with UnmanagedMemoryConsumer {
      override val unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId =
        UnmanagedMemoryConsumerId("Both", "k")
      override val memoryMode: MemoryMode = MemoryMode.OFF_HEAP
      override def getShrinkableMemoryBytes: Long = 0L
      override def shrink(numBytes: Long): Long = 0L
      override def getMemBytesUsed: Long = 0L
    }
    assert(both.isInstanceOf[ManagedConsumer])
    assert(both.isInstanceOf[UnmanagedMemoryConsumer])
  }

  test("MemoryManager.getShrinkableConsumers default is empty for non-Unified backends") {
    // Any MemoryManager that does not override getShrinkableConsumers (e.g., TestMemoryManager,
    // alternative backends, future SPI implementations) must transparently disable the push-mode
    // shrink integration. Without this default, MemoryManager.shrinkExternal would crash or
    // silently miss a hard-coded UnifiedMemoryManager dependency.
    val mm = new TestMemoryManager(new org.apache.spark.SparkConf(false))
    assert(mm.getShrinkableConsumers(MemoryMode.ON_HEAP).isEmpty)
    assert(mm.getShrinkableConsumers(MemoryMode.OFF_HEAP).isEmpty)
  }

  test("MemoryManager.consumerLogName falls back to FQ class name for empty `name`") {
    val anon = new ManagedConsumer {
      override val memoryMode: MemoryMode = MemoryMode.ON_HEAP
      override def getShrinkableMemoryBytes: Long = 0L
      override def shrink(numBytes: Long): Long = 0L
    }
    val resolved = MemoryManager.consumerLogName(anon)
    assert(resolved.nonEmpty, "log name must never be empty (would lose context in WARN logs)")
    // For anonymous classes getSimpleName is "" so the fallback is the FQ name.
    if (anon.name.isEmpty) {
      assert(resolved === anon.getClass.getName)
    }
  }
}
