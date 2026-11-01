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

import org.apache.spark.{SparkConf, SparkFunSuite}

class MemoryConsumerSuite extends SparkFunSuite {

  test("case class consumers have identity equality") {
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(new SparkConf()), 0)
    val consumer = CaseClassConsumer(taskMemoryManager, 1)
    val other = CaseClassConsumer(taskMemoryManager, 1)

    // The case class does not generate value-based equals/hashCode because MemoryConsumer
    // already defines them.
    assert(consumer != other)
    assert(consumer == consumer)
    assert(consumer.hashCode === System.identityHashCode(consumer))
  }

  test("case class consumers with equal arguments spill for each other") {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(100)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    val consumer = CaseClassConsumer(taskMemoryManager, 1)
    val other = CaseClassConsumer(taskMemoryManager, 1)

    consumer.use(60)
    // Only 40 bytes are free, so TaskMemoryManager asks `consumer` to spill on behalf of `other`,
    // which must not be mistaken for a self-triggered spill.
    other.use(60)
    assert(consumer.getUsed === 0)
    assert(other.getUsed === 60)

    other.free(60)
    assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0)
  }
}

private case class CaseClassConsumer(tmm: TaskMemoryManager, id: Int)
  extends TestMemoryConsumer(tmm) {

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    // Written with `==` on purpose, to check that it calls the identity equals of
    // MemoryConsumer. Real consumers should still compare `trigger` with `eq`, as the
    // MemoryConsumer#spill Javadoc says.
    if (trigger == this) 0L else super.spill(size, trigger)
  }
}
