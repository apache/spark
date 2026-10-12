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
    // A spill triggered by an equal-looking consumer is not mistaken for a self-triggered one.
    assert(consumer.spill(Long.MaxValue, other) === 1)
    assert(consumer.spill(Long.MaxValue, consumer) === 0)
  }
}

private case class CaseClassConsumer(tmm: TaskMemoryManager, id: Int)
  extends MemoryConsumer(tmm, MemoryMode.ON_HEAP) {

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    // Written with `==` on purpose: it calls equals, which is identity for memory consumers.
    if (trigger == this) 0L else 1L
  }
}
