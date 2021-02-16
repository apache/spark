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

/**
 * Tests of [[TestMemoryManager]] itself.
 */
class TestMemoryManagerSuite extends SparkFunSuite {
  test("tracks allocated execution memory by task") {
    val testMemoryManager = new TestMemoryManager(new SparkConf())

    assert(testMemoryManager.getExecutionMemoryUsageForTask(0) == 0)
    assert(testMemoryManager.getExecutionMemoryUsageForTask(1) == 0)

    testMemoryManager.acquireExecutionMemory(10, 0, MemoryMode.ON_HEAP)
    testMemoryManager.acquireExecutionMemory(5, 1, MemoryMode.ON_HEAP)
    testMemoryManager.acquireExecutionMemory(5, 0, MemoryMode.ON_HEAP)
    assert(testMemoryManager.getExecutionMemoryUsageForTask(0) == 15)
    assert(testMemoryManager.getExecutionMemoryUsageForTask(1) == 5)

    testMemoryManager.releaseExecutionMemory(10, 0, MemoryMode.ON_HEAP)
    assert(testMemoryManager.getExecutionMemoryUsageForTask(0) == 5)

    testMemoryManager.releaseAllExecutionMemoryForTask(0)
    testMemoryManager.releaseAllExecutionMemoryForTask(1)
    assert(testMemoryManager.getExecutionMemoryUsageForTask(0) == 0)
    assert(testMemoryManager.getExecutionMemoryUsageForTask(1) == 0)
  }

  test("markconsequentOOM") {
    val testMemoryManager = new TestMemoryManager(new SparkConf())
    assert(testMemoryManager.acquireExecutionMemory(1, 0, MemoryMode.ON_HEAP) == 1)
    testMemoryManager.markconsequentOOM(2)
    assert(testMemoryManager.acquireExecutionMemory(1, 0, MemoryMode.ON_HEAP) == 0)
    assert(testMemoryManager.acquireExecutionMemory(1, 0, MemoryMode.ON_HEAP) == 0)
    assert(testMemoryManager.acquireExecutionMemory(1, 0, MemoryMode.ON_HEAP) == 1)
  }
}
