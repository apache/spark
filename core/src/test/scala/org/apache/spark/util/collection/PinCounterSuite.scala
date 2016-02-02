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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{BlockId, TestBlockId}

class PinCounterSuite extends SparkFunSuite with BeforeAndAfterEach {

  private var refCounter: PinCounter[BlockId] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    refCounter = new PinCounter()
  }

  override protected def afterEach(): Unit = {
    refCounter = null
    super.afterEach()
  }

  test("initial pin counts are zero") {
    assert(refCounter.getNumberOfMapEntries === 0)
    assert(refCounter.getPinCount(TestBlockId("dummy")) === 0)
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("error when releasing more times than retained") {
    intercept[IllegalStateException] {
      refCounter.unpinForTask(taskAttemptId = 0L, TestBlockId("dummy"))
    }
  }

  test("retain and release from a single task") {
    val block = TestBlockId("dummy")
    val taskAttemptId = 0L
    refCounter.pinForTask(taskAttemptId, block)
    assert(refCounter.getPinCount(block) === 1)
    refCounter.pinForTask(taskAttemptId, block)
    assert(refCounter.getPinCount(block) === 2)
    refCounter.unpinForTask(taskAttemptId, block)
    assert(refCounter.getPinCount(block) === 1)
    refCounter.releaseAllPinsForTask(taskAttemptId)
    assert(refCounter.getPinCount(block) === 0L)
    // Ensure that we didn't leak memory / map entries:
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("retain and release from multiple tasks") {
    val block = TestBlockId("dummy")
    val taskA = 0L
    val taskB = 1L

    refCounter.pinForTask(taskA, block)
    refCounter.pinForTask(taskA, block)
    refCounter.pinForTask(taskB, block)
    refCounter.pinForTask(taskB, block)
    refCounter.pinForTask(taskA, block)

    assert(refCounter.getPinCount(block) === 5)

    refCounter.unpinForTask(taskA, block)
    assert(refCounter.getPinCount(block) === 4)

    refCounter.releaseAllPinsForTask(taskA)
    assert(refCounter.getPinCount(block) === 2)

    refCounter.unpinForTask(taskB, block)
    refCounter.unpinForTask(taskB, block)
    assert(refCounter.getPinCount(block) === 0)

    refCounter.releaseAllPinsForTask(taskB)

    // Ensure that we didn't leak memory / map entries:
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("counts are per-object") {
    val blockA = TestBlockId("blockA")
    val blockB = TestBlockId("blockB")
    val taskAttemptId = 0L

    refCounter.pinForTask(taskAttemptId, blockA)
    assert(refCounter.getPinCount(blockA) === 1)
    assert(refCounter.getPinCount(blockB) === 0)

    refCounter.pinForTask(taskAttemptId, blockB)
    refCounter.pinForTask(taskAttemptId, blockB)
    assert(refCounter.getPinCount(blockA) === 1)
    assert(refCounter.getPinCount(blockB) === 2)

    // Ensure that we didn't leak memory / map entries:
    refCounter.releaseAllPinsForTask(taskAttemptId)
    assert(refCounter.getNumberOfMapEntries === 0)
  }

}
