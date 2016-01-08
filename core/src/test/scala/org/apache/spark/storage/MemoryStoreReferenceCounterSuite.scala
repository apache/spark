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

package org.apache.spark.storage

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite

class MemoryStoreReferenceCounterSuite extends SparkFunSuite with BeforeAndAfterEach {

  private var refCounter: MemoryStoreReferenceCounter = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    refCounter = new MemoryStoreReferenceCounter()
  }

  override protected def afterEach(): Unit = {
    refCounter = null
    super.afterEach()
  }

  test("blocks' initial reference counts are zero") {
    assert(refCounter.getNumberOfMapEntries === 0)
    assert(refCounter.getReferenceCountForBlock(TestBlockId("dummy")) === 0)
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("error when releasing a block more times than it has been pinned") {
    intercept[IllegalStateException] {
      refCounter.releaseBlockForTask(taskAttemptId = 0L, TestBlockId("dummy"))
    }
  }

  test("retain and release block from a single task") {
    val block = TestBlockId("dummy")
    val taskAttemptId = 0L
    refCounter.retainBlockForTask(taskAttemptId, block)
    assert(refCounter.getReferenceCountForBlock(block) === 1)
    refCounter.retainBlockForTask(taskAttemptId, block)
    assert(refCounter.getReferenceCountForBlock(block) === 2)
    refCounter.releaseBlockForTask(taskAttemptId, block)
    assert(refCounter.getReferenceCountForBlock(block) === 1)
    refCounter.releaseAllBlocksForTaskAttempt(taskAttemptId)
    assert(refCounter.getReferenceCountForBlock(block) === 0L)
    // Ensure that we didn't leak memory / map entries:
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("retain and release block from multiple tasks") {
    val block = TestBlockId("dummy")
    val taskA = 0L
    val taskB = 1L

    refCounter.retainBlockForTask(taskA, block)
    refCounter.retainBlockForTask(taskA, block)
    refCounter.retainBlockForTask(taskB, block)
    refCounter.retainBlockForTask(taskB, block)
    refCounter.retainBlockForTask(taskA, block)

    assert(refCounter.getReferenceCountForBlock(block) === 5)

    refCounter.releaseBlockForTask(taskA, block)
    assert(refCounter.getReferenceCountForBlock(block) === 4)

    refCounter.releaseAllBlocksForTaskAttempt(taskA)
    assert(refCounter.getReferenceCountForBlock(block) === 2)

    refCounter.releaseBlockForTask(taskB, block)
    refCounter.releaseBlockForTask(taskB, block)
    assert(refCounter.getReferenceCountForBlock(block) === 0)

    refCounter.releaseAllBlocksForTaskAttempt(taskB)

    // Ensure that we didn't leak memory / map entries:
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("counts are per-block") {
    val blockA = TestBlockId("blockA")
    val blockB = TestBlockId("blockB")
    val taskAttemptId = 0L

    refCounter.retainBlockForTask(taskAttemptId, blockA)
    assert(refCounter.getReferenceCountForBlock(blockA) === 1)
    assert(refCounter.getReferenceCountForBlock(blockB) === 0)

    refCounter.retainBlockForTask(taskAttemptId, blockB)
    refCounter.retainBlockForTask(taskAttemptId, blockB)
    assert(refCounter.getReferenceCountForBlock(blockA) === 1)
    assert(refCounter.getReferenceCountForBlock(blockB) === 2)

    // Ensure that we didn't leak memory / map entries:
    refCounter.releaseAllBlocksForTaskAttempt(taskAttemptId)
    assert(refCounter.getNumberOfMapEntries === 0)
  }

}
