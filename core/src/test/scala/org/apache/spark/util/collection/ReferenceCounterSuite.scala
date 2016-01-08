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

class ReferenceCounterSuite extends SparkFunSuite with BeforeAndAfterEach {

  private var refCounter: ReferenceCounter[BlockId] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    refCounter = new ReferenceCounter()
  }

  override protected def afterEach(): Unit = {
    refCounter = null
    super.afterEach()
  }

  test("initial reference counts are zero") {
    assert(refCounter.getNumberOfMapEntries === 0)
    assert(refCounter.getReferenceCount(TestBlockId("dummy")) === 0)
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("error when releasing more times than retained") {
    intercept[IllegalStateException] {
      refCounter.releaseForTask(taskAttemptId = 0L, TestBlockId("dummy"))
    }
  }

  test("retain and release from a single task") {
    val block = TestBlockId("dummy")
    val taskAttemptId = 0L
    refCounter.retainForTask(taskAttemptId, block)
    assert(refCounter.getReferenceCount(block) === 1)
    refCounter.retainForTask(taskAttemptId, block)
    assert(refCounter.getReferenceCount(block) === 2)
    refCounter.releaseForTask(taskAttemptId, block)
    assert(refCounter.getReferenceCount(block) === 1)
    refCounter.releaseAllReferencesForTask(taskAttemptId)
    assert(refCounter.getReferenceCount(block) === 0L)
    // Ensure that we didn't leak memory / map entries:
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("retain and release from multiple tasks") {
    val block = TestBlockId("dummy")
    val taskA = 0L
    val taskB = 1L

    refCounter.retainForTask(taskA, block)
    refCounter.retainForTask(taskA, block)
    refCounter.retainForTask(taskB, block)
    refCounter.retainForTask(taskB, block)
    refCounter.retainForTask(taskA, block)

    assert(refCounter.getReferenceCount(block) === 5)

    refCounter.releaseForTask(taskA, block)
    assert(refCounter.getReferenceCount(block) === 4)

    refCounter.releaseAllReferencesForTask(taskA)
    assert(refCounter.getReferenceCount(block) === 2)

    refCounter.releaseForTask(taskB, block)
    refCounter.releaseForTask(taskB, block)
    assert(refCounter.getReferenceCount(block) === 0)

    refCounter.releaseAllReferencesForTask(taskB)

    // Ensure that we didn't leak memory / map entries:
    assert(refCounter.getNumberOfMapEntries === 0)
  }

  test("counts are per-object") {
    val blockA = TestBlockId("blockA")
    val blockB = TestBlockId("blockB")
    val taskAttemptId = 0L

    refCounter.retainForTask(taskAttemptId, blockA)
    assert(refCounter.getReferenceCount(blockA) === 1)
    assert(refCounter.getReferenceCount(blockB) === 0)

    refCounter.retainForTask(taskAttemptId, blockB)
    refCounter.retainForTask(taskAttemptId, blockB)
    assert(refCounter.getReferenceCount(blockA) === 1)
    assert(refCounter.getReferenceCount(blockB) === 2)

    // Ensure that we didn't leak memory / map entries:
    refCounter.releaseAllReferencesForTask(taskAttemptId)
    assert(refCounter.getNumberOfMapEntries === 0)
  }

}
