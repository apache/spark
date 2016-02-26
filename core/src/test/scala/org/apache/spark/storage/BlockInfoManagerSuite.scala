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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.implicitConversions

import org.scalatest.BeforeAndAfterEach
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkException, SparkFunSuite, TaskContext, TaskContextImpl}


class BlockInfoManagerSuite extends SparkFunSuite with BeforeAndAfterEach {

  private implicit val ec = ExecutionContext.global
  private var blockInfoManager: BlockInfoManager = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    blockInfoManager = new BlockInfoManager()
    for (t <- 0 to 4) {
      blockInfoManager.registerTask(t)
    }
  }

  override protected def afterEach(): Unit = {
    try {
      blockInfoManager = null
    } finally {
      super.afterEach()
    }
  }

  private implicit def stringToBlockId(str: String): BlockId = {
    TestBlockId(str)
  }

  private def newBlockInfo(): BlockInfo = {
    new BlockInfo(StorageLevel.MEMORY_ONLY, tellMaster = false)
  }

  private def withTaskId[T](taskAttemptId: Long)(block: => T): T = {
    try {
      TaskContext.setTaskContext(new TaskContextImpl(0, 0, taskAttemptId, 0, null, null))
      block
    } finally {
      TaskContext.unset()
    }
  }

  test("initial memory usage") {
    assert(blockInfoManager.size === 0)
  }

  test("get non-existent block") {
    assert(blockInfoManager.get("non-existent-block").isEmpty)
    assert(blockInfoManager.lockForReading("non-existent-block").isEmpty)
    assert(blockInfoManager.lockForWriting("non-existent-block").isEmpty)
  }

  test("basic lockNewBlockForWriting") {
    val initialNumMapEntries = blockInfoManager.getNumberOfMapEntries
    val blockInfo = newBlockInfo()
    withTaskId(1) {
      assert(blockInfoManager.lockNewBlockForWriting("block", blockInfo))
      assert(blockInfoManager.get("block").get eq blockInfo)
      assert(!blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      assert(blockInfoManager.get("block").get eq blockInfo)
      assert(blockInfo.readerCount === 0)
      assert(blockInfo.writerTask === 1)
      blockInfoManager.unlock("block")
      assert(blockInfo.readerCount === 0)
      assert(blockInfo.writerTask === BlockInfo.NO_WRITER)
    }
    assert(blockInfoManager.size === 1)
    assert(blockInfoManager.getNumberOfMapEntries === initialNumMapEntries + 1)
  }

  test("read locks are reentrant") {
    withTaskId(1) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
      assert(blockInfoManager.lockForReading("block").isDefined)
      assert(blockInfoManager.lockForReading("block").isDefined)
      assert(blockInfoManager.get("block").get.readerCount === 2)
      assert(blockInfoManager.get("block").get.writerTask === BlockInfo.NO_WRITER)
      blockInfoManager.unlock("block")
      assert(blockInfoManager.get("block").get.readerCount === 1)
      blockInfoManager.unlock("block")
      assert(blockInfoManager.get("block").get.readerCount === 0)
    }
  }

  test("multiple tasks can hold read locks") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
    }
    withTaskId(1) { assert(blockInfoManager.lockForReading("block").isDefined) }
    withTaskId(2) { assert(blockInfoManager.lockForReading("block").isDefined) }
    withTaskId(3) { assert(blockInfoManager.lockForReading("block").isDefined) }
    withTaskId(4) { assert(blockInfoManager.lockForReading("block").isDefined) }
    assert(blockInfoManager.get("block").get.readerCount === 4)
  }

  test("single task can hold write lock") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
    }
    withTaskId(1) {
      assert(blockInfoManager.lockForWriting("block").isDefined)
      assert(blockInfoManager.get("block").get.writerTask === 1)
    }
    withTaskId(2) {
      assert(blockInfoManager.lockForWriting("block", blocking = false).isEmpty)
      assert(blockInfoManager.get("block").get.writerTask === 1)
    }
  }

  test("cannot call lockForWriting while already holding a write lock") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
    }
    withTaskId(1) {
      assert(blockInfoManager.lockForWriting("block").isDefined)
      intercept[IllegalStateException] {
        blockInfoManager.lockForWriting("block")
      }
      blockInfoManager.assertBlockIsLockedForWriting("block")
    }
  }

  test("assertBlockIsLockedForWriting throws exception if block is not locked") {
    intercept[SparkException] {
      blockInfoManager.assertBlockIsLockedForWriting("block")
    }
    withTaskId(BlockInfo.NON_TASK_WRITER) {
      intercept[SparkException] {
        blockInfoManager.assertBlockIsLockedForWriting("block")
      }
    }
  }

  test("downgrade lock") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.downgradeLock("block")
    }
    withTaskId(1) {
      assert(blockInfoManager.lockForReading("block").isDefined)
    }
    assert(blockInfoManager.get("block").get.readerCount === 2)
    assert(blockInfoManager.get("block").get.writerTask === BlockInfo.NO_WRITER)
  }

  test("write lock will block readers") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
    }
    val get1Future = Future {
      withTaskId(1) {
        blockInfoManager.lockForReading("block")
      }
    }
    val get2Future = Future {
      withTaskId(2) {
        blockInfoManager.lockForReading("block")
      }
    }
    Thread.sleep(300)  // Hack to try to ensure that both future tasks are waiting
    withTaskId(0) {
      blockInfoManager.unlock("block")
    }
    assert(Await.result(get1Future, 1.seconds).isDefined)
    assert(Await.result(get2Future, 1.seconds).isDefined)
    assert(blockInfoManager.get("block").get.readerCount === 2)
  }

  test("read locks will block writer") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
      blockInfoManager.lockForReading("block")
    }
    val write1Future = Future {
      withTaskId(1) {
        blockInfoManager.lockForWriting("block")
      }
    }
    val write2Future = Future {
      withTaskId(2) {
        blockInfoManager.lockForWriting("block")
      }
    }
    Thread.sleep(300)  // Hack to try to ensure that both future tasks are waiting
    withTaskId(0) {
      blockInfoManager.unlock("block")
    }
    assert(
      Await.result(Future.firstCompletedOf(Seq(write1Future, write2Future)), 1.seconds).isDefined)
    val firstWriteWinner = if (write1Future.isCompleted) 1 else 2
    withTaskId(firstWriteWinner) {
      blockInfoManager.unlock("block")
    }
    assert(Await.result(write1Future, 1.seconds).isDefined)
    assert(Await.result(write2Future, 1.seconds).isDefined)
  }

  test("removing a non-existent block throws IllegalArgumentException") {
    withTaskId(0) {
      intercept[IllegalArgumentException] {
        blockInfoManager.removeBlock("non-existent-block")
      }
    }
  }

  test("removing a block without holding any locks throws IllegalStateException") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
      intercept[IllegalStateException] {
        blockInfoManager.removeBlock("block")
      }
    }
  }

  test("removing a block while holding only a read lock throws IllegalStateException") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
      assert(blockInfoManager.lockForReading("block").isDefined)
      intercept[IllegalStateException] {
        blockInfoManager.removeBlock("block")
      }
    }
  }

  test("removing a block causes blocked callers to receive None") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
    }
    val getFuture = Future {
      withTaskId(1) {
        blockInfoManager.lockForReading("block")
      }
    }
    val writeFuture = Future {
      withTaskId(2) {
        blockInfoManager.lockForWriting("block")
      }
    }
    Thread.sleep(300)  // Hack to try to ensure that both future tasks are waiting
    withTaskId(0) {
      blockInfoManager.removeBlock("block")
    }
    assert(Await.result(getFuture, 1.seconds).isEmpty)
    assert(Await.result(writeFuture, 1.seconds).isEmpty)
  }

  test("releaseAllLocksForTask releases write locks") {
    val initialNumMapEntries = blockInfoManager.getNumberOfMapEntries
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
    }
    assert(blockInfoManager.getNumberOfMapEntries === initialNumMapEntries + 3)
    blockInfoManager.releaseAllLocksForTask(0)
    assert(blockInfoManager.getNumberOfMapEntries === initialNumMapEntries)
  }
}
