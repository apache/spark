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

import java.util.Properties

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.scalatest.BeforeAndAfterEach
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkException, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.ThreadUtils


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
    new BlockInfo(StorageLevel.MEMORY_ONLY, ClassTag.Any, tellMaster = false)
  }

  private def withTaskId[T](taskAttemptId: Long)(block: => T): T = {
    try {
      TaskContext.setTaskContext(
        new TaskContextImpl(0, 0, 0, taskAttemptId, 0,
          1, null, new Properties, null, TaskMetrics.empty, 1))
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
      assert(blockInfo.readerCount === 0)
      assert(blockInfo.writerTask === 1)
      // Downgrade lock so that second call doesn't block:
      blockInfoManager.downgradeLock("block")
      assert(blockInfo.readerCount === 1)
      assert(blockInfo.writerTask === BlockInfo.NO_WRITER)
      assert(!blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      assert(blockInfo.readerCount === 2)
      assert(blockInfoManager.get("block").get eq blockInfo)
      assert(blockInfo.readerCount === 2)
      assert(blockInfo.writerTask === BlockInfo.NO_WRITER)
      blockInfoManager.unlock("block")
      blockInfoManager.unlock("block")
      assert(blockInfo.readerCount === 0)
      assert(blockInfo.writerTask === BlockInfo.NO_WRITER)
    }
    assert(blockInfoManager.size === 1)
    assert(blockInfoManager.getNumberOfMapEntries === initialNumMapEntries + 1)
  }

  test("lockNewBlockForWriting blocks while write lock is held, then returns false after release") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
    }
    val lock1Future = Future {
      withTaskId(1) {
        blockInfoManager.lockNewBlockForWriting("block", newBlockInfo())
      }
    }
    val lock2Future = Future {
      withTaskId(2) {
        blockInfoManager.lockNewBlockForWriting("block", newBlockInfo())
      }
    }
    Thread.sleep(300)  // Hack to try to ensure that both future tasks are waiting
    withTaskId(0) {
      blockInfoManager.downgradeLock("block")
    }
    // After downgrading to a read lock, both threads should wake up and acquire the shared
    // read lock.
    assert(!ThreadUtils.awaitResult(lock1Future, 1.seconds))
    assert(!ThreadUtils.awaitResult(lock2Future, 1.seconds))
    assert(blockInfoManager.get("block").get.readerCount === 3)
  }

  test("lockNewBlockForWriting blocks while write lock is held, then returns true after removal") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
    }
    val lock1Future = Future {
      withTaskId(1) {
        blockInfoManager.lockNewBlockForWriting("block", newBlockInfo())
      }
    }
    val lock2Future = Future {
      withTaskId(2) {
        blockInfoManager.lockNewBlockForWriting("block", newBlockInfo())
      }
    }
    Thread.sleep(300)  // Hack to try to ensure that both future tasks are waiting
    withTaskId(0) {
      blockInfoManager.removeBlock("block")
    }
    // After removing the block, the write lock is released. Both threads should wake up but only
    // one should acquire the write lock. The second thread should block until the winner of the
    // write race releases its lock.
    val winningFuture: Future[Boolean] =
      ThreadUtils.awaitReady(Future.firstCompletedOf(Seq(lock1Future, lock2Future)), 1.seconds)
    assert(winningFuture.value.get.get)
    val winningTID = blockInfoManager.get("block").get.writerTask
    assert(winningTID === 1 || winningTID === 2)
    val losingFuture: Future[Boolean] = if (winningTID == 1) lock2Future else lock1Future
    assert(!losingFuture.isCompleted)
    // Once the writer releases its lock, the blocked future should wake up again and complete.
    withTaskId(winningTID) {
      blockInfoManager.unlock("block")
    }
    assert(!ThreadUtils.awaitResult(losingFuture, 1.seconds))
    assert(blockInfoManager.get("block").get.readerCount === 1)
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

  test("cannot grab a writer lock while already holding a write lock") {
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
      blockInfoManager.unlock("block")
    }
    withTaskId(1) {
      assert(blockInfoManager.lockForWriting("block").isDefined)
      assert(blockInfoManager.lockForWriting("block", false).isEmpty)
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
    assert(ThreadUtils.awaitResult(get1Future, 1.seconds).isDefined)
    assert(ThreadUtils.awaitResult(get2Future, 1.seconds).isDefined)
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
      ThreadUtils.awaitResult(
        Future.firstCompletedOf(Seq(write1Future, write2Future)), 1.seconds).isDefined)
    val firstWriteWinner = if (write1Future.isCompleted) 1 else 2
    withTaskId(firstWriteWinner) {
      blockInfoManager.unlock("block")
    }
    assert(ThreadUtils.awaitResult(write1Future, 1.seconds).isDefined)
    assert(ThreadUtils.awaitResult(write2Future, 1.seconds).isDefined)
  }

  test("removing a non-existent block throws SparkException") {
    withTaskId(0) {
      intercept[SparkException] {
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
    assert(ThreadUtils.awaitResult(getFuture, 1.seconds).isEmpty)
    assert(ThreadUtils.awaitResult(writeFuture, 1.seconds).isEmpty)
  }

  test("releaseAllLocksForTask releases write locks") {
    val initialNumMapEntries = blockInfoManager.getNumberOfMapEntries
    assert(initialNumMapEntries == 12)
    withTaskId(0) {
      assert(blockInfoManager.lockNewBlockForWriting("block", newBlockInfo()))
    }
    assert(blockInfoManager.getNumberOfMapEntries === initialNumMapEntries + 2)
    blockInfoManager.releaseAllLocksForTask(0)
    assert(blockInfoManager.getNumberOfMapEntries === initialNumMapEntries - 1)
  }
}
