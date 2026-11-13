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

package org.apache.spark.sql.execution.python

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, TestMemoryConsumer, TestMemoryManager}
import org.apache.spark.security.{CryptoStreamUtils, EncryptionFunSuite}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}
import org.apache.spark.util.{ThreadUtils, Utils}

class RowQueueSuite extends SparkFunSuite with EncryptionFunSuite {

  test("in-memory queue") {
    val page = MemoryBlock.fromLongArray(new Array[Long](1<<10))
    val queue = new InMemoryRowQueue(page, 1) {
      override def close(): Unit = {}
    }
    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val n = page.size() / (4 + row.getSizeInBytes)
    var i = 0
    while (i < n) {
      row.setLong(0, i)
      assert(queue.add(row), "fail to add")
      i += 1
    }
    assert(!queue.add(row), "should not add more")
    i = 0
    while (i < n) {
      val row = queue.remove()
      assert(row != null, "fail to poll")
      assert(row.getLong(0) == i, "does not match")
      i += 1
    }
    assert(queue.remove() == null, "should be empty")
    queue.close()
  }

  private def createSerializerManager(conf: SparkConf): SerializerManager = {
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    new SerializerManager(new JavaSerializer(conf), conf, ioEncryptionKey)
  }

  encryptionTest("disk queue") { conf =>
    val serManager = createSerializerManager(conf)
    val dir = Utils.createTempDir().getCanonicalFile
    Utils.createDirectory(dir)
    val queue = DiskRowQueue(new File(dir, "buffer"), 1, serManager)
    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val n = 1000
    var i = 0
    while (i < n) {
      row.setLong(0, i)
      assert(queue.add(row), "fail to add")
      i += 1
    }
    val first = queue.remove()
    assert(first != null, "first should not be null")
    assert(first.getLong(0) == 0, "first should be 0")
    assert(!queue.add(row), "should not add more")
    i = 1
    while (i < n) {
      val row = queue.remove()
      assert(row != null, "fail to poll")
      assert(row.getLong(0) == i, "does not match")
      i += 1
    }
    assert(queue.remove() == null, "should be empty")
    queue.close()
  }

  test("hybrid queue uses disk for an exact-fit partial page") {
    val conf = new SparkConf(false)
    val serManager = createSerializerManager(conf)
    val mem = new TestMemoryManager(conf)
    var pageFreed = false
    val taskM = new TaskMemoryManager(mem, 0) {
      override def allocatePage(size: Long, consumer: MemoryConsumer): MemoryBlock = {
        MemoryAllocator.HEAP.allocate(20)
      }

      override def freePage(page: MemoryBlock, consumer: MemoryConsumer): Unit = {
        pageFreed = true
        MemoryAllocator.HEAP.free(page)
      }
    }
    val queue = HybridRowQueue(taskM, Utils.createTempDir().getCanonicalFile, 1, serManager)
    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)

    assert(queue.add(row) === QueueMode.DISK)
    assert(pageFreed)
    assert(queue.getUsed === 0)
    assert(queue.remove().getSizeInBytes === 16)
    queue.close()
  }

  Seq(true, false).foreach { isOffHeap =>
    encryptionTest(s"hybrid queue (offHeap=$isOffHeap)") { conf =>
      conf.set(MEMORY_OFFHEAP_ENABLED, isOffHeap)
      if (isOffHeap) conf.set(MEMORY_OFFHEAP_SIZE, 1000L)
      val serManager = createSerializerManager(conf)
      val mem = new TestMemoryManager(conf)
      mem.limit(4<<10)
      val taskM = new TaskMemoryManager(mem, 0)
      val queue = HybridRowQueue(taskM, Utils.createTempDir().getCanonicalFile, 1, serManager)
      val mode = if (isOffHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP
      assert(queue.getMode === mode)
      val row = new UnsafeRow(1)
      row.pointTo(new Array[Byte](16), 16)
      val n = (4<<10) / 16 * 3
      var i = 0
      while (i < n) {
        row.setLong(0, i)
        queue.add(row)
        i += 1
      }
      assert(queue.numQueues() > 1, "should have more than one queue")
      queue.spill(1<<20, null)
      i = 0
      while (i < n) {
        val row = queue.remove()
        assert(row != null, "fail to poll")
        assert(row.getLong(0) == i, "does not match")
        i += 1
      }

      // fill again and spill
      i = 0
      while (i < n) {
        row.setLong(0, i)
        queue.add(row)
        i += 1
      }
      assert(queue.numQueues() > 1, "should have more than one queue")
      queue.spill(1<<20, null)
      assert(queue.numQueues() > 1, "should have more than one queue")
      i = 0
      while (i < n) {
        val row = queue.remove()
        assert(row != null, "fail to poll")
        assert(row.getLong(0) == i, "does not match")
        i += 1
      }
      queue.close()
    }
  }

  // Small pages so that a few rows span several in-memory queues.
  private val smallPageSize = 1024L
  private val smallPageConf = new SparkConf(false).set(BUFFER_PAGESIZE, smallPageSize)

  /** Creates two distinct queues with the same constructor arguments. */
  private def queuesWithSameArguments(
      taskM: TaskMemoryManager): (HybridRowQueue, HybridRowQueue) = {
    val serManager = createSerializerManager(smallPageConf)
    val tempDir = Utils.createTempDir().getCanonicalFile
    // The second queue uses named arguments to check that they match the former case class.
    (HybridRowQueue(taskM, tempDir, 1, serManager),
      HybridRowQueue(memManager = taskM, tempDir = tempDir, numFields = 1, serMgr = serManager))
  }

  private def addRows(queue: HybridRowQueue, n: Int): Unit = {
    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    (0 until n).foreach { i =>
      row.setLong(0, i)
      assert(queue.add(row) === QueueMode.IN_MEMORY)
    }
  }

  private def drainAndClose(queue: HybridRowQueue, n: Int): Unit = {
    (0 until n).foreach { i =>
      assert(queue.remove().getLong(0) === i)
    }
    queue.close()
  }

  test("hybrid queues with the same fields are distinct memory consumers") {
    val mem = new TestMemoryManager(smallPageConf)
    val taskM = new TaskMemoryManager(mem, 0)
    val (queue1, queue2) = queuesWithSameArguments(taskM)
    assert(queue1 != queue2)
    assert(queue1.toString.startsWith("HybridRowQueue(numFields=1, lockFree=false)@"))
    assert(queue1.toString != queue2.toString)

    // queue1 is registered first and holds a single page, which it never spills because the
    // last queue is kept for writing. queue2 holds several pages and can release all but one.
    addRows(queue1, 1)
    val n = 150
    addRows(queue2, n)
    assert(queue2.numQueues() > 2)
    val queue2Used = queue2.getUsed

    // Memory pressure from another consumer must be able to spill queue2.
    mem.limit(0)
    val consumer = new TestMemoryConsumer(taskM)
    consumer.use(smallPageSize)
    assert(consumer.getUsed === smallPageSize)
    assert(queue2.getUsed === smallPageSize)
    assert(queue2.getUsed < queue2Used)
    consumer.free(smallPageSize)

    drainAndClose(queue1, 1)
    drainAndClose(queue2, n)
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }

  test("hybrid queue spills for another queue with the same fields") {
    val taskM = new TaskMemoryManager(new TestMemoryManager(smallPageConf), 0)
    val (queue1, queue2) = queuesWithSameArguments(taskM)
    val n = 150
    addRows(queue1, n)
    assert(queue1.numQueues() > 2)
    val used = queue1.getUsed
    // Check the self-triggered spill while queue1 still has spillable in-memory pages.
    assert(queue1.spill(Long.MaxValue, queue1) === 0)
    assert(queue1.getUsed === used)
    assert(queue1.spill(Long.MaxValue, queue2) === used - smallPageSize)
    drainAndClose(queue1, n)
    queue2.close()
  }

  test("closing a hybrid queue does not deadlock with a concurrent spill of it") {
    val closeFreeingPage = new CountDownLatch(1)
    val spillHoldingTaskMemoryManager = new CountDownLatch(1)
    @volatile var closingThread: Thread = null
    val taskM = new TaskMemoryManager(new TestMemoryManager(smallPageConf), 0) {
      override def freePage(page: MemoryBlock, consumer: MemoryConsumer): Unit = {
        if (Thread.currentThread() eq closingThread) {
          // Pause close() at its first page free until the spilling thread holds this
          // TaskMemoryManager's monitor, so the two lock orders are forced to overlap.
          closeFreeingPage.countDown()
          assert(spillHoldingTaskMemoryManager.await(10, TimeUnit.SECONDS))
        }
        super.freePage(page, consumer)
      }
    }
    val (queue1, queue2) = queuesWithSameArguments(taskM)
    addRows(queue2, 150)
    assert(queue2.numQueues() > 2)

    val pool = ThreadUtils.newDaemonFixedThreadPool(2, "row-queue-close-spill")
    try {
      val closing = pool.submit[Unit](() => {
        closingThread = Thread.currentThread()
        queue2.close()
      })
      // Mimic TaskMemoryManager.acquireExecutionMemory, which holds its monitor while it asks
      // another consumer to spill.
      val spilling = pool.submit[Long](() => {
        assert(closeFreeingPage.await(10, TimeUnit.SECONDS))
        taskM.synchronized {
          spillHoldingTaskMemoryManager.countDown()
          queue2.spill(Long.MaxValue, queue1)
        }
      })
      ThreadUtils.awaitResult(closing, 10.seconds)
      // close() has already detached all of queue2's pages, so the spill has nothing to release.
      assert(ThreadUtils.awaitResult(spilling, 10.seconds) === 0L)
    } finally {
      pool.shutdownNow()
    }
    assert(queue2.getUsed === 0)
    queue1.close()
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }

  test("hybrid queue enforces maxMemoryBytes cap without exhausting execution memory") {
    val taskM = new TaskMemoryManager(new TestMemoryManager(smallPageConf), 0)
    val serManager = createSerializerManager(smallPageConf)
    val tempDir = Utils.createTempDir().getCanonicalFile
    val maxBytes = 2 * smallPageSize
    val queue = HybridRowQueue(
      taskM, tempDir, 1, serManager, maxMemoryBytes = maxBytes)

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val n = 500
    (0 until n).foreach { i =>
      row.setLong(0, i)
      queue.add(row)
      assert(queue.getUsed <= maxBytes)
    }
    assert(queue.getUsed === maxBytes)
    assert(queue.getNumElementsQueuedOnDisk() > 0)
    // Excess rows are split across multiple bounded disk segments rather than a single giant file.
    val spillFilesBeforeDrain = tempDir.listFiles()
    assert(spillFilesBeforeDrain != null && spillFilesBeforeDrain.length > 1)

    (0 until n).foreach { i =>
      val out = queue.remove()
      assert(out != null)
      assert(out.getLong(0) === i)
    }
    queue.close()
    assert(tempDir.listFiles().isEmpty)
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }

  test("hybrid queue with maxMemoryBytes = 0 uses disk only") {
    val taskM = new TaskMemoryManager(new TestMemoryManager(smallPageConf), 0)
    val serManager = createSerializerManager(smallPageConf)
    val tempDir = Utils.createTempDir().getCanonicalFile
    val queue = HybridRowQueue(taskM, tempDir, 1, serManager, maxMemoryBytes = 0L)

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val n = 200
    (0 until n).foreach { i =>
      row.setLong(0, i)
      assert(queue.add(row) === QueueMode.DISK)
      assert(queue.getUsed === 0L)
    }
    drainAndClose(queue, n)
    assert(tempDir.listFiles().isEmpty)
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }

  test("hybrid queue recovers to in-memory pages after capped memory is drained") {
    val taskM = new TaskMemoryManager(new TestMemoryManager(smallPageConf), 0)
    val serManager = createSerializerManager(smallPageConf)
    val tempDir = Utils.createTempDir().getCanonicalFile
    val queue = HybridRowQueue(
      taskM, tempDir, 1, serManager, maxMemoryBytes = smallPageSize)

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val firstBatch = 120
    val firstModes = (0 until firstBatch).map { i =>
      row.setLong(0, i)
      queue.add(row)
    }
    assert(firstModes.head === QueueMode.IN_MEMORY)
    assert(firstModes.last === QueueMode.DISK)
    assert(queue.getUsed === smallPageSize)

    // Drain past the first in-memory page so its memory is freed back under maxMemoryBytes.
    val drained = 60
    (0 until drained).foreach { i =>
      assert(queue.remove().getLong(0) === i)
    }
    assert(queue.getUsed === 0L)

    // Subsequent writes finish the active disk segment and recover to an in-memory page.
    val secondBatch = 80
    val secondModes = (firstBatch until (firstBatch + secondBatch)).map { i =>
      row.setLong(0, i)
      queue.add(row)
    }
    assert(secondModes.contains(QueueMode.IN_MEMORY))
    assert(queue.getUsed === smallPageSize)

    (drained until (firstBatch + secondBatch)).foreach { i =>
      assert(queue.remove().getLong(0) === i)
    }
    queue.close()
    assert(tempDir.listFiles().isEmpty)
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }

  test("hybrid queue honors maxMemoryBytes smaller than pageSizeBytes") {
    val taskM = new TaskMemoryManager(new TestMemoryManager(smallPageConf), 0)
    val serManager = createSerializerManager(smallPageConf)
    val tempDir = Utils.createTempDir().getCanonicalFile
    val subPageCap = smallPageSize / 2
    val queue = HybridRowQueue(taskM, tempDir, 1, serManager, maxMemoryBytes = subPageCap)

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val n = 100
    val modes = (0 until n).map { i =>
      row.setLong(0, i)
      val mode = queue.add(row)
      assert(queue.getUsed <= subPageCap)
      mode
    }
    assert(modes.head === QueueMode.IN_MEMORY)
    assert(modes.last === QueueMode.DISK)
    drainAndClose(queue, n)
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }

  test("hybrid queue rotates disk segments and recovers to memory under default unlimited cap") {
    val mem = new TestMemoryManager(smallPageConf)
    mem.limit(0)
    val taskM = new TaskMemoryManager(mem, 0)
    val serManager = createSerializerManager(smallPageConf)
    val tempDir = Utils.createTempDir().getCanonicalFile
    val queue = HybridRowQueue(taskM, tempDir, 1, serManager)

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    val firstBatch = 200
    (0 until firstBatch).foreach { i =>
      row.setLong(0, i)
      assert(queue.add(row) === QueueMode.DISK)
    }
    val spillFilesBeforeRecovery = tempDir.listFiles()
    assert(spillFilesBeforeRecovery != null && spillFilesBeforeRecovery.length > 1)

    // Once execution memory becomes available again, writing rotates back to in-memory pages.
    mem.limit(smallPageSize)
    val secondBatch = 80
    val recoveryModes = (firstBatch until (firstBatch + secondBatch)).map { i =>
      row.setLong(0, i)
      queue.add(row)
    }
    assert(recoveryModes.contains(QueueMode.IN_MEMORY))

    drainAndClose(queue, firstBatch + secondBatch)
    assert(tempDir.listFiles().isEmpty)
    assert(taskM.cleanUpAllAllocatedMemory() === 0)
  }
}
