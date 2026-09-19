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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.security.{CryptoStreamUtils, EncryptionFunSuite}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}
import org.apache.spark.util.Utils

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

  encryptionTest("SPARK-59601: closeOutputStream flushes the queue and keeps it readable") {
    conf =>
    val serManager = createSerializerManager(conf)
    val dir = Utils.createTempDir().getCanonicalFile
    Utils.createDirectory(dir)
    val file = new File(dir, "buffer")
    val queue = DiskRowQueue(file, 1, serManager)
    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](16), 16)
    // Few enough rows that they all sit in the output stream's buffer rather than reaching
    // the file, so that the flush below is observable.
    val n = 10
    var i = 0
    while (i < n) {
      row.setLong(0, i)
      assert(queue.add(row), "fail to add")
      i += 1
    }
    val bufferedLength = file.length()

    queue.closeOutputStream()
    assert(file.length() > bufferedLength, "closeOutputStream should flush the rows to disk")
    // Idempotent: remove() also calls it, and a queue may be completed more than once.
    queue.closeOutputStream()
    // The queue is complete, so it takes no more rows...
    assert(!queue.add(row), "should not accept rows once the output stream is closed")

    // ...but is still fully readable. Reading must not be gated on the output stream still
    // being open, or the first read of a completed queue fails.
    i = 0
    while (i < n) {
      val returned = queue.remove()
      assert(returned != null, "fail to poll")
      assert(returned.getLong(0) == i, "does not match")
      i += 1
    }
    assert(queue.remove() == null, "should be empty")
    queue.close()
  }

  encryptionTest("SPARK-59601: spilling does not hold an output stream per spilled queue") {
    conf =>
    val serManager = createSerializerManager(conf)
    val mem = new TestMemoryManager(conf)
    mem.limit(4<<10)
    val taskM = new TaskMemoryManager(mem, 0)
    val dir = Utils.createTempDir().getCanonicalFile
    val queue = HybridRowQueue(taskM, dir, 1, serManager)
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
    val bytesAfterSpill = dir.listFiles().map(_.length()).sum
    assert(bytesAfterSpill > 0, "spilling should have written queues to disk")

    // Reading the first row opens the first spilled queue for reading. If that queue were
    // still holding an open output stream, this first read would be what finally closes and
    // flushes it, and the file would grow. Every spilled queue is complete at spill time, so
    // nothing should still be buffered by then and the on-disk size must not change here.
    val firstRow = queue.remove()
    assert(firstRow != null, "fail to poll")
    assert(firstRow.getLong(0) == 0, "does not match")
    val bytesAfterFirstRead = dir.listFiles().map(_.length()).sum
    assert(bytesAfterFirstRead == bytesAfterSpill,
      s"spilled rows were still buffered in an open output stream: on-disk bytes grew from " +
        s"$bytesAfterSpill to $bytesAfterFirstRead on the first read")

    i = 1
    while (i < n) {
      val returned = queue.remove()
      assert(returned != null, "fail to poll")
      assert(returned.getLong(0) == i, "does not match")
      i += 1
    }
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
}
