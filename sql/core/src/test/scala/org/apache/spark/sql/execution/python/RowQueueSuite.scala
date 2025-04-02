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
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.security.{CryptoStreamUtils, EncryptionFunSuite}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.memory.MemoryBlock
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
    dir.mkdirs()
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
        assert(queue.add(row), "fail to add")
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
        assert(queue.add(row), "fail to add")
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
