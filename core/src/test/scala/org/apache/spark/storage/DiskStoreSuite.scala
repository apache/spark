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

import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.{Arrays, Random}

import com.google.common.io.{ByteStreams, Files}
import io.netty.channel.FileRegion

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.network.util.{ByteArrayWritableChannel, JavaUtils}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

class DiskStoreSuite extends SparkFunSuite {

  test("reads of memory-mapped and non memory-mapped files are equivalent") {
    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf)

    // It will cause error when we tried to re-open the filestore and the
    // memory-mapped byte buffer tot he file has not been GC on Windows.
    assume(!Utils.isWindows)
    val confKey = config.STORAGE_MEMORY_MAP_THRESHOLD.key

    // Create a non-trivial (not all zeros) byte array
    val bytes = Array.tabulate[Byte](1000)(_.toByte)
    val byteBuffer = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))

    val blockId = BlockId("rdd_1_2")
    val diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)

    val diskStoreMapped = new DiskStore(conf.clone().set(confKey, "0"), diskBlockManager,
      securityManager)
    diskStoreMapped.putBytes(blockId, byteBuffer)
    val mapped = diskStoreMapped.getBytes(blockId).toByteBuffer()
    assert(diskStoreMapped.remove(blockId))

    val diskStoreNotMapped = new DiskStore(conf.clone().set(confKey, "1m"), diskBlockManager,
      securityManager)
    diskStoreNotMapped.putBytes(blockId, byteBuffer)
    val notMapped = diskStoreNotMapped.getBytes(blockId).toByteBuffer()

    // Not possible to do isInstanceOf due to visibility of HeapByteBuffer
    assert(notMapped.getClass.getName.endsWith("HeapByteBuffer"),
      "Expected HeapByteBuffer for un-mapped read")
    assert(mapped.isInstanceOf[MappedByteBuffer],
      "Expected MappedByteBuffer for mapped read")

    def arrayFromByteBuffer(in: ByteBuffer): Array[Byte] = {
      val array = new Array[Byte](in.remaining())
      in.get(array)
      array
    }

    assert(Arrays.equals(new ChunkedByteBuffer(mapped).toArray, bytes))
    assert(Arrays.equals(new ChunkedByteBuffer(notMapped).toArray, bytes))
  }

  test("block size tracking") {
    val conf = new SparkConf()
    val diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)
    val diskStore = new DiskStore(conf, diskBlockManager, new SecurityManager(conf))

    val blockId = BlockId("rdd_1_2")
    diskStore.put(blockId) { chan =>
      val buf = ByteBuffer.wrap(new Array[Byte](32))
      while (buf.hasRemaining()) {
        chan.write(buf)
      }
    }

    assert(diskStore.getSize(blockId) === 32L)
    diskStore.remove(blockId)
    assert(diskStore.getSize(blockId) === 0L)
  }

  test("blocks larger than 2gb") {
    val conf = new SparkConf()
      .set(config.MEMORY_MAP_LIMIT_FOR_TESTS.key, "10k")
    val diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)
    val diskStore = new DiskStore(conf, diskBlockManager, new SecurityManager(conf))

    val blockId = BlockId("rdd_1_2")
    diskStore.put(blockId) { chan =>
      val arr = new Array[Byte](1024)
      for {
        _ <- 0 until 20
      } {
        val buf = ByteBuffer.wrap(arr)
        while (buf.hasRemaining()) {
          chan.write(buf)
        }
      }
    }

    val blockData = diskStore.getBytes(blockId)
    assert(blockData.size == 20 * 1024)

    val chunkedByteBuffer = blockData.toChunkedByteBuffer(ByteBuffer.allocate)
    val chunks = chunkedByteBuffer.chunks
    assert(chunks.size === 2)
    for (chunk <- chunks) {
      assert(chunk.limit() === 10 * 1024)
    }

    val e = intercept[IllegalArgumentException]{
      blockData.toByteBuffer()
    }

    assert(e.getMessage ===
      s"requirement failed: can't create a byte buffer of size ${blockData.size}" +
      " since it exceeds 10.0 KiB.")
  }

  test("block data encryption") {
    val testData = new Array[Byte](128 * 1024)
    new Random().nextBytes(testData)

    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf, Some(CryptoStreamUtils.createKey(conf)))
    val diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)
    val diskStore = new DiskStore(conf, diskBlockManager, securityManager)

    val blockId = BlockId("rdd_1_2")
    diskStore.put(blockId) { chan =>
      val buf = ByteBuffer.wrap(testData)
      while (buf.hasRemaining()) {
        chan.write(buf)
      }
    }

    assert(diskStore.getSize(blockId) === testData.length)

    val diskData = Files.toByteArray(diskBlockManager.getFile(blockId.name))
    assert(!Arrays.equals(testData, diskData))

    val blockData = diskStore.getBytes(blockId)
    assert(blockData.isInstanceOf[EncryptedBlockData])
    assert(blockData.size === testData.length)
    Map(
      "input stream" -> readViaInputStream _,
      "chunked byte buffer" -> readViaChunkedByteBuffer _,
      "nio byte buffer" -> readViaNioBuffer _,
      "managed buffer" -> readViaManagedBuffer _
    ).foreach { case (name, fn) =>
      val readData = fn(blockData)
      assert(readData.length === blockData.size, s"Size of data read via $name did not match.")
      assert(Arrays.equals(testData, readData), s"Data read via $name did not match.")
    }
  }

  private def readViaInputStream(data: BlockData): Array[Byte] = {
    val is = data.toInputStream()
    try {
      ByteStreams.toByteArray(is)
    } finally {
      is.close()
    }
  }

  private def readViaChunkedByteBuffer(data: BlockData): Array[Byte] = {
    val buf = data.toChunkedByteBuffer(ByteBuffer.allocate _)
    try {
      buf.toArray
    } finally {
      buf.dispose()
    }
  }

  private def readViaNioBuffer(data: BlockData): Array[Byte] = {
    JavaUtils.bufferToArray(data.toByteBuffer())
  }

  private def readViaManagedBuffer(data: BlockData): Array[Byte] = {
    val region = data.toNetty().asInstanceOf[FileRegion]
    val byteChannel = new ByteArrayWritableChannel(data.size.toInt)

    while (region.transferred() < region.count()) {
      region.transferTo(byteChannel, region.transferred())
    }

    byteChannel.close()
    byteChannel.getData
  }

}
