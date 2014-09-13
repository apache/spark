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

package org.apache.spark.network.netty

import java.io.{RandomAccessFile, File}
import java.nio.ByteBuffer
import java.util.{Collections, HashSet}
import java.util.concurrent.{TimeUnit, Semaphore}

import scala.collection.JavaConversions._

import io.netty.buffer.Unpooled

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.Span
import org.scalatest.time.Seconds

import org.apache.spark.SparkConf
import org.apache.spark.network._
import org.apache.spark.storage.{BlockNotFoundException, StorageLevel}


/**
* Test cases that create real clients and servers and connect.
*/
class ServerClientIntegrationSuite extends FunSuite with BeforeAndAfterAll {

  val bufSize = 100000
  var buf: ByteBuffer = _
  var testFile: File = _
  var server: BlockServer = _
  var clientFactory: BlockClientFactory = _

  val bufferBlockId = "buffer_block"
  val fileBlockId = "file_block"

  val fileContent = new Array[Byte](1024)
  scala.util.Random.nextBytes(fileContent)

  override def beforeAll() = {
    buf = ByteBuffer.allocate(bufSize)
    for (i <- 1 to bufSize) {
      buf.put(i.toByte)
    }
    buf.flip()

    testFile = File.createTempFile("netty-test-file", "txt")
    val fp = new RandomAccessFile(testFile, "rw")
    fp.write(fileContent)
    fp.close()

    server = new BlockServer(new NettyConfig(new SparkConf), new BlockDataManager {
      override def getBlockData(blockId: String): ManagedBuffer = {
        if (blockId == bufferBlockId) {
          new NioManagedBuffer(buf)
        } else if (blockId == fileBlockId) {
          new FileSegmentManagedBuffer(testFile, 10, testFile.length - 25)
        } else {
          throw new BlockNotFoundException(blockId)
        }
      }

      /**
       * Put the block locally, using the given storage level.
       */
      def putBlockData(blockId: String, data: ManagedBuffer, level: StorageLevel): Unit = ???
    })

    clientFactory = new BlockClientFactory(new SparkConf)
  }

  override def afterAll() = {
    server.close()
    clientFactory.close()
  }

  /** A ByteBuf for buffer_block */
  lazy val byteBufferBlockReference = Unpooled.wrappedBuffer(buf)

  /** A ByteBuf for file_block */
  lazy val fileBlockReference = Unpooled.wrappedBuffer(fileContent, 10, fileContent.length - 25)

  def fetchBlocks(blockIds: Seq[String]): (Set[String], Set[ManagedBuffer], Set[String]) = {
    val client = clientFactory.createClient(server.hostName, server.port)
    val sem = new Semaphore(0)
    val receivedBlockIds = Collections.synchronizedSet(new HashSet[String])
    val errorBlockIds = Collections.synchronizedSet(new HashSet[String])
    val receivedBuffers = Collections.synchronizedSet(new HashSet[ManagedBuffer])

    client.fetchBlocks(
      blockIds,
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          errorBlockIds.add(blockId)
          sem.release()
        }

        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          receivedBlockIds.add(blockId)
          receivedBuffers.add(data)
          sem.release()
        }
      }
    )
    if (!sem.tryAcquire(blockIds.size, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server")
    }
    client.close()
    (receivedBlockIds.toSet, receivedBuffers.toSet, errorBlockIds.toSet)
  }

  test("fetch a ByteBuffer block") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(bufferBlockId))
    assert(blockIds === Set(bufferBlockId))
    assert(buffers.map(_.convertToNetty()) === Set(byteBufferBlockReference))
    assert(failBlockIds.isEmpty)
  }

  test("fetch a FileSegment block via zero-copy send") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(fileBlockId))
    assert(blockIds === Set(fileBlockId))
    assert(buffers.map(_.convertToNetty()) === Set(fileBlockReference))
    assert(failBlockIds.isEmpty)
  }

  test("fetch a non-existent block") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq("random-block"))
    assert(blockIds.isEmpty)
    assert(buffers.isEmpty)
    assert(failBlockIds === Set("random-block"))
  }

  test("fetch both ByteBuffer block and FileSegment block") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(bufferBlockId, fileBlockId))
    assert(blockIds === Set(bufferBlockId, fileBlockId))
    assert(buffers.map(_.convertToNetty()) === Set(byteBufferBlockReference, fileBlockReference))
    assert(failBlockIds.isEmpty)
  }

  test("fetch both ByteBuffer block and a non-existent block") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(bufferBlockId, "random-block"))
    assert(blockIds === Set(bufferBlockId))
    assert(buffers.map(_.convertToNetty()) === Set(byteBufferBlockReference))
    assert(failBlockIds === Set("random-block"))
  }

  test("shutting down server should also close client") {
    val client = clientFactory.createClient(server.hostName, server.port)
    server.close()
    eventually(timeout(Span(5, Seconds))) { assert(!client.isActive) }
  }
}
