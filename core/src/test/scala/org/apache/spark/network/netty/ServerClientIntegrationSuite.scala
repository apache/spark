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
import java.util.concurrent.Semaphore

import scala.collection.JavaConversions._

import io.netty.buffer.Unpooled

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.network.netty.client.{ReferenceCountedBuffer, BlockFetchingClientFactory}
import org.apache.spark.network.netty.server.BlockServer
import org.apache.spark.storage.{FileSegment, BlockDataProvider}


class ServerClientIntegrationSuite extends FunSuite with BeforeAndAfterAll {

  val bufSize = 100000
  var buf: ByteBuffer = _
  var testFile: File = _
  var server: BlockServer = _
  var clientFactory: BlockFetchingClientFactory = _

  val bufferBlockId = "buffer_block"
  val fileBlockId = "file_block"

  override def beforeAll() = {
    buf = ByteBuffer.allocate(bufSize)
    for (i <- 1 to bufSize) {
      buf.put(i.toByte)
    }
    buf.flip()

    val url = Thread.currentThread.getContextClassLoader.getResource("netty-test-file.txt")
    testFile = new File(url.toURI)

    server = new BlockServer(new SparkConf, new BlockDataProvider {
      override def getBlockData(blockId: String): Either[FileSegment, ByteBuffer] = {
        if (blockId == bufferBlockId) {
          Right(buf)
        } else if (blockId == fileBlockId) {
          Left(new FileSegment(testFile, 10, testFile.length - 25))
        } else {
          throw new Exception("Unknown block id " + blockId)
        }
      }
    })

    clientFactory = new BlockFetchingClientFactory(new SparkConf)
  }

  override def afterAll() = {
    server.stop()
    clientFactory.stop()
  }

  lazy val byteBufferBlockReference = Unpooled.wrappedBuffer(buf)

  lazy val fileBlockReference = {
    val bytes = new Array[Byte](testFile.length.toInt)
    val fp = new RandomAccessFile(testFile, "r")
    fp.read(bytes)
    fp.close()
    Unpooled.wrappedBuffer(bytes, 10, testFile.length.toInt - 25)
  }

  test("fetch a ByteBuffer block") {
    val client = clientFactory.createClient(server.hostName, server.port)
    val sem = new Semaphore(0)
    var receivedBlockId: String = null
    var receivedBuffer = null.asInstanceOf[ReferenceCountedBuffer]

    client.fetchBlocks(
      Seq(bufferBlockId),
      (blockId, buf) => {
        receivedBlockId = blockId
        buf.retain()
        receivedBuffer = buf
        sem.release()
      },
      (blockId, errorMsg) => sem.release()
    )

    // This should block until the blocks are fetched
    sem.acquire()

    assert(receivedBlockId === bufferBlockId)
    assert(receivedBuffer.underlying == byteBufferBlockReference)
    receivedBuffer.release()
    client.close()
  }

  test("fetch a FileSegment block via zero-copy send") {
    val client = clientFactory.createClient(server.hostName, server.port)
    val sem = new Semaphore(0)
    var receivedBlockId: String = null
    var receivedBuffer = null.asInstanceOf[ReferenceCountedBuffer]

    client.fetchBlocks(
      Seq(fileBlockId),
      (blockId, buf) => {
        receivedBlockId = blockId
        buf.retain()
        receivedBuffer = buf
        sem.release()
      },
      (blockId, errorMsg) => sem.release()
    )

    // This should block until the blocks are fetched
    sem.acquire()

    assert(receivedBlockId === fileBlockId)
    assert(receivedBuffer.underlying == fileBlockReference)
    receivedBuffer.release()
    client.close()
  }

  test("fetch both ByteBuffer block and FileSegment block") {
    val client = clientFactory.createClient(server.hostName, server.port)
    val sem = new Semaphore(0)
    val receivedBlockIds = Collections.synchronizedSet(new HashSet[String])
    val receivedBuffers = Collections.synchronizedSet(new HashSet[ReferenceCountedBuffer])

    client.fetchBlocks(
      Seq(bufferBlockId, fileBlockId),
      (blockId, buf) => {
        receivedBlockIds.add(blockId)
        buf.retain()
        receivedBuffers.add(buf)
        sem.release()
      },
      (blockId, errorMsg) => sem.release()
    )

    sem.acquire(2)
    assert(receivedBlockIds.contains(bufferBlockId))
    assert(receivedBlockIds.contains(fileBlockId))

    val byteBufferReference = byteBufferBlockReference
    val fileReference = fileBlockReference

    assert(receivedBuffers.map(_.underlying) === Set(byteBufferReference, fileReference))
    receivedBuffers.foreach(_.release())
    client.close()
  }
}
