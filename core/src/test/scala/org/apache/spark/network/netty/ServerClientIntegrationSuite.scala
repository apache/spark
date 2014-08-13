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

  /** A ByteBuf for buffer_block */
  lazy val byteBufferBlockReference = Unpooled.wrappedBuffer(buf)

  /** A ByteBuf for file_block */
  lazy val fileBlockReference = {
    val bytes = new Array[Byte](testFile.length.toInt)
    val fp = new RandomAccessFile(testFile, "r")
    fp.read(bytes)
    fp.close()
    Unpooled.wrappedBuffer(bytes, 10, testFile.length.toInt - 25)
  }

  def fetchBlocks(blockIds: Seq[String]): (Set[String], Set[ReferenceCountedBuffer], Set[String]) =
  {
    val client = clientFactory.createClient(server.hostName, server.port)
    val sem = new Semaphore(0)
    val receivedBlockIds = Collections.synchronizedSet(new HashSet[String])
    val errorBlockIds = Collections.synchronizedSet(new HashSet[String])
    val receivedBuffers = Collections.synchronizedSet(new HashSet[ReferenceCountedBuffer])

    client.fetchBlocks(
      blockIds,
      (blockId, buf) => {
        receivedBlockIds.add(blockId)
        buf.retain()
        receivedBuffers.add(buf)
        sem.release()
      },
      (blockId, errorMsg) => {
        errorBlockIds.add(blockId)
        sem.release()
      }
    )
    sem.acquire(blockIds.size)
    client.close()
    (receivedBlockIds.toSet, receivedBuffers.toSet, errorBlockIds.toSet)
  }

  test("fetch a ByteBuffer block") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(bufferBlockId))
    assert(blockIds === Set(bufferBlockId))
    assert(buffers.map(_.underlying) === Set(byteBufferBlockReference))
    assert(failBlockIds.isEmpty)
    buffers.foreach(_.release())
  }

  test("fetch a FileSegment block via zero-copy send") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(fileBlockId))
    assert(blockIds === Set(fileBlockId))
    assert(buffers.map(_.underlying) === Set(fileBlockReference))
    assert(failBlockIds.isEmpty)
    buffers.foreach(_.release())
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
    assert(buffers.map(_.underlying) === Set(byteBufferBlockReference, fileBlockReference))
    assert(failBlockIds.isEmpty)
    buffers.foreach(_.release())
  }

  test("fetch both ByteBuffer block and a non-existent block") {
    val (blockIds, buffers, failBlockIds) = fetchBlocks(Seq(bufferBlockId, "random-block"))
    assert(blockIds === Set(bufferBlockId))
    assert(buffers.map(_.underlying) === Set(byteBufferBlockReference))
    assert(failBlockIds === Set("random-block"))
    buffers.foreach(_.release())
  }
}
