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

package org.apache.spark.network.netty.server

import java.io.{RandomAccessFile, File}
import java.nio.ByteBuffer

import io.netty.buffer.{Unpooled, ByteBuf}
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler, DefaultFileRegion}
import io.netty.channel.embedded.EmbeddedChannel

import org.scalatest.FunSuite

import org.apache.spark.storage.{BlockDataProvider, FileSegment}


class BlockServerHandlerSuite extends FunSuite {

  test("ByteBuffer block") {
    val expectedBlockId = "test_bytebuffer_block"
    val buf = ByteBuffer.allocate(10000)
    for (i <- 1 to 10000) {
      buf.put(i.toByte)
    }
    buf.flip()

    val channel = new EmbeddedChannel(new BlockServerHandler(new BlockDataProvider {
      override def getBlockData(blockId: String): Either[FileSegment, ByteBuffer] = Right(buf)
    }))

    channel.writeInbound(expectedBlockId)
    assert(channel.outboundMessages().size === 2)

    val out1 = channel.readOutbound().asInstanceOf[BlockHeader]
    val out2 = channel.readOutbound().asInstanceOf[ByteBuf]

    assert(out1.blockId === expectedBlockId)
    assert(out1.blockSize === buf.remaining)
    assert(out1.error === None)

    assert(out2.equals(Unpooled.wrappedBuffer(buf)))

    channel.close()
  }

  test("FileSegment block via zero-copy") {
    val expectedBlockId = "test_file_block"

    // Create random file data
    val fileContent = new Array[Byte](1024)
    scala.util.Random.nextBytes(fileContent)
    val testFile = File.createTempFile("netty-test-file", "txt")
    val fp = new RandomAccessFile(testFile, "rw")
    fp.write(fileContent)
    fp.close()

    val channel = new EmbeddedChannel(new BlockServerHandler(new BlockDataProvider {
      override def getBlockData(blockId: String): Either[FileSegment, ByteBuffer] = {
        Left(new FileSegment(testFile, 15, testFile.length - 25))
      }
    }))

    channel.writeInbound(expectedBlockId)
    assert(channel.outboundMessages().size === 2)

    val out1 = channel.readOutbound().asInstanceOf[BlockHeader]
    val out2 = channel.readOutbound().asInstanceOf[DefaultFileRegion]

    assert(out1.blockId === expectedBlockId)
    assert(out1.blockSize === testFile.length - 25)
    assert(out1.error === None)

    assert(out2.count === testFile.length - 25)
    assert(out2.position === 15)
  }

  test("pipeline exception propagation") {
    val blockServerHandler = new BlockServerHandler(new BlockDataProvider {
      override def getBlockData(blockId: String): Either[FileSegment, ByteBuffer] = ???
    })
    val exceptionHandler = new SimpleChannelInboundHandler[String]() {
      override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
        throw new Exception("this is an error")
      }
    }

    val channel = new EmbeddedChannel(exceptionHandler, blockServerHandler)
    assert(channel.isOpen)
    channel.writeInbound("a message to trigger the error")
    assert(!channel.isOpen)
  }
}
