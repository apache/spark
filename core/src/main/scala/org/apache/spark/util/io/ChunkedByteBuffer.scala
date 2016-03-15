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

package org.apache.spark.util.io

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import com.google.common.primitives.UnsignedBytes
import io.netty.buffer.{ByteBuf, Unpooled}

import org.apache.spark.network.util.ByteArrayWritableChannel
import org.apache.spark.storage.BlockManager

private[spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) {
  require(chunks != null, "chunks must not be null")
  require(chunks.nonEmpty, "Cannot create a ChunkedByteBuffer with no chunks")
  require(chunks.forall(_.limit() > 0), "chunks must be non-empty")
  require(chunks.forall(_.position() == 0), "chunks' positions must be 0")

  val limit: Long = chunks.map(_.limit().asInstanceOf[Long]).sum

  def this(byteBuffer: ByteBuffer) = {
    this(Array(byteBuffer))
  }

  def writeFully(channel: WritableByteChannel): Unit = {
    assertNotDisposed()
    for (bytes <- getChunks()) {
      while (bytes.remaining > 0) {
        channel.write(bytes)
      }
    }
  }

  def toNetty: ByteBuf = {
    assertNotDisposed()
    Unpooled.wrappedBuffer(getChunks(): _*)
  }

  def toArray: Array[Byte] = {
    assertNotDisposed()
    if (limit >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
        s"cannot call toArray because buffer size ($limit bytes) exceeds maximum array size")
    }
    val byteChannel = new ByteArrayWritableChannel(limit.toInt)
    writeFully(byteChannel)
    byteChannel.close()
    byteChannel.getData
  }

  def toInputStream: InputStream = {
    assertNotDisposed()
    new ChunkedByteBufferInputStream(getChunks().iterator)
  }

  def toDestructiveInputStream: InputStream = {
    val is = new ChunkedByteBufferInputStream(chunks.iterator)
    chunks = null
    is
  }

  def getChunks(): Array[ByteBuffer] = {
    assertNotDisposed()
    chunks.map(_.duplicate())
  }

  def copy(): ChunkedByteBuffer = {
    assertNotDisposed()
    val copiedChunks = getChunks().map { chunk =>
      // TODO: accept an allocator in this copy method to integrate with mem. accounting systems
      val newChunk = ByteBuffer.allocate(chunk.limit())
      newChunk.put(chunk)
      newChunk.flip()
      newChunk
    }
    new ChunkedByteBuffer(copiedChunks)
  }

  def dispose(): Unit = {
    assertNotDisposed()
    chunks.foreach(BlockManager.dispose)
    chunks = null
  }

  private def assertNotDisposed(): Unit = {
    if (chunks == null) {
      throw new IllegalStateException("Cannot call methods on a disposed ChunkedByteBuffer")
    }
  }
}

private class ChunkedByteBufferInputStream(chunks: Iterator[ByteBuffer]) extends InputStream {

  private[this] var currentChunk: ByteBuffer = chunks.next()

  override def available(): Int = {
    while (!currentChunk.hasRemaining && chunks.hasNext) {
      BlockManager.dispose(currentChunk)
      currentChunk = chunks.next()
    }
    currentChunk.remaining()
  }

//  override def skip(n: Long): Long = {
//    // TODO(josh): check contract
//    var i = n
//    while (i > 0) {
//      read()
//      i -= 1
//    }
//    n
//  }

  override def read(): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      BlockManager.dispose(currentChunk)
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      UnsignedBytes.toInt(currentChunk.get())
    } else {
      BlockManager.dispose(currentChunk)
      currentChunk = null
      -1
    }
  }

  // TODO(josh): implement
//  override def read(b: Array[Byte]): Int = super.read(b)
//
//  override def read(b: Array[Byte], off: Int, len: Int): Int = super.read(b, off, len)

  override def close(): Unit = {
    if (currentChunk != null) {
      BlockManager.dispose(currentChunk)
      while (chunks.hasNext) {
        currentChunk = chunks.next()
        BlockManager.dispose(currentChunk)
      }
    }
    currentChunk = null
  }
}
