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

private[spark] class ChunkedByteBuffer(_chunks: Array[ByteBuffer]) {

  require(_chunks.nonEmpty, "Cannot create a ChunkedByteBuffer with no chunks")
  require(_chunks.forall(_.limit() > 0), "chunks must be non-empty")

  def this(byteBuffer: ByteBuffer) = {
    this(Array(byteBuffer))
  }

  private[this] val chunks: Array[ByteBuffer] = {
    _chunks.map(_.duplicate().rewind().asInstanceOf[ByteBuffer])  // doesn't actually copy bytes
  }

  val limit: Long = chunks.map(_.limit().asInstanceOf[Long]).sum

  def writeFully(channel: WritableByteChannel): Unit = {
    for (bytes <- getChunks()) {
      while (bytes.remaining > 0) {
        channel.write(bytes)
      }
    }
  }

  def toNetty: ByteBuf = Unpooled.wrappedBuffer(getChunks(): _*)

  def toArray: Array[Byte] = {
    if (limit >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
        s"cannot call toArray because buffer size ($limit bytes) exceeds maximum array size")
    }
    val byteChannel = new ByteArrayWritableChannel(limit.toInt)
    writeFully(byteChannel)
    byteChannel.close()
    byteChannel.getData
  }

  def toInputStream(dispose: Boolean): InputStream = new ChunkedByteBufferInputStream(this, dispose)

  def getChunks(): Array[ByteBuffer] = chunks.map(_.duplicate())

  def copy(): ChunkedByteBuffer = {
    val copiedChunks = getChunks().map { chunk =>
      // TODO: accept an allocator in this copy method, etc.
      val newChunk = ByteBuffer.allocate(chunk.limit())
      newChunk.put(chunk)
    }
    new ChunkedByteBuffer(copiedChunks)
  }

  def dispose(): Unit = {
    chunks.foreach(BlockManager.dispose)
  }
}


// TODO(josh): implement dispose

private class ChunkedByteBufferInputStream(
    chunkedBuffer: ChunkedByteBuffer,
    dispose: Boolean = false) extends InputStream {

  private[this] val chunksIterator: Iterator[ByteBuffer] = chunkedBuffer.getChunks().iterator
  private[this] var currentChunk: ByteBuffer = chunksIterator.next()
  assert(currentChunk.position() == 0)

  override def available(): Int = {
    while (!currentChunk.hasRemaining && chunksIterator.hasNext) {
      currentChunk = chunksIterator.next()
      assert(currentChunk.position() == 0)
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
    if (!currentChunk.hasRemaining && chunksIterator.hasNext) {
      currentChunk = chunksIterator.next()
      assert(currentChunk.position() == 0)
    }
    if (currentChunk.hasRemaining) {
      UnsignedBytes.toInt(currentChunk.get())
    } else {
      -1
    }
  }

  // TODO(josh): implement
//  override def read(b: Array[Byte]): Int = super.read(b)
//
//  override def read(b: Array[Byte], off: Int, len: Int): Int = super.read(b, off, len)

  override def close(): Unit = {
    currentChunk = null
  }
}
