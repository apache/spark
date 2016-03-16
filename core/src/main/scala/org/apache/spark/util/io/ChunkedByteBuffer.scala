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
import org.apache.spark.storage.StorageUtils

private[spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) {
  require(chunks != null, "chunks must not be null")
  require(chunks.forall(_.limit() > 0), "chunks must be non-empty")
  require(chunks.forall(_.position() == 0), "chunks' positions must be 0")

  val limit: Long = chunks.map(_.limit().asInstanceOf[Long]).sum

  def this(byteBuffer: ByteBuffer) = {
    this(Array(byteBuffer))
  }

  def writeFully(channel: WritableByteChannel): Unit = {
    for (bytes <- getChunks()) {
      while (bytes.remaining > 0) {
        channel.write(bytes)
      }
    }
  }

  def toNetty: ByteBuf = {
    Unpooled.wrappedBuffer(getChunks(): _*)
  }

  /**
   * Copy this buffer into a new byte array.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the maximum array size.
   */
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

  /**
   * Copy this buffer into a new ByteBuffer.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the max ByteBuffer size.
   */
  def toByteBuffer: ByteBuffer = {
    if (chunks.length == 1) {
      chunks.head.duplicate()
    } else {
      ByteBuffer.wrap(toArray)
    }
  }

  def toInputStream(dispose: Boolean = false): InputStream = {
    new ChunkedByteBufferInputStream(this, dispose)
  }

  def getChunks(): Array[ByteBuffer] = {
    chunks.map(_.duplicate())
  }

  def copy(): ChunkedByteBuffer = {
    val copiedChunks = getChunks().map { chunk =>
      // TODO: accept an allocator in this copy method to integrate with mem. accounting systems
      val newChunk = ByteBuffer.allocate(chunk.limit())
      newChunk.put(chunk)
      newChunk.flip()
      newChunk
    }
    new ChunkedByteBuffer(copiedChunks)
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(): Unit = {
    chunks.foreach(StorageUtils.dispose)
  }
}

/**
 * Reads data from a ChunkedByteBuffer, and optionally cleans it up using StorageUtils.dispose()
 * at the end of the stream (e.g. to close a memory-mapped file).
 */
private class ChunkedByteBufferInputStream(
    var chunkedByteBuffer: ChunkedByteBuffer,
    dispose: Boolean)
  extends InputStream {

  private[this] var chunks = chunkedByteBuffer.getChunks().iterator
  private[this] var currentChunk: ByteBuffer = {
    if (chunks.hasNext) {
      chunks.next()
    } else {
      null
    }
  }

  override def read(): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      StorageUtils.dispose(currentChunk)
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      UnsignedBytes.toInt(currentChunk.get())
    } else {
      close()
      -1
    }
  }

  // TODO(josh): implement
//  override def read(b: Array[Byte]): Int = super.read(b)
//  override def read(b: Array[Byte], off: Int, len: Int): Int = super.read(b, off, len)
//  override def skip(n: Long): Long = super.skip(n)

  override def close(): Unit = {
    if (currentChunk != null) {
      if (dispose) {
        chunkedByteBuffer.dispose()
      }
    }
    chunkedByteBuffer = null
    chunks = null
    currentChunk = null
  }
}
