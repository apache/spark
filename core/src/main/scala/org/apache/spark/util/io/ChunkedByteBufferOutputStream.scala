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

import java.io.OutputStream
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.StorageUtils

/**
 * An OutputStream that writes to fixed-size chunks of byte arrays.
 *
 * @param chunkSize size of each chunk, in bytes.
 */
private[spark] class ChunkedByteBufferOutputStream(
    chunkSize: Int,
    allocator: Int => ByteBuffer)
  extends OutputStream {

  private[this] var toChunkedByteBufferWasCalled = false

  private val chunks = new ArrayBuffer[ByteBuffer]

  /** Index of the last chunk. Starting with -1 when the chunks array is empty. */
  private[this] var lastChunkIndex = -1

  /**
   * Next position to write in the last chunk.
   *
   * If this equals chunkSize, it means for next write we need to allocate a new chunk.
   * This can also never be 0.
   */
  private[this] var position = chunkSize
  private[this] var _size = 0

  def size: Long = _size

  override def write(b: Int): Unit = {
    allocateNewChunkIfNeeded()
    chunks(lastChunkIndex).put(b.toByte)
    position += 1
    _size += 1
  }

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    var written = 0
    while (written < len) {
      allocateNewChunkIfNeeded()
      val thisBatch = math.min(chunkSize - position, len - written)
      chunks(lastChunkIndex).put(bytes, written + off, thisBatch)
      written += thisBatch
      position += thisBatch
    }
    _size += len
  }

  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    require(!toChunkedByteBufferWasCalled, "cannot write after toChunkedByteBuffer() is called")
    if (position == chunkSize) {
      chunks += allocator(chunkSize)
      lastChunkIndex += 1
      position = 0
    }
  }

  def toChunkedByteBuffer: ChunkedByteBuffer = {
    require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
    toChunkedByteBufferWasCalled = true
    if (lastChunkIndex == -1) {
      new ChunkedByteBuffer(Array.empty[ByteBuffer])
    } else {
      // Copy the first n-1 chunks to the output, and then create an array that fits the last chunk.
      // An alternative would have been returning an array of ByteBuffers, with the last buffer
      // bounded to only the last chunk's position. However, given our use case in Spark (to put
      // the chunks in block manager), only limiting the view bound of the buffer would still
      // require the block manager to store the whole chunk.
      val ret = new Array[ByteBuffer](chunks.size)
      for (i <- 0 until chunks.size - 1) {
        ret(i) = chunks(i)
        ret(i).flip()
      }
      if (position == chunkSize) {
        ret(lastChunkIndex) = chunks(lastChunkIndex)
        ret(lastChunkIndex).flip()
      } else {
        ret(lastChunkIndex) = allocator(position)
        chunks(lastChunkIndex).flip()
        ret(lastChunkIndex).put(chunks(lastChunkIndex))
        ret(lastChunkIndex).flip()
        StorageUtils.dispose(chunks(lastChunkIndex))
      }
      new ChunkedByteBuffer(ret)
    }
  }
}
