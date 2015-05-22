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

package org.apache.spark.util.collection

import java.io.OutputStream

import scala.collection.mutable.ArrayBuffer

/**
 * A logical byte buffer that wraps a list of byte arrays. All the byte arrays have equal size. The
 * advantage of this over a standard ArrayBuffer is that it can grow without claiming large amounts
 * of memory and needing to copy the full contents. The disadvantage is that the contents don't
 * occupy a contiguous segment of memory.
 */
private[spark] class ChainedBuffer(chunkSize: Int) {
  private val chunkSizeLog2 = (math.log(chunkSize) / math.log(2)).toInt
  assert(math.pow(2, chunkSizeLog2).toInt == chunkSize,
    s"ChainedBuffer chunk size $chunkSize must be a power of two")
  private val chunks: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]]()
  private var _size: Int = _

  /**
   * Feed bytes from this buffer into a BlockObjectWriter.
   *
   * @param pos Offset in the buffer to read from.
   * @param os OutputStream to read into.
   * @param len Number of bytes to read.
   */
  def read(pos: Int, os: OutputStream, len: Int): Unit = {
    if (pos + len > _size) {
      throw new IndexOutOfBoundsException(
        s"Read of $len bytes at position $pos would go past size ${_size} of buffer")
    }
    var chunkIndex = pos >> chunkSizeLog2
    var posInChunk = pos - (chunkIndex << chunkSizeLog2)
    var written = 0
    while (written < len) {
      val toRead = math.min(len - written, chunkSize - posInChunk)
      os.write(chunks(chunkIndex), posInChunk, toRead)
      written += toRead
      chunkIndex += 1
      posInChunk = 0
    }
  }

  /**
   * Read bytes from this buffer into a byte array.
   *
   * @param pos Offset in the buffer to read from.
   * @param bytes Byte array to read into.
   * @param offs Offset in the byte array to read to.
   * @param len Number of bytes to read.
   */
  def read(pos: Int, bytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (pos + len > _size) {
      throw new IndexOutOfBoundsException(
        s"Read of $len bytes at position $pos would go past size of buffer")
    }
    var chunkIndex = pos >> chunkSizeLog2
    var posInChunk = pos - (chunkIndex << chunkSizeLog2)
    var written = 0
    while (written < len) {
      val toRead = math.min(len - written, chunkSize - posInChunk)
      System.arraycopy(chunks(chunkIndex), posInChunk, bytes, offs + written, toRead)
      written += toRead
      chunkIndex += 1
      posInChunk = 0
    }
  }

  /**
   * Write bytes from a byte array into this buffer.
   *
   * @param pos Offset in the buffer to write to.
   * @param bytes Byte array to write from.
   * @param offs Offset in the byte array to write from.
   * @param len Number of bytes to write.
   */
  def write(pos: Int, bytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (pos > _size) {
      throw new IndexOutOfBoundsException(
        s"Write at position $pos starts after end of buffer ${_size}")
    }
    // Grow if needed
    val endChunkIndex = (pos + len - 1) >> chunkSizeLog2
    while (endChunkIndex >= chunks.length) {
      chunks += new Array[Byte](chunkSize)
    }

    var chunkIndex = pos >> chunkSizeLog2
    var posInChunk = pos - (chunkIndex << chunkSizeLog2)
    var written = 0
    while (written < len) {
      val toWrite = math.min(len - written, chunkSize - posInChunk)
      System.arraycopy(bytes, offs + written, chunks(chunkIndex), posInChunk, toWrite)
      written += toWrite
      chunkIndex += 1
      posInChunk = 0
    }

    _size = math.max(_size, pos + len)
  }

  /**
   * Total size of buffer that can be written to without allocating additional memory.
   */
  def capacity: Int = chunks.size * chunkSize

  /**
   * Size of the logical buffer.
   */
  def size: Int = _size
}

/**
 * Output stream that writes to a ChainedBuffer.
 */
private[spark] class ChainedBufferOutputStream(chainedBuffer: ChainedBuffer) extends OutputStream {
  private var pos = 0

  override def write(b: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  override def write(bytes: Array[Byte], offs: Int, len: Int): Unit = {
    chainedBuffer.write(pos, bytes, offs, len)
    pos += len
  }
}
