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
 * of memory and needing to copy the full contents.
 */
private[spark] class ChainedBuffer private(val chunks: ArrayBuffer[Array[Byte]], chunkSize: Int) {
  private val chunkSizeLog2 = (math.log(chunkSize) / math.log(2)).toInt
  assert(math.pow(2, chunkSizeLog2).toInt == chunkSize)
  private var _size: Long = _

  /**
   * Read bytes from this buffer into a byte array.
   *
   * @param pos Offset in the buffer to read from.
   * @param bytes Byte array to read into.
   * @param offs Offset in the byte array to read to.
   * @param len Number of bytes to read.
   */
  def read(pos: Long, bytes: Array[Byte], offs: Int, len: Int): Unit = {
    var chunkIndex = (pos >> chunkSizeLog2).toInt
    var posInChunk = (pos - (chunkIndex << chunkSizeLog2)).toInt
    var moved = 0
    while (moved < len) {
      val toRead = math.min(len - moved, chunkSize - posInChunk)
      System.arraycopy(chunks(chunkIndex), posInChunk, bytes, offs + moved, toRead)
      moved += toRead
      chunkIndex += 1
      posInChunk = 0
    }
  }

  def read(pos:Long): Byte = {
    val chunkIndex = (pos >> chunkSizeLog2).toInt
    val posInChunk = (pos - (chunkIndex << chunkSizeLog2)).toInt
    chunks(chunkIndex)(posInChunk)
  }

  /**
   * Write bytes from a byte array into this buffer.
   *
   * @param pos Offset in the buffer to write to.
   * @param bytes Byte array to write from.
   * @param offs Offset in the byte array to write from.
   * @param len Number of bytes to write.
   */
  def write(pos: Long, bytes: Array[Byte], offs: Int, len: Int): Unit = {
    // Grow if needed
    val endChunkIndex = ((pos + len - 1) >> chunkSizeLog2).toInt
    while (endChunkIndex >= chunks.length) {
      chunks += new Array[Byte](chunkSize)
    }

    var chunkIndex = (pos >> chunkSizeLog2).toInt
    var posInChunk = (pos - (chunkIndex << chunkSizeLog2)).toInt
    var moved = 0
    while (moved < len) {
      val toWrite = math.min(len - moved, chunkSize - posInChunk)
      System.arraycopy(bytes, offs + moved, chunks(chunkIndex), posInChunk, toWrite)
      moved += toWrite
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
  def size: Long = _size
}

private[spark] object ChainedBuffer {
  def withInitialSize(chunkSize: Int, minInitialSize: Long = 0): ChainedBuffer = {
    val nChunks = (((minInitialSize - 1) / chunkSize).toInt) + 1
    val chunks = new ArrayBuffer[Array[Byte]](nChunks)
    (0 until nChunks).foreach{idx => chunks(idx) = new Array[Byte](chunkSize)}
    new ChainedBuffer(chunks, chunkSize)
  }
}

/**
 * Output stream that writes to a ChainedBuffer.
 */
private[spark] class ChainedBufferOutputStream(chainedBuffer: ChainedBuffer) extends OutputStream {
  private var _pos = 0

  override def write(b: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  override def write(bytes: Array[Byte], offs: Int, len: Int): Unit = {
    chainedBuffer.write(_pos, bytes, offs, len)
    _pos += len
  }

  def pos: Int = _pos
}