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

import scala.collection.mutable.ArrayBuffer


/**
 * An OutputStream that writes to fixed-size chunks of byte arrays.
 *
 * @param chunkSize size of each chunk, in bytes.
 */
private[spark]
class ByteArrayChunkOutputStream(chunkSize: Int) extends OutputStream {

  private val chunks = new ArrayBuffer[Array[Byte]]

  /** Index of the last chunk. Starting with -1 when the chunks array is empty. */
  private var lastChunkIndex = -1

  /**
   * Next position to write in the last chunk.
   *
   * If this equals chunkSize, it means for next write we need to allocate a new chunk.
   * This can also never be 0.
   */
  private var position = chunkSize

  private[spark] var size: Long = 0L

  override def write(b: Int): Unit = {
    allocateNewChunkIfNeeded()
    chunks(lastChunkIndex)(position) = b.toByte
    position += 1
    size += 1
  }

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    var written = 0
    while (written < len) {
      allocateNewChunkIfNeeded()
      val thisBatch = math.min(chunkSize - position, len - written)
      System.arraycopy(bytes, written + off, chunks(lastChunkIndex), position, thisBatch)
      written += thisBatch
      position += thisBatch
    }
    size += len
  }

  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    if (position == chunkSize) {
      chunks += new Array[Byte](chunkSize)
      lastChunkIndex += 1
      position = 0
    }
  }

  def toArrays: Array[Array[Byte]] = {
    if (lastChunkIndex == -1) {
      new Array[Array[Byte]](0)
    } else {
      // Copy the first n-1 chunks to the output, and then create an array that fits the last chunk.
      // An alternative would have been returning an array of ByteBuffers, with the last buffer
      // bounded to only the last chunk's position. However, given our use case in Spark (to put
      // the chunks in block manager), only limiting the view bound of the buffer would still
      // require the block manager to store the whole chunk.
      val ret = new Array[Array[Byte]](chunks.size)
      for (i <- 0 until chunks.size - 1) {
        ret(i) = chunks(i)
      }
      if (position == chunkSize) {
        ret(lastChunkIndex) = chunks(lastChunkIndex)
      } else {
        ret(lastChunkIndex) = new Array[Byte](position)
        System.arraycopy(chunks(lastChunkIndex), 0, ret(lastChunkIndex), 0, position)
      }
      ret
    }
  }

  /**
   * Get a copy of the data between the two endpoints, start <= idx < until.  Always returns
   * an array of size (until - start).  Throws an IllegalArgumentException if
   * 0 <= start <= until <= size
   */
  def slice(start: Long, until: Long): Array[Byte] = {
    require((until - start) < Integer.MAX_VALUE, "max slice length = Integer.MAX_VALUE")
    require(start >= 0 && start <= until, s"start ($start) must be >= 0 and <= until ($until)")
    require(until >= start && until <= size,
      s"until ($until) must be >= start ($start) and <= size ($size)")
    var chunkStart = 0L
    var chunkIdx = 0
    val length = (until - start).toInt
    var foundStart = false
    val result = new Array[Byte](length)
    while (!foundStart) {
      val nextChunkStart = chunkStart + chunks(chunkIdx).size
      if (nextChunkStart > start) {
        foundStart = true
      } else {
        chunkStart = nextChunkStart
        chunkIdx += 1
      }
    }

    var remaining = length
    var pos = 0
    var offsetInChunk = (start - chunkStart).toInt
    while (remaining > 0) {
      val lenToCopy = math.min(remaining, chunks(chunkIdx).size - offsetInChunk)
      System.arraycopy(chunks(chunkIdx), offsetInChunk, result, pos, lenToCopy)
      chunkIdx += 1
      offsetInChunk = 0
      pos += lenToCopy
      remaining -= lenToCopy
    }
    result
  }


}
