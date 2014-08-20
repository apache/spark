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
 */
private[spark]
class ByteArrayChunkOutputStream(chunkSize: Int) extends OutputStream {

  private val chunks = new ArrayBuffer[Array[Byte]]

  private var latestChunkIndex = -1
  private var position = chunkSize

  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    if (position == chunkSize) {
      chunks += new Array[Byte](chunkSize)
      latestChunkIndex += 1
      position = 0
    }
  }

  def toArrays: Array[Array[Byte]] = {
    if (latestChunkIndex == -1) {
      new Array[Array[Byte]](0)
    } else {
      val ret = new Array[Array[Byte]](chunks.size)
      for (i <- 0 to chunks.size - 1) {
        ret(i) = chunks(i)
      }
      if (position == chunkSize) {
        ret(latestChunkIndex) = chunks(latestChunkIndex)
      } else {
        ret(latestChunkIndex) = new Array[Byte](position)
        System.arraycopy(chunks(latestChunkIndex), 0, ret(latestChunkIndex), 0, position)
      }
      ret
    }
  }

  override def write(b: Int): Unit = {
    allocateNewChunkIfNeeded()
    chunks(latestChunkIndex)(position) = b.toByte
    position += 1
  }

  override def write(bytes: Array[Byte]): Unit = write(bytes, 0, bytes.length)

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    var written = off
    val total = off + len
    while (written < total) {
      allocateNewChunkIfNeeded()
      val thisBatch = math.min(chunkSize - position, total - written)
      System.arraycopy(bytes, written, chunks(latestChunkIndex), position, thisBatch)
      written += thisBatch
      position += thisBatch
    }
  }
}
