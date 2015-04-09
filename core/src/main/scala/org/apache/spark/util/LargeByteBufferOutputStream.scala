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

package org.apache.spark.util

import java.io.OutputStream
import java.nio.ByteBuffer

import org.apache.spark.network.buffer.{LargeByteBufferHelper, WrappedLargeByteBuffer, LargeByteBuffer}
import org.apache.spark.util.io.ByteArrayChunkOutputStream

private[spark]
class LargeByteBufferOutputStream(chunkSize: Int = 65536)
  extends OutputStream {

  private[util] val output = new ByteArrayChunkOutputStream(chunkSize)

  override def write(b: Int): Unit = {
    output.write(b)
  }

  override def write(bytes: Array[Byte], offs: Int, len: Int): Unit = {
    output.write(bytes, offs, len)
  }

  def largeBuffer: LargeByteBuffer = {
    largeBuffer(LargeByteBufferHelper.MAX_CHUNK)
  }

  // exposed for testing
  private[util] def largeBuffer(maxChunk: Int): LargeByteBuffer = {
    // LargeByteBuffer is supposed to make a "best effort" to get all the data
    // in one nio.ByteBuffer, so we want to try to merge the smaller chunks together
    // as much as possible.  This is necessary b/c there are a number of parts of spark that
    // can only deal w/ one nio.ByteBuffer, and can't use a LargeByteBuffer yet.
    val totalSize = output.size
    val chunksNeeded = ((totalSize + maxChunk - 1) / maxChunk).toInt
    val chunks = new Array[Array[Byte]](chunksNeeded)
    var remaining = totalSize
    var pos = 0
    (0 until chunksNeeded).foreach { idx =>
      val nextSize = math.min(maxChunk, remaining).toInt
      chunks(idx) = output.slice(pos, pos + nextSize)
      pos += nextSize
      remaining -= nextSize
    }
    new WrappedLargeByteBuffer(chunks.map(ByteBuffer.wrap))
  }

  override def close(): Unit = {
    output.close()
  }
}
