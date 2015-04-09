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

import java.io.InputStream

import org.apache.spark.network.buffer.LargeByteBuffer
import org.apache.spark.storage.BlockManager

/**
 * Reads data from a LargeByteBuffer, and optionally cleans it up using buffer.dispose()
 * when the stream is closed (e.g. to close a memory-mapped file).
 */
private[spark]
class LargeByteBufferInputStream(private var buffer: LargeByteBuffer, dispose: Boolean = false)
  extends InputStream {

  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length).toInt
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val skipped = buffer.skip(bytes)
      skipped
    } else {
      0L
    }
  }

  // only for testing
  private[util] var disposed = false

  /**
   * Clean up the buffer, and potentially dispose of it
   */
  override def close() {
    if (buffer != null) {
      if (dispose) {
        buffer.dispose()
        disposed = true
      }
      buffer = null
    }
  }
}
