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

import org.apache.spark.storage.StorageUtils
import org.apache.spark.unsafe.Platform

/**
 * An output stream that dumps data into a direct byte buffer. The byte buffer grows in size
 * as more data is written to the stream.
 * @param capacity The initial capacity of the direct byte buffer
 */
private[spark] class DirectByteBufferOutputStream(capacity: Int) extends OutputStream {
  private var buffer = Platform.allocateDirectBuffer(capacity)

  def this() = this(32)

  override def write(b: Int): Unit = {
    ensureCapacity(buffer.position() + 1)
    buffer.put(b.toByte)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    ensureCapacity(buffer.position() + len)
    buffer.put(b, off, len)
  }

  private def ensureCapacity(minCapacity: Int): Unit = {
    if (minCapacity > buffer.capacity()) grow(minCapacity)
  }

  /**
   * Grows the current buffer to at least `minCapacity` capacity.
   * As a side effect, all references to the old buffer will be invalidated.
   */
  private def grow(minCapacity: Int): Unit = {
    val oldCapacity = buffer.capacity()
    var newCapacity = oldCapacity << 1
    if (newCapacity < minCapacity) newCapacity = minCapacity
    val oldBuffer = buffer
    oldBuffer.flip()
    val newBuffer = Platform.allocateDirectBuffer(newCapacity)
    newBuffer.put(oldBuffer)
    StorageUtils.dispose(oldBuffer)
    buffer = newBuffer
  }

  def reset(): Unit = buffer.clear()

  def size(): Int = buffer.position()

  /**
   * Any subsequent call to [[close()]], [[write()]], [[reset()]] will invalidate the buffer
   * returned by this method.
   */
  def toByteBuffer: ByteBuffer = {
    val outputBuffer = buffer.duplicate()
    outputBuffer.flip()
    outputBuffer
  }

  override def close(): Unit = {
    // Eagerly free the direct byte buffer without waiting for GC to reduce memory pressure.
    StorageUtils.dispose(buffer)
  }

}
