/*
 * DATABRICKS CONFIDENTIAL & PROPRIETARY
 * __________________
 *
 * Copyright 2020-present Databricks, Inc.
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
 * and its suppliers, if any.  The intellectual and technical concepts contained herein are
 * proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
 * patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
 * or reproduction of this information is strictly forbidden unless prior written permission is
 * obtained from Databricks, Inc.
 *
 * If you view or obtain a copy of this information and believe Databricks, Inc. may not have
 * intended it to be made available, please promptly report it to Databricks Legal Department
 * @ legal@databricks.com.
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
    val newBuffer = ByteBuffer.allocateDirect(newCapacity)
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
