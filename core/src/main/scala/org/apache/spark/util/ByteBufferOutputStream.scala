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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * Provide a zero-copy way to convert data in ByteArrayOutputStream to ByteBuffer
 */
private[spark] class ByteBufferOutputStream(capacity: Int) extends ByteArrayOutputStream(capacity) {

  def this() = this(32)

  def getCount(): Int = count

  private[this] var closed: Boolean = false

  override def write(b: Int): Unit = {
    require(!closed, "cannot write to a closed ByteBufferOutputStream")
    super.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    require(!closed, "cannot write to a closed ByteBufferOutputStream")
    super.write(b, off, len)
  }

  override def reset(): Unit = {
    require(!closed, "cannot reset a closed ByteBufferOutputStream")
    super.reset()
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      closed = true
    }
  }

  def toByteBuffer: ByteBuffer = {
    require(closed, "can only call toByteBuffer() after ByteBufferOutputStream has been closed")
    ByteBuffer.wrap(buf, 0, count)
  }
}
