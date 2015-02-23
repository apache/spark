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

package org.apache.spark.io

import java.io.OutputStream
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.io.IOConfig.BufferType

/**
 * byte array backed streams (FastByteArrayOutputStream, ByteArrayOutputStream, etc) are limited to
 * array length of 2 gig - since that is the array size limit.
 *
 * So we move from one to the next as soon as we hit the limit per stream.
 * And once done, asBuffers or toByteArrays can be used to pull data as a sequence of bytebuffers
 * or byte arrays.
 * @param initialSize initial size for the byte array stream ...
 */
class WrappedByteArrayOutputStream(private val initialSize: Int,
    ioConf: IOConfig) extends OutputStream with Logging {

  private val maxStreamSize = ioConf.getMaxBlockSize(BufferType.MEMORY)

  private val allStreams = new ArrayBuffer[SparkByteArrayOutputStream](4)

  private var current: SparkByteArrayOutputStream = null
  private var currentWritten = 0

  nextWriter()

  override def flush(): Unit = {
    current.flush()
  }

  override def write(b: Int): Unit = {
    if (currentWritten >= maxStreamSize) {
      nextWriter()
    }
    current.write(b)
    currentWritten += 1
  }


  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    // invariant checks - from OutputStream.java
    if (b == null) {
      throw new NullPointerException
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
      ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException
    } else if (len == 0) {
      return
    }

    // Else, write to stream.

    // common case first
    if (currentWritten + len < maxStreamSize) {
      current.write(b, off, len)
      currentWritten += len
      return
    }

    // We might need to split the write into two streams.
    var startOff = off
    var remaining = len

    while (remaining > 0) {
      var toCurrent = math.min(remaining, maxStreamSize - currentWritten)
      if (toCurrent > 0) {
        current.write(b, startOff, toCurrent)
        currentWritten += toCurrent
        remaining -= toCurrent
        startOff += toCurrent
      }

      if (currentWritten >= maxStreamSize) {
        // to next
        nextWriter()
      }
    }
  }

  def toLargeByteBuffer(): LargeByteBuffer = {
    current.compact()
    val seq = allStreams.filter(_.size > 0).map(_.toByteBuffer)
    val retval = LargeByteBuffer.fromBuffers(seq:_*)

    retval
  }

  private def nextWriter() {
    if (null != current) {
      current.flush()
      current.compact()
      current = null
    }

    current = new SparkByteArrayOutputStream(initialSize, ioConf)
    currentWritten = 0
    allStreams += current
  }
}


