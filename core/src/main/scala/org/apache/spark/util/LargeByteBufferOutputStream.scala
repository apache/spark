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

import org.apache.spark.network.buffer.{WrappedLargeByteBuffer, LargeByteBuffer}
import org.apache.spark.util.io.ByteArrayChunkOutputStream

private[spark]
class LargeByteBufferOutputStream(chunkSize: Int = 65536)
  extends OutputStream {

  val output = new ByteArrayChunkOutputStream(chunkSize)

  private var _pos = 0

  override def write(b: Int): Unit = {
    output.write(b)
  }

  override def write(bytes: Array[Byte], offs: Int, len: Int): Unit = {
    output.write(bytes, offs, len)
    _pos += len
  }

  def pos: Int = _pos

  def largeBuffer: LargeByteBuffer = {
    new WrappedLargeByteBuffer(output.toArrays.map{ByteBuffer.wrap})
  }

  override def close(): Unit = {
    output.close()
  }
}
