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

import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import scala.collection.JavaConverters._

import org.apache.spark.network.buffer.LargeByteBuffer
import org.apache.spark.util.collection.ChainedBuffer

class ChainedLargeByteBuffer(private[io] val underlying: ChainedBuffer) extends LargeByteBuffer {

  def capacity = underlying.capacity

  var _pos = 0l

  def get(dst: Array[Byte],offset: Int,length: Int): Unit = {
    underlying.read(_pos, dst, offset, length)
    _pos += length
  }

  def get(): Byte = {
    val b = underlying.read(_pos)
    _pos += 1
    b
  }

  def put(bytes: LargeByteBuffer): Unit = {
    ???
  }

  def position: Long = _pos
  def position(position: Long): Unit = {
    _pos = position
  }
  def remaining(): Long = {
    underlying.size - position
  }

  def duplicate(): ChainedLargeByteBuffer = {
    new ChainedLargeByteBuffer(underlying)
  }

  def rewind(): Unit = {
    _pos = 0
  }

  def limit(): Long = {
    capacity
  }

  def limit(newLimit: Long): Unit = {
    ???
  }

  def writeTo(channel:WritableByteChannel): Long = {
    var written = 0l
    underlying.chunks.foreach{bytes =>
      //TODO test this
      val buffer = ByteBuffer.wrap(bytes)
      while (buffer.hasRemaining)
        channel.write(buffer)
      written += bytes.length
    }
    written
  }

  override def firstByteBuffer(): ByteBuffer = {
    ByteBuffer.wrap(underlying.chunks(0))
  }

  override def nioBuffers(): java.util.List[ByteBuffer] = {
    underlying.chunks.map{bytes => ByteBuffer.wrap(bytes)}.asJava
  }
}
