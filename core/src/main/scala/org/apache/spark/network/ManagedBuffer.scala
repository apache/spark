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

package org.apache.spark.network

import java.io.{File, FileInputStream, InputStream}
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBufInputStream, ByteBuf, Unpooled}
import io.netty.channel.DefaultFileRegion

import org.apache.spark.storage.FileSegment
import org.apache.spark.util.ByteBufferInputStream


/**
 * Provides a buffer abstraction that allows pooling and reuse.
 */
abstract class ManagedBuffer {
  // Note that all the methods are defined with parenthesis because their implementations can
  // have side effects (io operations).

  def byteBuffer(): ByteBuffer = throw new UnsupportedOperationException

  def fileSegment(): Option[FileSegment] = None

  def inputStream(): InputStream = throw new UnsupportedOperationException

  def release(): Unit = throw new UnsupportedOperationException

  def size: Long

  private[network] def toNetty(): AnyRef
}


/**
 * A ManagedBuffer backed by a segment in a file.
 */
final class FileSegmentManagedBuffer(file: File, offset: Long, length: Long)
  extends ManagedBuffer {

  override def size: Long = length

  override private[network] def toNetty(): AnyRef = {
    val fileChannel = new FileInputStream(file).getChannel
    new DefaultFileRegion(fileChannel, offset, length)
  }
}


/**
 * A ManagedBuffer backed by [[java.nio.ByteBuffer]].
 */
final class NioByteBufferManagedBuffer(buf: ByteBuffer) extends ManagedBuffer {

  override def byteBuffer() = buf

  override def inputStream() = new ByteBufferInputStream(buf)

  override def size: Long = buf.remaining()

  override private[network] def toNetty(): AnyRef = Unpooled.wrappedBuffer(buf)
}


/**
 * A ManagedBuffer backed by a Netty [[ByteBuf]].
 */
final class NettyByteBufManagedBuffer(buf: ByteBuf) extends ManagedBuffer {

  override def byteBuffer() = buf.nioBuffer()

  override def inputStream() = new ByteBufInputStream(buf)

  override def release(): Unit = buf.release()

  override def size: Long = buf.readableBytes()

  override private[network] def toNetty(): AnyRef = buf
}
