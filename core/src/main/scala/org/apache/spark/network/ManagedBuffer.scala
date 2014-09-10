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

import java.io.{FileInputStream, RandomAccessFile, File, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import com.google.common.io.ByteStreams
import io.netty.buffer.{ByteBufInputStream, ByteBuf}

import org.apache.spark.util.ByteBufferInputStream


/**
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 *
 * - FileSegmentManagedBuffer: data backed by part of a file
 * - NioByteBufferManagedBuffer: data backed by a NIO ByteBuffer
 * - NettyByteBufManagedBuffer: data backed by a Netty ByteBuf
 */
sealed abstract class ManagedBuffer {
  // Note that all the methods are defined with parenthesis because their implementations can
  // have side effects (io operations).

  /** Number of bytes of the data. */
  def size: Long

  /**
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  def nioByteBuffer(): ByteBuffer

  /**
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  def inputStream(): InputStream
}


/**
 * A [[ManagedBuffer]] backed by a segment in a file
 */
final class FileSegmentManagedBuffer(val file: File, val offset: Long, val length: Long)
  extends ManagedBuffer {

  override def size: Long = length

  override def nioByteBuffer(): ByteBuffer = {
    val channel = new RandomAccessFile(file, "r").getChannel
    channel.map(MapMode.READ_ONLY, offset, length)
  }

  override def inputStream(): InputStream = {
    val is = new FileInputStream(file)
    is.skip(offset)
    ByteStreams.limit(is, length)
  }
}


/**
 * A [[ManagedBuffer]] backed by [[java.nio.ByteBuffer]].
 */
final class NioByteBufferManagedBuffer(buf: ByteBuffer) extends ManagedBuffer {

  override def size: Long = buf.remaining()

  override def nioByteBuffer() = buf.duplicate()

  override def inputStream() = new ByteBufferInputStream(buf)
}


/**
 * A [[ManagedBuffer]] backed by a Netty [[ByteBuf]].
 */
final class NettyByteBufManagedBuffer(buf: ByteBuf) extends ManagedBuffer {

  override def size: Long = buf.readableBytes()

  override def nioByteBuffer() = buf.nioBuffer()

  override def inputStream() = new ByteBufInputStream(buf)

  // TODO(rxin): Promote this to top level ManagedBuffer interface and add documentation for it.
  def release(): Unit = buf.release()
}
