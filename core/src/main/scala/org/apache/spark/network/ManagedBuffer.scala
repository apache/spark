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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

import scala.util.Try

import com.google.common.io.ByteStreams
import io.netty.buffer.{ByteBufInputStream, ByteBuf}

import org.apache.spark.util.{ByteBufferInputStream, Utils}


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

  /**
   * Memory mapping is expensive and can destabilize the JVM (SPARK-1145, SPARK-3889).
   * Avoid unless there's a good reason not to.
   */
  private val MIN_MEMORY_MAP_BYTES = 2 * 1024 * 1024;

  override def size: Long = length

  override def nioByteBuffer(): ByteBuffer = {
    var channel: FileChannel = null
    try {
      channel = new RandomAccessFile(file, "r").getChannel
      // Just copy the buffer if it's sufficiently small, as memory mapping has a high overhead.
      if (length < MIN_MEMORY_MAP_BYTES) {
        val buf = ByteBuffer.allocate(length.toInt)
        channel.position(offset)
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) {
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=$offset\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
          }
        }
        buf.flip()
        buf
      } else {
        channel.map(MapMode.READ_ONLY, offset, length)
      }
    } catch {
      case e: IOException =>
        Try(channel.size).toOption match {
          case Some(fileLen) =>
            throw new IOException(s"Error in reading $this (actual file length $fileLen)", e)
          case None =>
            throw new IOException(s"Error in opening $this", e)
        }
    } finally {
      if (channel != null) {
        Utils.tryLog(channel.close())
      }
    }
  }

  override def inputStream(): InputStream = {
    var is: FileInputStream = null
    try {
      is = new FileInputStream(file)
      ByteStreams.skipFully(is, offset)
      ByteStreams.limit(is, length)
    } catch {
      case e: IOException =>
        if (is != null) {
          Utils.tryLog(is.close())
        }
        Try(file.length).toOption match {
          case Some(fileLen) =>
            throw new IOException(s"Error in reading $this (actual file length $fileLen)", e)
          case None =>
            throw new IOException(s"Error in opening $this", e)
        }
      case e: Throwable =>
        if (is != null) {
          Utils.tryLog(is.close())
        }
        throw e
    }
  }

  override def toString: String = s"${getClass.getName}($file, $offset, $length)"
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
