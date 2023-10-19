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

package org.apache.spark.util.io

import java.io.{Externalizable, File, FileInputStream, InputStream, ObjectInput, ObjectOutput}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import com.google.common.io.ByteStreams
import com.google.common.primitives.UnsignedBytes
import io.netty.handler.stream.ChunkedStream
import org.apache.commons.io.IOUtils

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.util.{ByteArrayWritableChannel, LimitedInputStream}
import org.apache.spark.storage.{EncryptedManagedBuffer, StorageUtils}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils

/**
 * Read-only byte buffer which is physically stored as multiple chunks rather than a single
 * contiguous array.
 *
 * @param chunks an array of [[ByteBuffer]]s. Each buffer in this array must have position == 0.
 *               Ownership of these buffers is transferred to the ChunkedByteBuffer, so if these
 *               buffers may also be used elsewhere then the caller is responsible for copying
 *               them as needed.
 */
private[spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) extends Externalizable {
  require(chunks != null, "chunks must not be null")
  require(!chunks.contains(null), "chunks must not contain null")
  require(chunks.forall(_.position() == 0), "chunks' positions must be 0")

  // Chunk size in bytes
  private val bufferWriteChunkSize =
    Option(SparkEnv.get).map(_.conf.get(config.BUFFER_WRITE_CHUNK_SIZE))
      .getOrElse(config.BUFFER_WRITE_CHUNK_SIZE.defaultValue.get).toInt

  private[this] var disposed: Boolean = false

  /**
   * This size of this buffer, in bytes. Using var here for serialization purpose (need to set a
   * object after default construction)
   */
  private var _size: Long = chunks.map(_.limit().asInstanceOf[Long]).sum

  def size: Long = _size

  def this() = {
    this(Array.empty[ByteBuffer])
  }

  def this(byteBuffer: ByteBuffer) = {
    this(Array(byteBuffer))
  }

  /**
   * Write this buffer to a channel.
   */
  def writeFully(channel: WritableByteChannel): Unit = {
    for (bytes <- getChunks()) {
      val originalLimit = bytes.limit()
      while (bytes.hasRemaining) {
        // If `bytes` is an on-heap ByteBuffer, the Java NIO API will copy it to a temporary direct
        // ByteBuffer when writing it out. This temporary direct ByteBuffer is cached per thread.
        // Its size has no limit and can keep growing if it sees a larger input ByteBuffer. This may
        // cause significant native memory leak, if a large direct ByteBuffer is allocated and
        // cached, as it's never released until thread exits. Here we write the `bytes` with
        // fixed-size slices to limit the size of the cached direct ByteBuffer.
        // Please refer to http://www.evanjones.ca/java-bytebuffer-leak.html for more details.
        val ioSize = Math.min(bytes.remaining(), bufferWriteChunkSize)
        bytes.limit(bytes.position() + ioSize)
        channel.write(bytes)
        bytes.limit(originalLimit)
      }
    }
  }

  /**
   * Writes to the provided ObjectOutput with zero copy if possible.
   */
  override def writeExternal(out: ObjectOutput): Unit = {
    // We want to keep the chunks layout
    out.writeInt(chunks.length)
    val chunksCopy = getChunks()
    chunksCopy.foreach(buffer => out.writeInt(buffer.limit()))
    chunksCopy.foreach(Utils.writeByteBuffer(_, out))
  }

  override def readExternal(in: ObjectInput): Unit = {
    val chunksNum = in.readInt()
    val indices = 0 until chunksNum
    val chunksSize = indices.map(_ => in.readInt())
    val chunks = new Array[ByteBuffer](chunksNum)

    // We deserialize all chunks into on-heap buffer by default. If we have use case in the future
    // where we want to preserve the on-heap/off-heap nature of chunks, then we need to record the
    // `isDirect` property of each chunk during serialization
    indices.foreach { i =>
      val chunkSize = chunksSize(i)
      chunks(i) = {
        val arr = new Array[Byte](chunkSize)
        in.readFully(arr, 0, chunkSize)
        ByteBuffer.wrap(arr)
      }
    }
    this.chunks = chunks
    this._size = chunks.map(_.limit().toLong).sum
  }

  /**
   * Wrap this in a custom "FileRegion" which allows us to transfer over 2 GB.
   */
  def toNetty: ChunkedByteBufferFileRegion = {
    new ChunkedByteBufferFileRegion(this, bufferWriteChunkSize)
  }

  /**
   * Wrap this in a ChunkedStream which allows us to provide the data in a manner
   * compatible with SSL encryption
   */
  def toNettyForSsl: ChunkedStream = {
    new ChunkedStream(toInputStream(), bufferWriteChunkSize)
  }

  /**
   * Copy this buffer into a new byte array.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the maximum array size.
   */
  def toArray: Array[Byte] = {
    if (size >= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      throw new UnsupportedOperationException(
        s"cannot call toArray because buffer size ($size bytes) exceeds maximum array size")
    }
    val byteChannel = new ByteArrayWritableChannel(size.toInt)
    writeFully(byteChannel)
    byteChannel.close()
    byteChannel.getData
  }

  /**
   * Convert this buffer to a ByteBuffer. If this buffer is backed by a single chunk, its underlying
   * data will not be copied. Instead, it will be duplicated. If this buffer is backed by multiple
   * chunks, the data underlying this buffer will be copied into a new byte buffer. As a result, it
   * is suggested to use this method only if the caller does not need to manage the memory
   * underlying this buffer.
   *
   * @throws UnsupportedOperationException if this buffer's size exceeds the max ByteBuffer size.
   */
  def toByteBuffer: ByteBuffer = {
    if (chunks.length == 1) {
      chunks.head.duplicate()
    } else {
      ByteBuffer.wrap(toArray)
    }
  }

  /**
   * Creates an input stream to read data from this ChunkedByteBuffer.
   *
   * @param dispose if true, [[dispose()]] will be called at the end of the stream
   *                in order to close any memory-mapped files which back this buffer.
   */
  def toInputStream(dispose: Boolean = false): InputStream = {
    new ChunkedByteBufferInputStream(this, dispose)
  }

  /**
   * Get duplicates of the ByteBuffers backing this ChunkedByteBuffer.
   */
  def getChunks(): Array[ByteBuffer] = {
    chunks.map(_.duplicate())
  }

  /**
   * Make a copy of this ChunkedByteBuffer, copying all of the backing data into new buffers.
   * The new buffer will share no resources with the original buffer.
   *
   * @param allocator a method for allocating byte buffers
   */
  def copy(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val copiedChunks = getChunks().map { chunk =>
      val newChunk = allocator(chunk.limit())
      newChunk.put(chunk)
      newChunk.flip()
      newChunk
    }
    new ChunkedByteBuffer(copiedChunks)
  }

  /**
   * Attempt to clean up any ByteBuffer in this ChunkedByteBuffer which is direct or memory-mapped.
   * See [[StorageUtils.dispose]] for more information.
   */
  def dispose(): Unit = {
    if (!disposed) {
      chunks.foreach(StorageUtils.dispose)
      disposed = true
    }
  }

}

private[spark] object ChunkedByteBuffer {
  private val CHUNK_BUFFER_SIZE: Int = 1024 * 1024
  private val MINIMUM_CHUNK_BUFFER_SIZE: Int = 1024

  def fromManagedBuffer(data: ManagedBuffer): ChunkedByteBuffer = {
    data match {
      case f: FileSegmentManagedBuffer =>
        fromFile(f.getFile, f.getOffset, f.getLength)
      case e: EncryptedManagedBuffer =>
        e.blockData.toChunkedByteBuffer(ByteBuffer.allocate _)
      case other =>
        new ChunkedByteBuffer(other.nioByteBuffer())
    }
  }

  def fromFile(file: File): ChunkedByteBuffer = {
    fromFile(file, 0, file.length())
  }

  private def fromFile(
      file: File,
      offset: Long,
      length: Long): ChunkedByteBuffer = {
    // We do *not* memory map the file, because we may end up putting this into the memory store,
    // and spark currently is not expecting memory-mapped buffers in the memory store, it conflicts
    // with other parts that manage the lifecycle of buffers and dispose them.  See SPARK-25422.
    val is = new FileInputStream(file)
    ByteStreams.skipFully(is, offset)
    val in = new LimitedInputStream(is, length)
    val chunkSize = math.min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH, length).toInt
    val out = new ChunkedByteBufferOutputStream(chunkSize, ByteBuffer.allocate _)
    Utils.tryWithSafeFinally {
      IOUtils.copy(in, out)
    } {
      in.close()
      out.close()
    }
    out.toChunkedByteBuffer
  }

  /**
   * Try to estimate appropriate chunk size so that it's not too large (waste memory) or too
   * small (too many segments)
   */
  def estimateBufferChunkSize(estimatedSize: Long = -1): Int = {
    if (estimatedSize < 0) {
      CHUNK_BUFFER_SIZE
    } else {
      Math.max(Math.min(estimatedSize, CHUNK_BUFFER_SIZE).toInt, MINIMUM_CHUNK_BUFFER_SIZE)
    }
  }
}

/**
 * Reads data from a ChunkedByteBuffer.
 *
 * @param dispose if true, `ChunkedByteBuffer.dispose()` will be called at the end of the stream
 *                in order to close any memory-mapped files which back the buffer.
 */
private[spark] class ChunkedByteBufferInputStream(
    var chunkedByteBuffer: ChunkedByteBuffer,
    dispose: Boolean)
  extends InputStream {

  // Filter out empty chunks since `read()` assumes all chunks are non-empty.
  private[this] var chunks = chunkedByteBuffer.getChunks().filter(_.hasRemaining).iterator
  private[this] var currentChunk: ByteBuffer = {
    if (chunks.hasNext) {
      chunks.next()
    } else {
      null
    }
  }

  override def available(): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      currentChunk.remaining
    } else {
      0
    }
  }

  override def read(): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      UnsignedBytes.toInt(currentChunk.get())
    } else {
      close()
      -1
    }
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      val amountToGet = math.min(currentChunk.remaining(), length)
      currentChunk.get(dest, offset, amountToGet)
      amountToGet
    } else {
      close()
      -1
    }
  }

  override def skip(bytes: Long): Long = {
    if (currentChunk != null) {
      val amountToSkip = math.min(bytes, currentChunk.remaining).toInt
      currentChunk.position(currentChunk.position() + amountToSkip)
      if (currentChunk.remaining() == 0) {
        if (chunks.hasNext) {
          currentChunk = chunks.next()
        } else {
          close()
        }
      }
      amountToSkip
    } else {
      0L
    }
  }

  override def close(): Unit = {
    if (chunkedByteBuffer != null && dispose) {
      chunkedByteBuffer.dispose()
    }
    chunkedByteBuffer = null
    chunks = null
    currentChunk = null
  }
}
