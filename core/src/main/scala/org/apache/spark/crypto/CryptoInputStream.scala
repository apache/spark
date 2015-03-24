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
package org.apache.spark.crypto

import java.io.{IOException, InputStream, FilterInputStream}
import java.lang.UnsupportedOperationException
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.security.GeneralSecurityException
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue

import com.google.common.base.Preconditions

/**
 * CryptoInputStream decrypts data. It is not thread-safe. AES CTR mode is
 * required in order to ensure that the plain text and cipher text have a 1:1
 * mapping. The decryption is buffer based. The key points of the decryption
 * are (1) calculating the counter and (2) padding through stream position:
 * <p/>
 * counter = base + pos/(algorithm blocksize);
 * padding = pos%(algorithm blocksize);
 * <p/>
 * The underlying stream offset is maintained as state.
 */
class CryptoInputStream(in: InputStream, codecVal: CryptoCodec,
                        bufferSizeVal: Integer, keyVal: Array[Byte], ivVal: Array[Byte],
                        streamOffsetVal: Long) extends FilterInputStream(in: InputStream) with
ReadableByteChannel {
  var oneByteBuf: Array[Byte] = new Array[Byte](1)
  var codec: CryptoCodec = codecVal

  var bufferSize: Integer = CryptoStreamUtils.checkBufferSize(codecVal, bufferSizeVal)
  /**
   * Input data buffer. The data starts at inBuffer.position() and ends at
   * to inBuffer.limit().
   */
  var inBuffer: ByteBuffer = ByteBuffer.allocateDirect(bufferSizeVal)

  /**
   * The decrypted data buffer. The data starts at outBuffer.position() and
   * ends at outBuffer.limit()
   */
  var outBuffer: ByteBuffer = ByteBuffer.allocateDirect(bufferSizeVal)
  var streamOffset: Long = streamOffsetVal // Underlying stream offset.

  /**
   * Whether the underlying stream supports
   * {@link org.apache.hadoop.fs.ByteBufferReadable}
   */
  var usingByteBufferRead: Boolean = false
  var usingByteBufferReadInitialized: Boolean = false
  /**
   * Padding = pos%(algorithm blocksize) Padding is put into {@link #inBuffer}
   * before any other data goes in. The purpose of padding is to put the input
   * data at proper position.
   */
  var padding: Byte = '0'
  var closed: Boolean = false
  var key: Array[Byte] = keyVal.clone()
  var initIV: Array[Byte] = ivVal.clone()
  var iv: Array[Byte] = ivVal.clone()
  var isReadableByteChannel: Boolean = in.isInstanceOf[ReadableByteChannel]

  /** DirectBuffer pool */
  val bufferPool: Queue[ByteBuffer] =
    new ConcurrentLinkedQueue[ByteBuffer]()
  /** Decryptor pool */
  val decryptorPool: Queue[Decryptor] =
    new ConcurrentLinkedQueue[Decryptor]()

  var tmpBuf: Array[Byte] = null
  var decryptor: Decryptor = getDecryptor
  CryptoStreamUtils.checkCodec(codecVal)
  resetStreamOffset(streamOffset)


  def this(in: InputStream, codec: CryptoCodec,
           bufferSize: Integer, key: Array[Byte], iv: Array[Byte]) {
    this(in, codec, bufferSize, key, iv, 0)
  }

  def this(in: InputStream, codec: CryptoCodec,
           key: Array[Byte], iv: Array[Byte]) {
    this(in, codec, CryptoStreamUtils.getBufferSize, key, iv)
  }

  def getWrappedStream(): InputStream = in

  /**
   * Decryption is buffer based.
   * If there is data in {@link #outBuffer}, then read it out of this buffer.
   * If there is no data in {@link #outBuffer}, then read more from the
   * underlying stream and do the decryption.
   * @param b the buffer into which the decrypted data is read.
   * @param off the buffer offset.
   * @param len the maximum number of decrypted data bytes to read.
   * @return int the total number of decrypted data bytes read into the buffer.
   * @throws IOException
   */
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    checkStream
    if (b == null) {
      throw new NullPointerException()
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException()
    } else if (len == 0) {
      0
    } else {
      val remaining: Integer = outBuffer.remaining()
      if (remaining > 0) {
        val n: Integer = Math.min(len, remaining)
        outBuffer.get(b, off, n)
        n
      } else {
        var n: Integer = 0
        /*
         * Check whether the underlying stream is {@link ByteBufferReadable},
         * it can avoid bytes copy.
         */
        if (usingByteBufferReadInitialized == false) {
          usingByteBufferReadInitialized = true
          if (isReadableByteChannel) {
            try {

              n = (in.asInstanceOf[ReadableByteChannel]).read(inBuffer)
              usingByteBufferRead = true
            } catch {
              case e: UnsupportedOperationException =>
                usingByteBufferRead = false
            }
          } else {
            usingByteBufferRead = false
          }
          if (!usingByteBufferRead) {
            n = readFromUnderlyingStream(inBuffer)
          }
        } else {
          if (usingByteBufferRead) {
            n = (in.asInstanceOf[ReadableByteChannel]).read(inBuffer)
          } else {
            n = readFromUnderlyingStream(inBuffer)
          }
        }
        if (n <= 0) {
          n
        } else {
          streamOffset += n // Read n bytes
          decrypt(decryptor, inBuffer, outBuffer, padding)
          padding = afterDecryption(decryptor, inBuffer, streamOffset, iv)
          n = Math.min(len, outBuffer.remaining())
          outBuffer.get(b, off, n)
          n
        }
      }
    }
  }

  /** Read data from underlying stream. */
  def readFromUnderlyingStream(inBuffer: ByteBuffer): Int = {
    val toRead: Int = inBuffer.remaining
    val tmp: Array[Byte] = getTmpBuf
    val n: Int = in.read(tmp, 0, toRead)
    if (n > 0) {
      inBuffer.put(tmp, 0, n)
    }
    n
  }


  def getTmpBuf: Array[Byte] = {
    if (tmpBuf == null) {
      tmpBuf = new Array[Byte](bufferSize)
    }
    tmpBuf
  }

  /**
   * Do the decryption using inBuffer as input and outBuffer as output.
   * Upon return, inBuffer is cleared the decrypted data starts at
   * outBuffer.position() and ends at outBuffer.limit()
   */
  def decrypt(decryptor: Decryptor, inBuffer: ByteBuffer, outBuffer: ByteBuffer, padding: Byte) {
    Preconditions.checkState(inBuffer.position >= padding)
    if (inBuffer.position != padding) {
      inBuffer.flip
      outBuffer.clear
      decryptor.decrypt(inBuffer, outBuffer)
      inBuffer.clear
      outBuffer.flip
      if (padding > 0) {
        outBuffer.position(padding)
      }
    }
  }

  /**
   * This method is executed immediately after decryption. Check whether
   * decryptor should be updated and recalculate padding if needed.
   */
  def afterDecryption(decryptor: Decryptor, inBuffer: ByteBuffer, position: Long,
                      iv: Array[Byte]): Byte = {
    var padding: Byte = 0
    if (decryptor.isContextReset) {
      updateDecryptor(decryptor, position, iv)
      padding = getPadding(position)
      inBuffer.position(padding)
    }
    padding
  }

  def getCounter(position: Long): Long = {
    position / codec.getCipherSuite.algoBlockSize
  }

  def getPadding(position: Long): Byte = {
    (position % codec.getCipherSuite.algoBlockSize).asInstanceOf[Byte]
  }

  /** Calculate the counter and iv, update the decryptor. */
  def updateDecryptor(decryptor: Decryptor, position: Long, iv: Array[Byte]) {
    val counter: Long = getCounter(position)
    codec.calculateIV(initIV, counter, iv)
    decryptor.init(key, iv)
  }

  /**
   * Reset the underlying stream offset clear {@link #inBuffer} and
   * {@link #outBuffer}. This Typically happens during {@link #seek(long)}
   * or {@link #skip(long)}.
   */
  def resetStreamOffset(offset: Long) {
    streamOffset = offset
    inBuffer.clear
    outBuffer.clear
    outBuffer.limit(0)
    updateDecryptor(decryptor, offset, iv)
    padding = getPadding(offset)
    inBuffer.position(padding)
  }

  override def close {
    if (!closed) {
      super.close
      freeBuffers
      closed = true
    }
  }

  /** Skip n bytes */
  override def skip(nVal: Long): Long = {
    var n: Long = nVal
    Preconditions.checkArgument(n >= 0, "Negative skip length.", new Array[String](1))
    checkStream
    if (n == 0) {
      0
    }
    else if (n <= outBuffer.remaining) {
      val pos: Int = outBuffer.position + n.asInstanceOf[Int]
      outBuffer.position(pos)
      n
    }
    else {
      n -= outBuffer.remaining
      var skipped: Long = in.skip(n)
      if (skipped < 0) {
        skipped = 0
      }
      val pos: Long = streamOffset + skipped
      skipped += outBuffer.remaining
      resetStreamOffset(pos)
      skipped
    }
  }

  /** ByteBuffer read. */
  override def read(buf: ByteBuffer): Int = {
    checkStream
    if (isReadableByteChannel) {
      val unread: Int = outBuffer.remaining
      if (unread > 0) {
        val toRead: Int = buf.remaining
        if (toRead <= unread) {
          val limit: Int = outBuffer.limit
          outBuffer.limit(outBuffer.position + toRead)
          buf.put(outBuffer)
          outBuffer.limit(limit)
          toRead
        }
        else {
          buf.put(outBuffer)
        }
      }
      val pos: Int = buf.position
      val n: Int = (in.asInstanceOf[ReadableByteChannel]).read(buf)
      if (n > 0) {
        streamOffset += n
        decrypt(buf, n, pos)
      }
      if (n >= 0) {
        unread + n
      }
      else {
        if (unread == 0) {
          -1
        }
        else {
          unread
        }
      }
    }
    else {
      var n: Int = 0
      if (buf.hasArray) {
        n = read(buf.array, buf.position, buf.remaining)
        if (n > 0) {
          buf.position(buf.position + n)
        }
      }
      else {
        val tmp: Array[Byte] = new Array[Byte](buf.remaining)
        n = read(tmp)
        if (n > 0) {
          buf.put(tmp, 0, n)
        }
      }
      n
    }
  }

  /**
   * Decrypt all data in buf: total n bytes from given start position.
   * Output is also buf and same start position.
   * buf.position() and buf.limit() should be unchanged after decryption.
   */
  def decrypt(buf: ByteBuffer, n: Int, start: Int) {
    val pos: Int = buf.position
    val limit: Int = buf.limit
    var len: Int = 0
    while (len < n) {
      buf.position(start + len)
      buf.limit(start + len + Math.min(n - len, inBuffer.remaining))
      inBuffer.put(buf)
      try {
        decrypt(decryptor, inBuffer, outBuffer, padding)
        buf.position(start + len)
        buf.limit(limit)
        len += outBuffer.remaining
        buf.put(outBuffer)
      }
      finally {
        padding = afterDecryption(decryptor, inBuffer, streamOffset - (n - len), iv)
      }
    }
    buf.position(pos)
  }

  override def available: Int = {
    checkStream
    in.available + outBuffer.remaining
  }

  override def markSupported: Boolean = {
    false
  }

  override def mark(readLimit: Int) {
  }

  override def reset() {
    throw new IOException("Mark/reset not supported")
  }

  override def read: Int = {
    if ((read(oneByteBuf, 0, 1) == -1)) -1 else (oneByteBuf(0) & 0xff)
  }

  def checkStream() {
    if (closed) {
      throw new IOException("Stream closed")
    }
  }

  /** Forcibly free the direct buffers. */
  def freeBuffers {
    CryptoStreamUtils.freeDB(inBuffer)
    CryptoStreamUtils.freeDB(outBuffer)
    cleanBufferPool
  }

  /** Clean direct buffer pool */
  def cleanBufferPool {
    var buf: ByteBuffer = null
    while ((({
      buf = bufferPool.poll
      buf
    })) != null) {
      CryptoStreamUtils.freeDB(buf)
    }
  }

  /** Get decryptor */
  def getDecryptor: Decryptor = {
    try {
      decryptor = codec.createDecryptor
    }
    catch {
      case e: GeneralSecurityException => {
        throw new IOException(e)
      }
    }
    decryptor
  }

  def isOpen: Boolean = {
    !closed
  }
}


