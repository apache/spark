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

import java.io.{IOException, FilterOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.security.GeneralSecurityException

import com.google.common.base.Preconditions

import org.apache.spark.Logging

/**
 * CryptoOutputStream encrypts data. It is not thread-safe. AES CTR mode is
 * required in order to ensure that the plain text and cipher text have a 1:1
 * mapping. The encryption is buffer based. The key points of the encryption are
 * (1) calculating counter and (2) padding through stream position.
 * <p/>
 * counter = base + pos/(algorithm blocksize);
 * padding = pos%(algorithm blocksize);
 * <p/>
 * The underlying stream offset is maintained as state.
 */
class CryptoOutputStream(out: OutputStream, codecVal: CryptoCodec, bufferSizeVal: Int,
                         keyVal: Array[Byte], ivVal: Array[Byte], streamOffsetVal: Long) extends
FilterOutputStream(out: OutputStream) with Logging {
  var codec: CryptoCodec = null
  var encryptor: Encryptor = null
  var bufferSize: Int = 0
  /**
   * Input data buffer. The data starts at inBuffer.position() and ends at
   * inBuffer.limit().
   */
  var inBuffer: ByteBuffer = null
  /**
   * Encrypted data buffer. The data starts at outBuffer.position() and ends at
   * outBuffer.limit();
   */
  var outBuffer: ByteBuffer = null
  var streamOffset: Long = 0
  /**
   * Padding = pos%(algorithm blocksize); Padding is put into {@link #inBuffer}
   * before any other data goes in. The purpose of padding is to put input data
   * at proper position.
   */
  var padding: Byte = 0
  var closed: Boolean = false
  var key: Array[Byte] = null
  var initIV: Array[Byte] = null
  var iv: Array[Byte] = null
  var tmpBuf: Array[Byte] = null
  val oneByteBuf: Array[Byte] = new Array[Byte](1)

  CryptoStreamUtils.checkCodec(codecVal)
  bufferSize = CryptoStreamUtils.checkBufferSize(codecVal, bufferSizeVal)
  codec = codecVal
  key = keyVal.clone
  initIV = ivVal.clone
  iv = ivVal.clone
  inBuffer = ByteBuffer.allocateDirect(bufferSize)
  outBuffer = ByteBuffer.allocateDirect(bufferSize)
  streamOffset = streamOffsetVal
  try {
    encryptor = codec.createEncryptor
  }
  catch {
    case e: GeneralSecurityException => {
      throw e
    }
  }
  updateEncryptor


  def this(out: OutputStream, codec: CryptoCodec, bufferSize: Int, key: Array[Byte],
           iv: Array[Byte]) {
    this(out, codec, bufferSize, key, iv, 0)
  }

  def this(out: OutputStream, codec: CryptoCodec, key: Array[Byte], iv: Array[Byte], streamOffset:
  Long) {
    this(out, codec, CryptoStreamUtils.getBufferSize, key, iv, streamOffset)
  }

  def this(out: OutputStream, codec: CryptoCodec, key: Array[Byte], iv: Array[Byte]) {
    this(out, codec, key, iv, 0)
  }

  def getWrappedStream: OutputStream = {
    out
  }

  /**
   * Encryption is buffer based.
   * If there is enough room in {@link #inBuffer}, then write to this buffer.
   * If {@link #inBuffer} is full, then do encryption and write data to the
   * underlying stream.
   * @param b the data.
   * @param offVal the start offset in the data.
   * @param lenVal the number of bytes to write.
   * @throws IOException
   */
  override def write(b: Array[Byte], offVal: Int, lenVal: Int) {
    var off = offVal
    var len = lenVal
    checkStream
    if (b == null) {
      throw new NullPointerException
    }
    else if (off < 0 || len < 0 || off > b.length || len > b.length - off) {
      throw new IndexOutOfBoundsException
    }
    while (len > 0) {
      val remaining: Int = inBuffer.remaining
      if (len < remaining) {
        inBuffer.put(b, off, len)
        len = 0
      }
      else {
        inBuffer.put(b, off, remaining)
        off += remaining
        len -= remaining
        encrypt
      }
    }
  }

  /**
   * Do the encryption, input is {@link #inBuffer} and output is
   * {@link #outBuffer}.
   */
  def encrypt {
    Preconditions.checkState(inBuffer.position >= padding)
    if (inBuffer.position != padding) {
      inBuffer.flip
      outBuffer.clear
      encryptor.encrypt(inBuffer, outBuffer)
      inBuffer.clear
      outBuffer.flip
      if (padding > 0) {
        outBuffer.position(padding)
        padding = 0
      }
      val len: Int = outBuffer.remaining
      val tmp: Array[Byte] = getTmpBuf
      outBuffer.get(tmp, 0, len)
      out.write(tmp, 0, len)
      streamOffset += len
      if (encryptor.isContextReset) {
        updateEncryptor
      }
    }
  }

  /** Update the {@link #encryptor}: calculate counter and {@link #padding}. */
  def updateEncryptor {
    val counter: Long = streamOffset / codec.getCipherSuite.algoBlockSize
    padding = (streamOffset % codec.getCipherSuite.algoBlockSize).asInstanceOf[Byte]
    inBuffer.position(padding)
    codec.calculateIV(initIV, counter, iv)
    encryptor.init(key, iv)
  }

  def getTmpBuf: Array[Byte] = {
    if (tmpBuf == null) {
      tmpBuf = new Array[Byte](bufferSize)
    }
    tmpBuf
  }

  override def close {
    this.synchronized {
      if (!closed) {
        try {
          super.close
          freeBuffers
        } finally {
          closed = true
        }
      }
    }
  }

  /**
   * To flush, we need to encrypt the data in the buffer and write to the
   * underlying stream, then do the flush.
   */
  override def flush {
    checkStream
    encrypt
    super.flush
  }

  override def write(b: Int) {
    oneByteBuf(0) = (b & 0xff).asInstanceOf[Byte]
    write(oneByteBuf, 0, oneByteBuf.length)
  }

  def checkStream {
    if (closed) {
      throw new IOException("Stream closed")
    }
  }

  /** Forcibly free the direct buffers. */
  def freeBuffers {
    CryptoStreamUtils.freeDB(inBuffer)
    CryptoStreamUtils.freeDB(outBuffer)
  }
}

