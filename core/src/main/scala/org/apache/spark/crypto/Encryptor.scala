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

import java.nio.ByteBuffer

/**
 * Interface Encryptor
 */
trait Encryptor {
  /**
   * Initialize the encryptor and the internal encryption context.
   * @param key encryption key.
   * @param iv encryption initialization vector
   * @throws IOException if initialization fails
   */
  def init(key: Array[Byte], iv: Array[Byte])

  /**
   * Indicate whether the encryption context is reset.
   * <p/>
   * Certain modes, like CTR, require a different IV depending on the
   * position in the stream. Generally, the encryptor maintains any necessary
   * context for calculating the IV and counter so that no reinit is necessary
   * during the encryption. Reinit before each operation is inefficient.
   * @return boolean whether context is reset.
   */
  def isContextReset: Boolean

  /**
   * This presents a direct interface encrypting with direct ByteBuffers.
   * <p/>
   * This function does not always encrypt the entire buffer and may potentially
   * need to be called multiple times to process an entire buffer. The object
   * may hold the encryption context internally.
   * <p/>
   * Some implementations may require sufficient space in the destination
   * buffer to encrypt the entire input buffer.
   * <p/>
   * Upon return, inBuffer.position() will be advanced by the number of bytes
   * read and outBuffer.position() by bytes written. Implementations should
   * not modify inBuffer.limit() and outBuffer.limit().
   * <p/>
   * @param inBuffer a direct {@link ByteBuffer} to read from. inBuffer may
   * not be null and inBuffer.remaining() must be > 0
   * @param outBuffer a direct {@link ByteBuffer} to write to. outBuffer may
   * not be null and outBuffer.remaining() must be > 0
   * @throws IOException if encryption fails
   */
  def encrypt(inBuffer: ByteBuffer, outBuffer: ByteBuffer)
}
