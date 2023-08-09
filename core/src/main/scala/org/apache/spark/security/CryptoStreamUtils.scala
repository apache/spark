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
package org.apache.spark.security

import java.io.{Closeable, InputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}
import java.util.Properties
import java.util.concurrent.TimeUnit
import javax.crypto.KeyGenerator
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import scala.collection.JavaConverters._

import com.google.common.io.ByteStreams
import org.apache.commons.crypto.random._
import org.apache.commons.crypto.stream._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.{CryptoUtils, JavaUtils}

/**
 * A util class for manipulating IO encryption and decryption streams.
 */
private[spark] object CryptoStreamUtils extends Logging {

  // The initialization vector length in bytes.
  val IV_LENGTH_IN_BYTES = 16
  // The prefix of IO encryption related configurations in Spark configuration.
  val SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX = "spark.io.encryption.commons.config."

  /**
   * Helper method to wrap `OutputStream` with `CryptoOutputStream` for encryption.
   */
  def createCryptoOutputStream(
      os: OutputStream,
      sparkConf: SparkConf,
      key: Array[Byte]): OutputStream = {
    val params = new CryptoParams(key, sparkConf)
    val iv = createInitializationVector(params.conf)
    os.write(iv)
    new ErrorHandlingOutputStream(
      new CryptoOutputStream(params.transformation, params.conf, os, params.keySpec,
        new IvParameterSpec(iv)),
      os)
  }

  /**
   * Wrap a `WritableByteChannel` for encryption.
   */
  def createWritableChannel(
      channel: WritableByteChannel,
      sparkConf: SparkConf,
      key: Array[Byte]): WritableByteChannel = {
    val params = new CryptoParams(key, sparkConf)
    val iv = createInitializationVector(params.conf)
    val helper = new CryptoHelperChannel(channel)

    helper.write(ByteBuffer.wrap(iv))
    new ErrorHandlingWritableChannel(
      new CryptoOutputStream(params.transformation, params.conf, helper, params.keySpec,
        new IvParameterSpec(iv)),
      helper)
  }

  /**
   * Helper method to wrap `InputStream` with `CryptoInputStream` for decryption.
   */
  def createCryptoInputStream(
      is: InputStream,
      sparkConf: SparkConf,
      key: Array[Byte]): InputStream = {
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    ByteStreams.readFully(is, iv)
    val params = new CryptoParams(key, sparkConf)
    new ErrorHandlingInputStream(
      new CryptoInputStream(params.transformation, params.conf, is, params.keySpec,
        new IvParameterSpec(iv)),
      is)
  }

  /**
   * Wrap a `ReadableByteChannel` for decryption.
   */
  def createReadableChannel(
      channel: ReadableByteChannel,
      sparkConf: SparkConf,
      key: Array[Byte]): ReadableByteChannel = {
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    val buf = ByteBuffer.wrap(iv)
    JavaUtils.readFully(channel, buf)

    val params = new CryptoParams(key, sparkConf)
    new ErrorHandlingReadableChannel(
      new CryptoInputStream(params.transformation, params.conf, channel, params.keySpec,
        new IvParameterSpec(iv)),
      channel)
  }

  def toCryptoConf(conf: SparkConf): Properties = {
    CryptoUtils.toCryptoConf(SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX,
      conf.getAll.toMap.asJava.entrySet())
  }

  /**
   * Creates a new encryption key.
   */
  def createKey(conf: SparkConf): Array[Byte] = {
    val keyLen = conf.get(IO_ENCRYPTION_KEY_SIZE_BITS)
    val ioKeyGenAlgorithm = conf.get(IO_ENCRYPTION_KEYGEN_ALGORITHM)
    val keyGen = KeyGenerator.getInstance(ioKeyGenAlgorithm)
    keyGen.init(keyLen)
    keyGen.generateKey().getEncoded()
  }

  /**
   * This method to generate an IV (Initialization Vector) using secure random.
   */
  private[this] def createInitializationVector(properties: Properties): Array[Byte] = {
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    val initialIVStart = System.nanoTime()
    CryptoRandomFactory.getCryptoRandom(properties).nextBytes(iv)
    val initialIVFinish = System.nanoTime()
    val initialIVTime = TimeUnit.NANOSECONDS.toMillis(initialIVFinish - initialIVStart)
    if (initialIVTime > 2000) {
      logWarning(s"It costs ${initialIVTime} milliseconds to create the Initialization Vector " +
        s"used by CryptoStream")
    }
    iv
  }

  /**
   * This class is a workaround for CRYPTO-125, that forces all bytes to be written to the
   * underlying channel. Since the callers of this API are using blocking I/O, there are no
   * concerns with regards to CPU usage here.
   */
  private class CryptoHelperChannel(sink: WritableByteChannel) extends WritableByteChannel {

    override def write(src: ByteBuffer): Int = {
      val count = src.remaining()
      while (src.hasRemaining()) {
        sink.write(src)
      }
      count
    }

    override def isOpen(): Boolean = sink.isOpen()

    override def close(): Unit = sink.close()

  }

  /**
   * SPARK-25535. The commons-crypto library will throw InternalError if something goes
   * wrong, and leave bad state behind in the Java wrappers, so it's not safe to use them
   * afterwards. This wrapper detects that situation and avoids further calls into the
   * commons-crypto code, while still allowing the underlying streams to be closed.
   *
   * This should be removed once CRYPTO-141 is fixed (and Spark upgrades its commons-crypto
   * dependency).
   */
  trait BaseErrorHandler extends Closeable {

    private var closed = false

    /** The encrypted stream that may get into an unhealthy state. */
    protected def cipherStream: Closeable

    /**
     * The underlying stream that is being wrapped by the encrypted stream, so that it can be
     * closed even if there's an error in the crypto layer.
     */
    protected def original: Closeable

    protected def safeCall[T](fn: => T): T = {
      if (closed) {
        throw new IOException("Cipher stream is closed.")
      }
      try {
        fn
      } catch {
        case ie: InternalError =>
          closed = true
          original.close()
          throw ie
      }
    }

    override def close(): Unit = {
      if (!closed) {
        cipherStream.close()
      }
    }

  }

  // Visible for testing.
  class ErrorHandlingReadableChannel(
      protected val cipherStream: ReadableByteChannel,
      protected val original: ReadableByteChannel)
    extends ReadableByteChannel with BaseErrorHandler {

    override def read(src: ByteBuffer): Int = safeCall {
      cipherStream.read(src)
    }

    override def isOpen(): Boolean = cipherStream.isOpen()

  }

  private class ErrorHandlingInputStream(
      protected val cipherStream: InputStream,
      protected val original: InputStream)
    extends InputStream with BaseErrorHandler {

    override def read(b: Array[Byte]): Int = safeCall {
      cipherStream.read(b)
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = safeCall {
      cipherStream.read(b, off, len)
    }

    override def read(): Int = safeCall {
      cipherStream.read()
    }
  }

  private class ErrorHandlingWritableChannel(
      protected val cipherStream: WritableByteChannel,
      protected val original: WritableByteChannel)
    extends WritableByteChannel with BaseErrorHandler {

    override def write(src: ByteBuffer): Int = safeCall {
      cipherStream.write(src)
    }

    override def isOpen(): Boolean = cipherStream.isOpen()

  }

  private class ErrorHandlingOutputStream(
      protected val cipherStream: OutputStream,
      protected val original: OutputStream)
    extends OutputStream with BaseErrorHandler {

    override def flush(): Unit = safeCall {
      cipherStream.flush()
    }

    override def write(b: Array[Byte]): Unit = safeCall {
      cipherStream.write(b)
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = safeCall {
      cipherStream.write(b, off, len)
    }

    override def write(b: Int): Unit = safeCall {
      cipherStream.write(b)
    }
  }

  private class CryptoParams(key: Array[Byte], sparkConf: SparkConf) {

    val keySpec = new SecretKeySpec(key, "AES")
    val transformation = sparkConf.get(IO_CRYPTO_CIPHER_TRANSFORMATION)
    val conf = toCryptoConf(sparkConf)

  }

}
