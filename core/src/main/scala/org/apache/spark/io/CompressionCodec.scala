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

import java.io._
import java.util.Locale

import com.github.luben.zstd.{NoPool, RecyclingBufferPool, ZstdInputStreamNoFinalizer, ZstdOutputStreamNoFinalizer}
import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import com.ning.compress.lzf.parallel.PLZFOutputStream
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.xerial.snappy.{Snappy, SnappyInputStream, SnappyOutputStream}

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * CompressionCodec allows the customization of choosing different compression implementations
 * to be used in block storage.
 *
 * @note The wire protocol for a codec is not guaranteed compatible across versions of Spark.
 * This is intended for use as an internal compression utility within a single Spark application.
 */
@DeveloperApi
trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  private[spark] def compressedContinuousOutputStream(s: OutputStream): OutputStream = {
    compressedOutputStream(s)
  }

  def compressedInputStream(s: InputStream): InputStream

  private[spark] def compressedContinuousInputStream(s: InputStream): InputStream = {
    compressedInputStream(s)
  }
}

private[spark] object CompressionCodec {

  private[spark] def supportsConcatenationOfSerializedStreams(codec: CompressionCodec): Boolean = {
    (codec.isInstanceOf[SnappyCompressionCodec] || codec.isInstanceOf[LZFCompressionCodec]
      || codec.isInstanceOf[LZ4CompressionCodec] || codec.isInstanceOf[ZStdCompressionCodec])
  }

  val LZ4 = "lz4"
  val LZF = "lzf"
  val SNAPPY = "snappy"
  val ZSTD = "zstd"

  private[spark] val shortCompressionCodecNames = Map(
    LZ4 -> classOf[LZ4CompressionCodec].getName,
    LZF -> classOf[LZFCompressionCodec].getName,
    SNAPPY -> classOf[SnappyCompressionCodec].getName,
    ZSTD -> classOf[ZStdCompressionCodec].getName)

  def getCodecName(conf: SparkConf): String = {
    conf.get(IO_COMPRESSION_CODEC)
  }

  def createCodec(conf: SparkConf): CompressionCodec = {
    createCodec(conf, getCodecName(conf))
  }

  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val codecClass =
      shortCompressionCodecNames.getOrElse(codecName.toLowerCase(Locale.ROOT), codecName)
    val codec = try {
      val ctor =
        Utils.classForName[CompressionCodec](codecClass).getConstructor(classOf[SparkConf])
      Some(ctor.newInstance(conf))
    } catch {
      case _: ClassNotFoundException | _: IllegalArgumentException => None
    }
    codec.getOrElse(throw SparkCoreErrors.codecNotAvailableError(codecName))
  }

  /**
   * Return the short version of the given codec name.
   * If it is already a short name, just return it.
   */
  def getShortName(codecName: String): String = {
    val lowercasedCodec = codecName.toLowerCase(Locale.ROOT)
    if (shortCompressionCodecNames.contains(lowercasedCodec)) {
      lowercasedCodec
    } else {
      shortCompressionCodecNames
        .collectFirst { case (k, v) if v == codecName => k }
        .getOrElse { throw new SparkIllegalArgumentException(
          errorClass = "CODEC_SHORT_NAME_NOT_FOUND",
          messageParameters = Map("codecName" -> codecName))}
    }
  }

  val FALLBACK_COMPRESSION_CODEC = SNAPPY
  val ALL_COMPRESSION_CODECS = shortCompressionCodecNames.values.toSeq
}

/**
 * :: DeveloperApi ::
 * LZ4 implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.lz4.blockSize`.
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class LZ4CompressionCodec(conf: SparkConf) extends CompressionCodec {

  // SPARK-28102: if the LZ4 JNI libraries fail to initialize then `fastestInstance()` calls fall
  // back to non-JNI implementations but do not remember the fact that JNI failed to load, so
  // repeated calls to `fastestInstance()` will cause performance problems because the JNI load
  // will be repeatedly re-attempted and that path is slow because it throws exceptions from a
  // static synchronized method (causing lock contention). To avoid this problem, we cache the
  // result of the `fastestInstance()` calls ourselves (both factories are thread-safe).
  @transient private[this] lazy val lz4Factory: LZ4Factory = LZ4Factory.fastestInstance()
  @transient private[this] lazy val xxHashFactory: XXHashFactory = XXHashFactory.fastestInstance()

  private[this] val defaultSeed: Int = 0x9747b28c // LZ4BlockOutputStream.DEFAULT_SEED
  private[this] val blockSize = conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val syncFlush = false
    new LZ4BlockOutputStream(
      s,
      blockSize,
      lz4Factory.fastCompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      syncFlush)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    val disableConcatenationOfByteStream = false
    new LZ4BlockInputStream(
      s,
      lz4Factory.fastDecompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      disableConcatenationOfByteStream)
  }
}


/**
 * :: DeveloperApi ::
 * LZF implementation of [[org.apache.spark.io.CompressionCodec]].
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class LZFCompressionCodec(conf: SparkConf) extends CompressionCodec {
  private val parallelCompression = conf.get(IO_COMPRESSION_LZF_PARALLEL)

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    if (parallelCompression) {
      new PLZFOutputStream(s)
    } else {
      new LZFOutputStream(s).setFinishBlockOnFlush(true)
    }
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}


/**
 * :: DeveloperApi ::
 * Snappy implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.snappy.blockSize`.
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class SnappyCompressionCodec(conf: SparkConf) extends CompressionCodec {

  try {
    Snappy.getNativeLibraryVersion
  } catch {
    case e: Error => throw new IllegalArgumentException(e)
  }
  private[this] val blockSize = conf.get(IO_COMPRESSION_SNAPPY_BLOCKSIZE).toInt

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}

/**
 * :: DeveloperApi ::
 * ZStandard implementation of [[org.apache.spark.io.CompressionCodec]]. For more
 * details see - http://facebook.github.io/zstd/
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class ZStdCompressionCodec(conf: SparkConf) extends CompressionCodec {

  private val bufferSize = conf.get(IO_COMPRESSION_ZSTD_BUFFERSIZE).toInt
  // Default compression level for zstd compression to 1 because it is
  // fastest of all with reasonably high compression ratio.
  private val level = conf.get(IO_COMPRESSION_ZSTD_LEVEL)

  private val bufferPool = if (conf.get(IO_COMPRESSION_ZSTD_BUFFERPOOL_ENABLED)) {
    RecyclingBufferPool.INSTANCE
  } else {
    NoPool.INSTANCE
  }

  private val workers = conf.get(IO_COMPRESSION_ZSTD_WORKERS)

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    // Wrap the zstd output stream in a buffered output stream, so that we can
    // avoid overhead excessive of JNI call while trying to compress small amount of data.
    val os = new ZstdOutputStreamNoFinalizer(s, bufferPool).setLevel(level).setWorkers(workers)
    new BufferedOutputStream(os, bufferSize)
  }

  override private[spark] def compressedContinuousOutputStream(s: OutputStream) = {
    // SPARK-29322: Set "closeFrameOnFlush" to 'true' to let continuous input stream not being
    // stuck on reading open frame.
    val os = new ZstdOutputStreamNoFinalizer(s, bufferPool)
      .setLevel(level)
      .setWorkers(workers)
      .setCloseFrameOnFlush(true)
    new BufferedOutputStream(os, bufferSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    // Wrap the zstd input stream in a buffered input stream so that we can
    // avoid overhead excessive of JNI call while trying to uncompress small amount of data.
    new BufferedInputStream(new ZstdInputStreamNoFinalizer(s, bufferPool), bufferSize)
  }

  override def compressedContinuousInputStream(s: InputStream): InputStream = {
    // SPARK-26283: Enable reading from open frames of zstd (for eg: zstd compressed eventLog
    // Reading). By default `isContinuous` is false, and when we try to read from open frames,
    // `compressedInputStream` method above throws truncated error exception. This method set
    // `isContinuous` true to allow reading from open frames.
    new BufferedInputStream(
      new ZstdInputStreamNoFinalizer(s, bufferPool).setContinuous(true), bufferSize)
  }
}
