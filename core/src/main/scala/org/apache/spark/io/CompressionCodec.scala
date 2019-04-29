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

import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}
import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream}
import org.xerial.snappy.{Snappy, SnappyInputStream, SnappyOutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
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

  def compressedInputStream(s: InputStream): InputStream

  private[spark] def compressedContinuousInputStream(s: InputStream): InputStream = {
    compressedInputStream(s)
  }
}

private[spark] object CompressionCodec {

  private val configKey = IO_COMPRESSION_CODEC.key

  private[spark] def supportsConcatenationOfSerializedStreams(codec: CompressionCodec): Boolean = {
    (codec.isInstanceOf[SnappyCompressionCodec] || codec.isInstanceOf[LZFCompressionCodec]
      || codec.isInstanceOf[LZ4CompressionCodec] || codec.isInstanceOf[ZStdCompressionCodec])
  }

  private val shortCompressionCodecNames = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName,
    "zstd" -> classOf[ZStdCompressionCodec].getName)

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
    codec.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not available. " +
      s"Consider setting $configKey=$FALLBACK_COMPRESSION_CODEC"))
  }

  /**
   * Return the short version of the given codec name.
   * If it is already a short name, just return it.
   */
  def getShortName(codecName: String): String = {
    if (shortCompressionCodecNames.contains(codecName)) {
      codecName
    } else {
      shortCompressionCodecNames
        .collectFirst { case (k, v) if v == codecName => k }
        .getOrElse { throw new IllegalArgumentException(s"No short name for codec $codecName.") }
    }
  }

  val FALLBACK_COMPRESSION_CODEC = "snappy"
  val DEFAULT_COMPRESSION_CODEC = "lz4"
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

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt
    new LZ4BlockOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    val disableConcatenationOfByteStream = false
    new LZ4BlockInputStream(s, disableConcatenationOfByteStream)
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

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
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

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.get(IO_COMPRESSION_SNAPPY_BLOCKSIZE).toInt
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

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    // Wrap the zstd output stream in a buffered output stream, so that we can
    // avoid overhead excessive of JNI call while trying to compress small amount of data.
    new BufferedOutputStream(new ZstdOutputStream(s, level), bufferSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    // Wrap the zstd input stream in a buffered input stream so that we can
    // avoid overhead excessive of JNI call while trying to uncompress small amount of data.
    new BufferedInputStream(new ZstdInputStream(s), bufferSize)
  }

  override def compressedContinuousInputStream(s: InputStream): InputStream = {
    // SPARK-26283: Enable reading from open frames of zstd (for eg: zstd compressed eventLog
    // Reading). By default `isContinuous` is false, and when we try to read from open frames,
    // `compressedInputStream` method above throws truncated error exception. This method set
    // `isContinuous` true to allow reading from open frames.
    new BufferedInputStream(new ZstdInputStream(s).setContinuous(true), bufferSize)
  }
}
