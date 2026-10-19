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

import java.io.InputStream
import java.util.Locale

import scala.collection.Seq
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.io.{CompressionCodec => SparkCompressionCodec}

/**
 * An utility object to look up Hadoop compression codecs and create input streams.
 * In addition to standard Hadoop codecs, it also supports Spark's Zstandard codec
 * if Hadopp is not compiled with Zstandard support. Additionally, it supports
 * non-standard file extensions like `.zstd` and `.gzip` for Zstandard and Gzip codecs.
 */
object HadoopCodecStreams {
  private val ZSTD_EXTENSIONS = Seq(".zstd", ".zst")

  /**
   * Hadoop `Configuration` key carrying the read-side compression codec explicitly requested via
   * the `decompression` data source option. The value is a full codec class name, an
   * empty string to force "no compression" (read the file as-is), or absent to infer the codec
   * from the file name extension. Set by the file format / scan read paths and consumed by
   * [[getDecompressionCodec]] so it uniformly overrides extension-based detection for line-based,
   * multi-line, whole-text, and schema-inference reads.
   *
   * This is an internal key, deliberately outside the `spark.*` namespace so it is not mistaken for
   * a user-facing configuration. The read paths always overwrite or unset it, so a value inherited
   * from the session or Hadoop configuration is never honored.
   */
  val DECOMPRESSION_CODEC_CONF = "org.apache.spark.sql.execution.datasources.decompressionCodec"

  /**
   * Returns the codec to decompress `file` with. When a read compression option has been set (see
   * [[DECOMPRESSION_CODEC_CONF]]) it takes precedence over the file name extension: a codec class
   * name selects that codec, and an empty value means the file is read as-is. Otherwise the codec
   * is inferred from the file name extension.
   */
  def getDecompressionCodec(
    config: Configuration,
    file: Path): Option[CompressionCodec] = {
    Option(config.get(DECOMPRESSION_CODEC_CONF)) match {
      case Some(codecClassName) if codecClassName.nonEmpty =>
        // The compression codec was explicitly specified via the read option, so use it instead of
        // inferring from the file name extension. Resolve it the same way the write path does
        // (see CodecStreams.getCompressionCodec): load the class and instantiate it, so any
        // loadable CompressionCodec is supported, not just those registered with the factory.
        val codecClass = config.getClassByName(codecClassName)
        Some(ReflectionUtils.newInstance(codecClass, config).asInstanceOf[CompressionCodec])
      case Some(_) =>
        // The read option was explicitly set to `none`/`uncompressed`: read the file as-is,
        // ignoring any compression-related file name extension.
        None
      case None =>
        // No explicit compression option: infer the codec from the file name extension.
        getDecompressionCodecFromExtension(config, file)
    }
  }

  // get codec based on file name extension
  private def getDecompressionCodecFromExtension(
    config: Configuration,
    file: Path): Option[CompressionCodec] = {
    val factory = new CompressionCodecFactory(config)
    Option(factory.getCodec(file)).orElse {
      // Try some non-standards extensions for Zstandard and Gzip
      file.getName.toLowerCase() match {
        case name if name.endsWith(".zstd") =>
          Option(factory.getCodecByName(classOf[ZStandardCodec].getName))
        case name if name.endsWith(".gzip") =>
          Option(factory.getCodecByName(classOf[GzipCodec].getName))
        case _ => None
      }
    }
  }

  def createZstdInputStream(
    file: Path,
    inputStream: InputStream): Option[InputStream] = {
    createZstdInputStream(new Configuration(false), file, inputStream)
  }

  def createZstdInputStream(
    config: Configuration,
    file: Path,
    inputStream: InputStream): Option[InputStream] = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val fileName = file.getName.toLowerCase(Locale.ROOT)

    // Use Spark's Zstandard codec when the read option explicitly requested a Zstandard codec (in
    // which case the extension may not match), or, when no codec was forced, when the file name has
    // a Zstandard extension. A forced non-Zstandard codec is never replaced based on the extension.
    val useZstd = Option(config.get(DECOMPRESSION_CODEC_CONF)) match {
      case Some(codecClassName) => codecClassName == classOf[ZStandardCodec].getName
      case None => ZSTD_EXTENSIONS.exists(fileName.endsWith)
    }
    val isOpt = if (useZstd) {
      Some(
        SparkCompressionCodec
          .createCodec(sparkConf, SparkCompressionCodec.ZSTD)
          .compressedInputStream(inputStream)
      )
    } else {
      None
    }
    isOpt
  }

  def createInputStream(
    config: Configuration,
    file: Path): InputStream = {
    val fs = file.getFileSystem(config)
    val inputStream: InputStream = fs.open(file)

    try {
      getDecompressionCodec(config, file)
        .map { codec =>
          try {
            codec.createInputStream(inputStream)
          } catch {
            case e: RuntimeException =>
              // createInputStream may fail for ZSTD if hadoop is not already compiled with ZSTD
              // support. In that case, we try to use Spark's Zstandard codec.
              createZstdInputStream(config, file, inputStream).getOrElse(throw e)
          }
        }.getOrElse(inputStream)
    } catch {
      case NonFatal(e) =>
        // No stream wraps the file stream yet, so close it rather than leak it. This covers a
        // forced codec that cannot be loaded or created as well as a decoder that fails.
        try {
          inputStream.close()
        } catch {
          case NonFatal(closeError) => e.addSuppressed(closeError)
        }
        throw e
    }
  }
}
