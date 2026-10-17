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

package org.apache.spark.sql.catalyst.util

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{SplittableCompressionCodec, ZStandardCodec}

import org.apache.spark.io.HadoopCodecStreams
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

object CompressionCodecs {
  private val shortCompressionCodecNames = HadoopCompressionCodec.values().map { codec =>
    codec.lowerCaseName() -> Option(codec.getCompressionCodec).map(_.getClass.getName).orNull
  }.toMap

  // Read-side short names extend the write-side set with Zstandard, which file-format writers do
  // not support yet (see the TODO in `HadoopCompressionCodec`) but readers do, via Spark's
  // built-in Zstandard codec when Hadoop lacks native support (see `HadoopCodecStreams`).
  private val readShortCompressionCodecNames = shortCompressionCodecNames ++ Map(
    "zstd" -> classOf[ZStandardCodec].getName,
    "zstandard" -> classOf[ZStandardCodec].getName)

  /**
   * Return the full version of the given codec class.
   * If it is already a class name, just return it.
   */
  def getCodecClassName(name: String): String = {
    getCodecClassName(name, shortCompressionCodecNames)
  }

  /**
   * Read-side counterpart of [[getCodecClassName]]. Accepts the same short names plus `zstd` /
   * `zstandard`, since the file readers can decompress Zstandard even though the writers cannot
   * produce it. Used to parse the `decompression` option.
   */
  def getReadCodecClassName(name: String): String = {
    getCodecClassName(name, readShortCompressionCodecNames)
  }

  private def getCodecClassName(
      name: String,
      shortNames: Map[String, String]): String = {
    val codecName = shortNames.getOrElse(name.toLowerCase(Locale.ROOT), name)
    try {
      // Validate the codec name
      if (codecName != null) {
        Utils.classForName(codecName)
      }
      codecName
    } catch {
      case _: ClassNotFoundException =>
        throw QueryExecutionErrors.codecNotAvailableError(
          codecName, shortNames.keys.mkString(", "))
    }
  }

  /**
   * Set compression configurations to Hadoop `Configuration`.
   * `codec` should be a full class path
   */
  def setCodecConfiguration(conf: Configuration, codec: String): Unit = {
    if (codec != null) {
      conf.set("mapreduce.output.fileoutputformat.compress", "true")
      conf.set("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString)
      conf.set("mapreduce.output.fileoutputformat.compress.codec", codec)
      conf.set("mapreduce.map.output.compress", "true")
      conf.set("mapreduce.map.output.compress.codec", codec)
    } else {
      // This infers the option `compression` is set to `uncompressed` or `none`.
      conf.set("mapreduce.output.fileoutputformat.compress", "false")
      conf.set("mapreduce.map.output.compress", "false")
    }
  }

  /**
   * Records the codec requested via the `decompression` data source option into the Hadoop
   * `Configuration`, so the file readers decompress with it instead of inferring the codec from
   * the file name extension (see [[HadoopCodecStreams.getDecompressionCodec]]). Called on the
   * driver while planning the read, so an unsupported setup fails before any task runs.
   *
   * @param decompressionCodec the parsed `decompression` option: `None` when absent (unset
   *                           the conf so the codec is inferred from the extension),
   *                           `Some(null)` for `none`/`uncompressed` (read the file as-is), or
   *                           `Some(className)` to force that codec.
   */
  def setReadCodecConfiguration(
      conf: Configuration,
      decompressionCodec: Option[String]): Unit = decompressionCodec match {
    case Some(codec) =>
      // Hadoop's `LineRecordReader` infers the codec from the file name extension and cannot be
      // told otherwise, so fail rather than silently ignore the option.
      if (!SQLConf.get.hadoopLineRecordReaderEnabled) {
        throw QueryExecutionErrors.decompressionCodecWithLegacyLineReaderError()
      }
      // An empty string encodes "no compression" (read as-is); a class name selects that codec.
      conf.set(HadoopCodecStreams.DECOMPRESSION_CODEC_CONF, Option(codec).getOrElse(""))
    case None =>
      // Unset rather than leave untouched, so a value inherited from the session or Hadoop
      // configuration cannot override the codec of a read that did not ask for it.
      conf.unset(HadoopCodecStreams.DECOMPRESSION_CODEC_CONF)
  }

  /**
   * Whether files decompressed with the given parsed `decompression` value can be split into
   * multiple partitions: `null` (`none`/`uncompressed`) reads the raw file and stays splittable,
   * and a codec class is splittable only if it is a `SplittableCompressionCodec` (e.g. bzip2).
   */
  def isSplittableReadCodec(codec: String): Boolean = {
    codec == null ||
      classOf[SplittableCompressionCodec].isAssignableFrom(Utils.classForName(codec))
  }
}
