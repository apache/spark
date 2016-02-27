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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{BZip2Codec, DeflateCodec, GzipCodec, Lz4Codec, SnappyCodec}
import org.apache.spark.util.ShortCompressionCodecNameMapper

import org.apache.spark.util.Utils

private[datasources] object CompressionCodecs {

  /** Maps the short versions of compression codec names to fully-qualified class names. */
  private val hadoopShortCodecNameMapper = new ShortCompressionCodecNameMapper {
    override def bzip2: Option[String] = Some(classOf[BZip2Codec].getCanonicalName)
    override def deflate: Option[String] = Some(classOf[DeflateCodec].getCanonicalName)
    override def gzip: Option[String] = Some(classOf[GzipCodec].getCanonicalName)
    override def lz4: Option[String] = Some(classOf[Lz4Codec].getCanonicalName)
    override def snappy: Option[String] = Some(classOf[SnappyCodec].getCanonicalName)
  }

  /**
   * Return the full version of the given codec class.
   * If it is already a class name, just return it.
   */
  def getCodecClassName(name: String): String = {
    val codecName = hadoopShortCodecNameMapper.get(name).getOrElse(name)
    try {
      // Validate the codec name
      Utils.classForName(codecName)
      codecName
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Codec [$codecName] " +
          s"is not available. Known codecs are " +
          s"${hadoopShortCodecNameMapper.getAsMap.keys.mkString(", ")}.")
    }
  }

  /**
   * Set compression configurations to Hadoop `Configuration`.
   * `codec` should be a full class path
   */
  def setCodecConfiguration(conf: Configuration, codec: String): Unit = {
    conf.set("mapreduce.output.fileoutputformat.compress", "true")
    conf.set("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString)
    conf.set("mapreduce.output.fileoutputformat.compress.codec", codec)
    conf.set("mapreduce.map.output.compress", "true")
    conf.set("mapreduce.map.output.compress.codec", codec)
  }
}
