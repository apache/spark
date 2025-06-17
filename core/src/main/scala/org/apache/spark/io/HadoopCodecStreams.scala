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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._

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

  // get codec based on file name extension
  def getDecompressionCodec(
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
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val fileName = file.getName.toLowerCase(Locale.ROOT)

    val isOpt = if (ZSTD_EXTENSIONS.exists(fileName.endsWith)) {
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

    getDecompressionCodec(config, file)
      .map { codec =>
        try {
          codec.createInputStream(inputStream)
        } catch {
          case e: RuntimeException =>
            // createInputStream may fail for ZSTD if hadoop is not already compiled with ZSTD
            // support. In that case, we try to use Spark's Zstandard codec.
            createZstdInputStream(file, inputStream).getOrElse(throw e)
        }
      }.getOrElse(inputStream)
  }
}
