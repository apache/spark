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

import java.io.{InputStream, OutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}


/**
 * CompressionCodec allows the customization of choosing different compression implementations
 * to be used in block storage.
 */
trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream
}


private[spark] object CompressionCodec {

  def createCodec(): CompressionCodec = {
    createCodec(System.getProperty(
      "spark.io.compression.codec", classOf[LZFCompressionCodec].getName))
  }

  def createCodec(codecName: String): CompressionCodec = {
    Class.forName(codecName, true, Thread.currentThread.getContextClassLoader)
      .newInstance().asInstanceOf[CompressionCodec]
  }
}


/**
 * LZF implementation of [[org.apache.spark.io.CompressionCodec]].
 */
class LZFCompressionCodec extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}


/**
 * Snappy implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by spark.io.compression.snappy.block.size.
 */
class SnappyCompressionCodec extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = System.getProperty("spark.io.compression.snappy.block.size", "32768").toInt
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}
