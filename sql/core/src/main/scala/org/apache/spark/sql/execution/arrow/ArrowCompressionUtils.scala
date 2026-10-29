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

package org.apache.spark.sql.execution.arrow

import com.github.luben.zstd.Zstd
import org.apache.arrow.compression.{Lz4CompressionCodec, ZstdCompressionCodec}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.compression.{CompressionCodec, CompressionUtil, NoCompressionCodec}

import org.apache.spark.SparkException

private[sql] object ArrowCompressionUtils {

  /**
   * Creates the write-side Arrow [[CompressionCodec]] for the codec selected by
   * `spark.sql.execution.arrow.compression.codec`, honoring
   * `spark.sql.execution.arrow.compression.zstd.level` for zstd.
   *
   * The codec instance must be constructed directly rather than through
   * `CompressionCodec.Factory.INSTANCE.createCodec(codecType)`: the codec type enum does not
   * carry a compression level, so that factory overload always builds a codec at the default
   * level, silently dropping the configured one. The level only matters on the write side; the
   * read side looks up the codec by the type recorded in the IPC message, so it is unaffected.
   *
   * For zstd it must also be a [[BoundedZstdCompressionCodec]], which neither factory overload
   * returns, not even `createCodec(codecType, compressionLevel)`, which does keep the level.
   */
  def createCompressionCodec(
      codecName: String,
      zstdCompressionLevel: Int): CompressionCodec = {
    codecName match {
      case "none" => NoCompressionCodec.INSTANCE
      case "zstd" => new BoundedZstdCompressionCodec(zstdCompressionLevel)
      case "lz4" => new Lz4CompressionCodec()
      case other =>
        throw SparkException.internalError(
          s"Unsupported Arrow compression codec: $other. Supported values: none, zstd, lz4")
    }
  }
}

/**
 * A [[ZstdCompressionCodec]] that never lets zstd write past its output buffer. arrow-java up to
 * 19.0 passes `compressBound(len) + 8` as the destination capacity while pointing zstd 8 bytes
 * into a buffer of that size, so zstd can overrun it by up to 8 bytes; here the capacity is
 * `compressBound(len)`. Remove once on Arrow 20.0.0 (apache/arrow-java#1119).
 */
private[sql] class BoundedZstdCompressionCodec(compressionLevel: Int)
  extends ZstdCompressionCodec(compressionLevel) {

  override protected def doCompress(
      allocator: BufferAllocator,
      uncompressedBuffer: ArrowBuf): ArrowBuf = {
    val srcSize = uncompressedBuffer.writerIndex()
    val maxSize = Zstd.compressBound(srcSize)
    val compressedBuffer = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + maxSize)
    val bytesWritten = Zstd.compressUnsafe(
      compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
      maxSize,
      uncompressedBuffer.memoryAddress(),
      srcSize,
      compressionLevel)
    if (Zstd.isError(bytesWritten)) {
      compressedBuffer.close()
      throw new RuntimeException("Error compressing: " + Zstd.getErrorName(bytesWritten))
    }
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + bytesWritten)
    compressedBuffer
  }
}
