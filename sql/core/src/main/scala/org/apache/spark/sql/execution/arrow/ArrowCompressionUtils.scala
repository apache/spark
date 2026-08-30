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

import org.apache.arrow.compression.{Lz4CompressionCodec, ZstdCompressionCodec}
import org.apache.arrow.vector.compression.{CompressionCodec, NoCompressionCodec}

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
   */
  def createCompressionCodec(
      codecName: String,
      zstdCompressionLevel: Int): CompressionCodec = {
    codecName match {
      case "none" => NoCompressionCodec.INSTANCE
      case "zstd" => new ZstdCompressionCodec(zstdCompressionLevel)
      case "lz4" => new Lz4CompressionCodec()
      case other =>
        throw SparkException.internalError(
          s"Unsupported Arrow compression codec: $other. Supported values: none, zstd, lz4")
    }
  }
}
