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

package org.apache.spark.sql.hive.orc

/**
 * Options for the ORC data source.
 */
private[orc] class OrcOptions(@transient private val parameters: Map[String, String])
  extends Serializable {

  import OrcOptions._

  /**
   * Compression codec to use. By default snappy compression.
   * Acceptable values are defined in [[shortOrcCompressionCodecNames]].
   */
  val compressionCodec: String = {
    // `orc.compress` is a ORC configuration. So, here we respect this as an option but
    // `compression` has higher precedence than `orc.compress`. It means if both are set,
    // we will use `compression`.
    val orcCompressionConf = parameters.get(OrcRelation.ORC_COMPRESSION)
    val codecName = parameters
      .get("compression")
      .orElse(orcCompressionConf)
      .getOrElse("snappy").toLowerCase
    if (!shortOrcCompressionCodecNames.contains(codecName)) {
      val availableCodecs = shortOrcCompressionCodecNames.keys.map(_.toLowerCase)
      throw new IllegalArgumentException(s"Codec [$codecName] " +
        s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
    }
    shortOrcCompressionCodecNames(codecName)
  }
}

private[orc] object OrcOptions {
  // The ORC compression short names
  private val shortOrcCompressionCodecNames = Map(
    "none" -> "NONE",
    "uncompressed" -> "NONE",
    "snappy" -> "SNAPPY",
    "zlib" -> "ZLIB",
    "lzo" -> "LZO")
}
