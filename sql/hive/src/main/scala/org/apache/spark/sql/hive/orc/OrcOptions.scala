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

import org.apache.hadoop.conf.Configuration

/**
 * Options for the ORC data source.
 */
private[orc] class OrcOptions(
    @transient private val parameters: Map[String, String],
    @transient private val conf: Configuration)
  extends Serializable {

  import OrcOptions._

  /**
   * Compression codec to use. By default use the value specified in Hadoop configuration.
   * Acceptable values are defined in [[shortOrcCompressionCodecNames]].
   */
  val compressionCodec: String = {
    val default = conf.get(ORC_COMPRESSION, "NONE")

    // Because the ORC configuration value in `default` is not guaranteed to be the same
    // with keys in `shortOrcCompressionCodecNames` in Spark, this value should not be
    // used as the key for `shortOrcCompressionCodecNames` but just a return value.
    parameters.get("compression") match {
      case None => default
      case Some(name) =>
        val lowerCaseName = name.toLowerCase
        if (!shortOrcCompressionCodecNames.contains(lowerCaseName)) {
          val availableCodecs = shortOrcCompressionCodecNames.keys.map(_.toLowerCase)
          throw new IllegalArgumentException(s"Codec [$lowerCaseName] " +
            s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
        }
        shortOrcCompressionCodecNames(lowerCaseName)
    }
  }
}

private[orc] object OrcOptions {
  // The references of Hive's classes will be minimized.
  val ORC_COMPRESSION = "orc.compression"

  // The ORC compression short names
  private val shortOrcCompressionCodecNames = Map(
    "none" -> "NONE",
    "uncompressed" -> "NONE",
    "snappy" -> "SNAPPY",
    "zlib" -> "ZLIB",
    "lzo" -> "LZO")
}
