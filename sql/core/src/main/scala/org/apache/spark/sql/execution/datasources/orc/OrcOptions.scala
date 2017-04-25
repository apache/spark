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

package org.apache.spark.sql.execution.datasources.orc

import java.util.Locale

import org.apache.orc.OrcConf

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for ORC data source.
 */
private[orc] class OrcOptions(
    @transient private val parameters: CaseInsensitiveMap[String],
    @transient private val sqlConf: SQLConf)
  extends Serializable {

  import OrcOptions._

  def this(parameters: Map[String, String], sqlConf: SQLConf) =
    this(CaseInsensitiveMap(parameters), sqlConf)

  /**
   * Compression codec to use. By default use the value specified in SQLConf.
   * Acceptable values are defined in [[shortOrcCompressionCodecNames]].
   */
  val compressionCodecClassName: String = {
    val codecName = parameters
      .get("compression")
      .orElse(parameters.get(OrcConf.COMPRESS.getAttribute))
      .getOrElse("snappy").toLowerCase(Locale.ROOT)
    OrcOptions.shortOrcCompressionCodecNames(codecName)
  }
}

object OrcOptions {
  // The orc compression short names
  private[orc] val shortOrcCompressionCodecNames = Map(
    "none" -> "NONE",
    "uncompressed" -> "NONE",
    "snappy" -> "SNAPPY",
    "zlib" -> "ZLIB",
    "lzo" -> "LZO")

  private[orc] val extensionsForCompressionCodecNames = Map(
    "NONE" -> "",
    "SNAPPY" -> ".snappy",
    "ZLIB" -> ".zlib",
    "LZO" -> ".lzo")
}
