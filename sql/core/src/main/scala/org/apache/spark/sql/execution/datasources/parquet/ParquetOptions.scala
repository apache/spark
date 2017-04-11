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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.Locale

import org.apache.parquet.hadoop.metadata.CompressionCodecName

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for the Parquet data source.
 */
private[parquet] class ParquetOptions(
    @transient private val parameters: CaseInsensitiveMap[String],
    @transient private val sqlConf: SQLConf)
  extends Serializable {

  import ParquetOptions._

  def this(parameters: Map[String, String], sqlConf: SQLConf) =
    this(CaseInsensitiveMap(parameters), sqlConf)

  /**
   * Compression codec to use. By default use the value specified in SQLConf.
   * Acceptable values are defined in [[shortParquetCompressionCodecNames]].
   */
  val compressionCodecClassName: String = {
    val codecName = parameters.getOrElse("compression",
      sqlConf.parquetCompressionCodec).toLowerCase(Locale.ROOT)
    if (!shortParquetCompressionCodecNames.contains(codecName)) {
      val availableCodecs =
        shortParquetCompressionCodecNames.keys.map(_.toLowerCase(Locale.ROOT))
      throw new IllegalArgumentException(s"Codec [$codecName] " +
        s"is not available. Available codecs are ${availableCodecs.mkString(", ")}.")
    }
    shortParquetCompressionCodecNames(codecName).name()
  }

  /**
   * Whether it merges schemas or not. When the given Parquet files have different schemas,
   * the schemas can be merged.  By default use the value specified in SQLConf.
   */
  val mergeSchema: Boolean = parameters
    .get(MERGE_SCHEMA)
    .map(_.toBoolean)
    .getOrElse(sqlConf.isParquetSchemaMergingEnabled)
}


object ParquetOptions {
  val MERGE_SCHEMA = "mergeSchema"

  // The parquet compression short names
  private val shortParquetCompressionCodecNames = Map(
    "none" -> CompressionCodecName.UNCOMPRESSED,
    "uncompressed" -> CompressionCodecName.UNCOMPRESSED,
    "snappy" -> CompressionCodecName.SNAPPY,
    "gzip" -> CompressionCodecName.GZIP,
    "lzo" -> CompressionCodecName.LZO)
}
