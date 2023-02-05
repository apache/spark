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

import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for the Parquet data source.
 */
class ParquetOptions(
    @transient private val parameters: CaseInsensitiveMap[String],
    @transient private val sqlConf: SQLConf)
  extends FileSourceOptions(parameters) {

  import ParquetOptions._

  def this(parameters: Map[String, String], sqlConf: SQLConf) =
    this(CaseInsensitiveMap(parameters), sqlConf)

  /**
   * Compression codec to use. By default use the value specified in SQLConf.
   * Acceptable values are defined in [[shortParquetCompressionCodecNames]].
   */
  val compressionCodecClassName: String = {
    // `compression`, `parquet.compression`(i.e., ParquetOutputFormat.COMPRESSION), and
    // `spark.sql.parquet.compression.codec`
    // are in order of precedence from highest to lowest.
    val parquetCompressionConf = parameters.get(PARQUET_COMPRESSION)
    val codecName = parameters
      .get(COMPRESSION)
      .orElse(parquetCompressionConf)
      .getOrElse(sqlConf.parquetCompressionCodec)
      .toLowerCase(Locale.ROOT)
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

  /**
   * The rebasing mode for the DATE and TIMESTAMP_MICROS, TIMESTAMP_MILLIS values in reads.
   */
  def datetimeRebaseModeInRead: String = parameters
    .get(DATETIME_REBASE_MODE)
    .getOrElse(sqlConf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ))
  /**
   * The rebasing mode for INT96 timestamp values in reads.
   */
  def int96RebaseModeInRead: String = parameters
    .get(INT96_REBASE_MODE)
    .getOrElse(sqlConf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ))
}


object ParquetOptions extends DataSourceOptions {
  // The parquet compression short names
  private val shortParquetCompressionCodecNames = Map(
    "none" -> CompressionCodecName.UNCOMPRESSED,
    "uncompressed" -> CompressionCodecName.UNCOMPRESSED,
    "snappy" -> CompressionCodecName.SNAPPY,
    "gzip" -> CompressionCodecName.GZIP,
    "lzo" -> CompressionCodecName.LZO,
    "lz4" -> CompressionCodecName.LZ4,
    "brotli" -> CompressionCodecName.BROTLI,
    "zstd" -> CompressionCodecName.ZSTD)

  def getParquetCompressionCodecName(name: String): String = {
    shortParquetCompressionCodecNames(name).name()
  }

  val MERGE_SCHEMA = newOption("mergeSchema")
  val PARQUET_COMPRESSION = newOption(ParquetOutputFormat.COMPRESSION)
  val COMPRESSION = newOption("compression")

  // The option controls rebasing of the DATE and TIMESTAMP values between
  // Julian and Proleptic Gregorian calendars. It impacts on the behaviour of the Parquet
  // datasource similarly to the SQL config `spark.sql.parquet.datetimeRebaseModeInRead`,
  // and can be set to the same values: `EXCEPTION`, `LEGACY` or `CORRECTED`.
  val DATETIME_REBASE_MODE = newOption("datetimeRebaseMode")

  // The option controls rebasing of the INT96 timestamp values between Julian and Proleptic
  // Gregorian calendars. It impacts on the behaviour of the Parquet datasource similarly to
  // the SQL config `spark.sql.parquet.int96RebaseModeInRead`.
  // The valid option values are: `EXCEPTION`, `LEGACY` or `CORRECTED`.
  val INT96_REBASE_MODE = newOption("int96RebaseMode")
}
