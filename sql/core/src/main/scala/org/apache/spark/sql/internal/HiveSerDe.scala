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

package org.apache.spark.sql.internal

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat

case class HiveSerDe(
  inputFormat: Option[String] = None,
  outputFormat: Option[String] = None,
  serde: Option[String] = None)

object HiveSerDe {
  val serdeMap = Map(
    "sequencefile" ->
      HiveSerDe(
        inputFormat = Option("org.apache.hadoop.mapred.SequenceFileInputFormat"),
        outputFormat = Option("org.apache.hadoop.mapred.SequenceFileOutputFormat"),
        serde = Option("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")),

    "rcfile" ->
      HiveSerDe(
        inputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),
        serde = Option("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe")),

    "orc" ->
      HiveSerDe(
        inputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
        serde = Option("org.apache.hadoop.hive.ql.io.orc.OrcSerde")),

    "parquet" ->
      HiveSerDe(
        inputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        serde = Option("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")),

    "textfile" ->
      HiveSerDe(
        inputFormat = Option("org.apache.hadoop.mapred.TextInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
        serde = Option("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")),

    "avro" ->
      HiveSerDe(
        inputFormat = Option("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"),
        outputFormat = Option("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"),
        serde = Option("org.apache.hadoop.hive.serde2.avro.AvroSerDe")))

  // `HiveSerDe` in `serdeMap` should be distinct.
  val serdeInverseMap: Map[HiveSerDe, String] = serdeMap.flatMap {
    case ("sequencefile", _) => None
    case ("rcfile", _) => None
    case ("textfile", serde) => Some((serde, "text"))
    case pair => Some(pair.swap)
  }

  /**
   * Get the Hive SerDe information from the data source abbreviation string or classname.
   *
   * @param source Currently the source abbreviation can be one of the following:
   *               SequenceFile, RCFile, ORC, PARQUET, and case insensitive.
   * @return HiveSerDe associated with the specified source
   */
  def sourceToSerDe(source: String): Option[HiveSerDe] = {
    val key = source.toLowerCase(Locale.ROOT) match {
      case s if s.startsWith("org.apache.spark.sql.parquet") => "parquet"
      case s if s.startsWith("org.apache.spark.sql.execution.datasources.parquet") => "parquet"
      case s if s.startsWith("org.apache.spark.sql.orc") => "orc"
      case s if s.startsWith("org.apache.spark.sql.hive.orc") => "orc"
      case s if s.startsWith("org.apache.spark.sql.execution.datasources.orc") => "orc"
      case s if s.equals("orcfile") => "orc"
      case s if s.equals("parquetfile") => "parquet"
      case s if s.equals("avrofile") => "avro"
      case s => s
    }

    serdeMap.get(key)
  }

  /**
   * Get the Spark data source name from the Hive SerDe information.
   *
   * @param serde Hive SerDe information.
   * @return Spark data source name associated with the specified Hive Serde.
   */
  def serdeToSource(serde: HiveSerDe): Option[String] = serdeInverseMap.get(serde)

  def getDefaultStorage(conf: SQLConf): CatalogStorageFormat = {
    // To respect hive-site.xml, it peeks Hadoop configuration from existing Spark session,
    // as an easy workaround. See SPARK-27555.
    val defaultFormatKey = "hive.default.fileformat"
    val defaultValue = {
      val defaultFormatValue = "textfile"
      SparkSession.getActiveSession.map { session =>
        session.sessionState.newHadoopConf().get(defaultFormatKey, defaultFormatValue)
      }.getOrElse(defaultFormatValue)
    }
    val defaultStorageType = conf.getConfString("hive.default.fileformat", defaultValue)
    val defaultHiveSerde = sourceToSerDe(defaultStorageType)
    CatalogStorageFormat.empty.copy(
      inputFormat = defaultHiveSerde.flatMap(_.inputFormat)
        .orElse(Some("org.apache.hadoop.mapred.TextInputFormat")),
      outputFormat = defaultHiveSerde.flatMap(_.outputFormat)
        .orElse(Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")),
      serde = defaultHiveSerde.flatMap(_.serde)
        .orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
  }
}
