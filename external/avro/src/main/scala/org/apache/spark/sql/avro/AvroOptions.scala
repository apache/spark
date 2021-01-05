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

package org.apache.spark.sql.avro

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailFastMode, ParseMode}
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for Avro Reader and Writer stored in case insensitive manner.
 */
private[sql] class AvroOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration) extends Logging with Serializable {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  /**
   * Optional schema provided by a user in JSON format.
   *
   * When reading Avro, this option can be set to an evolved schema, which is compatible but
   * different with the actual Avro schema. The deserialization schema will be consistent with
   * the evolved schema. For example, if we set an evolved schema containing one additional
   * column with a default value, the reading result in Spark will contain the new column too.
   *
   * When writing Avro, this option can be set if the expected output Avro schema doesn't match the
   * schema converted by Spark. For example, the expected schema of one column is of "enum" type,
   * instead of "string" type in the default converted schema.
   */
  val schema: Option[String] = parameters.get("avroSchema")

  /**
   * Top level record name in write result, which is required in Avro spec.
   * See https://avro.apache.org/docs/1.8.2/spec.html#schema_record .
   * Default value is "topLevelRecord"
   */
  val recordName: String = parameters.getOrElse("recordName", "topLevelRecord")

  /**
   * Record namespace in write result. Default value is "".
   * See Avro spec for details: https://avro.apache.org/docs/1.8.2/spec.html#schema_record .
   */
  val recordNamespace: String = parameters.getOrElse("recordNamespace", "")

  /**
   * The `ignoreExtension` option controls ignoring of files without `.avro` extensions in read.
   * If the option is enabled, all files (with and without `.avro` extension) are loaded.
   * If the option is not set, the Hadoop's config `avro.mapred.ignore.inputs.without.extension`
   * is taken into account. If the former one is not set too, file extensions are ignored.
   */
  @deprecated("Use the general data source option pathGlobFilter for filtering file names", "3.0")
  val ignoreExtension: Boolean = {
    val ignoreFilesWithoutExtensionByDefault = false
    val ignoreFilesWithoutExtension = conf.getBoolean(
      AvroFileFormat.IgnoreFilesWithoutExtensionProperty,
      ignoreFilesWithoutExtensionByDefault)

    parameters
      .get(AvroOptions.ignoreExtensionKey)
      .map(_.toBoolean)
      .getOrElse(!ignoreFilesWithoutExtension)
  }

  /**
   * The `compression` option allows to specify a compression codec used in write.
   * Currently supported codecs are `uncompressed`, `snappy`, `deflate`, `bzip2` and `xz`.
   * If the option is not set, the `spark.sql.avro.compression.codec` config is taken into
   * account. If the former one is not set too, the `snappy` codec is used by default.
   */
  val compression: String = {
    parameters.get("compression").getOrElse(SQLConf.get.avroCompressionCodec)
  }

  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(FailFastMode)
}

private[sql] object AvroOptions {
  def apply(parameters: Map[String, String]): AvroOptions = {
    val hadoopConf = SparkSession
      .getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new AvroOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }

  val ignoreExtensionKey = "ignoreExtension"
}
