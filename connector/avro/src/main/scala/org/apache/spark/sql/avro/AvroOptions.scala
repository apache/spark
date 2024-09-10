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

import java.net.URI

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailFastMode, ParseMode}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for Avro Reader and Writer stored in case insensitive manner.
 */
private[sql] class AvroOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration)
  extends FileSourceOptions(parameters) with Logging {

  import AvroOptions._

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  /**
   * Optional schema provided by a user in schema file or in JSON format.
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
  val schema: Option[Schema] = {
    parameters.get(AVRO_SCHEMA).map(new Schema.Parser().setValidateDefaults(false).parse).orElse({
      val avroUrlSchema = parameters.get(AVRO_SCHEMA_URL).map(url => {
        log.debug("loading avro schema from url: " + url)
        val fs = FileSystem.get(new URI(url), conf)
        val in = fs.open(new Path(url))
        try {
          new Schema.Parser().setValidateDefaults(false).parse(in)
        } finally {
          in.close()
        }
      })
      avroUrlSchema
    })
  }

  /**
   * Iff true, perform Catalyst-to-Avro schema matching based on field position instead of field
   * name. This allows for a structurally equivalent Catalyst schema to be used with an Avro schema
   * whose field names do not match. Defaults to false.
   */
  val positionalFieldMatching: Boolean =
    parameters.get(POSITIONAL_FIELD_MATCHING).exists(_.toBoolean)

  /**
   * Top level record name in write result, which is required in Avro spec.
   * See https://avro.apache.org/docs/1.11.3/specification/#schema-record .
   * Default value is "topLevelRecord"
   */
  val recordName: String = parameters.getOrElse(RECORD_NAME, "topLevelRecord")

  /**
   * Record namespace in write result. Default value is "".
   * See Avro spec for details: https://avro.apache.org/docs/1.11.3/specification/#schema-record .
   */
  val recordNamespace: String = parameters.getOrElse(RECORD_NAMESPACE, "")

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
      .get(IGNORE_EXTENSION)
      .map(_.toBoolean)
      .getOrElse(!ignoreFilesWithoutExtension)
  }

  /**
   * The `compression` option allows to specify a compression codec used in write.
   * Currently supported codecs are `uncompressed`, `snappy`, `deflate`, `bzip2`, `xz` and
   * `zstandard`. If the option is not set, the `spark.sql.avro.compression.codec` config is
   * taken into account. If the former one is not set too, the `snappy` codec is used by default.
   */
  val compression: String = {
    parameters.get(COMPRESSION).getOrElse(SQLConf.get.avroCompressionCodec)
  }

  val parseMode: ParseMode =
    parameters.get(MODE).map(ParseMode.fromString).getOrElse(FailFastMode)

  /**
   * The rebasing mode for the DATE and TIMESTAMP_MICROS, TIMESTAMP_MILLIS values in reads.
   */
  val datetimeRebaseModeInRead: String = parameters
    .get(DATETIME_REBASE_MODE)
    .getOrElse(SQLConf.get.getConf(SQLConf.AVRO_REBASE_MODE_IN_READ))

  val useStableIdForUnionType: Boolean =
    parameters.get(STABLE_ID_FOR_UNION_TYPE).map(_.toBoolean).getOrElse(false)

  val stableIdPrefixForUnionType: String = parameters
    .getOrElse(STABLE_ID_PREFIX_FOR_UNION_TYPE, "member_")

  val recursiveFieldMaxDepth: Int =
    parameters.get(RECURSIVE_FIELD_MAX_DEPTH).map(_.toInt).getOrElse(-1)

  if (recursiveFieldMaxDepth > RECURSIVE_FIELD_MAX_DEPTH_LIMIT) {
    throw QueryCompilationErrors.avroOptionsException(
      RECURSIVE_FIELD_MAX_DEPTH,
      s"Should not be greater than $RECURSIVE_FIELD_MAX_DEPTH_LIMIT.")
  }
}

private[sql] object AvroOptions extends DataSourceOptions {
  def apply(parameters: Map[String, String]): AvroOptions = {
    val hadoopConf = SparkSession
      .getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new AvroOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }

  val IGNORE_EXTENSION = newOption("ignoreExtension")
  val MODE = newOption("mode")
  val RECORD_NAME = newOption("recordName")
  val COMPRESSION = newOption("compression")
  val AVRO_SCHEMA = newOption("avroSchema")
  val AVRO_SCHEMA_URL = newOption("avroSchemaUrl")
  val RECORD_NAMESPACE = newOption("recordNamespace")
  val POSITIONAL_FIELD_MATCHING = newOption("positionalFieldMatching")
  // The option controls rebasing of the DATE and TIMESTAMP values between
  // Julian and Proleptic Gregorian calendars. It impacts on the behaviour of the Avro
  // datasource similarly to the SQL config `spark.sql.avro.datetimeRebaseModeInRead`,
  // and can be set to the same values: `EXCEPTION`, `LEGACY` or `CORRECTED`.
  val DATETIME_REBASE_MODE = newOption("datetimeRebaseMode")
  // If it is set to true, Avro schema is deserialized into Spark SQL schema, and the Avro Union
  // type is transformed into a structure where the field names remain consistent with their
  // respective types. The resulting field names are converted to lowercase, e.g. member_int or
  // member_string. If two user-defined type names or a user-defined type name and a built-in
  // type name are identical regardless of case, an exception will be raised. However, in other
  // cases, the field names can be uniquely identified.
  val STABLE_ID_FOR_UNION_TYPE = newOption("enableStableIdentifiersForUnionType")
  // When STABLE_ID_FOR_UNION_TYPE is enabled, the option allows to configure the prefix for fields
  // of Avro Union type.
  val STABLE_ID_PREFIX_FOR_UNION_TYPE = newOption("stableIdentifierPrefixForUnionType")

  /**
   * Adds support for recursive fields. If this option is not specified or is set to 0, recursive
   * fields are not permitted. Setting it to 1 drops all recursive fields, 2 allows recursive
   * fields to be recursed once, and 3 allows it to be recursed twice and so on, up to 15.
   * Values larger than 15 are not allowed in order to avoid inadvertently creating very large
   * schemas. If an avro message has depth beyond this limit, the Spark struct returned is
   * truncated after the recursion limit.
   *
   * Examples: Consider an Avro schema with a recursive field:
   * {"type" : "record", "name" : "Node", "fields" : [{"name": "Id", "type": "int"},
   * {"name": "Next", "type": ["null", "Node"]}]}
   * The following lists the parsed schema with different values for this setting.
   *  1:  `struct<Id: int>`
   *  2:  `struct<Id: int, Next: struct<Id: int>>`
   *  3:  `struct<Id: int, Next: struct<Id: int, Next: struct<Id: int>>>`
   * and so on.
   */
  val RECURSIVE_FIELD_MAX_DEPTH = newOption("recursiveFieldMaxDepth")

  val RECURSIVE_FIELD_MAX_DEPTH_LIMIT: Int = 15
}
