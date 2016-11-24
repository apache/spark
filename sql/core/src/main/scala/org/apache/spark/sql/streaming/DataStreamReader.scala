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

package org.apache.spark.sql.streaming

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a streaming [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[SparkSession.readStream]] to access this.
 *
 * @since 2.0.0
 */
@Experimental
final class DataStreamReader private[sql](sparkSession: SparkSession) extends Logging {
  /**
   * :: Experimental ::
   * Specifies the input data source format.
   *
   * @since 2.0.0
   */
  @Experimental
  def format(source: String): DataStreamReader = {
    this.source = source
    this
  }

  /**
   * :: Experimental ::
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 2.0.0
   */
  @Experimental
  def schema(schema: StructType): DataStreamReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * :: Experimental ::
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: String): DataStreamReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * :: Experimental ::
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: Boolean): DataStreamReader = option(key, value.toString)

  /**
   * :: Experimental ::
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: Long): DataStreamReader = option(key, value.toString)

  /**
   * :: Experimental ::
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: Double): DataStreamReader = option(key, value.toString)

  /**
   * :: Experimental ::
   * (Scala-specific) Adds input options for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def options(options: scala.collection.Map[String, String]): DataStreamReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * :: Experimental ::
   * Adds input options for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def options(options: java.util.Map[String, String]): DataStreamReader = {
    this.options(options.asScala)
    this
  }


  /**
   * :: Experimental ::
   * Loads input data stream in as a [[DataFrame]], for data streams that don't require a path
   * (e.g. external key-value stores).
   *
   * @since 2.0.0
   */
  @Experimental
  def load(): DataFrame = {
    val dataSource =
      DataSource(
        sparkSession,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap)
    Dataset.ofRows(sparkSession, StreamingRelation(dataSource))
  }

  /**
   * :: Experimental ::
   * Loads input in as a [[DataFrame]], for data streams that read from some path.
   *
   * @since 2.0.0
   */
  @Experimental
  def load(path: String): DataFrame = {
    option("path", path).load()
  }

  /**
   * :: Experimental ::
   * Loads a JSON file stream (one object per line) and returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * You can set the following JSON-specific options to deal with non-standard JSON files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
   * <li>`prefersDecimal` (default `false`): infers all floating-point values as a decimal
   * type. If the values do not fit in decimal, then it infers them as doubles.</li>
   * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
   * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
   * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
   * </li>
   * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
   * (e.g. 00012)</li>
   * <li>`allowBackslashEscapingAnyCharacter` (default `false`): allows accepting quoting of all
   * character using backslash quoting mechanism</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   * during parsing.
   *   <ul>
   *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts
   *     the malformed string into a new field configured by `columnNameOfCorruptRecord`. When
   *     a schema is set by user, it sets `null` for extra fields.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  @Experimental
  def json(path: String): DataFrame = format("json").load(path)

  /**
   * :: Experimental ::
   * Loads a CSV file stream and returns the result as a [[DataFrame]].
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using [[schema]].
   *
   * You can set the following CSV-specific options to deal with CSV files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`sep` (default `,`): sets the single character as a separator for each
   * field and value.</li>
   * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
   * type.</li>
   * <li>`quote` (default `"`): sets the single character used for escaping quoted values where
   * the separator can be part of the value. If you would like to turn off quotations, you need to
   * set not `null` but an empty string. This behaviour is different form
   * `com.databricks.spark.csv`.</li>
   * <li>`escape` (default `\`): sets the single character used for escaping quotes inside
   * an already quoted value.</li>
   * <li>`comment` (default empty string): sets the single character used for skipping lines
   * beginning with this character. By default, it is disabled.</li>
   * <li>`header` (default `false`): uses the first line as names of columns.</li>
   * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
   * requires one extra pass over the data.</li>
   * <li>`ignoreLeadingWhiteSpace` (default `false`): defines whether or not leading whitespaces
   * from values being read should be skipped.</li>
   * <li>`ignoreTrailingWhiteSpace` (default `false`): defines whether or not trailing
   * whitespaces from values being read should be skipped.</li>
   * <li>`nullValue` (default empty string): sets the string representation of a null value. Since
   * 2.0.1, this applies to all supported types including the string type.</li>
   * <li>`nanValue` (default `NaN`): sets the string representation of a non-number" value.</li>
   * <li>`positiveInf` (default `Inf`): sets the string representation of a positive infinity
   * value.</li>
   * <li>`negativeInf` (default `-Inf`): sets the string representation of a negative infinity
   * value.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
   * a record can have.</li>
   * <li>`maxCharsPerColumn` (default `1000000`): defines the maximum number of characters allowed
   * for any given value being read.</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   *    during parsing.
   *   <ul>
   *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record. When
   *       a schema is set by user, it sets `null` for extra fields.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  @Experimental
  def csv(path: String): DataFrame = format("csv").load(path)

  /**
   * :: Experimental ::
   * Loads a Parquet file stream, returning the result as a [[DataFrame]].
   *
   * You can set the following Parquet-specific option(s) for reading Parquet files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`mergeSchema` (default is the value specified in `spark.sql.parquet.mergeSchema`): sets
   * whether we should merge schemas collected from all Parquet part-files. This will override
   * `spark.sql.parquet.mergeSchema`.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  @Experimental
  def parquet(path: String): DataFrame = {
    format("parquet").load(path)
  }

  /**
   * :: Experimental ::
   * Loads text files and returns a [[DataFrame]] whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   *
   * Each line in the text files is a new row in the resulting DataFrame. For example:
   * {{{
   *   // Scala:
   *   spark.readStream.text("/path/to/directory/")
   *
   *   // Java:
   *   spark.readStream().text("/path/to/directory/")
   * }}}
   *
   * You can set the following text-specific options to deal with text files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  @Experimental
  def text(path: String): DataFrame = format("text").load(path)


  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
}
