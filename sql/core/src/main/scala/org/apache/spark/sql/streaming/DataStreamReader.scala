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

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
final class DataStreamReader private[sql](sparkSession: SparkSession) extends Logging {
  /**
   * Specifies the input data source format.
   *
   * @since 2.0.0
   */
  def format(source: String): DataStreamReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 2.0.0
   */
  def schema(schema: StructType): DataStreamReader = {
    if (schema != null) {
      val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
      this.userSpecifiedSchema = Option(replaced)
    }
    this
  }

  /**
   * Specifies the schema by using the input DDL-formatted string. Some data sources (e.g. JSON) can
   * infer the input schema automatically from data. By specifying the schema here, the underlying
   * data source can skip the schema inference step, and thus speed up data loading.
   *
   * @since 2.3.0
   */
  def schema(schemaString: String): DataStreamReader = {
    schema(StructType.fromDDL(schemaString))
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a time zone ID
   * to be used to parse timestamps in the JSON/CSV datasources or partition values. The following
   * formats of `timeZone` are supported:
   *   <ul>
   *     <li> Region-based zone ID: It should have the form 'area/city', such as
   *         'America/Los_Angeles'.</li>
   *     <li> Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00'
   *          or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
   *   </ul>
   * Other short names like 'CST' are not recommended to use because they can be ambiguous.
   * If it isn't set, the current value of the SQL config `spark.sql.session.timeZone` is
   * used by default.
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): DataStreamReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataStreamReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataStreamReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataStreamReader = option(key, value.toString)

  /**
   * (Scala-specific) Adds input options for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a time zone ID
   * to be used to parse timestamps in the JSON/CSV datasources or partition values. The following
   * formats of `timeZone` are supported:
   *   <ul>
   *     <li> Region-based zone ID: It should have the form 'area/city', such as
   *         'America/Los_Angeles'.</li>
   *     <li> Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00'
   *          or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
   *   </ul>
   * Other short names like 'CST' are not recommended to use because they can be ambiguous.
   * If it isn't set, the current value of the SQL config `spark.sql.session.timeZone` is
   * used by default.
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * (Java-specific) Adds input options for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a time zone ID
   * to be used to parse timestamps in the JSON/CSV datasources or partition values. The following
   * formats of `timeZone` are supported:
   *   <ul>
   *     <li> Region-based zone ID: It should have the form 'area/city', such as
   *         'America/Los_Angeles'.</li>
   *     <li> Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00'
   *          or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
   *   </ul>
   * Other short names like 'CST' are not recommended to use because they can be ambiguous.
   * If it isn't set, the current value of the SQL config `spark.sql.session.timeZone` is
   * used by default.
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): DataStreamReader = {
    this.options(options.asScala)
    this
  }


  /**
   * Loads input data stream in as a `DataFrame`, for data streams that don't require a path
   * (e.g. external key-value stores).
   *
   * @since 2.0.0
   */
  def load(): DataFrame = loadInternal(None)

  private def loadInternal(path: Option[String]): DataFrame = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "read files of Hive data source directly.")
    }

    val optionsWithPath = if (path.isEmpty) {
      extraOptions
    } else {
      extraOptions + ("path" -> path.get)
    }

    val ds = DataSource.lookupDataSource(source, sparkSession.sqlContext.conf).
      getConstructor().newInstance()
    // We need to generate the V1 data source so we can pass it to the V2 relation as a shim.
    // We can't be sure at this point whether we'll actually want to use V2, since we don't know the
    // writer or whether the query is continuous.
    val v1DataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = source,
      options = optionsWithPath.originalMap)
    val v1Relation = ds match {
      case _: StreamSourceProvider => Some(StreamingRelation(v1DataSource))
      case _ => None
    }
    ds match {
      // file source v2 does not support streaming yet.
      case provider: TableProvider if !provider.isInstanceOf[FileDataSourceV2] =>
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          source = provider, conf = sparkSession.sessionState.conf)
        val finalOptions = sessionOptions.filterKeys(!optionsWithPath.contains(_)).toMap ++
            optionsWithPath.originalMap
        val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
        val table = DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
        import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
        table match {
          case _: SupportsRead if table.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
            Dataset.ofRows(
              sparkSession,
              StreamingRelationV2(
                Some(provider), source, table, dsOptions,
                table.schema.toAttributes, None, None, v1Relation))

          // fallback to v1
          // TODO (SPARK-27483): we should move this fallback logic to an analyzer rule.
          case _ => Dataset.ofRows(sparkSession, StreamingRelation(v1DataSource))
        }

      case _ =>
        // Code path for data source v1.
        Dataset.ofRows(sparkSession, StreamingRelation(v1DataSource))
    }
  }

  /**
   * Loads input in as a `DataFrame`, for data streams that read from some path.
   *
   * @since 2.0.0
   */
  def load(path: String): DataFrame = {
    if (!sparkSession.sessionState.conf.legacyPathOptionBehavior &&
        extraOptions.contains("path")) {
      throw new AnalysisException("There is a 'path' option set and load() is called with a path" +
        "parameter. Either remove the path option, or call load() without the parameter. " +
        s"To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.")
    }
    loadInternal(Some(path))
  }

  /**
   * Loads a JSON file stream and returns the results as a `DataFrame`.
   *
   * <a href="http://jsonlines.org/">JSON Lines</a> (newline-delimited JSON) is supported by
   * default. For JSON (one record per file), set the `multiLine` option to true.
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
   * <li>`allowUnquotedControlChars` (default `false`): allows JSON Strings to contain unquoted
   * control characters (ASCII characters with value less than 32, including tab and line feed
   * characters) or not.</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   * during parsing.
   *   <ul>
   *     <li>`PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a
   *     field configured by `columnNameOfCorruptRecord`, and sets malformed fields to `null`. To
   *     keep corrupt records, an user can set a string type field named
   *     `columnNameOfCorruptRecord` in an user-defined schema. If a schema does not have the
   *     field, it drops corrupt records during parsing. When inferring a schema, it implicitly
   *     adds a `columnNameOfCorruptRecord` field in an output schema.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at
   * <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>.
   * This applies to date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>.
   * This applies to timestamp type.</li>
   * <li>`multiLine` (default `false`): parse one record, which may span multiple lines,
   * per file</li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * <li>`dropFieldIfAllNull` (default `false`): whether to ignore column of all null values or
   * empty array/struct during schema inference.</li>
   * <li>`locale` (default is `en-US`): sets a locale as language tag in IETF BCP 47 format.
   * For instance, this is used while parsing dates and timestamps.</li>
   * <li>`pathGlobFilter`: an optional glob pattern to only include files with paths matching
   * the pattern. The syntax follows <code>org.apache.hadoop.fs.GlobFilter</code>.
   * It does not change the behavior of partition discovery.</li>
   * <li>`recursiveFileLookup`: recursively scan a directory for files. Using this option
   * disables partition discovery</li>
   * <li>`allowNonNumericNumbers` (default `true`): allows JSON parser to recognize set of
   * "Not-a-Number" (NaN) tokens as legal floating number values:
   *   <ul>
   *     <li>`+INF` for positive infinity, as well as alias of `+Infinity` and `Infinity`.
   *     <li>`-INF` for negative infinity, alias `-Infinity`.
   *     <li>`NaN` for other not-a-numbers, like result of division by zero.
   *   </ul>
   * </li>
   * </ul>
   *
   * @since 2.0.0
   */
  def json(path: String): DataFrame = format("json").load(path)

  /**
   * Loads a CSV file stream and returns the result as a `DataFrame`.
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   *
   * You can set the following CSV-specific options to deal with CSV files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`sep` (default `,`): sets a single character as a separator for each
   * field and value.</li>
   * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
   * type.</li>
   * <li>`quote` (default `"`): sets a single character used for escaping quoted values where
   * the separator can be part of the value. If you would like to turn off quotations, you need to
   * set not `null` but an empty string. This behaviour is different form
   * `com.databricks.spark.csv`.</li>
   * <li>`escape` (default `\`): sets a single character used for escaping quotes inside
   * an already quoted value.</li>
   * <li>`charToEscapeQuoteEscaping` (default `escape` or `\0`): sets a single character used for
   * escaping the escape for the quote character. The default value is escape character when escape
   * and quote characters are different, `\0` otherwise.</li>
   * <li>`comment` (default empty string): sets a single character used for skipping lines
   * beginning with this character. By default, it is disabled.</li>
   * <li>`header` (default `false`): uses the first line as names of columns.</li>
   * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
   * requires one extra pass over the data.</li>
   * <li>`ignoreLeadingWhiteSpace` (default `false`): a flag indicating whether or not leading
   * whitespaces from values being read should be skipped.</li>
   * <li>`ignoreTrailingWhiteSpace` (default `false`): a flag indicating whether or not trailing
   * whitespaces from values being read should be skipped.</li>
   * <li>`nullValue` (default empty string): sets the string representation of a null value. Since
   * 2.0.1, this applies to all supported types including the string type.</li>
   * <li>`emptyValue` (default empty string): sets the string representation of an empty value.</li>
   * <li>`nanValue` (default `NaN`): sets the string representation of a non-number" value.</li>
   * <li>`positiveInf` (default `Inf`): sets the string representation of a positive infinity
   * value.</li>
   * <li>`negativeInf` (default `-Inf`): sets the string representation of a negative infinity
   * value.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at
   * <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>.
   * This applies to date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
   *   Datetime Patterns</a>.
   * This applies to timestamp type.</li>
   * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
   * a record can have.</li>
   * <li>`maxCharsPerColumn` (default `-1`): defines the maximum number of characters allowed
   * for any given value being read. By default, it is -1 meaning unlimited length</li>
   * <li>`unescapedQuoteHandling` (default `STOP_AT_DELIMITER`): defines how the CsvParser
   * will handle values with unescaped quotes.
   *   <ul>
   *     <li>`STOP_AT_CLOSING_QUOTE`: If unescaped quotes are found in the input, accumulate
   *     the quote character and proceed parsing the value as a quoted value, until a closing
   *     quote is found.</li>
   *     <li>`BACK_TO_DELIMITER`: If unescaped quotes are found in the input, consider the value
   *     as an unquoted value. This will make the parser accumulate all characters of the current
   *     parsed value until the delimiter is found. If no delimiter is found in the value, the
   *     parser will continue accumulating characters from the input until a delimiter or line
   *     ending is found.</li>
   *     <li>`STOP_AT_DELIMITER`: If unescaped quotes are found in the input, consider the value
   *     as an unquoted value. This will make the parser accumulate all characters until the
   *     delimiter or a line ending is found in the input.</li>
   *     <li>`SKIP_VALUE`: If unescaped quotes are found in the input, the content parsed
   *     for the given value will be skipped and the value set in nullValue will be produced
   *     instead.</li>
   *     <li>`RAISE_ERROR`: If unescaped quotes are found in the input, a TextParsingException
   *     will be thrown.</li>
   *   </ul>
   * </li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   *    during parsing. It supports the following case-insensitive modes.
   *   <ul>
   *     <li>`PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a
   *     field configured by `columnNameOfCorruptRecord`, and sets malformed fields to `null`.
   *     To keep corrupt records, an user can set a string type field named
   *     `columnNameOfCorruptRecord` in an user-defined schema. If a schema does not have
   *     the field, it drops corrupt records during parsing. A record with less/more tokens
   *     than schema is not a corrupted record to CSV. When it meets a record having fewer
   *     tokens than the length of the schema, sets `null` to extra fields. When the record
   *     has more tokens than the length of the schema, it drops extra tokens.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`multiLine` (default `false`): parse one record, which may span multiple lines.</li>
   * <li>`locale` (default is `en-US`): sets a locale as language tag in IETF BCP 47 format.
   * For instance, this is used while parsing dates and timestamps.</li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing. Maximum length is 1 character.</li>
   * <li>`pathGlobFilter`: an optional glob pattern to only include files with paths matching
   * the pattern. The syntax follows <code>org.apache.hadoop.fs.GlobFilter</code>.
   * It does not change the behavior of partition discovery.</li>
   * <li>`recursiveFileLookup`: recursively scan a directory for files. Using this option
   * disables partition discovery</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def csv(path: String): DataFrame = format("csv").load(path)

  /**
   * Loads a ORC file stream, returning the result as a `DataFrame`.
   *
   * You can set the following ORC-specific option(s) for reading ORC files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`mergeSchema` (default is the value specified in `spark.sql.orc.mergeSchema`): sets whether
   * we should merge schemas collected from all ORC part-files. This will override
   * `spark.sql.orc.mergeSchema`.</li>
   * <li>`pathGlobFilter`: an optional glob pattern to only include files with paths matching
   * the pattern. The syntax follows <code>org.apache.hadoop.fs.GlobFilter</code>.
   * It does not change the behavior of partition discovery.</li>
   * <li>`recursiveFileLookup`: recursively scan a directory for files. Using this option
   * disables partition discovery</li>
   * </ul>
   *
   * @since 2.3.0
   */
  def orc(path: String): DataFrame = {
    format("orc").load(path)
  }

  /**
   * Loads a Parquet file stream, returning the result as a `DataFrame`.
   *
   * You can set the following Parquet-specific option(s) for reading Parquet files:
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * <li>`mergeSchema` (default is the value specified in `spark.sql.parquet.mergeSchema`): sets
   * whether we should merge schemas collected from all
   * Parquet part-files. This will override
   * `spark.sql.parquet.mergeSchema`.</li>
   * <li>`pathGlobFilter`: an optional glob pattern to only include files with paths matching
   * the pattern. The syntax follows <code>org.apache.hadoop.fs.GlobFilter</code>.
   * It does not change the behavior of partition discovery.</li>
   * <li>`recursiveFileLookup`: recursively scan a directory for files. Using this option
   * disables partition discovery</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def parquet(path: String): DataFrame = {
    format("parquet").load(path)
  }

  /**
   * Define a Streaming DataFrame on a Table. The DataSource corresponding to the table should
   * support streaming mode.
   * @param tableName The name of the table
   * @since 3.1.0
   */
  def table(tableName: String): DataFrame = {
    require(tableName != null, "The table name can't be null")
    val identifier = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    Dataset.ofRows(
      sparkSession,
      UnresolvedRelation(
        identifier,
        new CaseInsensitiveStringMap(extraOptions.toMap.asJava),
        isStreaming = true))
  }

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   * The text files must be encoded as UTF-8.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame. For example:
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
   * <li>`wholetext` (default `false`): If true, read a file as a single row and not split by "\n".
   * </li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * <li>`pathGlobFilter`: an optional glob pattern to only include files with paths matching
   * the pattern. The syntax follows <code>org.apache.hadoop.fs.GlobFilter</code>.
   * It does not change the behavior of partition discovery.</li>
   * <li>`recursiveFileLookup`: recursively scan a directory for files. Using this option
   * disables partition discovery</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def text(path: String): DataFrame = format("text").load(path)

  /**
   * Loads text file(s) and returns a `Dataset` of String. The underlying schema of the Dataset
   * contains a single string column named "value".
   * The text files must be encoded as UTF-8.
   *
   * If the directory structure of the text files contains partitioning information, those are
   * ignored in the resulting Dataset. To include partitioning information as columns, use `text`.
   *
   * By default, each line in the text file is a new element in the resulting Dataset. For example:
   * {{{
   *   // Scala:
   *   spark.readStream.textFile("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.readStream().textFile("/path/to/spark/README.md")
   * }}}
   *
   * You can set the text-specific options as specified in `DataStreamReader.text`.
   *
   * @param path input path
   * @since 2.1.0
   */
  def textFile(path: String): Dataset[String] = {
    if (userSpecifiedSchema.nonEmpty) {
      throw new AnalysisException("User specified schema not supported with `textFile`")
    }
    text(path).select("value").as[String](sparkSession.implicits.newStringEncoder)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)
}
