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

package org.apache.spark.sql

import java.util.{Locale, Properties}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.univocity.parsers.csv.CsvParser

import org.apache.spark.Partition
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, FailureSafeParser}
import org.apache.spark.sql.execution.datasources.csv._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.read` to access this.
 *
 * @since 1.4.0
 */
@InterfaceStability.Stable
class DataFrameReader private[sql](sparkSession: SparkSession) extends Logging {

  /**
   * Specifies the input data source format.
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 1.4.0
   */
  def schema(schema: StructType): DataFrameReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * Specifies the schema by using the input DDL-formatted string. Some data sources (e.g. JSON) can
   * infer the input schema automatically from data. By specifying the schema here, the underlying
   * data source can skip the schema inference step, and thus speed up data loading.
   *
   * {{{
   *   spark.read.schema("a INT, b STRING, c DOUBLE").csv("test.csv")
   * }}}
   *
   * @since 2.3.0
   */
  def schema(schemaString: String): DataFrameReader = {
    this.userSpecifiedSchema = Option(StructType.fromDDL(schemaString))
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to parse timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataFrameReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataFrameReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataFrameReader = option(key, value.toString)

  /**
   * (Scala-specific) Adds input options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to parse timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds input options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to parse timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameReader = {
    this.options(options.asScala)
    this
  }

  /**
   * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
   * key-value stores).
   *
   * @since 1.4.0
   */
  def load(): DataFrame = {
    load(Seq.empty: _*) // force invocation of `load(...varargs...)`
  }

  /**
   * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   *
   * @since 1.4.0
   */
  def load(path: String): DataFrame = {
    // force invocation of `load(...varargs...)`
    option(DataSourceOptions.PATH_KEY, path).load(Seq.empty: _*)
  }

  /**
   * Loads input in as a `DataFrame`, for data sources that support multiple paths.
   * Only works if the source is a HadoopFsRelationProvider.
   *
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def load(paths: String*): DataFrame = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "read files of Hive data source directly.")
    }

    val cls = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
    if (classOf[DataSourceV2].isAssignableFrom(cls)) {
      val ds = cls.newInstance().asInstanceOf[DataSourceV2]
      if (ds.isInstanceOf[ReadSupport]) {
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          ds = ds, conf = sparkSession.sessionState.conf)
        val pathsOption = {
          val objectMapper = new ObjectMapper()
          DataSourceOptions.PATHS_KEY -> objectMapper.writeValueAsString(paths.toArray)
        }
        Dataset.ofRows(sparkSession, DataSourceV2Relation.create(
          ds, sessionOptions ++ extraOptions.toMap + pathsOption,
          userSpecifiedSchema = userSpecifiedSchema))
      } else {
        loadV1Source(paths: _*)
      }
    } else {
      loadV1Source(paths: _*)
    }
  }

  private def loadV1Source(paths: String*) = {
    // Code path for data source v1.
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }

  /**
   * Construct a `DataFrame` representing the database table accessible via JDBC URL
   * url named table and connection properties.
   *
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, properties: Properties): DataFrame = {
    assertNoSpecifiedSchema("jdbc")
    // properties should override settings in extraOptions.
    this.extraOptions ++= properties.asScala
    // explicit url and dbtable should override all
    this.extraOptions ++= Seq(JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table)
    format("jdbc").load()
  }

  /**
   * Construct a `DataFrame` representing the database table accessible via JDBC URL
   * url named table. Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`.
   * @param table Name of the table in the external database.
   * @param columnName the name of a column of numeric, date, or timestamp type
   *                   that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride.
   * @param upperBound the maximum value of `columnName` used to decide partition stride.
   * @param numPartitions the number of partitions. This, along with `lowerBound` (inclusive),
   *                      `upperBound` (exclusive), form partition strides for generated WHERE
   *                      clause expressions used to split the column `columnName` evenly. When
   *                      the input is less than 1, the number is set to 1.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "fetchsize" can be used to control the
   *                             number of rows per fetch and "queryTimeout" can be used to wait
   *                             for a Statement object to execute to the given number of seconds.
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties): DataFrame = {
    // columnName, lowerBound, upperBound and numPartitions override settings in extraOptions.
    this.extraOptions ++= Map(
      JDBCOptions.JDBC_PARTITION_COLUMN -> columnName,
      JDBCOptions.JDBC_LOWER_BOUND -> lowerBound.toString,
      JDBCOptions.JDBC_UPPER_BOUND -> upperBound.toString,
      JDBCOptions.JDBC_NUM_PARTITIONS -> numPartitions.toString)
    jdbc(url, table, connectionProperties)
  }

  /**
   * Construct a `DataFrame` representing the database table accessible via JDBC URL
   * url named table using connection properties. The `predicates` parameter gives a list
   * expressions suitable for inclusion in WHERE clauses; each one defines one partition
   * of the `DataFrame`.
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param predicates Condition in the where clause for each partition.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "fetchsize" can be used to control the
   *                             number of rows per fetch.
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: Properties): DataFrame = {
    assertNoSpecifiedSchema("jdbc")
    // connectionProperties should override settings in extraOptions.
    val params = extraOptions ++ connectionProperties.asScala
    val options = new JDBCOptions(url, table, params)
    val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i) : Partition
    }
    val relation = JDBCRelation(parts, options)(sparkSession)
    sparkSession.baseRelationToDataFrame(relation)
  }

  /**
   * Loads a JSON file and returns the results as a `DataFrame`.
   *
   * See the documentation on the overloaded `json()` method with varargs for more details.
   *
   * @since 1.4.0
   */
  def json(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    json(Seq(path): _*)
  }

  /**
   * Loads JSON files and returns the results as a `DataFrame`.
   *
   * <a href="http://jsonlines.org/">JSON Lines</a> (newline-delimited JSON) is supported by
   * default. For JSON (one record per file), set the `multiLine` option to true.
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * You can set the following JSON-specific options to deal with non-standard JSON files:
   * <ul>
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
   *     field configured by `columnNameOfCorruptRecord`, and sets other fields to `null`. To
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
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * <li>`multiLine` (default `false`): parse one record, which may span multiple lines,
   * per file</li>
   * <li>`encoding` (by default it is not set): allows to forcibly set one of standard basic
   * or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. If the encoding
   * is not specified and `multiLine` is set to `true`, it will be detected automatically.</li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * <li>`samplingRatio` (default is 1.0): defines fraction of input JSON objects used
   * for schema inferring.</li>
   * <li>`dropFieldIfAllNull` (default `false`): whether to ignore column of all null values or
   * empty array/struct during schema inference.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def json(paths: String*): DataFrame = format("json").load(paths : _*)

  /**
   * Loads a `JavaRDD[String]` storing JSON objects (<a href="http://jsonlines.org/">JSON
   * Lines text format or newline-delimited JSON</a>) and returns the result as
   * a `DataFrame`.
   *
   * Unless the schema is specified using `schema` function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  @deprecated("Use json(Dataset[String]) instead.", "2.2.0")
  def json(jsonRDD: JavaRDD[String]): DataFrame = json(jsonRDD.rdd)

  /**
   * Loads an `RDD[String]` storing JSON objects (<a href="http://jsonlines.org/">JSON Lines
   * text format or newline-delimited JSON</a>) and returns the result as a `DataFrame`.
   *
   * Unless the schema is specified using `schema` function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  @deprecated("Use json(Dataset[String]) instead.", "2.2.0")
  def json(jsonRDD: RDD[String]): DataFrame = {
    json(sparkSession.createDataset(jsonRDD)(Encoders.STRING))
  }

  /**
   * Loads a `Dataset[String]` storing JSON objects (<a href="http://jsonlines.org/">JSON Lines
   * text format or newline-delimited JSON</a>) and returns the result as a `DataFrame`.
   *
   * Unless the schema is specified using `schema` function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonDataset input Dataset with one JSON object per record
   * @since 2.2.0
   */
  def json(jsonDataset: Dataset[String]): DataFrame = {
    val parsedOptions = new JSONOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    val schema = userSpecifiedSchema.getOrElse {
      TextInputJsonDataSource.inferFromDataset(jsonDataset, parsedOptions)
    }

    verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val createParser = CreateJacksonParser.string _
    val parsed = jsonDataset.rdd.mapPartitions { iter =>
      val rawParser = new JacksonParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input, createParser, UTF8String.fromString),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = jsonDataset.isStreaming)
  }

  /**
   * Loads a CSV file and returns the result as a `DataFrame`. See the documentation on the
   * other overloaded `csv()` method for more details.
   *
   * @since 2.0.0
   */
  def csv(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    csv(Seq(path): _*)
  }

  /**
   * Loads an `Dataset[String]` storing CSV rows and returns the result as a `DataFrame`.
   *
   * If the schema is not specified using `schema` function and `inferSchema` option is enabled,
   * this function goes through the input once to determine the input schema.
   *
   * If the schema is not specified using `schema` function and `inferSchema` option is disabled,
   * it determines the columns as string types and it reads only the first line to determine the
   * names and the number of fields.
   *
   * If the enforceSchema is set to `false`, only the CSV header in the first line is checked
   * to conform specified or inferred schema.
   *
   * @note if `header` option is set to `true` when calling this API, all lines same with
   * the header will be removed if exists.
   *
   * @param csvDataset input Dataset with one CSV row per record
   * @since 2.2.0
   */
  def csv(csvDataset: Dataset[String]): DataFrame = {
    val parsedOptions: CSVOptions = new CSVOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    val filteredLines: Dataset[String] =
      CSVUtils.filterCommentAndEmpty(csvDataset, parsedOptions)
    val maybeFirstLine: Option[String] = filteredLines.take(1).headOption

    val schema = userSpecifiedSchema.getOrElse {
      TextInputCSVDataSource.inferFromDataset(
        sparkSession,
        csvDataset,
        maybeFirstLine,
        parsedOptions)
    }

    verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val linesWithoutHeader = if (parsedOptions.headerFlag && maybeFirstLine.isDefined) {
      val firstLine = maybeFirstLine.get
      val parser = new CsvParser(parsedOptions.asParserSettings)
      val columnNames = parser.parseLine(firstLine)
      CSVDataSource.checkHeaderColumnNames(
        actualSchema,
        columnNames,
        csvDataset.getClass.getCanonicalName,
        parsedOptions.enforceSchema,
        sparkSession.sessionState.conf.caseSensitiveAnalysis)
      filteredLines.rdd.mapPartitions(CSVUtils.filterHeaderLine(_, firstLine, parsedOptions))
    } else {
      filteredLines.rdd
    }

    val parsed = linesWithoutHeader.mapPartitions { iter =>
      val rawParser = new UnivocityParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => Seq(rawParser.parse(input)),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = csvDataset.isStreaming)
  }

  /**
   * Loads CSV files and returns the result as a `DataFrame`.
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   *
   * You can set the following CSV-specific options to deal with CSV files:
   * <ul>
   * <li>`sep` (default `,`): sets a single character as a separator for each
   * field and value.</li>
   * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
   * type.</li>
   * <li>`quote` (default `"`): sets a single character used for escaping quoted values where
   * the separator can be part of the value. If you would like to turn off quotations, you need to
   * set not `null` but an empty string. This behaviour is different from
   * `com.databricks.spark.csv`.</li>
   * <li>`escape` (default `\`): sets a single character used for escaping quotes inside
   * an already quoted value.</li>
   * <li>`charToEscapeQuoteEscaping` (default `escape` or `\0`): sets a single character used for
   * escaping the escape for the quote character. The default value is escape character when escape
   * and quote characters are different, `\0` otherwise.</li>
   * <li>`comment` (default empty string): sets a single character used for skipping lines
   * beginning with this character. By default, it is disabled.</li>
   * <li>`header` (default `false`): uses the first line as names of columns.</li>
   * <li>`enforceSchema` (default `true`): If it is set to `true`, the specified or inferred schema
   * will be forcibly applied to datasource files, and headers in CSV files will be ignored.
   * If the option is set to `false`, the schema will be validated against all headers in CSV files
   * in the case when the `header` option is set to `true`. Field names in the schema
   * and column names in CSV headers are checked by their positions taking into account
   * `spark.sql.caseSensitive`. Though the default value is true, it is recommended to disable
   * the `enforceSchema` option to avoid incorrect results.</li>
   * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
   * requires one extra pass over the data.</li>
   * <li>`samplingRatio` (default is 1.0): defines fraction of rows used for schema inferring.</li>
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
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
   * a record can have.</li>
   * <li>`maxCharsPerColumn` (default `-1`): defines the maximum number of characters allowed
   * for any given value being read. By default, it is -1 meaning unlimited length</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   *    during parsing. It supports the following case-insensitive modes. Note that Spark tries
   *    to parse only required columns in CSV under column pruning. Therefore, corrupt records
   *    can be different based on required set of fields. This behavior can be controlled by
   *    `spark.sql.csv.parser.columnPruning.enabled` (enabled by default).
   *   <ul>
   *     <li>`PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a
   *     field configured by `columnNameOfCorruptRecord`, and sets other fields to `null`. To keep
   *     corrupt records, an user can set a string type field named `columnNameOfCorruptRecord`
   *     in an user-defined schema. If a schema does not have the field, it drops corrupt records
   *     during parsing. A record with less/more tokens than schema is not a corrupted record to
   *     CSV. When it meets a record having fewer tokens than the length of the schema, sets
   *     `null` to extra fields. When the record has more tokens than the length of the schema,
   *     it drops extra tokens.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`multiLine` (default `false`): parse one record, which may span multiple lines.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def csv(paths: String*): DataFrame = format("csv").load(paths : _*)

  /**
   * Loads a Parquet file, returning the result as a `DataFrame`. See the documentation
   * on the other overloaded `parquet()` method for more details.
   *
   * @since 2.0.0
   */
  def parquet(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    parquet(Seq(path): _*)
  }

  /**
   * Loads a Parquet file, returning the result as a `DataFrame`.
   *
   * You can set the following Parquet-specific option(s) for reading Parquet files:
   * <ul>
   * <li>`mergeSchema` (default is the value specified in `spark.sql.parquet.mergeSchema`): sets
   * whether we should merge schemas collected from all Parquet part-files. This will override
   * `spark.sql.parquet.mergeSchema`.</li>
   * </ul>
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def parquet(paths: String*): DataFrame = {
    format("parquet").load(paths: _*)
  }

  /**
   * Loads an ORC file and returns the result as a `DataFrame`.
   *
   * @param path input path
   * @since 1.5.0
   */
  def orc(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    orc(Seq(path): _*)
  }

  /**
   * Loads ORC files and returns the result as a `DataFrame`.
   *
   * @param paths input paths
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def orc(paths: String*): DataFrame = format("orc").load(paths: _*)

  /**
   * Returns the specified table as a `DataFrame`.
   *
   * @since 1.4.0
   */
  def table(tableName: String): DataFrame = {
    assertNoSpecifiedSchema("table")
    sparkSession.table(tableName)
  }

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any. See the documentation on
   * the other overloaded `text()` method for more details.
   *
   * @since 2.0.0
   */
  def text(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    text(Seq(path): _*)
  }

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame. For example:
   * {{{
   *   // Scala:
   *   spark.read.text("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.read().text("/path/to/spark/README.md")
   * }}}
   *
   * You can set the following text-specific option(s) for reading text files:
   * <ul>
   * <li>`wholetext` (default `false`): If true, read a file as a single row and not split by "\n".
   * </li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * </ul>
   *
   * @param paths input paths
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def text(paths: String*): DataFrame = format("text").load(paths : _*)

  /**
   * Loads text files and returns a [[Dataset]] of String. See the documentation on the
   * other overloaded `textFile()` method for more details.
   * @since 2.0.0
   */
  def textFile(path: String): Dataset[String] = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    textFile(Seq(path): _*)
  }

  /**
   * Loads text files and returns a [[Dataset]] of String. The underlying schema of the Dataset
   * contains a single string column named "value".
   *
   * If the directory structure of the text files contains partitioning information, those are
   * ignored in the resulting Dataset. To include partitioning information as columns, use `text`.
   *
   * By default, each line in the text files is a new row in the resulting DataFrame. For example:
   * {{{
   *   // Scala:
   *   spark.read.textFile("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.read().textFile("/path/to/spark/README.md")
   * }}}
   *
   * You can set the following textFile-specific option(s) for reading text files:
   * <ul>
   * <li>`wholetext` (default `false`): If true, read a file as a single row and not split by "\n".
   * </li>
   * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
   * that should be used for parsing.</li>
   * </ul>
   *
   * @param paths input path
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def textFile(paths: String*): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(paths : _*).select("value").as[String](sparkSession.implicits.newStringEncoder)
  }

  /**
   * A convenient function for schema validation in APIs.
   */
  private def assertNoSpecifiedSchema(operation: String): Unit = {
    if (userSpecifiedSchema.nonEmpty) {
      throw new AnalysisException(s"User specified schema not supported with `$operation`")
    }
  }

  /**
   * A convenient function for schema validation in datasources supporting
   * `columnNameOfCorruptRecord` as an option.
   */
  private def verifyColumnNameOfCorruptRecord(
      schema: StructType,
      columnNameOfCorruptRecord: String): Unit = {
    schema.getFieldIndex(columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = schema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw new AnalysisException(
          "The field for corrupt records must be string type and nullable")
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

}
