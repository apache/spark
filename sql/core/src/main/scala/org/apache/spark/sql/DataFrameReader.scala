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

import scala.jdk.CollectionConverters._

import org.apache.spark.Partition
import org.apache.spark.annotation.Stable
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils, FailureSafeParser}
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, XmlOptions}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.csv._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.json.JsonUtils.checkJsonSchema
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.execution.datasources.xml.TextInputXmlDataSource
import org.apache.spark.sql.execution.datasources.xml.XmlUtils.checkXmlSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.read` to access this.
 *
 * @since 1.4.0
 */
@Stable
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
    if (schema != null) {
      val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
      this.userSpecifiedSchema = Option(replaced)
      validateSingleVariantColumn()
    }
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
    schema(StructType.fromDDL(schemaString))
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameReader = {
    this.extraOptions = this.extraOptions + (key -> value)
    validateSingleVariantColumn()
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
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    this.extraOptions ++= options
    validateSingleVariantColumn()
    this
  }

  /**
   * Adds input options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
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
    if (sparkSession.sessionState.conf.legacyPathOptionBehavior) {
      option("path", path).load(Seq.empty: _*)
    } else {
      load(Seq(path): _*)
    }
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
      throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")
    }

    val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
    if (!legacyPathOptionBehavior &&
        (extraOptions.contains("path") || extraOptions.contains("paths")) && paths.nonEmpty) {
      throw QueryCompilationErrors.pathOptionNotSetCorrectlyWhenReadingError()
    }

    DataSource.lookupDataSourceV2(source, sparkSession.sessionState.conf).flatMap { provider =>
      DataSourceV2Utils.loadV2Source(sparkSession, provider, userSpecifiedSchema, extraOptions,
        source, paths: _*)
    }.getOrElse(loadV1Source(paths: _*))
  }

  private def loadV1Source(paths: String*) = {
    val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
    val (finalPaths, finalOptions) = if (!legacyPathOptionBehavior && paths.length == 1) {
      (Nil, extraOptions + ("path" -> paths.head))
    } else {
      (paths, extraOptions)
    }

    // Code path for data source v1.
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = finalPaths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = finalOptions.originalMap).resolveRelation())
  }

  /**
   * Construct a `DataFrame` representing the database table accessible via JDBC URL
   * url named table and connection properties.
   *
   * You can find the JDBC-specific option and parameter documentation for reading tables
   * via JDBC in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
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

  // scalastyle:off line.size.limit
  /**
   * Construct a `DataFrame` representing the database table accessible via JDBC URL
   * url named table. Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * You can find the JDBC-specific option and parameter documentation for reading tables via JDBC in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @param table Name of the table in the external database.
   * @param columnName Alias of `partitionColumn` option. Refer to `partitionColumn` in
   *                   <a href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   *                     Data Source Option</a> in the version you use.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "fetchsize" can be used to control the
   *                             number of rows per fetch and "queryTimeout" can be used to wait
   *                             for a Statement object to execute to the given number of seconds.
   * @since 1.4.0
   */
  // scalastyle:on line.size.limit
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
   * You can find the JDBC-specific option and parameter documentation for reading tables
   * via JDBC in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
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
   * You can find the JSON-specific options for reading JSON files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def json(paths: String*): DataFrame = {
    userSpecifiedSchema.foreach(checkJsonSchema)
    format("json").load(paths : _*)
  }

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

    userSpecifiedSchema.foreach(checkJsonSchema)
    val schema = userSpecifiedSchema.map {
      case s if !SQLConf.get.getConf(
        SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
      case other => other
    }.getOrElse {
      TextInputJsonDataSource.inferFromDataset(jsonDataset, parsedOptions)
    }

    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val createParser = CreateJacksonParser.string _
    val parsed = jsonDataset.rdd.mapPartitions { iter =>
      val rawParser = new JacksonParser(actualSchema, parsedOptions, allowArrayAsStructs = true)
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

    // For performance, short-circuit the collection of the first line when it won't be used:
    //   - TextInputCSVDataSource - Only uses firstLine to infer an unspecified schema
    //   - CSVHeaderChecker       - Only uses firstLine to check header, when headerFlag is true
    //   - CSVUtils               - Only uses firstLine to filter headers, when headerFlag is true
    // (If the downstream logic grows more complicated, consider refactoring to an approach that
    //  delegates this decision to the constituent consumers themselves.)
    val maybeFirstLine: Option[String] =
      if (userSpecifiedSchema.isEmpty || parsedOptions.headerFlag) {
        filteredLines.take(1).headOption
      } else {
        None
      }

    val schema = userSpecifiedSchema.map {
      case s if !SQLConf.get.getConf(
        SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
      case other => other
    }.getOrElse {
      TextInputCSVDataSource.inferFromDataset(
        sparkSession,
        csvDataset,
        maybeFirstLine,
        parsedOptions)
    }

    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val linesWithoutHeader: RDD[String] = maybeFirstLine.map { firstLine =>
      val headerChecker = new CSVHeaderChecker(
        actualSchema,
        parsedOptions,
        source = s"CSV source: $csvDataset")
      headerChecker.checkHeaderColumnNames(firstLine)
      filteredLines.rdd.mapPartitions(CSVUtils.filterHeaderLine(_, firstLine, parsedOptions))
    }.getOrElse(filteredLines.rdd)

    val parsed = linesWithoutHeader.mapPartitions { iter =>
      val rawParser = new UnivocityParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input),
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
   * You can find the CSV-specific options for reading CSV files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def csv(paths: String*): DataFrame = format("csv").load(paths : _*)

  /**
   * Loads a XML file and returns the result as a `DataFrame`. See the documentation on the
   * other overloaded `xml()` method for more details.
   *
   * @since 4.0.0
   */
  def xml(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    xml(Seq(path): _*)
  }

  /**
   * Loads XML files and returns the result as a `DataFrame`.
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   *
   * You can find the XML-specific options for reading XML files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 4.0.0
   */
  @scala.annotation.varargs
  def xml(paths: String*): DataFrame = {
    userSpecifiedSchema.foreach(checkXmlSchema)
    format("xml").load(paths: _*)
  }

  /**
   * Loads an `Dataset[String]` storing XML object and returns the result as a `DataFrame`.
   *
   * If the schema is not specified using `schema` function and `inferSchema` option is enabled,
   * this function goes through the input once to determine the input schema.
   *
   * @param xmlDataset input Dataset with one XML object per record
   * @since 4.0.0
   */
  def xml(xmlDataset: Dataset[String]): DataFrame = {
    val parsedOptions: XmlOptions = new XmlOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    userSpecifiedSchema.foreach(checkXmlSchema)

    val schema = userSpecifiedSchema.map {
      case s if !SQLConf.get.getConf(
        SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
      case other => other
    }.getOrElse {
      TextInputXmlDataSource.inferFromDataset(xmlDataset, parsedOptions)
    }

    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val parsed = xmlDataset.rdd.mapPartitions { iter =>
      val rawParser = new StaxXmlParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = xmlDataset.isStreaming)
  }

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
   * Parquet-specific option(s) for reading Parquet files can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
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
   * ORC-specific option(s) for reading ORC files can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @param paths input paths
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def orc(paths: String*): DataFrame = format("orc").load(paths: _*)

  /**
   * Returns the specified table/view as a `DataFrame`. If it's a table, it must support batch
   * reading and the returned DataFrame is the batch scan query plan of this table. If it's a view,
   * the returned DataFrame is simply the query plan of the view, which can either be a batch or
   * streaming query plan.
   *
   * @param tableName is either a qualified or unqualified name that designates a table or view.
   *                  If a database is specified, it identifies the table/view from the database.
   *                  Otherwise, it first attempts to find a temporary view with the given name
   *                  and then match the table/view from the current database.
   *                  Note that, the global temporary view database is also valid here.
   * @since 1.4.0
   */
  def table(tableName: String): DataFrame = {
    assertNoSpecifiedSchema("table")
    val multipartIdentifier =
      sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    Dataset.ofRows(sparkSession, UnresolvedRelation(multipartIdentifier,
      new CaseInsensitiveStringMap(extraOptions.toMap.asJava)))
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
   * The text files must be encoded as UTF-8.
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
   * You can find the text-specific options for reading text files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   *   Data Source Option</a> in the version you use.
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
   * The text files must be encoded as UTF-8.
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
   * You can set the text-specific options as specified in `DataFrameReader.text`.
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
      throw QueryCompilationErrors.userSpecifiedSchemaUnsupportedError(operation)
    }
  }

  /**
   * Ensure that the `singleVariantColumn` option cannot be used if there is also a user specified
   * schema.
   */
  private def validateSingleVariantColumn(): Unit = {
    if (extraOptions.get(JSONOptions.SINGLE_VARIANT_COLUMN).isDefined &&
      userSpecifiedSchema.isDefined) {
      throw QueryCompilationErrors.invalidSingleVariantColumn()
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

}
