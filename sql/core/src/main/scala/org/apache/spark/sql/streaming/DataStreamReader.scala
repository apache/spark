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
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.json.JsonUtils.checkJsonSchema
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.streaming.StreamingRelation
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
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * (Java-specific) Adds input options for the underlying data source.
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
      throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")
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
      throw QueryCompilationErrors.setPathOptionAndCallWithPathParameterError("load")
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
   * You can set the following option(s):
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * You can find the JSON-specific options for reading JSON file stream in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def json(path: String): DataFrame = {
    userSpecifiedSchema.foreach(checkJsonSchema)
    format("json").load(path)
  }

  /**
   * Loads a CSV file stream and returns the result as a `DataFrame`.
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * You can find the CSV-specific options for reading CSV file stream in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def csv(path: String): DataFrame = format("csv").load(path)

  /**
   * Loads a ORC file stream, returning the result as a `DataFrame`.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * ORC-specific option(s) for reading ORC file stream can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.3.0
   */
  def orc(path: String): DataFrame = {
    format("orc").load(path)
  }

  /**
   * Loads a Parquet file stream, returning the result as a `DataFrame`.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * Parquet-specific option(s) for reading Parquet file stream can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option">
   *   Data Source Option</a> in the version you use.
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
   * You can set the following option(s):
   * <ul>
   * <li>`maxFilesPerTrigger` (default: no max limit): sets the maximum number of new files to be
   * considered in every trigger.</li>
   * </ul>
   *
   * You can find the text-specific options for reading text files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   *   Data Source Option</a> in the version you use.
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
      throw QueryCompilationErrors.userSpecifiedSchemaUnsupportedError("textFile")
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
