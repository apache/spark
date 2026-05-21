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

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoders}
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
abstract class DataStreamReader {

  /**
   * Specifies the input data source format.
   *
   * @since 2.0.0
   */
  def format(source: String): this.type

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can skip
   * the schema inference step, and thus speed up data loading.
   *
   * @since 2.0.0
   */
  def schema(schema: StructType): this.type

  /**
   * Specifies the schema by using the input DDL-formatted string. Some data sources (e.g. JSON)
   * can infer the input schema automatically from data. By specifying the schema here, the
   * underlying data source can skip the schema inference step, and thus speed up data loading.
   *
   * @since 2.3.0
   */
  def schema(schemaString: String): this.type = {
    schema(StructType.fromDDL(schemaString))
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): this.type

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): this.type = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): this.type = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): this.type = option(key, value.toString)

  /**
   * (Scala-specific) Adds input options for the underlying data source.
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): this.type

  /**
   * (Java-specific) Adds input options for the underlying data source.
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }

  /**
   * Specifies a name for the streaming source. This name is used to identify the source in
   * checkpoint metadata and enables stable checkpoint locations for source evolution.
   *
   * @param sourceName
   *   the name to assign to this streaming source
   * @since 4.2.0
   */
  @Experimental
  def name(sourceName: String): this.type

  /**
   * Loads input data stream in as a `DataFrame`, for data streams that don't require a path (e.g.
   * external key-value stores).
   *
   * @since 2.0.0
   */
  def load(): DataFrame

  /**
   * Loads input in as a `DataFrame`, for data streams that read from some path.
   *
   * @since 2.0.0
   */
  def load(path: String): DataFrame

  /**
   * Returns the row-level changes (Change Data Capture) from the specified table as a streaming
   * `DataFrame`. Currently this API is only supported for Data Source V2 tables whose catalog
   * implements `TableCatalog.loadChangelog()`.
   *
   * Use `.option()` to specify the starting version/timestamp and processing options:
   * {{{
   *   spark.readStream
   *     .option("startingVersion", "10")
   *     .changes("my_table")
   * }}}
   *
   * Streaming reads support all of the same post-processing as batch reads -- `computeUpdates`,
   * `deduplicationMode = dropCarryovers`, and `deduplicationMode = netChanges`. The streaming
   * netChanges path holds per-row-identity state in the state store and emits the SPIP collapse
   * output once the global watermark advances past the last `_commit_timestamp` observed for that
   * row identity. Row identities only touched in the latest observed commit are therefore not
   * emitted until a later commit (with strictly greater `_commit_timestamp`) advances the
   * watermark past them, or the source terminates.
   *
   * Streaming netChanges differs from batch netChanges in scope. Batch netChanges collapses all
   * changes for a row identity over the entire requested version range. Streaming netChanges is
   * incremental: it collapses changes that fall within a single watermark window for a row
   * identity (i.e. up to the timer firing that emits its current net result). After a row
   * identity's net result has been emitted, subsequent commits on the same identity start a fresh
   * window and produce additional output rows -- streaming cannot retract previously emitted
   * results to match the batch range-scoped collapse. For a query that observes id=1 inserted at
   * v1 and deleted at v3 with another commit at v2 in between, batch netChanges over [v1..v3]
   * cancels to no row, while streaming emits an `insert` (when v2 advances the watermark past v1)
   * followed later by a `delete` (when end-of-stream or another commit advances the watermark
   * past v3).
   *
   * Because the streaming netChanges path uses `transformWithState`, the state store provider
   * must be RocksDB. Set `spark.sql.streaming.stateStore.providerClass` to
   * `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` before starting a
   * streaming netChanges query; the default HDFS-backed provider is rejected at query start.
   *
   * When the requested options engage post-processing (carry-over removal, update detection, or
   * netChanges), the rewrite injects an internal `EventTimeWatermark` on `_commit_timestamp` and
   * a stateful streaming operator (an aggregate for the row-level passes, a `transformWithState`
   * for netChanges). Two implications follow:
   *   - A commit's events are emitted in the next micro-batch after the commit is read
   *     (append-mode aggregate eviction is `eventTime &lt;= watermark`, and the watermark
   *     advances to the max `_commit_timestamp` observed in the previous batch). A stream that
   *     reads its last commit and stops will keep that commit's events in state until a
   *     subsequent (no-data) micro-batch fires.
   *   - The query is constrained to `Append` output mode; `Update` and `Complete` are rejected at
   *     writer-start time with `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`. The internal
   *     watermark metadata is stripped from the user-visible `_commit_timestamp` output, so
   *     downstream user-supplied watermarks on other columns do not interact with it via the
   *     global multi-watermark policy.
   *
   * @param tableName
   *   a qualified or unqualified name that designates a table.
   * @since 4.2.0
   */
  def changes(tableName: String): DataFrame

  /**
   * Loads a JSON file stream and returns the results as a `DataFrame`.
   *
   * <a href="http://jsonlines.org/">JSON Lines</a> (newline-delimited JSON) is supported by
   * default. For JSON (one record per file), set the `multiLine` option to true.
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * You can set the following option(s): <ul> <li>`maxFilesPerTrigger` (default: no max limit):
   * sets the maximum number of new files to be considered in every trigger.</li>
   * <li>`maxBytesPerTrigger` (default: no max limit): sets the maximum total size of new files to
   * be considered in every trigger.</li> </ul>
   *
   * You can find the JSON-specific options for reading JSON file stream in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def json(path: String): DataFrame = {
    validateJsonSchema()
    format("json").load(path)
  }

  /**
   * Loads a CSV file stream and returns the result as a `DataFrame`.
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   *
   * You can set the following option(s): <ul> <li>`maxFilesPerTrigger` (default: no max limit):
   * sets the maximum number of new files to be considered in every trigger.</li>
   * <li>`maxBytesPerTrigger` (default: no max limit): sets the maximum total size of new files to
   * be considered in every trigger.</li> </ul>
   *
   * You can find the CSV-specific options for reading CSV file stream in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def csv(path: String): DataFrame = format("csv").load(path)

  /**
   * Loads a XML file stream and returns the result as a `DataFrame`.
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using `schema`.
   *
   * You can set the following option(s): <ul> <li>`maxFilesPerTrigger` (default: no max limit):
   * sets the maximum number of new files to be considered in every trigger.</li>
   * <li>`maxBytesPerTrigger` (default: no max limit): sets the maximum total size of new files to
   * be considered in every trigger.</li> </ul>
   *
   * You can find the XML-specific options for reading XML file stream in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 4.0.0
   */
  def xml(path: String): DataFrame = {
    validateXmlSchema()
    format("xml").load(path)
  }

  /**
   * Loads a ORC file stream, returning the result as a `DataFrame`.
   *
   * You can set the following option(s): <ul> <li>`maxFilesPerTrigger` (default: no max limit):
   * sets the maximum number of new files to be considered in every trigger.</li>
   * <li>`maxBytesPerTrigger` (default: no max limit): sets the maximum total size of new files to
   * be considered in every trigger.</li> </ul>
   *
   * ORC-specific option(s) for reading ORC file stream can be found in <a href=
   * "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option"> Data
   * Source Option</a> in the version you use.
   *
   * @since 2.3.0
   */
  def orc(path: String): DataFrame = {
    format("orc").load(path)
  }

  /**
   * Loads a Parquet file stream, returning the result as a `DataFrame`.
   *
   * You can set the following option(s): <ul> <li>`maxFilesPerTrigger` (default: no max limit):
   * sets the maximum number of new files to be considered in every trigger.</li>
   * <li>`maxBytesPerTrigger` (default: no max limit): sets the maximum total size of new files to
   * be considered in every trigger.</li> </ul>
   *
   * Parquet-specific option(s) for reading Parquet file stream can be found in <a href=
   * "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option"> Data
   * Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def parquet(path: String): DataFrame = {
    format("parquet").load(path)
  }

  /**
   * Define a Streaming DataFrame on a Table. The DataSource corresponding to the table should
   * support streaming mode.
   * @param tableName
   *   The name of the table
   * @since 3.1.0
   */
  def table(tableName: String): DataFrame

  /**
   * Loads text files and returns a `DataFrame` whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any. The text files must be encoded
   * as UTF-8.
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
   * You can set the following option(s): <ul> <li>`maxFilesPerTrigger` (default: no max limit):
   * sets the maximum number of new files to be considered in every trigger.</li>
   * <li>`maxBytesPerTrigger` (default: no max limit): sets the maximum total size of new files to
   * be considered in every trigger.</li> </ul>
   *
   * You can find the text-specific options for reading text files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def text(path: String): DataFrame = format("text").load(path)

  /**
   * Loads text file(s) and returns a `Dataset` of String. The underlying schema of the Dataset
   * contains a single string column named "value". The text files must be encoded as UTF-8.
   *
   * If the directory structure of the text files contains partitioning information, those are
   * ignored in the resulting Dataset. To include partitioning information as columns, use `text`.
   *
   * By default, each line in the text file is a new element in the resulting Dataset. For
   * example:
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
   * @param path
   *   input path
   * @since 2.1.0
   */
  def textFile(path: String): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(path).select("value").as(Encoders.STRING)
  }

  protected def assertNoSpecifiedSchema(operation: String): Unit

  protected def validateJsonSchema(): Unit = ()

  protected def validateXmlSchema(): Unit = ()

  /**
   * Validates that a streaming source name only contains alphanumeric characters and underscores.
   *
   * @param sourceName
   *   the source name to validate
   * @throws IllegalArgumentException
   *   if the source name is null, empty, or contains invalid characters
   */
  private[sql] def validateSourceName(sourceName: String): Unit = {
    require(sourceName != null, "Source name cannot be null")
    require(sourceName.nonEmpty, "Source name cannot be empty")

    val validNamePattern: Regex = "^[a-zA-Z0-9_]+$".r
    if (!validNamePattern.pattern.matcher(sourceName).matches()) {
      throw new AnalysisException(
        errorClass = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
        messageParameters = Map("sourceName" -> sourceName))
    }
  }

}
