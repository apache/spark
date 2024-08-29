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
package org.apache.spark.sql.api

import scala.jdk.CollectionConverters._

import _root_.java.util

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.CompilationErrors

/**
 * Interface used to write a [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.write` to access this.
 *
 * @since 1.4.0
 */
@Stable
abstract class DataFrameWriter {

  /**
   * Specifies the behavior when data or table already exists. Options include:
   * <ul>
   * <li>`SaveMode.Overwrite`: overwrite the existing data.</li>
   * <li>`SaveMode.Append`: append the data.</li>
   * <li>`SaveMode.Ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`SaveMode.ErrorIfExists`: throw an exception at runtime.</li>
   * </ul>
   * <p>
   * The default option is `ErrorIfExists`.
   *
   * @since 1.4.0
   */
  def mode(saveMode: SaveMode): this.type = {
    this.mode = saveMode
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include:
   * <ul>
   * <li>`overwrite`: overwrite the existing data.</li>
   * <li>`append`: append the data.</li>
   * <li>`ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`error` or `errorifexists`: default option, throw an exception at runtime.</li>
   * </ul>
   *
   * @since 1.4.0
   */
  def mode(saveMode: String): this.type = {
    saveMode.toLowerCase(util.Locale.ROOT) match {
      case "overwrite" => mode(SaveMode.Overwrite)
      case "append" => mode(SaveMode.Append)
      case "ignore" => mode(SaveMode.Ignore)
      case "error" | "errorifexists" | "default" => mode(SaveMode.ErrorIfExists)
      case _ => throw CompilationErrors.invalidSaveModeError(saveMode)
    }
  }

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   *
   * @since 1.4.0
   */
  def format(source: String): this.type = {
    this.source = source
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): this.type = {
    this.extraOptions = this.extraOptions + (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): this.type = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): this.type = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): this.type = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): this.type = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def options(options: util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   * <ul>
   * <li>year=2016/month=01/</li>
   * <li>year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): this.type = {
    this.partitioningColumns = Option(colNames)
    validatePartitioning()
    this
  }

  /**
   * Buckets the output by the given columns. If specified, the output is laid out on the file
   * system similar to Hive's bucketing scheme, but with a different bucket hash function
   * and is not compatible with Hive's bucketing.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 2.0
   */
  @scala.annotation.varargs
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): this.type = {
    this.numBuckets = Option(numBuckets)
    this.bucketColumnNames = Option(colName +: colNames)
    validatePartitioning()
    this
  }

  /**
   * Sorts the output in each bucket by the given columns.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 2.0
   */
  @scala.annotation.varargs
  def sortBy(colName: String, colNames: String*): this.type = {
    this.sortColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Clusters the output by the given columns on the storage. The rows with matching values in the
   * specified clustering columns will be consolidated within the same group.
   *
   * For instance, if you cluster a dataset by date, the data sharing the same date will be stored
   * together in a file. This arrangement improves query efficiency when you apply selective
   * filters to these clustering columns, thanks to data skipping.
   *
   * @since 4.0
   */
  @scala.annotation.varargs
  def clusterBy(colName: String, colNames: String*): this.type = {
    this.clusteringColumns = Option(colName +: colNames)
    validatePartitioning()
    this
  }

  /**
   * Saves the content of the `DataFrame` at the specified path.
   *
   * @since 1.4.0
   */
  def save(path: String): Unit

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * @since 1.4.0
   */
  def save(): Unit

  /**
   * Inserts the content of the `DataFrame` to the specified table. It requires that
   * the schema of the `DataFrame` is the same as the schema of the table.
   *
   * @note Unlike `saveAsTable`, `insertInto` ignores the column names and just uses position-based
   *       resolution. For example:
   * @note SaveMode.ErrorIfExists and SaveMode.Ignore behave as SaveMode.Append in `insertInto` as
   *       `insertInto` is not a table creating operation.
   *
   * {{{
   *    scala> Seq((1, 2)).toDF("i", "j").write.mode("overwrite").saveAsTable("t1")
   *    scala> Seq((3, 4)).toDF("j", "i").write.insertInto("t1")
   *    scala> Seq((5, 6)).toDF("a", "b").write.insertInto("t1")
   *    scala> sql("select * from t1").show
   *    +---+---+
   *    |  i|  j|
   *    +---+---+
   *    |  5|  6|
   *    |  3|  4|
   *    |  1|  2|
   *    +---+---+
   * }}}
   *
   *       Because it inserts data to an existing table, format or options will be ignored.
   * @since 1.4.0
   */
  def insertInto(tableName: String): Unit

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   * When `mode` is `Overwrite`, the schema of the `DataFrame` does not need to be
   * the same as that of the existing table.
   *
   * When `mode` is `Append`, if there is an existing table, we will use the format and options of
   * the existing table. The column order in the schema of the `DataFrame` doesn't need to be same
   * as that of the existing table. Unlike `insertInto`, `saveAsTable` will use the column names to
   * find the correct column positions. For example:
   *
   * {{{
   *    scala> Seq((1, 2)).toDF("i", "j").write.mode("overwrite").saveAsTable("t1")
   *    scala> Seq((3, 4)).toDF("j", "i").write.mode("append").saveAsTable("t1")
   *    scala> sql("select * from t1").show
   *    +---+---+
   *    |  i|  j|
   *    +---+---+
   *    |  1|  2|
   *    |  4|  3|
   *    +---+---+
   * }}}
   *
   * In this method, save mode is used to determine the behavior if the data source table exists in
   * Spark catalog. We will always overwrite the underlying data of data source (e.g. a table in
   * JDBC data source) if the table doesn't exist in Spark catalog, and will always append to the
   * underlying data of data source if the table already exists.
   *
   * When the DataFrame is created from a non-partitioned `HadoopFsRelation` with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @since 1.4.0
   */
  def saveAsTable(tableName: String): Unit

  /**
   * Saves the content of the `DataFrame` to an external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * JDBC-specific option and parameter documentation for storing tables via JDBC in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @param table Name of the table in the external database.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "batchsize" can be used to control the
   *                             number of rows per insert. "isolationLevel" can be one of
   *                             "NONE", "READ_COMMITTED", "READ_UNCOMMITTED", "REPEATABLE_READ",
   *                             or "SERIALIZABLE", corresponding to standard transaction
   *                             isolation levels defined by JDBC's Connection object, with default
   *                             of "READ_UNCOMMITTED".
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, connectionProperties: util.Properties): Unit = {
    assertNotPartitioned("jdbc")
    assertNotBucketed("jdbc")
    assertNotClustered("jdbc")
    // connectionProperties should override settings in extraOptions.
    this.extraOptions ++= connectionProperties.asScala
    // explicit url and dbtable should override all
    this.extraOptions ++= Seq("url" -> url, "dbtable" -> table)
    format("jdbc").save()
  }

  /**
   * Saves the content of the `DataFrame` in JSON format (<a href="http://jsonlines.org/">
   * JSON Lines text format or newline-delimited JSON</a>) at the specified path.
   * This is equivalent to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * You can find the JSON-specific options for writing JSON files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.4.0
   */
  def json(path: String): Unit = {
    format("json").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in Parquet format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * Parquet-specific option(s) for writing Parquet files can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.4.0
   */
  def parquet(path: String): Unit = {
    format("parquet").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in ORC format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * ORC-specific option(s) for writing ORC files can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.5.0
   */
  def orc(path: String): Unit = {
    format("orc").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in a text file at the specified path.
   * The DataFrame must have only one column that is of string type.
   * Each row becomes a new line in the output file. For example:
   * {{{
   *   // Scala:
   *   df.write.text("/path/to/output")
   *
   *   // Java:
   *   df.write().text("/path/to/output")
   * }}}
   * The text files will be encoded as UTF-8.
   *
   * You can find the text-specific options for writing text files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.6.0
   */
  def text(path: String): Unit = {
    format("text").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in CSV format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("csv").save(path)
   * }}}
   *
   * You can find the CSV-specific options for writing CSV files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def csv(path: String): Unit = {
    format("csv").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in XML format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("xml").save(path)
   * }}}
   *
   * Note that writing a XML file from `DataFrame` having a field `ArrayType` with
   * its element as `ArrayType` would have an additional nested field for the element.
   * For example, the `DataFrame` having a field below,
   *
   *    {@code fieldA [[data1], [data2]]}
   *
   * would produce a XML file below.
   *    {@code
   *    <fieldA>
   *        <item>data1</item>
   *    </fieldA>
   *    <fieldA>
   *        <item>data2</item>
   *    </fieldA>}
   *
   * Namely, roundtrip in writing and reading can end up in different schema structure.
   *
   * You can find the XML-specific options for writing XML files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option">
   * Data Source Option</a> in the version you use.
   */
  def xml(path: String): Unit = {
    format("xml").save(path)
  }

  protected def isBucketed(): Boolean = {
    if (sortColumnNames.isDefined && numBuckets.isEmpty) {
      throw CompilationErrors.sortByWithoutBucketingError()
    }
    numBuckets.isDefined
  }

  protected def assertNotBucketed(operation: String): Unit = {
    if (isBucketed()) {
      if (sortColumnNames.isEmpty) {
        throw CompilationErrors.bucketByUnsupportedByOperationError(operation)
      } else {
        throw CompilationErrors.bucketByAndSortByUnsupportedByOperationError(operation)
      }
    }
  }

  protected def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw CompilationErrors.operationNotSupportPartitioningError(operation)
    }
  }

  protected def assertNotClustered(operation: String): Unit = {
    if (clusteringColumns.isDefined) {
      throw CompilationErrors.operationNotSupportClusteringError(operation)
    }
  }

  /**
   * Validate that clusterBy is not used with partitionBy or bucketBy.
   */
  protected def validatePartitioning(): Unit = {
    if (clusteringColumns.nonEmpty) {
      if (partitioningColumns.nonEmpty) {
        throw CompilationErrors.clusterByWithPartitionedBy()
      }
      if (isBucketed()) {
        throw CompilationErrors.clusterByWithBucketing()
      }
    }
  }

  protected def defaultSourceOption: String = ""

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  protected var source: String = defaultSourceOption

  protected var mode: SaveMode = SaveMode.ErrorIfExists

  protected var extraOptions: CaseInsensitiveMap[String] = CaseInsensitiveMap[String](Map.empty)

  protected var partitioningColumns: Option[Seq[String]] = None

  protected var bucketColumnNames: Option[Seq[String]] = None

  protected var numBuckets: Option[Int] = None

  protected var sortColumnNames: Option[Seq[String]] = None

  protected var clusteringColumns: Option[Seq[String]] = None
}
