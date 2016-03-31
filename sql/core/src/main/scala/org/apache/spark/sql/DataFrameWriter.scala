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

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.{SqlParser, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{Project, InsertIntoTable}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.{CreateTableUsingAsSelect, ResolvedDataSource}
import org.apache.spark.sql.sources.HadoopFsRelation


/**
 * :: Experimental ::
 * Interface used to write a [[DataFrame]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[DataFrame.write]] to access this.
 *
 * @since 1.4.0
 */
@Experimental
final class DataFrameWriter private[sql](df: DataFrame) {

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `SaveMode.Overwrite`: overwrite the existing data.
   *   - `SaveMode.Append`: append the data.
   *   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
   *   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.
   *
   * @since 1.4.0
   */
  def mode(saveMode: SaveMode): DataFrameWriter = {
    this.mode = saveMode
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   *
   * @since 1.4.0
   */
  def mode(saveMode: String): DataFrameWriter = {
    this.mode = saveMode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "default" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
        "Accepted modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
    this
  }

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameWriter = {
    this.source = source
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameWriter = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameWriter = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameWriter = {
    this.options(options.asScala)
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme.
   *
   * This was initially applicable for Parquet but in 1.5+ covers JSON, text, ORC and avro as well.
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataFrameWriter = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Saves the content of the [[DataFrame]] at the specified path.
   *
   * @since 1.4.0
   */
  def save(path: String): Unit = {
    this.extraOptions += ("path" -> path)
    save()
  }

  /**
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * @since 1.4.0
   */
  def save(): Unit = {
    ResolvedDataSource(
      df.sqlContext,
      source,
      partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),
      mode,
      extraOptions.toMap,
      df)
  }

  /**
   * Inserts the content of the [[DataFrame]] to the specified table. It requires that
   * the schema of the [[DataFrame]] is the same as the schema of the table.
   *
   * Because it inserts data to an existing table, format or options will be ignored.
   *
   * @since 1.4.0
   */
  def insertInto(tableName: String): Unit = {
    insertInto(SqlParser.parseTableIdentifier(tableName))
  }

  private def insertInto(tableIdent: TableIdentifier): Unit = {
    val partitions = normalizedParCols.map(_.map(col => col -> (None: Option[String])).toMap)
    val overwrite = mode == SaveMode.Overwrite

    // A partitioned relation's schema can be different from the input logicalPlan, since
    // partition columns are all moved after data columns. We Project to adjust the ordering.
    // TODO: this belongs to the analyzer.
    val input = normalizedParCols.map { parCols =>
      val (inputPartCols, inputDataCols) = df.logicalPlan.output.partition { attr =>
        parCols.contains(attr.name)
      }
      Project(inputDataCols ++ inputPartCols, df.logicalPlan)
    }.getOrElse(df.logicalPlan)

    df.sqlContext.executePlan(
      InsertIntoTable(
        UnresolvedRelation(tableIdent),
        partitions.getOrElse(Map.empty[String, Option[String]]),
        input,
        overwrite,
        ifNotExists = false)).toRdd
  }

  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map { parCols =>
    parCols.map { col =>
      df.logicalPlan.output
        .map(_.name)
        .find(df.sqlContext.analyzer.resolver(_, col))
        .getOrElse(throw new AnalysisException(s"Partition column $col not found in existing " +
          s"columns (${df.logicalPlan.output.map(_.name).mkString(", ")})"))
    }
  }

  /**
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   * When `mode` is `Overwrite`, the schema of the [[DataFrame]] does not need to be
   * the same as that of the existing table.
   * When `mode` is `Append`, the schema of the [[DataFrame]] need to be
   * the same as that of the existing table, and format or options will be ignored.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @since 1.4.0
   */
  def saveAsTable(tableName: String): Unit = {
    saveAsTable(SqlParser.parseTableIdentifier(tableName))
  }

  private def saveAsTable(tableIdent: TableIdentifier): Unit = {
    val tableExists = df.sqlContext.catalog.tableExists(tableIdent)

    (tableExists, mode) match {
      case (true, SaveMode.Ignore) =>
        // Do nothing

      case (true, SaveMode.ErrorIfExists) =>
        throw new AnalysisException(s"Table $tableIdent already exists.")

      case (true, SaveMode.Append) =>
        // If it is Append, we just ask insertInto to handle it. We will not use insertInto
        // to handle saveAsTable with Overwrite because saveAsTable can change the schema of
        // the table. But, insertInto with Overwrite requires the schema of data be the same
        // the schema of the table.
        insertInto(tableIdent)

      case _ =>
        val cmd =
          CreateTableUsingAsSelect(
            tableIdent,
            source,
            temporary = false,
            partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),
            mode,
            extraOptions.toMap,
            df.logicalPlan)
        df.sqlContext.executePlan(cmd).toRdd
    }
  }

  /**
   * Saves the content of the [[DataFrame]] to a external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   *
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties)
    val conn = JdbcUtils.createConnectionFactory(url, props)()

    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(df, url)
        val sql = s"CREATE TABLE $table ($schema)"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } finally {
          statement.close()
        }
      }
    } finally {
      conn.close()
    }

    JdbcUtils.saveTable(df, url, table, props)
  }

  /**
   * Saves the content of the [[DataFrame]] in JSON format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * @since 1.4.0
   */
  def json(path: String): Unit = format("json").save(path)

  /**
   * Saves the content of the [[DataFrame]] in Parquet format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * @since 1.4.0
   */
  def parquet(path: String): Unit = format("parquet").save(path)

  /**
   * Saves the content of the [[DataFrame]] in ORC format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * @since 1.5.0
   * @note Currently, this method can only be used together with `HiveContext`.
   */
  def orc(path: String): Unit = format("orc").save(path)

  /**
   * Saves the content of the [[DataFrame]] in a text file at the specified path.
   * The DataFrame must have only one column that is of string type.
   * Each row becomes a new line in the output file. For example:
   * {{{
   *   // Scala:
   *   df.write.text("/path/to/output")
   *
   *   // Java:
   *   df.write().text("/path/to/output")
   * }}}
   *
   * @since 1.6.0
   */
  def text(path: String): Unit = format("text").save(path)

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sqlContext.conf.defaultDataSourceName

  private var mode: SaveMode = SaveMode.ErrorIfExists

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  private var partitioningColumns: Option[Seq[String]] = None

}
