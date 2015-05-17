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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.sources.{ResolvedDataSource, CreateTableUsingAsSelect}


/**
 * :: Experimental ::
 * Interface used to write a [[DataFrame]] to external storage systems (e.g. file systems,
 * key-value stores, etc).
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
    this.options(scala.collection.JavaConversions.mapAsScalaMap(options))
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme.
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
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * @since 1.4.0
   */
  def saveAsTable(tableName: String): Unit = {
    val cmd =
      CreateTableUsingAsSelect(
        tableName,
        source,
        temporary = false,
        partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),
        mode,
        extraOptions.toMap,
        df.logicalPlan)
    df.sqlContext.executePlan(cmd).toRdd
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

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sqlContext.conf.defaultDataSourceName

  private var mode: SaveMode = SaveMode.ErrorIfExists

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  private var partitioningColumns: Option[Seq[String]] = None

}
