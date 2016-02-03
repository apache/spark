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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.execution.streaming.StreamExecution

/**
 * :: Experimental ::
 * Interface used to start a streaming query query execution.
 *
 * @since 2.0.0
 */
@Experimental
final class DataStreamWriter private[sql](df: DataFrame) {

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   *
   * @since 2.0.0
   */
  def format(source: String): DataStreamWriter = {
    this.source = source
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): DataStreamWriter = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamWriter = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): DataStreamWriter = {
    this.options(options.asScala)
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme.\
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataStreamWriter = {
    this.partitioningColumns = colNames
    this
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives.  The returned [[ContinuousQuery]] object can be used to interact with
   * the stream.
   * @since 2.0.0
   */
  def start(path: String): ContinuousQuery = {
    this.extraOptions += ("path" -> path)
    start()
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives.  The returned [[ContinuousQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  def start(): ContinuousQuery = {
    val sink = ResolvedDataSource.createSink(
      df.sqlContext,
      source,
      extraOptions.toMap,
      normalizedParCols)

    new StreamExecution(df.sqlContext, df.logicalPlan, sink)
  }

  private def normalizedParCols: Seq[String] = {
    partitioningColumns.map { col =>
      df.logicalPlan.output
        .map(_.name)
        .find(df.sqlContext.analyzer.resolver(_, col))
        .getOrElse(throw new AnalysisException(s"Partition column $col not found in existing " +
            s"columns (${df.logicalPlan.output.map(_.name).mkString(", ")})"))
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sqlContext.conf.defaultDataSourceName

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  private var partitioningColumns: Seq[String] = Nil

}
