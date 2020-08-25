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

import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table}
import org.apache.spark.sql.connector.catalog.TableCapability.{STREAMING_WRITE, TRUNCATE}

@Experimental
final class DataStreamWriterV2[T] private[sql](table: String, ds: Dataset[T]) {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._
  import df.sparkSession.sessionState.analyzer.CatalogAndIdentifier

  private val df: DataFrame = ds.toDF()

  private val sparkSession = ds.sparkSession

  private var trigger: Trigger = Trigger.ProcessingTime(0L)

  private var extraOptions = new mutable.HashMap[String, String]()

  private val tableName = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(table)

  private val (catalog, identifier) = {
    val CatalogAndIdentifier(catalog, identifier) = tableName
    (catalog.asTableCatalog, identifier)
  }

  def trigger(trigger: Trigger): DataStreamWriterV2[T] = {
    this.trigger = trigger
    this
  }

  def queryName(queryName: String): DataStreamWriterV2[T] = {
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  def option(key: String, value: String): DataStreamWriterV2[T] = {
    this.extraOptions += (key -> value)
    this
  }

  def option(key: String, value: Boolean): DataStreamWriterV2[T] = option(key, value.toString)

  def option(key: String, value: Long): DataStreamWriterV2[T] = option(key, value.toString)

  def option(key: String, value: Double): DataStreamWriterV2[T] = option(key, value.toString)

  def options(options: scala.collection.Map[String, String]): DataStreamWriterV2[T] = {
    this.extraOptions ++= options
    this
  }

  def options(options: java.util.Map[String, String]): DataStreamWriterV2[T] = {
    this.options(options.asScala)
    this
  }

  def checkpointLocation(location: String): DataStreamWriterV2[T] = {
    this.extraOptions += "checkpointLocation" -> location
    this
  }

  @throws[NoSuchTableException]
  @throws[TimeoutException]
  def append(): StreamingQuery = {
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    loadTable(catalog, identifier) match {
      case Some(t: SupportsWrite) if t.supports(STREAMING_WRITE) =>
        start(t, OutputMode.Append())

      case Some(t) =>
        throw new IllegalArgumentException(s"Table ${t.name()} doesn't support streaming" +
          " write!")

      case _ =>
        throw new NoSuchTableException(identifier)
    }
  }

  @throws[NoSuchTableException]
  @throws[TimeoutException]
  def truncateAndAppend(): StreamingQuery = {
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    loadTable(catalog, identifier) match {
      case Some(t: SupportsWrite) if t.supports(STREAMING_WRITE) && t.supports(TRUNCATE) =>
        start(t, OutputMode.Complete())

      case Some(t) =>
        throw new IllegalArgumentException(s"Table ${t.name()} doesn't support streaming" +
          " write with truncate!")

      case _ =>
        throw new NoSuchTableException(identifier)
    }
  }

  private def start(table: Table, outputMode: OutputMode): StreamingQuery = {
    df.sparkSession.sessionState.streamingQueryManager.startQuery(
      extraOptions.get("queryName"),
      extraOptions.get("checkpointLocation"),
      df,
      extraOptions.toMap,
      table,
      outputMode,
      // Here we simply use default values of `useTempCheckpointLocation` and
      // `recoverFromCheckpointLocation`, which is required to be changed for some special built-in
      // data sources. They're not available in catalog, hence it's safe as of now, but once the
      // condition is broken we should take care of that.
      trigger = trigger)
  }
}
