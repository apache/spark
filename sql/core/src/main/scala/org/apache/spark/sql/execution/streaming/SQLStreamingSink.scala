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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.sources.v2.StreamingWriteSupportProvider
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.Utils

/**
 * The basic RunnableCommand for SQLStreaming, using Command.run to start a streaming query.
 *
 * @param sparkSession
 * @param extraOptions
 * @param partitionColumnNames
 * @param child
 */
case class SQLStreamingSink(sparkSession: SparkSession,
    table: CatalogTable,
    child: LogicalPlan)
  extends RunnableCommand {

  private val sqlConf = sparkSession.sqlContext.conf

  /**
   * The given column name may not be equal to any of the existing column names if we were in
   * case-insensitive context. Normalize the given column name to the real one so that we don't
   * need to care about case sensitivity afterwards.
   */
  private def normalize(df: DataFrame, columnName: String, columnType: String): String = {
    val validColumnNames = df.logicalPlan.output.map(_.name)
    validColumnNames.find(sparkSession.sessionState.analyzer.resolver(_, columnName))
      .getOrElse(throw new AnalysisException(s"$columnType column $columnName not found in " +
        s"existing columns (${validColumnNames.mkString(", ")})"))
  }

  /**
   * Parse spark.sqlstreaming.trigger.seconds to Trigger
   */
  private def parseTrigger(): Trigger = {
    val trigger = Utils.timeStringAsMs(sqlConf.sqlStreamTrigger)
    Trigger.ProcessingTime(trigger, TimeUnit.MILLISECONDS)
  }

  /**
   * Running by queryExecution.executeCollect()
   * @param sparkSession
   * @return return empty rdds, save as DDLCommands
   */
  override def run(sparkSession: SparkSession): Seq[Row] = {

    ///////////////////////////////////////////////////////////////////////////////////////
    // Builder pattern config options
    ///////////////////////////////////////////////////////////////////////////////////////
    val df = Dataset.ofRows(sparkSession, child)
    val outputMode = InternalOutputModes(sqlConf.sqlStreamOutputMode)
    val normalizedParCols = table.partitionColumnNames.map {
      normalize(df, _, "Partition")
    }

    val ds = DataSource.lookupDataSource(table.provider.get, sparkSession.sessionState.conf)
    val disabledSources = sparkSession.sqlContext.conf.disabledV2StreamingWriters.split(",")
    var options = table.storage.properties
    val sink = ds.newInstance() match {
      case w: StreamingWriteSupportProvider
        if !disabledSources.contains(w.getClass.getCanonicalName) =>
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          w, df.sparkSession.sessionState.conf)
        options = sessionOptions ++ options
        w
      case _ =>
        val ds = DataSource(
          df.sparkSession,
          className = table.provider.get,
          options = options,
          partitionColumns = normalizedParCols)
        ds.createSink(outputMode)
    }

    sparkSession.sessionState.streamingQueryManager.startQuery(
      sqlConf.sqlStreamQueryName,
      None,
      df,
      table.storage.properties,
      sink,
      outputMode,
      trigger = parseTrigger()
    ).awaitTermination()

    Seq.empty[Row]
  }
}
