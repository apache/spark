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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.CheckAnalysis
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * Check whether sqlstreaming is valid, call failAnalysis if failed
 * @param sparkSession
 */
class ValidSQLStreaming(sparkSession: SparkSession, sqlConf: SQLConf)
  extends Rule[LogicalPlan] with CheckAnalysis {

  /**
   * Check whether logicalPlan is valid, if valid then create SQLStreamingSink.
   * If user use InsertIntoTable, then use table meta to create SQLStreamingSink,
   * if not, then create ConsoleSink to print result to Console
   * @param tableMeta the CatalogTable for stream source
   * @param child the logicalPlan of dataframe
   * @return
   */
  private def createSQLStreamingSink(tableMeta: Option[CatalogTable],
      child: LogicalPlan): LogicalPlan = {

    checkSQLStreamingEnable()

    tableMeta match {
      case Some(table) =>
        // Used when user define the sink meta
        if (!table.isStreaming) {
          failAnalysis(s"Not supported to insert write into " +
            s"none stream table ${table.qualifiedName}")
        }
        val resolveStreamRelation = new ResolveStreamRelation(sparkSession.sessionState.catalog,
          sqlConf, sparkSession)
        val streamingRelation = resolveStreamRelation.lookupStreamingRelation(table)
        val extraOptions = streamingRelation.asInstanceOf[StreamingRelation].dataSource.options
        val partitionColumnNames = table.partitionColumnNames
        SQLStreamingSink(sparkSession, extraOptions, partitionColumnNames, child)

      case None =>
        // Used when user use just select stream * from table.
        var extraOptions = Map("numRows" -> sqlConf.sqlStreamConsoleOutputRows)
        extraOptions += ("source" -> "console")
        val partitionColumnNames = Seq()
        SQLStreamingSink(sparkSession, extraOptions, partitionColumnNames, child)
    }
  }

  /**
   * Remove unused WithStreamDefinition when use alias query
   * @param plan the logicalPlan of dataframe
   * @return logicalPlan
   */
  private def removeUnusedWithStreamDefinition(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperators {
      case logical: WithStreamDefinition =>
        logical.child
    }

  /**
   * Default Disable SQLStreaming. User must set true to run SQLStreaming
   */
  private def checkSQLStreamingEnable(): Unit = {
    if (!sqlConf.sqlStreamQueryEnable) {
      failAnalysis("Disable SQLStreaming for default. If you want to use SQLSteaming, " +
        s"set ${SQLConf.SQLSTREAM_QUERY_ENABLE.key}=true")
    }
  }

  /**
   * Insert overwrite directory are not supported now,
   * thus this program doesn't check valid insert overwrite directory
   * @param plan
   * @return
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.analyzed && plan.isStreaming) {
      plan match {

        case InsertIntoTable(LogicalRelation(_, _, Some(tableMeta), _), _, child, _, _)
          if child.resolved =>
          createSQLStreamingSink(Some(tableMeta), removeUnusedWithStreamDefinition(child))

        case WithStreamDefinition(child) if child.resolved =>
          createSQLStreamingSink(None, removeUnusedWithStreamDefinition(child))

        case _ =>
          plan
      }
    } else {
      plan
    }
  }
}
