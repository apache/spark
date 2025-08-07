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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.pipelines.logging.{FlowProgressEventLogger, PipelineEvent}

trait PipelineUpdateContext {

  /** The SparkSession for this update. */
  def spark: SparkSession

  /** Filter for which tables should be refreshed when performing this update. */
  def refreshTables: TableFilter

  /** Filter for which tables should be full refreshed when performing this update. */
  def fullRefreshTables: TableFilter

  def resetCheckpointFlows: FlowFilter

  /**
   * Filter for which flows should be refreshed when performing this update. Should be a superset of
   * fullRefreshFlows.
   */
  final def refreshFlows: FlowFilter = {
    val flowFilterForTables = (refreshTables, fullRefreshTables) match {
      case (AllTables, _) => AllFlows
      case (_, AllTables) => AllFlows
      case (SomeTables(tablesRefresh), SomeTables(tablesFullRefresh)) =>
        FlowsForTables(tablesRefresh ++ tablesFullRefresh)
      case (SomeTables(tables), _) => FlowsForTables(tables)
      case (_, SomeTables(tables)) => FlowsForTables(tables)
      case _ => NoFlows
    }
    UnionFlowFilter(flowFilterForTables, resetCheckpointFlows)
  }

  /** Callback to invoke for internal events that are emitted during a run of a pipeline. */
  def eventCallback: PipelineEvent => Unit

  /** Emits internal flow progress events into the event buffer. */
  def flowProgressEventLogger: FlowProgressEventLogger

  /** The unresolved graph for this update. */
  def unresolvedGraph: DataflowGraph

  /** Defines operations relates to end to end execution of a `DataflowGraph`. */
  val pipelineExecution: PipelineExecution = new PipelineExecution(context = this)
}
