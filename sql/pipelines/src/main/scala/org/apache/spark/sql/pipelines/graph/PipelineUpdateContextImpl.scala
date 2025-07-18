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

/**
 * An implementation of the PipelineUpdateContext trait used in production.
 * @param unresolvedGraph The graph (unresolved) to be executed in this update.
 * @param eventCallback A callback function to be called when an event is added to the event buffer.
 * @param refreshTables Filter for which tables should be refreshed when performing this update.
 * @param fullRefreshTables Filter for which tables should be full refreshed
 *                          when performing this update.
 */
class PipelineUpdateContextImpl(
    override val unresolvedGraph: DataflowGraph,
    override val eventCallback: PipelineEvent => Unit,
    override val refreshTables: TableFilter = AllTables,
    override val fullRefreshTables: TableFilter = NoTables
) extends PipelineUpdateContext {

  override val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
    throw new IllegalStateException("SparkSession is not available")
  )

  override val flowProgressEventLogger: FlowProgressEventLogger =
    new FlowProgressEventLogger(eventCallback = eventCallback)

  override val resetCheckpointFlows: FlowFilter = NoFlows
}
