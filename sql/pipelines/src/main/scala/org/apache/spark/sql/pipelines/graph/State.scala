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

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.AnalysisException

object State extends Logging {

  /**
   * Find the graph elements to reset given the current update context.
   * @param graph The graph to reset.
   * @param env The current update context.
   */
  private def findElementsToReset(graph: DataflowGraph, env: PipelineUpdateContext): Seq[Input] = {
    // If tableFilter is an instance of SomeTables, this is a refresh selection and all tables
    // to reset should be resettable; Otherwise, this is a full graph update, and we reset all
    // tables that are resettable.
    val specifiedTablesToReset = {
      val specifiedTables = env.fullRefreshTables.filter(graph.tables)
      env.fullRefreshTables match {
        case SomeTables(_) =>
          specifiedTables.foreach { t =>
            if (!PipelinesTableProperties.resetAllowed.fromMap(t.properties)) {
              throw new AnalysisException(
                "TABLE_NOT_RESETTABLE",
                Map("tableName" -> t.displayName)
              )
            }
          }
          specifiedTables
        case AllTables =>
          specifiedTables.filter(t => PipelinesTableProperties.resetAllowed.fromMap(t.properties))
        case NoTables => Seq.empty
      }
    }

    val specifiedSinksToReset = {
      env.fullRefreshTables match {
        case SomeTables(tablesAndSinks) =>
          graph.sinks.filter(sink => tablesAndSinks.contains(sink.identifier))
        case AllTables =>
          graph.sinks
        case NoTables => Seq.empty
      }
    }

    specifiedTablesToReset.flatMap(t => t +: graph.resolvedFlowsTo(t.identifier)) ++
    specifiedSinksToReset.flatMap(s => graph.resolvedFlowsTo(s.identifier))
  }

  /**
   * Performs the following on targets selected for full refresh:
   * - Clearing checkpoint data
   * - Truncating table data
   */
  def reset(resolvedGraph: DataflowGraph, env: PipelineUpdateContext): Seq[Input] = {
    val elementsToReset: Seq[Input] = findElementsToReset(resolvedGraph, env)

    elementsToReset.foreach {
      case f: ResolvedFlow => reset(f, env, resolvedGraph)
      case _ => // tables is handled in materializeTables since hive metastore does not support
                // removing all columns from a table.
    }

    elementsToReset
  }

  /**
   * Resets the checkpoint for the given flow by creating the next consecutive directory.
   */
  private def reset(flow: ResolvedFlow, env: PipelineUpdateContext, graph: DataflowGraph): Unit = {
    logInfo(log"Clearing out state for flow ${MDC(LogKeys.FLOW_NAME, flow.displayName)}")
    val flowMetadata = FlowSystemMetadata(env, flow, graph)
    flow match {
      case f if flowMetadata.latestCheckpointLocationOpt().isEmpty =>
        logInfo(
          s"Skipping resetting flow ${f.identifier} since its destination not been previously" +
          s"materialized and we can't find the checkpoint location."
        )
      case _ =>
        val hadoopConf = env.spark.sessionState.newHadoopConf()

        // Write a new checkpoint folder if needed
        val checkpointDir = new Path(flowMetadata.latestCheckpointLocation)
        val fs1 = checkpointDir.getFileSystem(hadoopConf)
        if (fs1.exists(checkpointDir)) {
          val nextVersion = checkpointDir.getName.toInt + 1
          val nextPath = new Path(checkpointDir.getParent, nextVersion.toString)
          fs1.mkdirs(nextPath)
          logInfo(
            log"Created new checkpoint for stream ${MDC(LogKeys.FLOW_NAME, flow.displayName)} " +
            log"at ${MDC(LogKeys.CHECKPOINT_PATH, nextPath.toString)}."
          )
        }
    }
  }
}
