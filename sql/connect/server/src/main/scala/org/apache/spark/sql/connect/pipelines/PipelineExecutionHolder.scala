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

package org.apache.spark.sql.connect.pipelines

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.pipelines.graph.{
  DataflowGraph,
  PipelineUpdateContext,
  PipelineUpdateContextImpl
}
import org.apache.spark.sql.pipelines.logging.PipelineEvent

/**
 * Holds the latest pipeline execution for each graph ID. This is used to manage the lifecycle of
 * pipeline executions.
 */
object PipelineExecutionHolder {

  private val executions = new ConcurrentHashMap[String, PipelineUpdateContext]()

  /**
   * Executes the pipeline for the given graph ID
   * @param graphId The id of the graph to be executed
   * @param unresolvedGraph The graph to be executed
   * @param eventCallback A callback which will be executed on events that are emitted during the
   *                      pipeline run
   */
  def executePipeline(
      graphId: String,
      unresolvedGraph: DataflowGraph,
      eventCallback: PipelineEvent => Unit): PipelineUpdateContext = {
    val pipelineUpdateContext = new PipelineUpdateContextImpl(unresolvedGraph, eventCallback)

    executions.compute(
      graphId,
      (_, existing) => {
        if (Option(existing).isDefined) {
          throw new IllegalStateException(
            s"Pipeline execution for graph ID $graphId already exists. " +
            s"Stop the existing execution before starting a new one."
          )
        }

        pipelineUpdateContext
      }
    )

    pipelineUpdateContext.pipelineExecution.runPipeline()
    pipelineUpdateContext
  }

  def getPipelineExecution(graphId: String): Option[PipelineUpdateContext] = {
    Option(executions.get(graphId))
  }

  def stopPipelineExecution(graphId: String): Unit = {
    executions.compute(graphId, (_, context) => {
      context.pipelineExecution.stopPipeline()
      // Remove the execution.
      null
    })
  }

  def stopAllPipelineExecutions(): Unit = {
    executions.forEach((_, context) => {
      if (context.pipelineExecution.executionStarted) {
        context.pipelineExecution.stopPipeline()
      }
    })
    executions.clear()
  }
}
