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

import org.apache.spark.sql.streaming.Trigger

/**
 * Plans execution of `Flow`s in a `DataflowGraph` by converting `Flow`s into
 * 'FlowExecution's.
 *
 * @param graph         `DataflowGraph` to help plan based on relationship to other elements.
 * @param updateContext `PipelineUpdateContext` for this pipeline update (shared across flows).
 * @param triggerFor    Function that returns the correct streaming Trigger for the specified
 *                      `Flow`.
 */
class FlowPlanner(
    graph: DataflowGraph,
    updateContext: PipelineUpdateContext,
    triggerFor: Flow => Trigger
) {

  /**
   * Turns a [[Flow]] into an executable [[FlowExecution]].
   */
  def plan(flow: ResolvedFlow): FlowExecution = {
    val output = graph.output(flow.destinationIdentifier)
    flow match {
      case cf: CompleteFlow =>
        new BatchTableWrite(
          graph = graph,
          flow = flow,
          identifier = cf.identifier,
          sqlConf = cf.sqlConf,
          destination = output.asInstanceOf[Table],
          updateContext = updateContext
        )
      case sf: StreamingFlow =>
        output match {
          case o: Table =>
            new StreamingTableWrite(
              graph = graph,
              flow = flow,
              identifier = sf.identifier,
              destination = o,
              updateContext = updateContext,
              sqlConf = sf.sqlConf,
              trigger = triggerFor(sf),
              checkpointPath = output.path
            )
          case _ =>
            throw new UnsupportedOperationException(
              s"Streaming flow ${sf.identifier} cannot write to non-table destination: " +
              s"${output.getClass.getSimpleName} (${flow.destinationIdentifier})"
            )
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to plan flow of type ${flow.getClass.getSimpleName}"
        )
    }
  }
}
