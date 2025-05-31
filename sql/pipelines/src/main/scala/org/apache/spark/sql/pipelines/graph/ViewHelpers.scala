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

import org.apache.spark.sql.catalyst.TableIdentifier

object ViewHelpers {

  /** Map of view identifier to corresponding unresolved flow */
  def persistedViewIdentifierToFlow(graph: DataflowGraph): Map[TableIdentifier, Flow] = {
    graph.persistedViews.map { v =>
      require(
        graph.flowsTo.get(v.identifier).isDefined,
        s"No flows to view ${v.identifier} were found"
      )
      val flowsToView = graph.flowsTo(v.identifier)
      require(
        flowsToView.size == 1,
        s"Expected a single flow to the view, found ${flowsToView.size} flows to ${v.identifier}"
      )
      (v.identifier, flowsToView.head)
    }.toMap
  }
}
