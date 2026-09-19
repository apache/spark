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

package org.apache.spark.graphframes.lib

import org.apache.spark.graphframes.{GraphFrame, Logging, WithMaxIter}
import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * Compute the strongly connected component (SCC) of each vertex and return a DataFrame with each
 * vertex assigned to the SCC containing that vertex.
 *
 * The resulting DataFrame contains all the original vertex information and one additional column:
 *   - component (`LongType`): unique ID for this component
 */
class StronglyConnectedComponents private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithMaxIter
    with Logging {

  def run(): DataFrame = {
    val res = StronglyConnectedComponents.run(graph, check(maxIter, "maxIter"))
    resultIsPersistent()
    res
  }
}

/** Strongly connected components algorithm implementation. */
private object StronglyConnectedComponents {
  private def run(graph: GraphFrame, numIter: Int): DataFrame = {
    val gx = graphxlib.StronglyConnectedComponents.run(graph.cachedTopologyGraphX, numIter)
    val res = GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(COMPONENT_ID)).vertices
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)
    res.count()
    gx.unpersist()
    res
  }

  private[graphframes] val COMPONENT_ID = "component"
}
