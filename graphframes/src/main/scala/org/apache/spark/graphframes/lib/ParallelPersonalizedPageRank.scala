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

import org.apache.spark.graphx
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithMaxIter

/**
 * Parallel Personalized PageRank algorithm implementation.
 *
 * This implementation uses the standalone [[GraphFrame]] interface and runs personalized PageRank
 * in parallel for a fixed number of iterations. This can be run by setting `maxIter`. The source
 * vertex Ids are set in `sourceIds`. A simple local implementation of this algorithm is as
 * follows.
 * {{{
 * var oldPR = Array.fill(n)( 1.0 )
 * val PR = (0 until n).map(i => if sourceIds.contains(i) alpha else 0.0)
 * for( iter <- 0 until maxIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *     if (sourceIds.contains(i)) PR[i] += alpha
 *   }
 * }
 * }}}
 *
 * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of neighbors
 * which link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * Note that this is not the "normalized" PageRank and as a consequence pages that have no inlinks
 * will have a PageRank of alpha. In particular, the pageranks may have some values greater than 1.
 *
 * The resulting vertices DataFrame contains one additional column:
 *   - pageranks (`VectorType`): the pageranks of this vertex from all input source vertices
 *
 * The resulting edges DataFrame contains one additional column:
 *   - weight (`DoubleType`): the normalized weight of this edge after running PageRank
 */
class ParallelPersonalizedPageRank private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithMaxIter
    with Logging {

  private var resetProb: Option[Double] = Some(0.15)
  private var srcIds: Array[Any] = Array()

  /** Source vertices for a Personalized Page Rank */
  def sourceIds(values: Array[Any]): this.type = {
    this.srcIds = values
    this
  }

  /** Reset probability "alpha" */
  def resetProbability(value: Double): this.type = {
    resetProb = Some(value)
    this
  }

  def run(): GraphFrame = {
    require(maxIter != None, "Max number of iterations maxIter() must be provided")
    require(srcIds.nonEmpty, "Source vertices Ids sourceIds() must be provided")
    val res = ParallelPersonalizedPageRank.run(graph, maxIter.get, resetProb.get, srcIds)
    resultIsPersistent()
    res
  }
}

private object ParallelPersonalizedPageRank {

  /** Default name for the pageranks column. */
  private val PAGERANKS = "pageranks"

  /** Default name for the weight column. */
  private val WEIGHT = "weight"

  /**
   * Run Personalized PageRank for a fixed number of iterations, for a set of starting nodes in
   * parallel. Returns a graph with vertex attributes containing the pageranks relative to all
   * starting nodes (as a vector) and edge attributes the normalized edge weight
   *
   * @param graph
   *   The graph on which to compute personalized pagerank
   * @param maxIter
   *   The number of iterations to run
   * @param resetProb
   *   The random reset probability
   * @param sourceIds
   *   The list of sources to compute personalized pagerank from
   * @return
   *   the graph with vertex attributes containing the pageranks relative to all starting nodes as
   *   a vector and edge attributes the normalized edge weight
   */
  def run(
      graph: GraphFrame,
      maxIter: Int,
      resetProb: Double,
      sourceIds: Array[Any]): GraphFrame = {
    val longSrcIds = sourceIds.map(GraphXConversions.integralId(graph, _))
    val gx = graphx.lib.PageRank.runParallelPersonalizedPageRank(
      graph.cachedTopologyGraphX,
      maxIter,
      resetProb,
      longSrcIds)
    val gf = GraphXConversions
      .fromGraphX(graph, gx, vertexNames = Seq(PAGERANKS), edgeNames = Seq(WEIGHT))
      .persist()
    gf.vertices.count()
    gf.edges.count()
    gx.unpersist()
    gf
  }
}
