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

import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging

/**
 * PageRank algorithm implementation. There are two implementations of PageRank.
 *
 * The first one uses the `org.apache.spark.graphx.graph` interface with `aggregateMessages` and
 * runs PageRank for a fixed number of iterations. This can be executed by setting `maxIter`.
 * Conceptually, the algorithm does the following:
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 1.0 )
 * for( iter <- 0 until maxIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * The second implementation uses the `org.apache.spark.graphx.Pregel` interface and runs PageRank
 * until convergence and this can be run by setting `tol`. Conceptually, the algorithm does the
 * following:
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 0.0 )
 * while( max(abs(PR - oldPr)) > tol ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
 *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
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
 *   - pagerank (`DoubleType`): the pagerank of this vertex
 *
 * The resulting edges DataFrame contains one additional column:
 *   - weight (`DoubleType`): the normalized weight of this edge after running PageRank
 */
class PageRank private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Logging {

  private var tol: Option[Double] = None
  private var resetProb: Option[Double] = Some(0.15)
  private var maxIter: Option[Int] = None
  private var srcId: Option[Any] = None

  /** Source vertex for a Personalized Page Rank (optional) */
  def sourceId(value: Any): this.type = {
    this.srcId = Some(value)
    this
  }

  /** Reset probability "alpha" */
  def resetProbability(value: Double): this.type = {
    resetProb = Some(value)
    this
  }

  def tol(value: Double): this.type = {
    tol = Some(value)
    this
  }

  def maxIter(value: Int): this.type = {
    maxIter = Some(value)
    this
  }

  def run(): GraphFrame = {
    val res = tol match {
      case Some(t) =>
        assert(maxIter.isEmpty, "You cannot specify maxIter() and tol() at the same time.")
        PageRank.runUntilConvergence(graph, t, resetProb.get, srcId)
      case None =>
        PageRank.run(graph, check(maxIter, "maxIter"), resetProb.get, srcId)
    }
    resultIsPersistent()
    res
  }
}

// TODO: srcID's type should be checked. The most futureproof check would be Encoder because it is
//       compatible with Datasets after that.
private object PageRank {

  /**
   * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight.
   *
   * @param graph
   *   the graph on which to compute PageRank
   * @param maxIter
   *   the number of iterations of PageRank to run
   * @param resetProb
   *   the random reset probability (alpha)
   * @return
   *   the graph containing with each vertex containing the PageRank and each edge containing the
   *   normalized weight.
   */
  def run(
      graph: GraphFrame,
      maxIter: Int,
      resetProb: Double = 0.15,
      srcId: Option[Any] = None): GraphFrame = {
    val longSrcId = srcId.map(GraphXConversions.integralId(graph, _))
    val gx =
      graphxlib.PageRank.runWithOptions(graph.cachedTopologyGraphX, maxIter, resetProb, longSrcId)
    val res = GraphXConversions
      .fromGraphX(graph, gx, vertexNames = Seq(PAGERANK), edgeNames = Seq(WEIGHT))
      .persist()
    res.vertices.count()
    res.edges.count()
    gx.unpersist()
    res
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @param graph
   *   the graph on which to compute PageRank
   * @param tol
   *   the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb
   *   the random reset probability (alpha)
   * @param srcId
   *   the source vertex for a Personalized Page Rank (optional)
   * @return
   *   the graph containing with each vertex containing the PageRank and each edge containing the
   *   normalized weight.
   */
  def runUntilConvergence(
      graph: GraphFrame,
      tol: Double,
      resetProb: Double = 0.15,
      srcId: Option[Any] = None): GraphFrame = {
    val longSrcId = srcId.map(GraphXConversions.integralId(graph, _))
    val gx = graphxlib.PageRank.runUntilConvergenceWithOptions(
      graph.cachedTopologyGraphX,
      tol,
      resetProb,
      longSrcId)
    GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(PAGERANK), edgeNames = Seq(WEIGHT))
  }

  /** Default name for the pagerank column. */
  private val PAGERANK = "pagerank"

  /** Default name for the weight column. */
  private val WEIGHT = "weight"
}
