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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import breeze.linalg.{Vector => BV}

import org.apache.spark.graphx._
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.NUM_ITERATIONS
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
 * PageRank algorithm implementation. There are two implementations of PageRank implemented.
 *
 * The first implementation uses the standalone `Graph` interface and runs PageRank
 * for a fixed number of iterations:
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * The second implementation uses the `Pregel` interface and runs PageRank until
 * convergence:
 *
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
 * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
 * neighbors which link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * @note This is not the "normalized" PageRank and as a consequence pages that have no
 * inlinks will have a PageRank of alpha.
 */
object PageRank extends Logging {


  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runWithOptions(graph, numIter, resetProb, None)
  }

  /**
   * Run an update pass of PageRank algorithm. Update the values of every node in the
   * pageRank
   *
   * @param rankGraph the current PageRank
   * @param personalized True if personalized pageRank
   * @param resetProb the random reset probability (alpha)
   * @param src the source vertex for a Personalized Page Rank
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight after a single update step.
   *
   */
  private def runUpdate(rankGraph: Graph[Double, Double], personalized: Boolean,
                resetProb: Double, src: VertexId): Graph[Double, Double] = {

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }
    // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
    // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
    val rankUpdates = rankGraph.aggregateMessages[Double](
      ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

    // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
    // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
    // edge partitions.
    val rPrb = if (personalized) {
      (src: VertexId, id: VertexId) => resetProb * delta(src, id)
    } else {
      (src: VertexId, id: VertexId) => resetProb
    }

    rankGraph.outerJoinVertices(rankUpdates) {
      (id, oldRank, msgSumOpt) => rPrb(src, id) + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
    }
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double] = {
    runWithOptions(graph, numIter, resetProb, srcId, normalized = true)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   * @param normalized whether or not to normalize rank sum
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   * @since 3.2.0
   */
  def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double,
      srcId: Option[VertexId], normalized: Boolean): Graph[Double, Double] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    // When running personalized pagerank, only the source vertex
    // has an attribute 1.0. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
        if (!(id != src && personalized)) 1.0 else 0.0
      }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()
      prevRankGraph = rankGraph

      rankGraph = runUpdate(rankGraph, personalized, resetProb, src)
      rankGraph.cache()
      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(log"PageRank finished iteration ${MDC(NUM_ITERATIONS, iteration)}.")
      prevRankGraph.vertices.unpersist()
      prevRankGraph.edges.unpersist()
      iteration += 1
    }

    if (normalized) {
      // SPARK-18847 If the graph has sinks (vertices with no outgoing edges),
      // correct the sum of ranks
      normalizeRankSum(rankGraph, personalized)
    } else {
      rankGraph
    }
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   * @param preRankGraph PageRank graph from which to keep iterating
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   */
  def runWithOptionsWithPreviousPageRank[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double, srcId: Option[VertexId],
      preRankGraph: Graph[Double, Double]): Graph[Double, Double] = {
    runWithOptionsWithPreviousPageRank(
      graph, numIter, resetProb, srcId, normalized = true, preRankGraph
    )
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   * @param normalized whether or not to normalize rank sum
   * @param preRankGraph PageRank graph from which to keep iterating
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   *
   * @since 3.2.0
   */
  def runWithOptionsWithPreviousPageRank[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double, srcId: Option[VertexId],
      normalized: Boolean, preRankGraph: Graph[Double, Double]): Graph[Double, Double] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    val graphVertices = graph.numVertices
    val prePageRankVertices = preRankGraph.numVertices
    require(graphVertices == prePageRankVertices, s"Graph and previous pageRankGraph" +
      s" must have the same number of vertices but got ${graphVertices} and ${prePageRankVertices}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    // When running personalized pagerank, only the source vertex
    // has an attribute 1.0. All others are set to 0.
    var rankGraph: Graph[Double, Double] = preRankGraph

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null

    while (iteration < numIter) {
      rankGraph.cache()
      prevRankGraph = rankGraph

      rankGraph = runUpdate(rankGraph, personalized, resetProb, src)
      rankGraph.cache()
      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(log"PageRank finished iteration ${MDC(NUM_ITERATIONS, iteration)}.")
      prevRankGraph.vertices.unpersist()
      prevRankGraph.edges.unpersist()
      iteration += 1
    }

    if (normalized) {
      // SPARK-18847 If the graph has sinks (vertices with no outgoing edges),
      // correct the sum of ranks
      normalizeRankSum(rankGraph, personalized)
    } else {
      rankGraph
    }
  }

  /**
   * Run Personalized PageRank for a fixed number of iterations, for a
   * set of starting nodes in parallel. Returns a graph with vertex attributes
   * containing the pagerank relative to all starting nodes (as a sparse vector) and
   * edge attributes the normalized edge weight
   *
   * @tparam VD The original vertex attribute (not used)
   * @tparam ED The original edge attribute (not used)
   *
   * @param graph The graph on which to compute personalized pagerank
   * @param numIter The number of iterations to run
   * @param resetProb The random reset probability
   * @param sources The list of sources to compute personalized pagerank from
   * @return the graph with vertex attributes
   *         containing the pagerank relative to all starting nodes (as a sparse vector
   *         indexed by the position of nodes in the sources list) and
   *         edge attributes the normalized edge weight
   */
  def runParallelPersonalizedPageRank[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      numIter: Int,
      resetProb: Double = 0.15,
      sources: Array[VertexId]): Graph[Vector, Double] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    require(sources.nonEmpty, s"The list of sources must be non-empty," +
      s" but got ${sources.mkString("[", ",", "]")}")

    val zero = Vectors.sparse(sources.length, List()).asBreeze
    // map of vid -> vector where for each vid, the _position of vid in source_ is set to 1.0
    val sourcesInitMap = sources.zipWithIndex.map { case (vid, i) =>
      val v = Vectors.sparse(sources.length, Array(i), Array(1.0)).asBreeze
      (vid, v)
    }.toMap

    val sc = graph.vertices.sparkContext
    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each source vertex with attribute 1.0.
    var rankGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices((vid, _) => sourcesInitMapBC.value.getOrElse(vid, zero))

    var i = 0
    while (i < numIter) {
      val prevRankGraph = rankGraph
      // Propagates the message along outbound edges
      // and adding start nodes back in with activation resetProb
      val rankUpdates = rankGraph.aggregateMessages[BV[Double]](
        ctx => ctx.sendToDst(ctx.srcAttr *:* ctx.attr),
        (a : BV[Double], b : BV[Double]) => a +:+ b, TripletFields.Src)

      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (vid, oldRank, msgSumOpt) =>
          val popActivations: BV[Double] = msgSumOpt.getOrElse(zero) *:* (1.0 - resetProb)
          val resetActivations = if (sourcesInitMapBC.value contains vid) {
            sourcesInitMapBC.value(vid) *:* resetProb
          } else {
            zero
          }
          popActivations +:+ resetActivations
        }.cache()

      rankGraph.edges.foreachPartition(_ => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist()
      prevRankGraph.edges.unpersist()

      logInfo(log"Parallel Personalized PageRank finished iteration ${MDC(NUM_ITERATIONS, i)}.")

      i += 1
    }

    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    val rankSums = rankGraph.vertices.values.fold(zero)(_ +:+ _)
    rankGraph.mapVertices { (vid, attr) =>
      Vectors.fromBreeze(attr /:/ rankSums)
    }
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
      runUntilConvergenceWithOptions(graph, tol, resetProb)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
        if (id == src) (0.0, Double.NegativeInfinity) else (0.0, 0.0)
      }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
      msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = if (lastDelta == Double.NegativeInfinity) {
        1.0
      } else {
        oldPR + (1.0 - resetProb) * msgSum
      }
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    val rankGraph = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)

    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    normalizeRankSum(rankGraph, personalized)
  }

  // Normalizes the sum of ranks to n (or 1 if personalized)
  private def normalizeRankSum(rankGraph: Graph[Double, Double], personalized: Boolean) = {
    val rankSum = rankGraph.vertices.values.sum()
    if (personalized) {
      rankGraph.mapVertices((id, rank) => rank / rankSum)
    } else {
      val numVertices = rankGraph.numVertices
      val correctionFactor = numVertices.toDouble / rankSum
      rankGraph.mapVertices((id, rank) => rank * correctionFactor)
    }
  }
}
