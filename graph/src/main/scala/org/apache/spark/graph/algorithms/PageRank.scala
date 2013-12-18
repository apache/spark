package org.apache.spark.graph.algorithms

import org.apache.spark.Logging
import org.apache.spark.graph._


object PageRank extends Logging {

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
   *
   * The following PageRank fixed point is computed for each vertex.
   *
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
   * where `alpha` is the random reset probability (typically 0.15),
   * `inNbrs[i]` is the set of neighbors whick link to `i` and
   * `outDeg[j]` is the out degree of vertex `j`.
   *
   * Note that this is not the "normalized" PageRank and as a consequence pages that have no
   * inlinks will have a PageRank of alpha.
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
   *
   */
  def run[VD: Manifest, ED: Manifest](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
  {

    /**
     * Initialize the pagerankGraph with each edge attribute having
     * weight 1/outDegree and each vertex with attribute 1.0.
     */
    val pagerankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees){
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices( (id, attr) => 1.0 )

    // Display statistics about pagerank
    logInfo(pagerankGraph.statistics.toString)

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: Vid, attr: Double, msgSum: Double): Double =
      resetProb + (1.0 - resetProb) * msgSum
    def sendMessage(edge: EdgeTriplet[Double, Double]) =
      Iterator((edge.dstId, edge.srcAttr * edge.attr))
    def messageCombiner(a: Double, b: Double): Double = a + b
    // The initial message received by all vertices in PageRank
    val initialMessage = 0.0

    // Execute pregel for a fixed number of iterations.
    Pregel(pagerankGraph, initialMessage, numIter)(
      vertexProgram, sendMessage, messageCombiner)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
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
   * where `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
   * neighbors whick link to `i` and `outDeg[j]` is the out degree of vertex `j`.
   *
   * Note that this is not the "normalized" PageRank and as a consequence pages that have no
   * inlinks will have a PageRank of alpha.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more * accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntillConvergence[VD: Manifest, ED: Manifest](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices( (id, attr) => (0.0, 0.0) )

    // Display statistics about pagerank
    logInfo(pagerankGraph.statistics.toString)

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: Vid, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
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
    val initialMessage = resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage)(vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank

  def runStandalone[VD: Manifest, ED: Manifest](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): VertexRDD[Double] = {

    // Initialize the ranks
    var ranks: VertexRDD[Double] = graph.vertices.mapValues((vid, attr) => resetProb).cache()

    // Initialize the delta graph where each vertex stores its delta and each edge knows its weight
    var deltaGraph: Graph[Double, Double] =
      graph.outerJoinVertices(graph.outDegrees)((vid, vdata, deg) => deg.getOrElse(0))
      .mapTriplets(e => 1.0 / e.srcAttr)
      .mapVertices((vid, degree) => resetProb).cache()
    var numDeltas: Long = ranks.count()

    var prevDeltas: Option[VertexRDD[Double]] = None

    var i = 0
    val weight = (1.0 - resetProb)
    while (numDeltas > 0) {
      // Compute new deltas. Only deltas that existed in the last round (i.e., were greater than
      // `tol`) get to send messages; those that were less than `tol` would send messages less than
      // `tol` as well.
      val deltas = deltaGraph
        .mapReduceTriplets[Double](
          et => Iterator((et.dstId, et.srcAttr * et.attr * weight)),
          _ + _,
          prevDeltas.map((_, EdgeDirection.Out)))
        .filter { case (vid, delta) => delta > tol }
        .cache()
      prevDeltas = Some(deltas)
      numDeltas = deltas.count()
      logInfo("Standalone PageRank: iter %d has %d deltas".format(i, numDeltas))

      // Update deltaGraph with the deltas
      deltaGraph = deltaGraph.outerJoinVertices(deltas) { (vid, old, newOpt) =>
        newOpt.getOrElse(old)
      }.cache()

      // Update ranks
      ranks = ranks.leftZipJoin(deltas) { (vid, oldRank, deltaOpt) =>
        oldRank + deltaOpt.getOrElse(0.0)
      }
      ranks.foreach(x => {}) // force the iteration for ease of debugging

      i += 1
    }

    ranks
  }

}
