package org.apache.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.Logging
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

/**
 * Implements the GraphLab gather-apply-scatter API.
 */
object GraphLab extends Logging {

  /**
   * Executes the GraphLab Gather-Apply-Scatter API.
   *
   * @param graph the graph on which to execute the GraphLab API
   * @param gatherFunc executed on each edge triplet
   *                   adjacent to a vertex. Returns an accumulator which
   *                   is then merged using the merge function.
   * @param mergeFunc an accumulative associative operation on the result of
   *                  the gather type.
   * @param applyFunc takes a vertex and the final result of the merge operations
   *                  on the adjacent edges and returns a new vertex value.
   * @param scatterFunc executed after the apply function. Takes
   *                    a triplet and signals whether the neighboring vertex program
   *                    must be recomputed.
   * @param startVertices a predicate to determine which vertices to start the computation on.
   *                      These will be the active vertices in the first iteration.
   * @param numIter the maximum number of iterations to run
   * @param gatherDirection the direction of edges to consider during the gather phase
   * @param scatterDirection the direction of edges to consider during the scatter phase
   *
   * @tparam VD the graph vertex attribute type
   * @tparam ED the graph edge attribute type
   * @tparam A the type accumulated during the gather phase
   * @return the resulting graph after the algorithm converges
   *
   * @note Unlike [[Pregel]], this implementation of [[GraphLab]] does not unpersist RDDs from
   * previous iterations. As a result, long-running iterative GraphLab programs will eventually fill
   * the Spark cache. Though Spark will evict RDDs from old iterations eventually, garbage
   * collection will take longer than necessary since it must examine the entire cache. This will be
   * fixed in a future update.
   */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
    (graph: Graph[VD, ED], numIter: Int,
     gatherDirection: EdgeDirection = EdgeDirection.In,
     scatterDirection: EdgeDirection = EdgeDirection.Out)
    (gatherFunc: (VertexID, EdgeTriplet[VD, ED]) => A,
     mergeFunc: (A, A) => A,
     applyFunc: (VertexID, VD, Option[A]) => VD,
     scatterFunc: (VertexID, EdgeTriplet[VD, ED]) => Boolean,
     startVertices: (VertexID, VD) => Boolean = (vid: VertexID, data: VD) => true)
    : Graph[VD, ED] = {


    // Add an active attribute to all vertices to track convergence.
    var activeGraph: Graph[(Boolean, VD), ED] = graph.mapVertices {
      case (id, data) => (startVertices(id, data), data)
    }.cache()

    // The gather function wrapper strips the active attribute and
    // only invokes the gather function on active vertices
    def gather(vid: VertexID, e: EdgeTriplet[(Boolean, VD), ED]): Option[A] = {
      if (e.vertexAttr(vid)._1) {
        val edgeTriplet = new EdgeTriplet[VD,ED]
        edgeTriplet.set(e)
        edgeTriplet.srcAttr = e.srcAttr._2
        edgeTriplet.dstAttr = e.dstAttr._2
        Some(gatherFunc(vid, edgeTriplet))
      } else {
        None
      }
    }

    // The apply function wrapper strips the vertex of the active attribute
    // and only invokes the apply function on active vertices
    def apply(vid: VertexID, data: (Boolean, VD), accum: Option[A]): (Boolean, VD) = {
      val (active, vData) = data
      if (active) (true, applyFunc(vid, vData, accum))
      else (false, vData)
    }

    // The scatter function wrapper strips the vertex of the active attribute
    // and only invokes the scatter function on active vertices
    def scatter(rawVertexID: VertexID, e: EdgeTriplet[(Boolean, VD), ED]): Option[Boolean] = {
      val vid = e.otherVertexId(rawVertexID)
      if (e.vertexAttr(vid)._1) {
        val edgeTriplet = new EdgeTriplet[VD,ED]
        edgeTriplet.set(e)
        edgeTriplet.srcAttr = e.srcAttr._2
        edgeTriplet.dstAttr = e.dstAttr._2
        Some(scatterFunc(vid, edgeTriplet))
      } else {
        None
      }
    }

    // Used to set the active status of vertices for the next round
    def applyActive(
        vid: VertexID, data: (Boolean, VD), newActiveOpt: Option[Boolean]): (Boolean, VD) = {
      val (prevActive, vData) = data
      (newActiveOpt.getOrElse(false), vData)
    }

    // Main Loop ---------------------------------------------------------------------
    var i = 0
    var numActive = activeGraph.numVertices
    var prevActiveGraph: Graph[(Boolean, VD), ED] = null
    while (i < numIter && numActive > 0) {

      // Gather
      val gathered: RDD[(VertexID, A)] =
        activeGraph.aggregateNeighbors(gather, mergeFunc, gatherDirection)

      // Apply
      val applied = activeGraph.outerJoinVertices(gathered)(apply).cache()

      // Scatter is basically a gather in the opposite direction so we reverse the edge direction
      val scattered: RDD[(VertexID, Boolean)] =
        applied.aggregateNeighbors(scatter, _ || _, scatterDirection.reverse)

      prevActiveGraph = activeGraph
      activeGraph = applied.outerJoinVertices(scattered)(applyActive).cache()

      // Calculate the number of active vertices.
      numActive = activeGraph.vertices.map{
        case (vid, data) => if (data._1) 1 else 0
        }.reduce(_ + _)
      logInfo("Number active vertices: " + numActive)

      i += 1
    }

    // Remove the active attribute from the vertex data before returning the graph
    activeGraph.mapVertices{case (vid, data) => data._2 }
  }
}
