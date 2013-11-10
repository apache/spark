package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.util.ClosureCleaner



/**
 * `GraphOps` contains additional functionality (syntatic sugar) for
 * the graph type and is implicitly constructed for each Graph object.
 * All operations in `GraphOps` are expressed in terms of the
 * efficient GraphX API.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 *
 */
class GraphOps[VD: ClassManifest, ED: ClassManifest](graph: Graph[VD, ED]) {

  /**
   * Compute the number of edges in the graph.
   */
  lazy val numEdges: Long = graph.edges.count()


  /**
   * Compute the number of vertices in the graph.
   */
  lazy val numVertices: Long = graph.vertices.count()


  /**
   * Compute the in-degree of each vertex in the Graph returning an
   * RDD.
   * @note Vertices with no in edges are not returned in the resulting RDD.
   */
  lazy val inDegrees: VertexSetRDD[Int] = degreesRDD(EdgeDirection.In)


  /**
   * Compute the out-degree of each vertex in the Graph returning an RDD.
   * @note Vertices with no out edges are not returned in the resulting RDD.
   */
  lazy val outDegrees: VertexSetRDD[Int] = degreesRDD(EdgeDirection.Out)


  /**
   * Compute the degrees of each vertex in the Graph returning an RDD.
   * @note Vertices with no edges are not returned in the resulting
   * RDD.
   */
  lazy val degrees: VertexSetRDD[Int] = degreesRDD(EdgeDirection.Both)


  /**
   * Compute the neighboring vertex degrees.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertex attributes.
   */
  private def degreesRDD(edgeDirection: EdgeDirection): VertexSetRDD[Int] = {
    if (edgeDirection == EdgeDirection.In) {
      graph.mapReduceTriplets(et => Array((et.dstId,1)), _+_)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.mapReduceTriplets(et => Array((et.srcId,1)), _+_)
    } else { // EdgeDirection.both
      graph.mapReduceTriplets(et => Array((et.srcId,1), (et.dstId,1)), _+_)
    }
  }


  /**
   * This function is used to compute a statistic for the neighborhood
   * of each vertex and returns a value for all vertices (including
   * those without neighbors).
   *
   * @note Because the a default value is provided all vertices will
   * have a corresponding entry in the returned RDD.
   *
   * @param mapFunc the function applied to each edge adjacent to each
   * vertex.  The mapFunc can optionally return None in which case it
   * does not contribute to the final sum.
   * @param reduceFunc the function used to merge the results of each
   * map operation.
   * @param default the default value to use for each vertex if it has
   * no neighbors or the map function repeatedly evaluates to none
   * @param direction the direction of edges to consider (e.g., In,
   * Out, Both).
   * @tparam VD2 The returned type of the aggregation operation.
   *
   * @return A Spark.RDD containing tuples of vertex identifiers and
   * their resulting value.  There will be exactly one entry for ever
   * vertex in the original graph.
   *
   * @example We can use this function to compute the average follower
   * age for each user
   *
   * {{{
   * val graph: Graph[Int,Int] = loadGraph()
   * val averageFollowerAge: RDD[(Int, Int)] =
   *   graph.aggregateNeighbors[(Int,Double)](
   *     (vid, edge) => (edge.otherVertex(vid).data, 1),
   *     (a, b) => (a._1 + b._1, a._2 + b._2),
   *     -1,
   *     EdgeDirection.In)
   *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
   * }}}
   *
   * @todo Should this return a graph with the new vertex values?
   *
   */
  def aggregateNeighbors[A: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      reduceFunc: (A, A) => A,
      dir: EdgeDirection)
    : VertexSetRDD[A] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // Define a new map function over edge triplets
    val mf = (et: EdgeTriplet[VD,ED]) => {
      // Compute the message to the dst vertex
      val dst =
        if (dir == EdgeDirection.In || dir == EdgeDirection.Both) {
          mapFunc(et.dstId, et)
        } else { Option.empty[A] }
      // Compute the message to the source vertex
      val src =
        if (dir == EdgeDirection.Out || dir == EdgeDirection.Both) {
          mapFunc(et.srcId, et)
        } else { Option.empty[A] }
      // construct the return array
      (src, dst) match {
        case (None, None) => Array.empty[(Vid, A)]
        case (Some(srcA),None) => Array((et.srcId, srcA))
        case (None, Some(dstA)) => Array((et.dstId, dstA))
        case (Some(srcA), Some(dstA)) =>
          Array((et.srcId, srcA), (et.dstId, dstA))
      }
    }

    ClosureCleaner.clean(mf)
    graph.mapReduceTriplets(mf, reduceFunc)
  } // end of aggregateNeighbors


  /**
   * Return the Ids of the neighboring vertices.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the vertex set of neighboring ids for each vertex.
   */
  def collectNeighborIds(edgeDirection: EdgeDirection) :
    VertexSetRDD[Array[Vid]] = {
    val nbrs = graph.aggregateNeighbors[Array[Vid]](
      (vid, edge) => Some(Array(edge.otherVertexId(vid))),
      (a, b) => a ++ b,
      edgeDirection)
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) => nbrsOpt.getOrElse(Array.empty[Vid]) }
  } // end of collectNeighborIds


  /**
   * Collect the neighbor vertex attributes for each vertex.
   *
   * @note This function could be highly inefficient on power-law
   * graphs where high degree vertices may force a large ammount of
   * information to be collected to a single location.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the vertex set of neighboring vertex attributes for each
   * vertex.
   */
  def collectNeighbors(edgeDirection: EdgeDirection) :
    VertexSetRDD[ Array[(Vid, VD)] ] = {
    val nbrs = graph.aggregateNeighbors[Array[(Vid,VD)]](
      (vid, edge) =>
        Some(Array( (edge.otherVertexId(vid), edge.otherVertexAttr(vid)) )),
      (a, b) => a ++ b,
      edgeDirection)

    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) => nbrsOpt.getOrElse(Array.empty[(Vid, VD)]) }
  } // end of collectNeighbor


  /**
   * Join the vertices with an RDD and then apply a function from the
   * the vertex and RDD entry to a new vertex value.  The input table
   * should contain at most one entry for each vertex.  If no entry is
   * provided the map function is skipped and the old value is used.
   *
   * @tparam U the type of entry in the table of updates
   * @param table the table to join with the vertices in the graph.
   * The table should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex
   * values.  The map function is invoked only for vertices with a
   * corresponding entry in the table otherwise the old vertex value
   * is used.
   *
   * @note for small tables this function can be much more efficient
   * than leftJoinVertices
   *
   * @example This function is used to update the vertices with new
   * values based on external data.  For example we could add the out
   * degree to each vertex record
   *
   * {{{
   * val rawGraph: Graph[Int,()] = Graph.textFile("webgraph")
   *   .mapVertices(v => 0)
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg )
   * }}}
   *
   */
  def joinVertices[U: ClassManifest](table: RDD[(Vid, U)])(mapFunc: (Vid, VD, U) => VD)
    : Graph[VD, ED] = {
    ClosureCleaner.clean(mapFunc)
    val uf = (id: Vid, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    ClosureCleaner.clean(uf)
    graph.outerJoinVertices(table)(uf)
  }

} // end of GraphOps
