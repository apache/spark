package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.util.ClosureCleaner


class GraphOps[VD: ClassManifest, ED: ClassManifest](graph: Graph[VD, ED]) {



  lazy val numEdges: Long = graph.edges.count()

  lazy val numVertices: Long = graph.vertices.count()

  lazy val inDegrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.In)

  lazy val outDegrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.Out)

  lazy val degrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.Both)


  /**
   * This function is used to compute a statistic for the neighborhood of each
   * vertex and returns a value for all vertices (including those without
   * neighbors).
   *
   * @note Because the a default value is provided all vertices will have a
   * corresponding entry in the returned RDD.
   *
   * @param mapFunc the function applied to each edge adjacent to each vertex.
   * The mapFunc can optionally return None in which case it does not
   * contribute to the final sum.
   * @param reduceFunc the function used to merge the results of each map
   * operation.
   * @param default the default value to use for each vertex if it has no
   * neighbors or the map function repeatedly evaluates to none
   * @param direction the direction of edges to consider (e.g., In, Out, Both).
   * @tparam VD2 The returned type of the aggregation operation.
   *
   * @return A Spark.RDD containing tuples of vertex identifiers and
   * their resulting value.  There will be exactly one entry for ever vertex in
   * the original graph.
   *
   * @example We can use this function to compute the average follower age
   * for each user
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
    : RDD[(Vid, A)] = {

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


  def collectNeighborIds(edgeDirection: EdgeDirection) : RDD[(Vid, Array[Vid])] = {
    val nbrs = graph.aggregateNeighbors[Array[Vid]](
      (vid, edge) => Some(Array(edge.otherVertexId(vid))),
      (a, b) => a ++ b,
      edgeDirection)

    graph.vertices.leftOuterJoin(nbrs).mapValues{
      case (_, Some(nbrs)) => nbrs
      case (_, None) => Array.empty[Vid]
    }
  }


  private def degreesRDD(edgeDirection: EdgeDirection): RDD[(Vid, Int)] = {
    graph.aggregateNeighbors((vid, edge) => Some(1), _+_, edgeDirection)
  }


  /**
   * Join the vertices with an RDD and then apply a function from the the
   * vertex and RDD entry to a new vertex value.  The input table should
   * contain at most one entry for each vertex.  If no entry is provided the
   * map function is skipped and the old value is used.
   *
   * @tparam U the type of entry in the table of updates
   * @param table the table to join with the vertices in the graph.  The table
   * should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.  The
   * map function is invoked only for vertices with a corresponding entry in
   * the table otherwise the old vertex value is used.
   *
   * @note for small tables this function can be much more efficient than
   * leftJoinVertices
   *
   * @example This function is used to update the vertices with new values
   * based on external data.  For example we could add the out degree to each
   * vertex record
   * {{{
   * val rawGraph: Graph[Int,()] = Graph.textFile("webgraph")
   *   .mapVertices(v => 0)
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg )
   * }}}
   *
   * @todo Should this function be curried to enable type inference?  For
   * example
   * {{{
   * graph.joinVertices(tbl)( (v, row) => row )
   * }}}
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

}
