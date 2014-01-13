package org.apache.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

/**
 * Contains additional functionality for [[Graph]]. All operations are expressed in terms of the
 * efficient GraphX API. This class is implicitly constructed for each Graph object.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
class GraphOps[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) {

  /** The number of edges in the graph. */
  lazy val numEdges: Long = graph.edges.count()

  /** The number of vertices in the graph. */
  lazy val numVertices: Long = graph.vertices.count()

  /**
   * The in-degree of each vertex in the graph.
   * @note Vertices with no in-edges are not returned in the resulting RDD.
   */
  lazy val inDegrees: VertexRDD[Int] = degreesRDD(EdgeDirection.In)

  /**
   * The out-degree of each vertex in the graph.
   * @note Vertices with no out-edges are not returned in the resulting RDD.
   */
  lazy val outDegrees: VertexRDD[Int] = degreesRDD(EdgeDirection.Out)

  /**
   * The degree of each vertex in the graph.
   * @note Vertices with no edges are not returned in the resulting RDD.
   */
  lazy val degrees: VertexRDD[Int] = degreesRDD(EdgeDirection.Both)

  /**
   * Computes the neighboring vertex degrees.
   *
   * @param edgeDirection the direction along which to collect neighboring vertex attributes
   */
  private def degreesRDD(edgeDirection: EdgeDirection): VertexRDD[Int] = {
    if (edgeDirection == EdgeDirection.In) {
      graph.mapReduceTriplets(et => Iterator((et.dstId,1)), _ + _)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.mapReduceTriplets(et => Iterator((et.srcId,1)), _ + _)
    } else { // EdgeDirection.both
      graph.mapReduceTriplets(et => Iterator((et.srcId,1), (et.dstId,1)), _ + _)
    }
  }

  /**
   * Computes a statistic for the neighborhood of each vertex.
   *
   * @param mapFunc the function applied to each edge adjacent to each vertex. The mapFunc can
   * optionally return `None`, in which case it does not contribute to the final sum.
   * @param reduceFunc the function used to merge the results of each map operation
   * @param direction the direction of edges to consider (e.g., In, Out, Both).
   * @tparam A the aggregation type
   *
   * @return an RDD containing tuples of vertex identifiers and
   * their resulting value. Vertices with no neighbors will not appear in the RDD.
   *
   * @example We can use this function to compute the average follower
   * age for each user:
   *
   * {{{
   * val graph: Graph[Int,Int] = GraphLoader.edgeListFile(sc, "webgraph")
   * val averageFollowerAge: RDD[(Int, Int)] =
   *   graph.aggregateNeighbors[(Int,Double)](
   *     (vid, edge) => Some((edge.otherVertex(vid).data, 1)),
   *     (a, b) => (a._1 + b._1, a._2 + b._2),
   *     -1,
   *     EdgeDirection.In)
   *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
   * }}}
   */
  def aggregateNeighbors[A: ClassTag](
      mapFunc: (VertexID, EdgeTriplet[VD, ED]) => Option[A],
      reduceFunc: (A, A) => A,
      dir: EdgeDirection)
    : VertexRDD[A] = {
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
        case (None, None) => Iterator.empty
        case (Some(srcA),None) => Iterator((et.srcId, srcA))
        case (None, Some(dstA)) => Iterator((et.dstId, dstA))
        case (Some(srcA), Some(dstA)) => Iterator((et.srcId, srcA), (et.dstId, dstA))
      }
    }
    graph.mapReduceTriplets(mf, reduceFunc)
  } // end of aggregateNeighbors

  /**
   * Collect the neighbor vertex ids for each vertex.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the set of neighboring ids for each vertex
   */
  def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexID]] = {
    val nbrs =
      if (edgeDirection == EdgeDirection.Both) {
        graph.mapReduceTriplets[Array[VertexID]](
          mapFunc = et => Iterator((et.srcId, Array(et.dstId)), (et.dstId, Array(et.srcId))),
          reduceFunc = _ ++ _
        )
      } else if (edgeDirection == EdgeDirection.Out) {
        graph.mapReduceTriplets[Array[VertexID]](
          mapFunc = et => Iterator((et.srcId, Array(et.dstId))),
          reduceFunc = _ ++ _)
      } else if (edgeDirection == EdgeDirection.In) {
        graph.mapReduceTriplets[Array[VertexID]](
          mapFunc = et => Iterator((et.dstId, Array(et.srcId))),
          reduceFunc = _ ++ _)
      } else {
        throw new SparkException("It doesn't make sense to collect neighbor ids without a direction.")
      }
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[VertexID])
    }
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
   * @return the vertex set of neighboring vertex attributes for each vertex
   */
  def collectNeighbors(edgeDirection: EdgeDirection) :
    VertexRDD[ Array[(VertexID, VD)] ] = {
    val nbrs = graph.aggregateNeighbors[Array[(VertexID,VD)]](
      (vid, edge) =>
        Some(Array( (edge.otherVertexId(vid), edge.otherVertexAttr(vid)) )),
      (a, b) => a ++ b,
      edgeDirection)

    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[(VertexID, VD)])
    }
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
   * @example This function is used to update the vertices with new
   * values based on external data.  For example we could add the out
   * degree to each vertex record
   *
   * {{{
   * val rawGraph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "webgraph")
   *   .mapVertices(v => 0)
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg )
   * }}}
   *
   */
  def joinVertices[U: ClassTag](table: RDD[(VertexID, U)])(mapFunc: (VertexID, VD, U) => VD)
    : Graph[VD, ED] = {
    val uf = (id: VertexID, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    graph.outerJoinVertices(table)(uf)
  }

  /**
   * Filter the graph by computing some values to filter on, and applying the predicates.
   *
   * @param preprocess a function to compute new vertex and edge data before filtering
   * @param epred edge pred to filter on after preprocess, see more details under
   *  [[org.apache.spark.graphx.Graph#subgraph]]
   * @param vpred vertex pred to filter on after prerocess, see more details under
   *  [[org.apache.spark.graphx.Graph#subgraph]]
   * @tparam VD2 vertex type the vpred operates on
   * @tparam ED2 edge type the epred operates on
   * @return a subgraph of the orginal graph, with its data unchanged
   *
   * @example This function can be used to filter the graph based on some property, without
   * changing the vertex and edge values in your program. For example, we could remove the vertices
   * in a graph with 0 outdegree
   *
   * {{{
   * graph.filter(
   *   graph => {
   *     val degrees: VertexRDD[Int] = graph.outDegrees
   *     graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
   *   },
   *   vpred = (vid: VertexID, deg:Int) => deg > 0
   * )
   * }}}
   *
   */
  def filter[VD2: ClassTag, ED2: ClassTag](
      preprocess: Graph[VD, ED] => Graph[VD2, ED2],
      epred: (EdgeTriplet[VD2, ED2]) => Boolean = (x: EdgeTriplet[VD2, ED2]) => true,
      vpred: (VertexID, VD2) => Boolean = (v:VertexID, d:VD2) => true): Graph[VD, ED] = {
    graph.mask(preprocess(graph).subgraph(epred, vpred))
  }

  /**
   * Execute a Pregel-like iterative vertex-parallel abstraction.  The
   * user-defined vertex-program `vprog` is executed in parallel on
   * each vertex receiving any inbound messages and computing a new
   * value for the vertex.  The `sendMsg` function is then invoked on
   * all out-edges and is used to compute an optional message to the
   * destination vertex. The `mergeMsg` function is a commutative
   * associative function used to combine messages destined to the
   * same vertex.
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param graph the input graph.
   *
   * @param initialMsg the message each vertex will receive at the on
   * the first iteration
   *
   * @param maxIterations the maximum number of iterations to run for
   *
   * @param activeDirection the direction of edges incident to a vertex that received a message in
   * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
   * out-edges of vertices that received a message in the previous round will run.
   *
   * @param vprog the user-defined vertex program which runs on each
   * vertex and receives the inbound message and computes a new vertex
   * value.  On the first iteration the vertex program is invoked on
   * all vertices and is passed the default message.  On subsequent
   * iterations the vertex program is only invoked on those vertices
   * that receive messages.
   *
   * @param sendMsg a user supplied function that is applied to out
   * edges of vertices that received messages in the current
   * iteration
   *
   * @param mergeMsg a user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type
   * A.  ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def pregel[A: ClassTag](
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Out)(
      vprog: (VertexID, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexID,A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {
    Pregel(graph, initialMsg, maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank]], method `runUntilConvergence`.
   */
  def pageRank(tol: Double, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.runUntilConvergence(graph, tol, resetProb)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank]], method `run`.
   */
  def staticPageRank(numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.run(graph, numIter, resetProb)
  }

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.ConnectedComponents]]
   */
  def connectedComponents(): Graph[VertexID, ED] = {
    ConnectedComponents.run(graph)
  }

  /**
   * Compute the number of triangles passing through each vertex.
   *
   * @see [[org.apache.spark.graphx.lib.TriangleCount]]
   */
  def triangleCount(): Graph[Int, ED] = {
    TriangleCount.run(graph)
  }

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.StronglyConnectedComponents]]
   */
  def stronglyConnectedComponents(numIter: Int): Graph[VertexID, ED] = {
    StronglyConnectedComponents.run(graph, numIter)
  }
} // end of GraphOps
