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

package org.apache.spark.graphx

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkException
import org.apache.spark.graphx.lib._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Contains additional functionality for [[Graph]]. All operations are expressed in terms of the
 * efficient GraphX API. This class is implicitly constructed for each Graph object.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
class GraphOps[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  /** The number of edges in the graph. */
  @transient lazy val numEdges: Long = graph.edges.count()

  /** The number of vertices in the graph. */
  @transient lazy val numVertices: Long = graph.vertices.count()

  /**
   * The in-degree of each vertex in the graph.
   * @note Vertices with no in-edges are not returned in the resulting RDD.
   */
  @transient lazy val inDegrees: VertexRDD[Int] =
    degreesRDD(EdgeDirection.In).setName("GraphOps.inDegrees")

  /**
   * The out-degree of each vertex in the graph.
   * @note Vertices with no out-edges are not returned in the resulting RDD.
   */
  @transient lazy val outDegrees: VertexRDD[Int] =
    degreesRDD(EdgeDirection.Out).setName("GraphOps.outDegrees")

  /**
   * The degree of each vertex in the graph.
   * @note Vertices with no edges are not returned in the resulting RDD.
   */
  @transient lazy val degrees: VertexRDD[Int] =
    degreesRDD(EdgeDirection.Either).setName("GraphOps.degrees")

  /**
   * Computes the neighboring vertex degrees.
   *
   * @param edgeDirection the direction along which to collect neighboring vertex attributes
   */
  private def degreesRDD(edgeDirection: EdgeDirection): VertexRDD[Int] = {
    if (edgeDirection == EdgeDirection.In) {
      graph.aggregateMessages(_.sendToDst(1), _ + _, TripletFields.None)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.aggregateMessages(_.sendToSrc(1), _ + _, TripletFields.None)
    } else { // EdgeDirection.Either
      graph.aggregateMessages(ctx => { ctx.sendToSrc(1); ctx.sendToDst(1) }, _ + _,
        TripletFields.None)
    }
  }

  /**
   * Collect the neighbor vertex ids for each vertex.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the set of neighboring ids for each vertex
   */
  def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexId]] = {
    val nbrs =
      if (edgeDirection == EdgeDirection.Either) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => { ctx.sendToSrc(Array(ctx.dstId)); ctx.sendToDst(Array(ctx.srcId)) },
          _ ++ _, TripletFields.None)
      } else if (edgeDirection == EdgeDirection.Out) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => ctx.sendToSrc(Array(ctx.dstId)),
          _ ++ _, TripletFields.None)
      } else if (edgeDirection == EdgeDirection.In) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => ctx.sendToDst(Array(ctx.srcId)),
          _ ++ _, TripletFields.None)
      } else {
        throw new SparkException("It doesn't make sense to collect neighbor ids without a " +
          "direction. (EdgeDirection.Both is not supported; use EdgeDirection.Either instead.)")
      }
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[VertexId])
    }
  } // end of collectNeighborIds

  /**
   * Collect the neighbor vertex attributes for each vertex.
   *
   * @note This function could be highly inefficient on power-law
   * graphs where high degree vertices may force a large amount of
   * information to be collected to a single location.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the vertex set of neighboring vertex attributes for each vertex
   */
  def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]] = {
    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Array[(VertexId, VD)]](
          ctx => {
            ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr)))
            ctx.sendToDst(Array((ctx.srcId, ctx.srcAttr)))
          },
          (a, b) => a ++ b, TripletFields.All)
      case EdgeDirection.In =>
        graph.aggregateMessages[Array[(VertexId, VD)]](
          ctx => ctx.sendToDst(Array((ctx.srcId, ctx.srcAttr))),
          (a, b) => a ++ b, TripletFields.Src)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Array[(VertexId, VD)]](
          ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr))),
          (a, b) => a ++ b, TripletFields.Dst)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
    graph.vertices.leftJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[(VertexId, VD)])
    }
  } // end of collectNeighbor

  /**
   * Returns an RDD that contains for each vertex v its local edges,
   * i.e., the edges that are incident on v, in the user-specified direction.
   * Warning: note that singleton vertices, those with no edges in the given
   * direction will not be part of the return value.
   *
   * @note This function could be highly inefficient on power-law
   * graphs where high degree vertices may force a large amount of
   * information to be collected to a single location.
   *
   * @param edgeDirection the direction along which to collect
   * the local edges of vertices
   *
   * @return the local edges for each vertex
   */
  def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]] = {
    edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Array[Edge[ED]]](
          ctx => {
            ctx.sendToSrc(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr)))
            ctx.sendToDst(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr)))
          },
          (a, b) => a ++ b, TripletFields.EdgeOnly)
      case EdgeDirection.In =>
        graph.aggregateMessages[Array[Edge[ED]]](
          ctx => ctx.sendToDst(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr))),
          (a, b) => a ++ b, TripletFields.EdgeOnly)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Array[Edge[ED]]](
          ctx => ctx.sendToSrc(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr))),
          (a, b) => a ++ b, TripletFields.EdgeOnly)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
  }

  /**
   * Remove self edges.
   *
   * @return a graph with all self edges removed
   */
  def removeSelfEdges(): Graph[VD, ED] = {
    graph.subgraph(epred = e => e.srcId != e.dstId)
  }

  /**
   * Join the vertices with an RDD and then apply a function from the
   * vertex and RDD entry to a new vertex value.  The input table
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
   *   .mapVertices((_, _) => 0)
   * val outDeg = rawGraph.outDegrees
   * val graph = rawGraph.joinVertices[Int](outDeg)
   *   ((_, _, outDeg) => outDeg)
   * }}}
   *
   */
  def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD)
    : Graph[VD, ED] = {
    val uf = (id: VertexId, data: VD, o: Option[U]) => {
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
   * @param vpred vertex pred to filter on after preprocess, see more details under
   *  [[org.apache.spark.graphx.Graph#subgraph]]
   * @tparam VD2 vertex type the vpred operates on
   * @tparam ED2 edge type the epred operates on
   * @return a subgraph of the original graph, with its data unchanged
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
   *   vpred = (vid: VertexId, deg:Int) => deg > 0
   * )
   * }}}
   *
   */
  def filter[VD2: ClassTag, ED2: ClassTag](
      preprocess: Graph[VD, ED] => Graph[VD2, ED2],
      epred: (EdgeTriplet[VD2, ED2]) => Boolean = (x: EdgeTriplet[VD2, ED2]) => true,
      vpred: (VertexId, VD2) => Boolean = (v: VertexId, d: VD2) => true): Graph[VD, ED] = {
    graph.mask(preprocess(graph).subgraph(epred, vpred))
  }

  /**
   * Picks a random vertex from the graph and returns its ID.
   */
  def pickRandomVertex(): VertexId = {
    val probability = 50.0 / graph.numVertices
    var found = false
    var retVal: VertexId = null.asInstanceOf[VertexId]
    while (!found) {
      val selectedVertices = graph.vertices.flatMap { vidVvals =>
        if (Random.nextDouble() < probability) { Some(vidVvals._1) }
        else { None }
      }
      if (selectedVertices.count() > 0) {
        found = true
        val collectedVertices = selectedVertices.collect()
        retVal = collectedVertices(Random.nextInt(collectedVertices.length))
      }
    }
   retVal
  }

  /**
   * Convert bi-directional edges into uni-directional ones.
   * Some graph algorithms (e.g., TriangleCount) assume that an input graph
   * has its edges in canonical direction.
   * This function rewrites the vertex ids of edges so that srcIds are smaller
   * than dstIds, and merges the duplicated edges.
   *
   * @param mergeFunc the user defined reduce function which should
   * be commutative and associative and is used to combine the output
   * of the map phase
   *
   * @return the resulting graph with canonical edges
   */
  def convertToCanonicalEdges(
      mergeFunc: (ED, ED) => ED = (e1, e2) => e1): Graph[VD, ED] = {
    val newEdges =
      graph.edges
        .map {
          case e if e.srcId < e.dstId => ((e.srcId, e.dstId), e.attr)
          case e => ((e.dstId, e.srcId), e.attr)
        }
        .reduceByKey(mergeFunc)
        .map(e => new Edge(e._1._1, e._1._2, e._2))
    Graph(graph.vertices, newEdges)
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
   * @tparam A the Pregel message type
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
      activeDirection: EdgeDirection = EdgeDirection.Either)(
      vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {
    Pregel(graph, initialMsg, maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#runUntilConvergence]]
   */
  def pageRank(tol: Double, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.runUntilConvergence(graph, tol, resetProb)
  }


  /**
   * Run personalized PageRank for a given vertex, such that all random walks
   * are started relative to the source node.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#runUntilConvergenceWithOptions]]
   */
  def personalizedPageRank(src: VertexId, tol: Double,
    resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.runUntilConvergenceWithOptions(graph, tol, resetProb, Some(src))
  }

  /**
   * Run parallel personalized PageRank for a given array of source vertices, such
   * that all random walks are started relative to the source vertices
   */
  def staticParallelPersonalizedPageRank(sources: Array[VertexId], numIter: Int,
    resetProb: Double = 0.15) : Graph[Vector, Double] = {
    PageRank.runParallelPersonalizedPageRank(graph, numIter, resetProb, sources)
  }

  /**
   * Run Personalized PageRank for a fixed number of iterations with
   * with all iterations originating at the source node
   * returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#runWithOptions]]
   */
  def staticPersonalizedPageRank(src: VertexId, numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.runWithOptions(graph, numIter, resetProb, Some(src))
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#run]]
   */
  def staticPageRank(numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.run(graph, numIter, resetProb)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight, optionally including
   * including a previous pageRank computation to be used as a start point for the new iterations
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#runWithOptionsWithPreviousPageRank]]
   */
  def staticPageRank(numIter: Int, resetProb: Double,
                     prePageRank: Graph[Double, Double]): Graph[Double, Double] = {
    PageRank.runWithOptionsWithPreviousPageRank(graph, numIter, resetProb, None, prePageRank)
  }

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @see `org.apache.spark.graphx.lib.ConnectedComponents.run`
   */
  def connectedComponents(): Graph[VertexId, ED] = {
    ConnectedComponents.run(graph)
  }

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @see `org.apache.spark.graphx.lib.ConnectedComponents.run`
   */
  def connectedComponents(maxIterations: Int): Graph[VertexId, ED] = {
    ConnectedComponents.run(graph, maxIterations)
  }

  /**
   * Compute the number of triangles passing through each vertex.
   *
   * @see [[org.apache.spark.graphx.lib.TriangleCount$#run]]
   */
  def triangleCount(): Graph[Int, ED] = {
    TriangleCount.run(graph)
  }

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.StronglyConnectedComponents$#run]]
   */
  def stronglyConnectedComponents(numIter: Int): Graph[VertexId, ED] = {
    StronglyConnectedComponents.run(graph, numIter)
  }
} // end of GraphOps
