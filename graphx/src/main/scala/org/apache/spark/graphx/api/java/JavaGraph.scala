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

package org.apache.spark.graphx.api.java

import java.lang.{Integer => JInt, Long => JLong, Boolean => JBoolean}
import java.util.{Iterator => JIterator}

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.google.common.base.Optional
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaRDD._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.JavaUtils
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2,
  Function3 => JFunction3, _}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class JavaGraph[VD, ED](val graph: Graph[VD, ED])(
  implicit val vdTag: ClassTag[VD], implicit val edTag: ClassTag[ED]) extends Serializable {

  /**
   * An RDD containing the vertices and their associated attributes.
   *
   * @note vertex ids are unique.
   * @return an RDD containing the vertices in this graph
   */
  @transient val vertices: JavaVertexRDD[VD] = new JavaVertexRDD[VD](graph.vertices)

  /**
   * An RDD containing the edges and their associated attributes.  The entries in the RDD contain
   * just the source id and target id along with the edge data.
   *
   * @return an RDD containing the edges in this graph
   *
   * @see [[Edge]] for the edge type.
   * @see [[triplets]] to get an RDD which contains all the edges
   * along with their vertex data.
   *
   */
  @transient val edges: JavaEdgeRDD[ED, VD] = new JavaEdgeRDD[ED, VD](graph.edges)

  /**
   * An RDD containing the edge triplets, which are edges along with the vertex data associated with
   * the adjacent vertices. The caller should use [[edges]] if the vertex data are not needed, i.e.
   * if only the edge data and adjacent vertex ids are needed.
   *
   * @return an RDD containing edge triplets
   *
   * @example This operation might be used to evaluate a graph
   * coloring where we would like to check that both vertices are a
   * different color.
   * {{{
   * type Color = Int
   * val graph: Graph[Color, Int] = GraphLoader.edgeListFile("hdfs://file.tsv")
   * val numInvalid = graph.triplets.map(e => if (e.src.data == e.dst.data) 1 else 0).sum
   * }}}
   */
  @transient val triplets: JavaRDD[EdgeTriplet[VD, ED]] = graph.triplets

  /**
   * Caches the vertices and edges associated with this graph at the specified storage level,
   * ignoring any target storage levels previously set.
   *
   * @param newLevel the level at which to cache the graph.
   *
   * @return A reference to this graph for convenience.
   */
  def persist(newLevel: StorageLevel): JavaGraph[VD, ED] = graph.persist(newLevel)

  /**
   * Caches the vertices and edges associated with this graph at the previously-specified target
   * storage levels, which default to `MEMORY_ONLY`. This is used to pin a graph in memory enabling
   * multiple queries to reuse the same construction process.
   */
  def cache(): JavaGraph[VD, ED] = graph.cache()

  /**
   * Repartitions the edges in the graph according to `partitionStrategy`.
   *
   * @param partitionStrategy the partitioning strategy to use when partitioning the edges
   * in the graph.
   */
  def partitionBy(partitionStrategy: PartitionStrategy): JavaGraph[VD, ED] =
    graph.partitionBy(partitionStrategy)

  /**
   * Repartitions the edges in the graph according to `partitionStrategy`.
   *
   * @param partitionStrategy the partitioning strategy to use when partitioning the edges
   * in the graph.
   * @param numPartitions the number of edge partitions in the new graph.
   */
  def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): JavaGraph[VD, ED] =
    graph.partitionBy(partitionStrategy, numPartitions)

  /**
   * Transforms each vertex attribute in the graph using the map function.
   *
   * @note The new graph has the same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from a vertex object to a new vertex value
   *
   * @tparam VD2 the new vertex data type
   *
   * @example We might use this operation to change the vertex values
   * from one type to another to initialize an algorithm.
   * {{{
   * val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
   * val root = 42
   * var bfsGraph = rawGraph.mapVertices[Int]((vid, data) => if (vid == root) 0 else Math.MaxValue)
   * }}}
   *
   */
  def mapVertices[VD2](map: JFunction2[JLong, VD, VD2]): JavaGraph[VD2, ED] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    new JavaGraph(graph.mapVertices((id, attr) => map(id, attr))(fakeClassTag, eq = null))
  }

  /**
   * Transforms each edge attribute in the graph using the map function.  The map function is not
   * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
   * use `mapTriplets`.
   *
   * @note This graph is not changed and that the new graph has the
   * same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge
   * attributes.
   *
   */
  def mapEdges[ED2](map: JFunction[Edge[ED], ED2]): JavaGraph[VD, ED2] = {
    implicit val ed2Tag: ClassTag[ED2] = fakeClassTag
    new JavaGraph(graph.mapEdges(map))
  }

  /**
   * Transforms each edge attribute using the map function, passing it the adjacent vertex
   * attributes as well. If adjacent vertex values are not required,
   * consider using `mapEdges` instead.
   *
   * @note This does not change the structure of the
   * graph or modify the values of this graph.  As a consequence
   * the underlying index structures can be reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge
   * attributes based on the attributes associated with each vertex.
   * {{{
   * val rawGraph: Graph[Int, Int] = someLoadFunction()
   * val graph = rawGraph.mapTriplets[Int]( edge =>
   *   edge.src.data - edge.dst.data)
   * }}}
   *
   */
  def mapTriplets[ED2](
      map: JFunction[EdgeTriplet[VD, ED], ED2],
      tripletFields: TripletFields): JavaGraph[VD, ED2] = {
    implicit val ed2Tag: ClassTag[ED2] = fakeClassTag
    new JavaGraph(graph.mapTriplets(map, tripletFields))
  }

  /**
   * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
   * graph contains an edge from b to a.
   */
  def reverse(): JavaGraph[VD, ED] = graph.reverse

  /**
   * Restricts the graph to only the vertices and edges satisfying the predicates. The resulting
   * subgraph satisifies
   *
   * {{{
   * V' = {v : for all v in V where vpred(v)}
   * E' = {(u,v): for all (u,v) in E where epred((u,v)) && vpred(u) && vpred(v)}
   * }}}
   *
   * @param epred the edge predicate, which takes a triplet and
   * evaluates to true if the edge is to remain in the subgraph.  Note
   * that only edges where both vertices satisfy the vertex
   * predicate are considered.
   *
   * @param vpred the vertex predicate, which takes a vertex object and
   * evaluates to true if the vertex is to be included in the subgraph
   *
   * @return the subgraph containing only the vertices and edges that
   * satisfy the predicates
   */
  def subgraph(
      epred: JFunction[EdgeTriplet[VD, ED], JBoolean],
      vpred: JFunction2[JLong, VD, JBoolean]): JavaGraph[VD, ED] =
    graph.subgraph(et => epred.call(et), (id, attr) => vpred.call(id, attr))

  /**
   * Restricts the graph to only the vertices and edges that are also in `other`, but keeps the
   * attributes from this graph.
   * @param other the graph to project this graph onto
   * @return a graph with vertices and edges that exist in both the current graph and `other`,
   * with vertex and edge data from the current graph
   */
  def mask[VD2, ED2](other: JavaGraph[VD2, ED2]): JavaGraph[VD, ED] =
    graph.mask(other)(fakeClassTag, fakeClassTag)

  /**
   * Merges multiple edges between two vertices into a single edge. For correct results, the graph
   * must have been partitioned using [[partitionBy]].
   *
   * @param merge the user-supplied commutative associative function to merge edge attributes
   *              for duplicate edges.
   *
   * @return The resulting graph with a single edge for each (source, dest) vertex pair.
   */
  def groupEdges(merge: JFunction2[ED, ED, ED]): JavaGraph[VD, ED] =
    graph.groupEdges(merge)

  /**
   * Aggregates values from the neighboring edges and vertices of each vertex. The user-supplied
   * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
   * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
   * destined to the same vertex.
   *
   * @tparam A the type of message to be sent to each vertex
   *
   * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
   *   [[EdgeContext]].
   * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
   *   combiner should be commutative and associative.
   * @param tripletFields which fields should be included in the [[EdgeContext]] passed to the
   *   `sendMsg` function. If not all fields are needed, specifying this can improve performance.
   *
   * @example We can use this function to compute the in-degree of each
   * vertex
   * {{{
   * val rawGraph: Graph[_, _] = Graph.textFile("twittergraph")
   * val inDeg: RDD[(VertexId, Int)] =
   *   aggregateMessages[Int](ctx => ctx.sendToDst(1), _ + _)
   * }}}
   *
   * @note By expressing computation at the edge level we achieve
   * maximum parallelism.  This is one of the core functions in the
   * Graph API in that enables neighborhood level computation. For
   * example this function can be used to count neighbors satisfying a
   * predicate or implement PageRank.
   *
   */
  def aggregateMessages[A](
      sendMsg: VoidFunction[EdgeContext[VD, ED, A]],
      mergeMsg: JFunction2[A, A, A],
      tripletFields: TripletFields): JavaVertexRDD[A] = {
    implicit val aTag: ClassTag[A] = fakeClassTag
    new JavaVertexRDD(graph.aggregateMessages(ctx => sendMsg.call(ctx), mergeMsg, tripletFields))
  }

  /**
   * Joins the vertices with entries in the `table` RDD and merges the results using `mapFunc`.  The
   * input table should contain at most one entry for each vertex.  If no entry in `other` is
   * provided for a particular vertex in the graph, the map function receives `None`.
   *
   * @tparam U the type of entry in the table of updates
   * @tparam VD2 the new vertex value type
   *
   * @param other the table to join with the vertices in the graph.
   *              The table should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.
   *                The map function is invoked for all vertices, even those
   *                that do not have a corresponding entry in the table.
   *
   * @example This function is used to update the vertices with new values based on external data.
   *          For example we could add the out-degree to each vertex record:
   *
   * {{{
   * val rawGraph: Graph[_, _] = Graph.textFile("webgraph")
   * val outDeg: RDD[(Long, Int)] = rawGraph.outDegrees
   * val graph = rawGraph.outerJoinVertices(outDeg) {
   *   (vid, data, optDeg) => optDeg.getOrElse(0)
   * }
   * }}}
   */
  def outerJoinVertices[U, VD2](
      other: JavaPairRDD[JLong, U],
      mapFunc: JFunction3[JLong, VD, Optional[U], VD2]): JavaGraph[VD2, ED] = {
    implicit val uTag: ClassTag[U] = fakeClassTag
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag

    val scalaOther: RDD[(VertexId, U)] = other.rdd.map(kv => (kv._1, kv._2))
    new JavaGraph(graph.outerJoinVertices(scalaOther) {
      (id, a, bOpt) => mapFunc.call(id, a, JavaUtils.optionToOptional(bOpt))
    })
  }

  /** The number of edges in the graph. */
  def numEdges(): Long = graph.numEdges

  /** The number of vertices in the graph. */
  def numVertices(): Long = graph.numVertices

  /**
   * The in-degree of each vertex in the graph.
   * @note Vertices with no in-edges are not returned in the resulting RDD.
   */
  def inDegrees(): JavaVertexRDD[JInt] = graph.inDegrees.mapValues((deg: Int) => deg: JInt)

  /**
   * The out-degree of each vertex in the graph.
   * @note Vertices with no out-edges are not returned in the resulting RDD.
   */
  def outDegrees(): JavaVertexRDD[JInt] = graph.outDegrees.mapValues((deg: Int) => deg: JInt)

  /**
   * The degree of each vertex in the graph.
   * @note Vertices with no edges are not returned in the resulting RDD.
   */
  def degrees(): VertexRDD[JInt] = graph.degrees.mapValues((deg: Int) => deg: JInt)

  /**
   * Collect the neighbor vertex ids for each vertex.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the set of neighboring ids for each vertex
   */
  def collectNeighborIds(edgeDirection: EdgeDirection): JavaVertexRDD[Array[Long]] =
    graph.collectNeighborIds(edgeDirection)

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
  def collectNeighbors(edgeDirection: EdgeDirection): JavaVertexRDD[Array[(JLong, VD)]] =
    graph.collectNeighbors(edgeDirection).mapValues((nbrs: Array[(Long, VD)]) =>
      nbrs.map(kv => (kv._1: JLong, kv._2)))

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
  def collectEdges(edgeDirection: EdgeDirection): JavaVertexRDD[Array[Edge[ED]]] =
    graph.collectEdges(edgeDirection)

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
   *   .mapVertices((_, _) => 0)
   * val outDeg = rawGraph.outDegrees
   * val graph = rawGraph.joinVertices[Int](outDeg)
   *   ((_, _, outDeg) => outDeg)
   * }}}
   *
   */
  def joinVertices[U](
      table: JavaPairRDD[JLong, U],
      mapFunc: JFunction3[JLong, VD, U, VD]): JavaGraph[VD, ED] = {
    implicit val uTag: ClassTag[U] = fakeClassTag
    val scalaTable: RDD[(VertexId, U)] = table.rdd.map(kv => (kv._1, kv._2))
    graph.joinVertices(scalaTable) { (vid, a, b) => mapFunc.call(vid, a, b) }
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
   *   vpred = (vid: Long, deg:Int) => deg > 0
   * )
   * }}}
   *
   */
  def filter[VD2, ED2](
      preprocess: JFunction[JavaGraph[VD, ED], JavaGraph[VD2, ED2]],
      epred: JFunction[EdgeTriplet[VD2, ED2], JBoolean],
      vpred: JFunction2[JLong, VD2, JBoolean]): JavaGraph[VD, ED] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    implicit val ed2Tag: ClassTag[ED2] = fakeClassTag
    graph.filter(
      origGraph => preprocess.call(origGraph),
      (et: EdgeTriplet[VD2, ED2]) => epred.call(et),
      (id, attr: VD2) => vpred.call(id, attr))
  }

  /**
   * Picks a random vertex from the graph and returns its ID.
   */
  def pickRandomVertex(): Long = graph.pickRandomVertex()

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
  def pregel[A](
      initialMsg: A,
      maxIterations: Int,
      activeDirection: EdgeDirection,
      vprog: JFunction3[JLong, VD, A, VD],
      sendMsg: PairFlatMapFunction[EdgeTriplet[VD, ED], JLong, A],
      mergeMsg: JFunction2[A, A, A])
    : JavaGraph[VD, ED] = {
    implicit val aTag: ClassTag[A] = fakeClassTag

    import scala.collection.JavaConverters._
    def scalaSendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)] =
      (e: EdgeTriplet[VD, ED]) => sendMsg.call(e).asScala.iterator.map(kv => (kv._1, kv._2))

    graph.pregel(initialMsg, maxIterations, activeDirection)(
      (vid, a, b) => vprog.call(vid, a, b), scalaSendMsg, mergeMsg)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#runUntilConvergence]]
   */
  def pageRank(tol: Double, resetProb: Double): JavaGraph[Double, Double] =
    graph.pageRank(tol, resetProb)

  /**
   * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank$#run]]
   */
  def staticPageRank(numIter: Int, resetProb: Double): JavaGraph[Double, Double] =
    graph.staticPageRank(numIter, resetProb)

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.ConnectedComponents$#run]]
   */
  def connectedComponents(): JavaGraph[JLong, ED] =
    graph.connectedComponents().mapVertices((id, cc) => (cc: JLong))

  /**
   * Compute the number of triangles passing through each vertex.
   *
   * @see [[org.apache.spark.graphx.lib.TriangleCount$#run]]
   */
  def triangleCount(): JavaGraph[JInt, ED] =
    graph.triangleCount().mapVertices((id, numTriangles) => (numTriangles: JInt))

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.StronglyConnectedComponents$#run]]
   */
  def stronglyConnectedComponents(numIter: Int): JavaGraph[JLong, ED] =
    graph.stronglyConnectedComponents(numIter).mapVertices((id, scc) => (scc: JLong))
}

object JavaGraph {

  /**
   * Construct a graph from a collection of edges encoded as vertex id pairs.
   *
   * @param rawEdges a collection of edges in (src, dst) form
   * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
   *
   * @return a graph with all edge attributes set to 1, all vertex attributes set to
   * `defaultValue`, and target storage level set to `MEMORY_ONLY`.
   */
  def fromEdgeTuples[VD](
      rawEdges: JavaPairRDD[JLong, JLong],
      defaultValue: VD): JavaGraph[VD, JInt] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    val scalaRawEdges: RDD[(VertexId, VertexId)] = rawEdges.rdd.map(kv => (kv._1, kv._2))
    Graph.fromEdgeTuples(scalaRawEdges, defaultValue, uniqueEdges = None)
      .mapEdges((e: Edge[Int]) => (e.attr: JInt))
  }

  /**
   * Construct a graph from a collection of edges encoded as vertex id pairs.
   *
   * @param rawEdges a collection of edges in (src, dst) form
   * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
   * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
   * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
   *
   * @return a graph with all edge attributes set to 1 and all vertex attributes set to
   * `defaultValue`.
   */
  def fromEdgeTuples[VD](
      rawEdges: JavaPairRDD[JLong, JLong],
      defaultValue: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): JavaGraph[VD, JInt] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    val scalaRawEdges: RDD[(VertexId, VertexId)] = rawEdges.rdd.map(kv => (kv._1, kv._2))
    Graph.fromEdgeTuples(scalaRawEdges, defaultValue, uniqueEdges = None,
      edgeStorageLevel = edgeStorageLevel, vertexStorageLevel = vertexStorageLevel)
      .mapEdges((e: Edge[Int]) => (e.attr: JInt))
  }

  /**
   * Construct a graph from a collection of edges encoded as vertex id pairs, repartitioning edges
   * and merging duplicate edges.
   *
   * @param rawEdges a collection of edges in (src, dst) form
   * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
   * @param partitionStrategy how to repartition the edges. In addition to repartitioning, if
   * multiple identical edges are found they are combined and the edge attribute is set to the
   * number of merged edges
   * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
   * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
   *
   * @return a graph with edge attributes containing the count of duplicate edges and all vertex
   * attributes set to `defaultValue`.
   */
  def fromEdgeTuples[VD](
      rawEdges: JavaPairRDD[JLong, JLong],
      defaultValue: VD,
      partitionStrategy: PartitionStrategy,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): JavaGraph[VD, JInt] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    val scalaRawEdges: RDD[(VertexId, VertexId)] = rawEdges.rdd.map(kv => (kv._1, kv._2))
    Graph.fromEdgeTuples(scalaRawEdges, defaultValue, uniqueEdges = Some(partitionStrategy),
      edgeStorageLevel = edgeStorageLevel, vertexStorageLevel = vertexStorageLevel)
      .mapEdges((e: Edge[Int]) => (e.attr: JInt))
  }

  /**
   * Construct a graph from a collection of edges.
   *
   * @param edges the RDD containing the set of edges in the graph
   * @param defaultValue the default vertex attribute to use for each vertex
   *
   * @return a graph with edge attributes described by `edges`, vertices
   *         given by all vertices in `edges` with value `defaultValue`, and target storage level
   *         set to `MEMORY_ONLY`.
   */
  def fromEdges[VD, ED](
      edges: JavaRDD[Edge[ED]],
      defaultValue: VD): JavaGraph[VD, ED] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    implicit val edTag: ClassTag[ED] = fakeClassTag
    Graph.fromEdges(edges, defaultValue)
  }

  /**
   * Construct a graph from a collection of edges.
   *
   * @param edges the RDD containing the set of edges in the graph
   * @param defaultValue the default vertex attribute to use for each vertex
   * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
   * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
   *
   * @return a graph with edge attributes described by `edges` and vertices
   *         given by all vertices in `edges` with value `defaultValue`
   */
  def fromEdges[VD, ED](
      edges: JavaRDD[Edge[ED]],
      defaultValue: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): JavaGraph[VD, ED] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    implicit val edTag: ClassTag[ED] = fakeClassTag
    Graph.fromEdges(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
  }

  /**
   * Construct a graph from a collection of vertices and
   * edges with attributes.  Duplicate vertices are picked arbitrarily and
   * vertices found in the edge collection but not in the input
   * vertices are assigned the default attribute.
   *
   * @tparam VD the vertex attribute type
   * @tparam ED the edge attribute type
   * @param vertices the "set" of vertices and their attributes
   * @param edges the collection of edges in the graph
   * @param defaultVertexAttr the default vertex attribute to use for vertices that are
   *                          mentioned in edges but not in vertices
   */
  def create[VD, ED](
      vertices: JavaPairRDD[JLong, VD],
      edges: JavaRDD[Edge[ED]],
      defaultVertexAttr: VD): JavaGraph[VD, ED] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    implicit val edTag: ClassTag[ED] = fakeClassTag
    val scalaVertices: RDD[(VertexId, VD)] = vertices.rdd.map(kv => (kv._1, kv._2))
    Graph(scalaVertices, edges, defaultVertexAttr)
  }

  /**
   * Construct a graph from a collection of vertices and
   * edges with attributes.  Duplicate vertices are picked arbitrarily and
   * vertices found in the edge collection but not in the input
   * vertices are assigned the default attribute.
   *
   * @tparam VD the vertex attribute type
   * @tparam ED the edge attribute type
   * @param vertices the "set" of vertices and their attributes
   * @param edges the collection of edges in the graph
   * @param defaultVertexAttr the default vertex attribute to use for vertices that are
   *                          mentioned in edges but not in vertices
   * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
   * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
   */
  def create[VD, ED](
      vertices: JavaPairRDD[JLong, VD],
      edges: JavaRDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): JavaGraph[VD, ED] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    implicit val edTag: ClassTag[ED] = fakeClassTag
    val scalaVertices: RDD[(VertexId, VD)] = vertices.rdd.map(kv => (kv._1, kv._2))
    Graph(scalaVertices, edges, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  implicit def fromGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): JavaGraph[VD, ED] =
    new JavaGraph[VD, ED](graph)

  implicit def toGraph[VD, ED](graph: JavaGraph[VD, ED]): Graph[VD, ED] =
    graph.graph
}
