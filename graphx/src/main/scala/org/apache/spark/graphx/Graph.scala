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

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.graphx.impl._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * The Graph abstractly represents a graph with arbitrary objects
 * associated with vertices and edges.  The graph provides basic
 * operations to access and manipulate the data associated with
 * vertices and edges as well as the underlying structure.  Like Spark
 * RDDs, the graph is a functional data-structure in which mutating
 * operations return new graphs.
 *
 * @note [[GraphOps]] contains additional convenience operations and graph algorithms.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
abstract class Graph[VD: ClassTag, ED: ClassTag] protected () extends Serializable {

  /**
   * An RDD containing the vertices and their associated attributes.
   *
   * @note vertex ids are unique.
   * @return an RDD containing the vertices in this graph
   */
  val vertices: VertexRDD[VD]

  /**
   * An RDD containing the edges and their associated attributes.  The entries in the RDD contain
   * just the source id and target id along with the edge data.
   *
   * @return an RDD containing the edges in this graph
   *
   * @see `Edge` for the edge type.
   * @see `Graph#triplets` to get an RDD which contains all the edges
   * along with their vertex data.
   *
   */
  val edges: EdgeRDD[ED]

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
  val triplets: RDD[EdgeTriplet[VD, ED]]

  /**
   * Caches the vertices and edges associated with this graph at the specified storage level,
   * ignoring any target storage levels previously set.
   *
   * @param newLevel the level at which to cache the graph.
   *
   * @return A reference to this graph for convenience.
   */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]

  /**
   * Caches the vertices and edges associated with this graph at the previously-specified target
   * storage levels, which default to `MEMORY_ONLY`. This is used to pin a graph in memory enabling
   * multiple queries to reuse the same construction process.
   */
  def cache(): Graph[VD, ED]

  /**
   * Mark this Graph for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with SparkContext.setCheckpointDir() and all references to its parent
   * RDDs will be removed. It is strongly recommended that this Graph is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def checkpoint(): Unit

  /**
   * Return whether this Graph has been checkpointed or not.
   * This returns true iff both the vertices RDD and edges RDD have been checkpointed.
   */
  def isCheckpointed: Boolean

  /**
   * Gets the name of the files to which this Graph was checkpointed.
   * (The vertices RDD and edges RDD are checkpointed separately.)
   */
  def getCheckpointFiles: Seq[String]

  /**
   * Uncaches both vertices and edges of this graph. This is useful in iterative algorithms that
   * build a new graph in each iteration.
   */
  def unpersist(blocking: Boolean = true): Graph[VD, ED]

  /**
   * Uncaches only the vertices of this graph, leaving the edges alone. This is useful in iterative
   * algorithms that modify the vertex attributes but reuse the edges. This method can be used to
   * uncache the vertex attributes of previous iterations once they are no longer needed, improving
   * GC performance.
   */
  def unpersistVertices(blocking: Boolean = true): Graph[VD, ED]

  /**
   * Repartitions the edges in the graph according to `partitionStrategy`.
   *
   * @param partitionStrategy the partitioning strategy to use when partitioning the edges
   * in the graph.
   */
  def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED]

  /**
   * Repartitions the edges in the graph according to `partitionStrategy`.
   *
   * @param partitionStrategy the partitioning strategy to use when partitioning the edges
   * in the graph.
   * @param numPartitions the number of edge partitions in the new graph.
   */
  def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED]

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
  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): Graph[VD2, ED]

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
  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2] = {
    mapEdges((pid, iter) => iter.map(map))
  }

  /**
   * Transforms each edge attribute using the map function, passing it a whole partition at a
   * time. The map function is given an iterator over edges within a logical partition as well as
   * the partition's ID, and it should return a new iterator over the new values of each edge. The
   * new iterator's elements must correspond one-to-one with the old iterator's elements. If
   * adjacent vertex values are desired, use `mapTriplets`.
   *
   * @note This does not change the structure of the
   * graph or modify the values of this graph.  As a consequence
   * the underlying index structures can be reused.
   *
   * @param map a function that takes a partition id and an iterator
   * over all the edges in the partition, and must return an iterator over
   * the new values for each edge in the order of the input iterator
   *
   * @tparam ED2 the new edge data type
   *
   */
  def mapEdges[ED2: ClassTag](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])
    : Graph[VD, ED2]

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
  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] = {
    mapTriplets((pid, iter) => iter.map(map), TripletFields.All)
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
   * @param tripletFields which fields should be included in the edge triplet passed to the map
   *   function. If not all fields are needed, specifying this can improve performance.
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
  def mapTriplets[ED2: ClassTag](
      map: EdgeTriplet[VD, ED] => ED2,
      tripletFields: TripletFields): Graph[VD, ED2] = {
    mapTriplets((pid, iter) => iter.map(map), tripletFields)
  }

  /**
   * Transforms each edge attribute a partition at a time using the map function, passing it the
   * adjacent vertex attributes as well. The map function is given an iterator over edge triplets
   * within a logical partition and should yield a new iterator over the new values of each edge in
   * the order in which they are provided.  If adjacent vertex values are not required, consider
   * using `mapEdges` instead.
   *
   * @note This does not change the structure of the
   * graph or modify the values of this graph.  As a consequence
   * the underlying index structures can be reused.
   *
   * @param map the iterator transform
   * @param tripletFields which fields should be included in the edge triplet passed to the map
   *   function. If not all fields are needed, specifying this can improve performance.
   *
   * @tparam ED2 the new edge data type
   *
   */
  def mapTriplets[ED2: ClassTag](
      map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields): Graph[VD, ED2]

  /**
   * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
   * graph contains an edge from b to a.
   */
  def reverse: Graph[VD, ED]

  /**
   * Restricts the graph to only the vertices and edges satisfying the predicates. The resulting
   * subgraph satisfies
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
      epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
      vpred: (VertexId, VD) => Boolean = ((v, d) => true))
    : Graph[VD, ED]

  /**
   * Restricts the graph to only the vertices and edges that are also in `other`, but keeps the
   * attributes from this graph.
   * @param other the graph to project this graph onto
   * @return a graph with vertices and edges that exist in both the current graph and `other`,
   * with vertex and edge data from the current graph
   */
  def mask[VD2: ClassTag, ED2: ClassTag](other: Graph[VD2, ED2]): Graph[VD, ED]

  /**
   * Merges multiple edges between two vertices into a single edge. For correct results, the graph
   * must have been partitioned using [[partitionBy]].
   *
   * @param merge the user-supplied commutative associative function to merge edge attributes
   *              for duplicate edges.
   *
   * @return The resulting graph with a single edge for each (source, dest) vertex pair.
   */
  def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]

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
   *   rawGraph.aggregateMessages[Int](ctx => ctx.sendToDst(1), _ + _)
   * }}}
   *
   * @note By expressing computation at the edge level we achieve
   * maximum parallelism.  This is one of the core functions in the
   * Graph API that enables neighborhood level computation. For
   * example this function can be used to count neighbors satisfying a
   * predicate or implement PageRank.
   *
   */
  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
    : VertexRDD[A] = {
    aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
  }

  /**
   * Aggregates values from the neighboring edges and vertices of each vertex. The user-supplied
   * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
   * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
   * destined to the same vertex.
   *
   * This variant can take an active set to restrict the computation and is intended for internal
   * use only.
   *
   * @tparam A the type of message to be sent to each vertex
   *
   * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
   *   [[EdgeContext]].
   * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
   *   combiner should be commutative and associative.
   * @param tripletFields which fields should be included in the [[EdgeContext]] passed to the
   *   `sendMsg` function. If not all fields are needed, specifying this can improve performance.
   * @param activeSetOpt an efficient way to run the aggregation on a subset of the edges if
   *   desired. This is done by specifying a set of "active" vertices and an edge direction. The
   *   `sendMsg` function will then run on only edges connected to active vertices by edges in the
   *   specified direction. If the direction is `In`, `sendMsg` will only be run on edges with
   *   destination in the active set. If the direction is `Out`, `sendMsg` will only be run on edges
   *   originating from vertices in the active set. If the direction is `Either`, `sendMsg` will be
   *   run on edges with *either* vertex in the active set. If the direction is `Both`, `sendMsg`
   *   will be run on edges with *both* vertices in the active set. The active set must have the
   *   same index as the graph's vertices.
   */
  private[graphx] def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)])
    : VertexRDD[A]

  /**
   * Joins the vertices with entries in the `table` RDD and merges the results using `mapFunc`.
   * The input table should contain at most one entry for each vertex.  If no entry in `other` is
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
   * val outDeg: RDD[(VertexId, Int)] = rawGraph.outDegrees
   * val graph = rawGraph.outerJoinVertices(outDeg) {
   *   (vid, data, optDeg) => optDeg.getOrElse(0)
   * }
   * }}}
   */
  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
    : Graph[VD2, ED]

  /**
   * The associated [[GraphOps]] object.
   */
  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
} // end of Graph


/**
 * The Graph object contains a collection of routines used to construct graphs from RDDs.
 */
object Graph {

  /**
   * Construct a graph from a collection of edges encoded as vertex id pairs.
   *
   * @param rawEdges a collection of edges in (src, dst) form
   * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
   * @param uniqueEdges if multiple identical edges are found they are combined and the edge
   * attribute is set to the sum.  Otherwise duplicate edges are treated as separate. To enable
   * `uniqueEdges`, a [[PartitionStrategy]] must be provided.
   * @param edgeStorageLevel the desired storage level at which to cache the edges if necessary
   * @param vertexStorageLevel the desired storage level at which to cache the vertices if necessary
   *
   * @return a graph with edge attributes containing either the count of duplicate edges or 1
   * (if `uniqueEdges` is `None`) and vertex attributes containing the total degree of each vertex.
   */
  def fromEdgeTuples[VD: ClassTag](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: VD,
      uniqueEdges: Option[PartitionStrategy] = None,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int] =
  {
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
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
  def fromEdges[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
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
  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD = null.asInstanceOf[VD],
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] = {
    GraphImpl(vertices, edges, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
  }

  /**
   * Implicitly extracts the [[GraphOps]] member from a graph.
   *
   * To improve modularity the Graph type only contains a small set of basic operations.
   * All the convenience operations are defined in the [[GraphOps]] class which may be
   * shared across multiple graph implementations.
   */
  implicit def graphToGraphOps[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphOps[VD, ED] = g.ops
} // end of Graph object
