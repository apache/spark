package org.apache.spark.graph


import org.apache.spark.rdd.RDD
import org.apache.spark.util.ClosureCleaner



/**
 * The Graph abstractly represents a graph with arbitrary objects associated
 * with vertices and edges.  The graph provides basic operations to access and
 * manipulate the data associated with vertices and edges as well as the
 * underlying structure.  Like Spark RDDs, the graph is a functional
 * data-structure in which mutating operations return new graphs.
 *
 * @tparam VD The type of object associated with each vertex.
 *
 * @tparam ED The type of object associated with each edge
 */
abstract class Graph[VD: ClassManifest, ED: ClassManifest] {


  def replication: Double

  def balance: Array[Int]


  /**
   * Get the vertices and their data.
   *
   * @return An RDD containing the vertices in this graph
   *
   * @see Vertex for the vertex type.
   *
   * @todo should vertices return tuples instead of vertex objects?
   */
  def vertices: RDD[(Vid,VD)]

  /**
   * Get the Edges and their data as an RDD.  The entries in the RDD contain
   * just the source id and target id along with the edge data.
   *
   * @return An RDD containing the edges in this graph
   *
   * @see Edge for the edge type.
   * @see edgesWithVertices to get an RDD which contains all the edges along
   * with their vertex data.
   *
   * @todo Should edges return 3 tuples instead of Edge objects?  In this case
   * we could rename EdgeTriplet to Edge?
   */
  def edges: RDD[Edge[ED]]

  /**
   * Get the edges with the vertex data associated with the adjacent pair of
   * vertices.
   *
   * @return An RDD containing edge triplets.
   *
   * @example This operation might be used to evaluate a graph coloring where
   * we would like to check that both vertices are a different color.
   * {{{
   * type Color = Int
   * val graph: Graph[Color, Int] = Graph.textFile("hdfs://file.tsv")
   * val numInvalid = graph.edgesWithVertices()
   *   .map(e => if(e.src.data == e.dst.data) 1 else 0).sum
   * }}}
   *
   * @see edges() If only the edge data and adjacent vertex ids are required.
   *
   */
  def triplets: RDD[EdgeTriplet[VD, ED]]

  /**
   * Return a graph that is cached when first created. This is used to pin a
   * graph in memory enabling multiple queries to reuse the same construction
   * process.
   *
   * @see RDD.cache() for a more detailed explanation of caching.
   */
  def cache(): Graph[VD, ED]

  /**
   * Construct a new graph where each vertex value has been transformed by the
   * map function.
   *
   * @note This graph is not changed and that the new graph has the same
   * structure.  As a consequence the underlying index structures can be
   * reused.
   *
   * @param map the function from a vertex object to a new vertex value.
   *
   * @tparam VD2 the new vertex data type
   *
   * @example We might use this operation to change the vertex values from one
   * type to another to initialize an algorithm.
   * {{{
   * val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
   * val root = 42
   * var bfsGraph = rawGraph
   *   .mapVertices[Int](v => if(v.id == 0) 0 else Math.MaxValue)
   * }}}
   *
   */
  def mapVertices[VD2: ClassManifest](map: (Vid, VD) => VD2): Graph[VD2, ED]

  /**
   * Construct a new graph where each the value of each edge is transformed by
   * the map operation.  This function is not passed the vertex value for the
   * vertices adjacent to the edge.  If vertex values are desired use the
   * mapEdgesWithVertices function.
   *
   * @note This graph is not changed and that the new graph has the same
   * structure.  As a consequence the underlying index structures can be
   * reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge attributes.
   *
   */
  def mapEdges[ED2: ClassManifest](map: Edge[ED] => ED2): Graph[VD, ED2]

  /**
   * Construct a new graph where each the value of each edge is transformed by
   * the map operation.  This function passes vertex values for the adjacent
   * vertices to the map function.  If adjacent vertex values are not required,
   * consider using the mapEdges function instead.
   *
   * @note This graph is not changed and that the new graph has the same
   * structure.  As a consequence the underlying index structures can be
   * reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge attributes based
   * on the attributes associated with each vertex.
   * {{{
   * val rawGraph: Graph[Int, Int] = someLoadFunction()
   * val graph = rawGraph.mapEdgesWithVertices[Int]( edge =>
   *   edge.src.data - edge.dst.data)
   * }}}
   *
   */
  def mapTriplets[ED2: ClassManifest](
    map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]


  // /**
  //  * Remove edges conntecting vertices that are not in the graph.
  //  *
  //  * @todo remove this function and ensure that for a graph G=(V,E):
  //  *     if (u,v) in E then u in V and v in V 
  //  */
  // def correctEdges(): Graph[VD, ED]

  /**
   * Construct a new graph with all the edges reversed.  If this graph contains
   * an edge from a to b then the returned graph contains an edge from b to a.
   *
   */
  def reverse: Graph[VD, ED]


  /**
   * This function takes a vertex and edge predicate and constructs the subgraph
   * that consists of vertices and edges that satisfy the predict.  The resulting 
   * graph contains the vertices and edges that satisfy:
   *
   * V' = {v : for all v in V where vpred(v)}
   * E' = {(u,v): for all (u,v) in E where epred((u,v)) && vpred(u) && vpred(v)}
   *
   * @param epred the edge predicate which takes a triplet and evaluates to true
   * if the edge is to remain in the subgraph.  Note that only edges in which both 
   * vertices satisfy the vertex predicate are considered.
   *
   * @param vpred the vertex predicate which takes a vertex object and evaluates
   * to true if the vertex is to be included in the subgraph
   *
   * @return the subgraph containing only the vertices and edges that satisfy the
   * predicates. 
   */
  def subgraph(epred: EdgeTriplet[VD,ED] => Boolean = (x => true), 
    vpred: (Vid, VD) => Boolean = ((v,d) => true) ): Graph[VD, ED]


  // /**
  //  * Combine the attrributes of edges connecting the same vertices.   
  //  *
  //  * @todo Do we want to support this function
  //  */
  // def combineEdges(reduce: (ED, ED) => ED): Graph[VD, ED]

  def groupEdgeTriplets[ED2: ClassManifest](f: Iterator[EdgeTriplet[VD,ED]] => ED2 ): Graph[VD,ED2]

  def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ): Graph[VD,ED2]



  def mapReduceTriplets[A: ClassManifest](
      mapFunc: EdgeTriplet[VD, ED] => Array[(Vid, A)],
      reduceFunc: (A, A) => A)
    : RDD[(Vid, A)] 


  // /**
  //  * This function is used to compute a statistic for the neighborhood of each
  //  * vertex.
  //  *
  //  * This is one of the core functions in the Graph API in that enables
  //  * neighborhood level computation.  For example this function can be used to
  //  * count neighbors satisfying a predicate or implement PageRank.
  //  *
  //  * @note The returned RDD may contain fewer entries than their are vertices
  //  * in the graph.  This is because some vertices may not have neighbors or the
  //  * map function may return None for all neighbors.
  //  *
  //  * @param mapFunc the function applied to each edge adjacent to each vertex.
  //  * The mapFunc can optionally return None in which case it does not
  //  * contribute to the final sum.
  //  * @param mergeFunc the function used to merge the results of each map
  //  * operation.
  //  * @param direction the direction of edges to consider (e.g., In, Out, Both).
  //  * @tparam VD2 The returned type of the aggregation operation.
  //  *
  //  * @return A Spark.RDD containing tuples of vertex identifiers and thee
  //  * resulting value.  Note that the returned RDD may contain fewer vertices
  //  * than in the original graph since some vertices may not have neighbors or
  //  * the map function could return None for all neighbors.
  //  *
  //  * @example We can use this function to compute the average follower age for
  //  * each user
  //  * {{{
  //  * val graph: Graph[Int,Int] = loadGraph()
  //  * val averageFollowerAge: RDD[(Int, Int)] =
  //  *   graph.aggregateNeighbors[(Int,Double)](
  //  *     (vid, edge) => (edge.otherVertex(vid).data, 1),
  //  *     (a, b) => (a._1 + b._1, a._2 + b._2),
  //  *     EdgeDirection.In)
  //  *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
  //  * }}}
  //  *
  //  */
  // def aggregateNeighbors[A: ClassManifest](
  //     mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[A],
  //     mergeFunc: (A, A) => A,
  //     direction: EdgeDirection)
  //   : Graph[(VD, Option[A]), ED]

  /**
   * This function is used to compute a statistic for the neighborhood of each
   * vertex and returns a value for all vertices (including those without
   * neighbors).
   *
   * This is one of the core functions in the Graph API in that enables
   * neighborhood level computation. For example this function can be used to
   * count neighbors satisfying a predicate or implement PageRank.
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
      direction: EdgeDirection)
    : RDD[(Vid, A)]


  /**
   * Join the vertices with an RDD and then apply a function from the the
   * vertex and RDD entry to a new vertex value and type.  The input table should
   * contain at most one entry for each vertex.  If no entry is provided the
   * map function is invoked passing none.
   *
   * @tparam U the type of entry in the table of updates
   * @tparam VD2 the new vertex value type
   *
   * @param table the table to join with the vertices in the graph.  The table
   * should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.  The
   * map function is invoked for all vertices, even those that do not have a
   * corresponding entry in the table.
   *
   * @example This function is used to update the vertices with new values
   * based on external data.  For example we could add the out degree to each
   * vertex record
   * {{{
   * val rawGraph: Graph[(),()] = Graph.textFile("webgraph")
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg.getOrElse(0) )
   * }}}
   *
   * @todo Should this function be curried to enable type inference?  For
   * example
   * {{{
   * graph.leftJoinVertices(tbl)( (v, row) => row.getOrElse(0) )
   * }}}
   * @todo Is leftJoinVertices the right name?
   */
  def outerJoinVertices[U: ClassManifest, VD2: ClassManifest](table: RDD[(Vid, U)])
      (mapFunc: (Vid, VD, Option[U]) => VD2)
    : Graph[VD2, ED]

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
    def uf(id: Vid, data: VD, o: Option[U]): VD = o match {
      case Some(u) => mapFunc(id, data, u)
      case None => data
    }
    outerJoinVertices(table)(uf)
  }

  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
}


object Graph {

  import org.apache.spark.graph.impl._
  import org.apache.spark.SparkContext._

  def apply(rawEdges: RDD[(Vid, Vid)], uniqueEdges: Boolean = true): Graph[Int, Int] = {
    // Reduce to unique edges.
    val edges: RDD[Edge[Int]] =
      if (uniqueEdges) {
        rawEdges.map((_, 1)).reduceByKey(_ + _).map { case ((s, t), cnt) => Edge(s, t, cnt) }
      } else {
        rawEdges.map { case (s, t) => Edge(s, t, 1) }
      }
    // Determine unique vertices
    val vertices: RDD[(Vid, Int)] = 
      edges.flatMap{ case Edge(s, t, cnt) => Array((s, 1), (t, 1)) }.reduceByKey(_ + _)
 
    // Return graph
    GraphImpl(vertices, edges)
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[(Vid,VD)], edges: RDD[Edge[ED]]): Graph[VD, ED] = {
    GraphImpl(vertices, edges)
  }

  implicit def graphToGraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) = g.ops
}
