package org.apache.spark.graph.impl

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner 
import org.apache.spark.util.ClosureCleaner

import org.apache.spark.rdd.RDD

import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._





/**
 * A Graph RDD that supports computation on graphs.
 */
class GraphImpl[VD: ClassManifest, ED: ClassManifest] protected (
    val numVertexPartitions: Int,
    val numEdgePartitions: Int,
    _rawVertices: RDD[Vertex[VD]],
    _rawEdges: RDD[Edge[ED]],
    _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
    _rawETable: RDD[(Pid, EdgePartition[ED])])
  extends Graph[VD, ED] {

  def this(vertices: RDD[Vertex[VD]], edges: RDD[Edge[ED]]) = {
    this(vertices.partitions.size, edges.partitions.size, vertices, edges, null, null)
  }

  def withPartitioner(numVertexPartitions: Int, numEdgePartitions: Int): Graph[VD, ED] = {
    if (_cached) {
      new GraphImpl(numVertexPartitions, numEdgePartitions, null, null, _rawVTable, _rawETable)
        .cache()
    } else {
      new GraphImpl(numVertexPartitions, numEdgePartitions, _rawVertices, _rawEdges, null, null)
    }
  }

  def withVertexPartitioner(numVertexPartitions: Int) = {
    withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  def withEdgePartitioner(numEdgePartitions: Int) = {
    withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  protected var _cached = false

  override def cache(): Graph[VD, ED] = {
    eTable.cache()
    vTable.cache()
    _cached = true
    this
  }

  override def reverse: Graph[VD, ED] = {
    newGraph(vertices, edges.map{ case Edge(s, t, e) => Edge(t, s, e) })
  }

  /** Return a RDD of vertices. */
  override def vertices: RDD[Vertex[VD]] = {
    if (!_cached && _rawVertices != null) {
      _rawVertices
    } else {
      vTable.map { case(vid, (data, pids)) => new Vertex(vid, data) }
    }
  }

  /** Return a RDD of edges. */
  override def edges: RDD[Edge[ED]] = {
    if (!_cached && _rawEdges != null) {
      _rawEdges
    } else {
      eTable.mapPartitions { iter => iter.next()._2.iterator }
    }
  }

  /** Return a RDD that brings edges with its source and destination vertices together. */
  override def triplets: RDD[EdgeTriplet[VD, ED]] = {
    new EdgeTripletRDD(vTableReplicated, eTable).mapPartitions { part => part.next()._2 }
  }

  override def mapVertices[VD2: ClassManifest](f: Vertex[VD] => VD2): Graph[VD2, ED] = {
    newGraph(vertices.map(v => Vertex(v.id, f(v))), edges)
  }

  override def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2] = {
    newGraph(vertices, edges.map(e => Edge(e.src, e.dst, f(e))))
  }

  override def mapTriplets[ED2: ClassManifest](f: EdgeTriplet[VD, ED] => ED2):
    Graph[VD, ED2] = {
    newGraph(vertices, triplets.map(e => Edge(e.src.id, e.dst.id, f(e))))
  }

  override def correctEdges(): Graph[VD, ED] = {
    val sc = vertices.context
    val vset = sc.broadcast(vertices.map(_.id).collect().toSet)
    val newEdges = edges.filter(e => vset.value.contains(e.src) && vset.value.contains(e.dst))
    Graph(vertices, newEdges)
  }


  override def subgraph(epred: EdgeTriplet[VD,ED] => Boolean = (_ => true), 
    vpred: Vertex[VD] => Boolean = (_ => true) ): Graph[VD, ED] = {

    // Restrict the set of vertices to those that satisfy the vertex predicate
    val newVertices = vertices.filter(vpred)
    // Restrict the set of edges to those that satisfy the vertex and the edge predicate.
    val newEdges = triplets.filter(t => vpred(t.src) && vpred(t.dst) && epred(t))
      .map( t => Edge(t.src.id, t.dst.id, t.data) )

    new GraphImpl(newVertices, newEdges)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def aggregateNeighbors[VD2: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[VD2],
      reduceFunc: (VD2, VD2) => VD2,
      default: VD2,
      gatherDirection: EdgeDirection)
    : RDD[(Vid, VD2)] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val newVTable = vTableReplicated.mapPartitions({ part =>
        part.map { v => (v._1, MutableTuple2(v._2, Option.empty[VD2])) }
      }, preservesPartitioning = true)

    new EdgeTripletRDD[MutableTuple2[VD, Option[VD2]], ED](newVTable, eTable)
      .mapPartitions { part =>
        val (vmap, edges) = part.next()
        val edgeSansAcc = new EdgeTriplet[VD, ED]()
        edgeSansAcc.src = new Vertex[VD]
        edgeSansAcc.dst = new Vertex[VD]
        edges.foreach { e: EdgeTriplet[MutableTuple2[VD, Option[VD2]], ED] =>
          edgeSansAcc.data = e.data
          edgeSansAcc.src.data = e.src.data._1
          edgeSansAcc.dst.data = e.dst.data._1
          edgeSansAcc.src.id = e.src.id
          edgeSansAcc.dst.id = e.dst.id
          if (gatherDirection == EdgeDirection.In || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.dst.data._2.get, tmp.get)) else e.dst.data._2
              }
          }
          if (gatherDirection == EdgeDirection.Out || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.src.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.src.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.src.data._2.get, tmp.get)) else e.src.data._2
              }
          }
        }
        vmap.long2ObjectEntrySet().fastIterator().filter(!_.getValue()._2.isEmpty).map{ entry =>
          (entry.getLongKey(), entry.getValue()._2)
        }
      }
      .map{ case (vid, aOpt) => (vid, aOpt.get) }
      .combineByKey((v: VD2) => v, reduceFunc, null, vertexPartitioner, false)
  }

  /**
   * Same as aggregateNeighbors but map function can return none and there is no default value.
   * As a consequence, the resulting table may be much smaller than the set of vertices.
   */
  override def aggregateNeighbors[A: ClassManifest](
    mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[A],
    reduceFunc: (A, A) => A,
    gatherDirection: EdgeDirection): Graph[(VD, Option[A]), ED] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val newVTable = vTableReplicated.mapPartitions({ part =>
        part.map { v => (v._1, MutableTuple2(v._2, Option.empty[A])) }
      }, preservesPartitioning = true)

    val newVertices: RDD[(Vid, A)] =
      new EdgeTripletRDD[MutableTuple2[VD, Option[A]], ED](newVTable, eTable)
        .mapPartitions { part =>
          val (vmap, edges) = part.next()
          val edgeSansAcc = new EdgeTriplet[VD, ED]()
          edgeSansAcc.src = new Vertex[VD]
          edgeSansAcc.dst = new Vertex[VD]
          edges.foreach { e: EdgeTriplet[MutableTuple2[VD, Option[A]], ED] =>
            edgeSansAcc.data = e.data
            edgeSansAcc.src.data = e.src.data._1
            edgeSansAcc.dst.data = e.dst.data._1
            edgeSansAcc.src.id = e.src.id
            edgeSansAcc.dst.id = e.dst.id
            if (gatherDirection == EdgeDirection.In || gatherDirection == EdgeDirection.Both) {
              e.dst.data._2 =
                if (e.dst.data._2.isEmpty) {
                  mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
                } else {
                  val tmp = mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
                  if (!tmp.isEmpty) Some(reduceFunc(e.dst.data._2.get, tmp.get)) else e.dst.data._2
                }
            }
            if (gatherDirection == EdgeDirection.Out || gatherDirection == EdgeDirection.Both) {
              e.src.data._2 =
                if (e.src.data._2.isEmpty) {
                  mapFunc(edgeSansAcc.src.id, edgeSansAcc)
                } else {
                  val tmp = mapFunc(edgeSansAcc.src.id, edgeSansAcc)
                  if (!tmp.isEmpty) Some(reduceFunc(e.src.data._2.get, tmp.get)) else e.src.data._2
                }
            }
          }
          vmap.long2ObjectEntrySet().fastIterator().filter(!_.getValue()._2.isEmpty).map{ entry =>
            (entry.getLongKey(), entry.getValue()._2)
          }
        }
        .map{ case (vid, aOpt) => (vid, aOpt.get) }
        .combineByKey((v: A) => v, reduceFunc, null, vertexPartitioner, false)

    this.leftJoinVertices(newVertices, (v: Vertex[VD], a: Option[A]) => (v.data, a))
  }

  override def leftJoinVertices[U: ClassManifest, VD2: ClassManifest](
      updates: RDD[(Vid, U)],
      updateF: (Vertex[VD], Option[U]) => VD2)
    : Graph[VD2, ED] = {

    ClosureCleaner.clean(updateF)

    val newVTable = vTable.leftOuterJoin(updates).mapPartitions({ iter =>
      iter.map { case (vid, ((vdata, pids), update)) =>
        val newVdata = updateF(Vertex(vid, vdata), update)
        (vid, (newVdata, pids))
      }
    }, preservesPartitioning = true).cache()

    new GraphImpl(newVTable.partitions.length, eTable.partitions.length, null, null, newVTable, eTable)
  }

  override def joinVertices[U: ClassManifest](
      updates: RDD[(Vid, U)],
      updateF: (Vertex[VD], U) => VD)
    : Graph[VD, ED] = {

    ClosureCleaner.clean(updateF)

    val newVTable = vTable.leftOuterJoin(updates).mapPartitions({ iter =>
      iter.map { case (vid, ((vdata, pids), update)) =>
        if (update.isDefined) {
          val newVdata = updateF(Vertex(vid, vdata), update.get)
          (vid, (newVdata, pids))
        } else {
          (vid, (vdata, pids))
        }
      }
    }, preservesPartitioning = true).cache()

    new GraphImpl(newVTable.partitions.length, eTable.partitions.length, null, null, newVTable, eTable)
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Internals hidden from callers
  //////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO: Support non-hash partitioning schemes.
  protected val vertexPartitioner = new HashPartitioner(numVertexPartitions)
  protected val edgePartitioner = new HashPartitioner(numEdgePartitions)

  /** Create a new graph but keep the current partitioning scheme. */
  protected def newGraph[VD2: ClassManifest, ED2: ClassManifest](
    vertices: RDD[Vertex[VD2]], edges: RDD[Edge[ED2]]): Graph[VD2, ED2] = {
    (new GraphImpl[VD2, ED2](vertices, edges)).withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  protected lazy val eTable: RDD[(Pid, EdgePartition[ED])] = {
    if (_rawETable == null) {
      createETable(_rawEdges, numEdgePartitions)
    } else {
      _rawETable
    }
  }

  protected lazy val vTable: RDD[(Vid, (VD, Array[Pid]))] = {
    if (_rawVTable == null) {
      createVTable(_rawVertices, eTable, numVertexPartitions)
    } else {
      _rawVTable
    }
  }

  protected lazy val vTableReplicated: RDD[(Vid, VD)] = {
    // Join vid2pid and vTable, generate a shuffle dependency on the joined result, and get
    // the shuffle id so we can use it on the slave.
    vTable
      .flatMap { case (vid, (vdata, pids)) => pids.iterator.map { pid => (pid, (vid, vdata)) } }
      .partitionBy(edgePartitioner)
      .mapPartitions(
        { part => part.map { case(pid, (vid, vdata)) => (vid, vdata) } },
        preservesPartitioning = true)
  }
}


object GraphImpl {


  protected def edgePartitionFunction1D(src: Vid, dst: Vid, numParts: Pid): Pid = {
    val mixingPrime: Vid = 1125899906842597L 
    (math.abs(src) * mixingPrime).toInt % numParts
  }



  /**
   * This function implements a classic 2D-Partitioning of a sparse matrix.  
   * Suppose we have a graph with 11 vertices that we want to partition 
   * over 9 machines.  We can use the following sparse matrix representation:
   *
   *       __________________________________
   *  v0   | P0 *     | P1       | P2    *  |      
   *  v1   |  ****    |  *       |          |
   *  v2   |  ******* |      **  |  ****    |
   *  v3   |  *****   |  *  *    |       *  |   
   *       ----------------------------------
   *  v4   | P3 *     | P4 ***   | P5 **  * |      
   *  v5   |  *  *    |  *       |          |
   *  v6   |       *  |      **  |  ****    |
   *  v7   |  * * *   |  *  *    |       *  |   
   *       ----------------------------------
   *  v8   | P6   *   | P7    *  | P8  *   *|      
   *  v9   |     *    |  *    *  |          |
   *  v10  |       *  |      **  |  *  *    |
   *  v11  | * <-E    |  ***     |       ** |   
   *       ----------------------------------
   *
   * The edge denoted by E connects v11 with v1 and is assigned to 
   * processor P6.  To get the processor number we divide the matrix
   * into sqrt(numProc) by sqrt(numProc) blocks.  Notice that edges
   * adjacent to v11 can only be in the first colum of 
   * blocks (P0, P3, P6) or the last row of blocks (P6, P7, P8).  
   * As a consequence we can guarantee that v11 will need to be 
   * replicated to at most 2 * sqrt(numProc) machines.
   *
   * Notice that P0 has many edges and as a consequence this 
   * partitioning would lead to poor work balance.  To improve
   * balance we first multiply each vertex id by a large prime 
   * to effectively suffle the vertex locations. 
   *
   * One of the limitations of this approach is that the number of
   * machines must either be a perfect square.  We partially address
   * this limitation by computing the machine assignment to the next 
   * largest perfect square and then mapping back down to the actual 
   * number of machines.  Unfortunately, this can also lead to work 
   * imbalance and so it is suggested that a perfect square is used. 
   *   
   *
   */
  protected def edgePartitionFunction2D(src: Vid, dst: Vid, 
    numParts: Pid, ceilSqrtNumParts: Pid): Pid = {
    val mixingPrime: Vid = 1125899906842597L 
    val col: Pid = ((math.abs(src) * mixingPrime) % ceilSqrtNumParts).toInt
    val row: Pid = ((math.abs(dst) * mixingPrime) % ceilSqrtNumParts).toInt
    (col * ceilSqrtNumParts + row) % numParts
  }


  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  protected def createETable[ED: ClassManifest](edges: RDD[Edge[ED]], numPartitions: Int)
    : RDD[(Pid, EdgePartition[ED])] = {
      val ceilSqrt: Pid = math.ceil(math.sqrt(numPartitions)).toInt 

    edges
      .map { e =>
        // Random partitioning based on the source vertex id.
        // val part: Pid = edgePartitionFunction1D(e.src, e.dst, numPartitions)
        val part: Pid = edgePartitionFunction2D(e.src, e.dst, numPartitions, ceilSqrt)

        // Should we be using 3-tuple or an optimized class
        (part, (e.src, e.dst, e.data))
        //  (math.abs(e.src) % numPartitions, (e.src, e.dst, e.data))
       
      }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex({ (pid, iter) =>
        val edgePartition = new EdgePartition[ED]
        iter.foreach { case (_, (src, dst, data)) => edgePartition.add(src, dst, data) }
        edgePartition.trim()
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true)
  }

  protected def createVTable[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[Vertex[VD]],
      eTable: RDD[(Pid, EdgePartition[ED])],
      numPartitions: Int)
    : RDD[(Vid, (VD, Array[Pid]))] = {
    val partitioner = new HashPartitioner(numPartitions)

    // A key-value RDD. The key is a vertex id, and the value is a list of
    // partitions that contains edges referencing the vertex.
    val vid2pid : RDD[(Vid, Seq[Pid])] = eTable.mapPartitions { iter =>
      val (pid, edgePartition) = iter.next()
      val vSet = new VertexSet
      var i = 0
      while (i < edgePartition.srcIds.size) {
        vSet.add(edgePartition.srcIds.getLong(i))
        vSet.add(edgePartition.dstIds.getLong(i))
        i += 1
      }
      vSet.iterator.map { vid => (vid.toLong, pid) }
    }.groupByKey(partitioner)

    vertices
      .map { v => (v.id, v.data) }
      .partitionBy(partitioner)
      .leftOuterJoin(vid2pid)
      .mapValues {
        case (vdata, None)       => (vdata, Array.empty[Pid])
        case (vdata, Some(pids)) => (vdata, pids.toArray)
      }
  }
}

