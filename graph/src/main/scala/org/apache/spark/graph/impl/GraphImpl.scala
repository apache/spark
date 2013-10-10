package org.apache.spark.graph.impl

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner 
import org.apache.spark.util.ClosureCleaner

import org.apache.spark.rdd.RDD

import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._
import org.apache.spark.graph.impl.MessageToPartitionRDDFunctions._


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


  override def replication(): Double = {
    val rep = vTable.map{ case (_, (_, a)) => a.size }.sum
    rep / vTable.count
  }

  override def balance(): Array[Int] = {
    eTable.map{ case (_, epart) => epart.data.size }.collect
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


  // Because of the edgepartitioner, we know that all edges with the same src and dst
  // will be in the same partition

  // We will want to keep the same partitioning scheme. Use newGraph() rather than
  // new GraphImpl()
  // TODO(crankshaw) is there a better way to do this using RDD.groupBy()
  // functions?

  override def groupEdgeTriplets[ED2: ClassManifest](f: Iterator[EdgeTriplet[VD,ED]] => ED2 ):
  //override def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ):
    Graph[VD,ED2] = {

      // I think that
      // myRDD.mapPartitions { part => 
      //  val (vmap, edges) = part.next()
      // gives me access to the vertex map and the set of
      // edges within that partition

      // This is what happens during mapPartitions
      // The iterator iterates over all partitions
      // val result: RDD[U] = new RDD[T]().mapPartitions(f: Iterator[T] => Iterator[U])

      // TODO(crankshaw) figure out how to actually get the new Edge RDD and what
      // type that should have
      val newEdges: RDD[Edge[ED2]] = triplets.mapPartitions { partIter =>
        // toList lets us operate on all EdgeTriplets in a single partition at once
        partIter
        .toList
        // groups all ETs in this partition that have the same src and dst
        // Because all ETs with the same src and dst will live on the same
        // partition due to the EdgePartitioner, this guarantees that these
        // ET groups will be complete.
        .groupBy { t: EdgeTriplet[VD, ED] => 
            //println("(" + t.src.id + ", " + t.dst.id + ", " + t.data + ")")
            (t.src.id, t.dst.id) }
        //.groupBy { e => (e.src, e.dst) }
        // Apply the user supplied supplied edge group function to
        // each group of edges
        // The result of this line is Map[(Long, Long, ED2]
        .mapValues { ts: List[EdgeTriplet[VD, ED]] => f(ts.toIterator) }
        // convert the resulting map back to a list of tuples
        .toList
        // TODO(crankshaw) needs an iterator over the tuples? Why can't I map over the list?
        .toIterator
        // map over those tuples that contain src and dst info plus the
        // new edge data to make my new edges
        .map { case ((src, dst), data) => Edge(src, dst, data) }

        // How do I convert from a scala map to a list?
        // I want to be able to apply a function like:
        // f: (key, value): (K, V) => result: [R]
        // so that I can transfrom a Map[K, V] to List[R]

        // Maybe look at collections.breakOut
        // see http://stackoverflow.com/questions/1715681/scala-2-8-breakout
        // and http://stackoverflow.com/questions/6998676/converting-a-scala-map-to-a-list

      }
      newGraph(vertices, newEdges)

  }


  override def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ):
    Graph[VD,ED2] = {

      val newEdges: RDD[Edge[ED2]] = edges.mapPartitions { partIter =>
        partIter.toList
        .groupBy { e: Edge[ED] => 
            println(e.src + " " + e.dst)
            (e.src, e.dst) }
        .mapValues { ts => f(ts.toIterator) }
        .toList
        .toIterator
        .map { case ((src, dst), data) => Edge(src, dst, data) }


      }
      newGraph(vertices, newEdges)

  }



  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def aggregateNeighbors[A: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      reduceFunc: (A, A) => A,
      default: A,
      gatherDirection: EdgeDirection)
    : Graph[(VD, Option[A]), ED] = {

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
        .combineByKey((v: A) => v, reduceFunc, null, vertexPartitioner, false)

    this.leftJoinVertices(newVertices, (v: Vertex[VD], a: Option[A]) => (v.data, a))
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
      .flatMap { case (vid, (vdata, pids)) =>
        pids.iterator.map { pid => MessageToPartition(pid, (vid, vdata)) }
      }
      .partitionBy(edgePartitioner)
      .mapPartitions({ part =>
        part.map { message => (message.data._1, message.data._2) }
      }, preservesPartitioning = true)
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
   * to effectively shuffle the vertex locations. 
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
   * Assign edges to an aribtrary machine corresponding to a 
   * random vertex cut.
   */
  protected def randomVertexCut(src: Vid, dst: Vid, numParts: Pid): Pid = {
    math.abs((src, dst).hashCode()) % numParts
  }


  /**
   * @todo(crankshaw) how does this effect load balancing?
   */
  protected def canonicalEdgePartitionFunction2D(srcOrig: Vid, dstOrig: Vid, 
    numParts: Pid, ceilSqrtNumParts: Pid): Pid = {
    val mixingPrime: Vid = 1125899906842597L 
    // Partitions by canonical edge direction
    val src = math.min(srcOrig, dstOrig)
    val dst = math.max(srcOrig, dstOrig)
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
        //val part: Pid = canonicalEdgePartitionFunction2D(e.src, e.dst, numPartitions, ceilSqrt)

        // Should we be using 3-tuple or an optimized class
        MessageToPartition(part, (e.src, e.dst, e.data))
        //  (math.abs(e.src) % numPartitions, (e.src, e.dst, e.data))
      }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex({ (pid, iter) =>
        val edgePartition = new EdgePartition[ED]
        iter.foreach { message =>
          val data = message.data
          edgePartition.add(data._1, data._2, data._3)
        }
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

