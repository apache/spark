package org.apache.spark.graph.impl

import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner 
import org.apache.spark.util.ClosureCleaner

import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.IndexedRDD
import org.apache.spark.rdd.RDDIndex


import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._
import org.apache.spark.graph.impl.MessageToPartitionRDDFunctions._

/**
 * The Iterator type returned when constructing edge triplets
 */
class EdgeTripletIterator[VD: ClassManifest, ED: ClassManifest](
  val vmap: VertexHashMap[VD],
  val edgePartition: EdgePartition[ED]) extends Iterator[EdgeTriplet[VD, ED]] {

  private var pos = 0
  private val et = new EdgeTriplet[VD, ED]
  
  override def hasNext: Boolean = pos < edgePartition.size
  override def next() = {
    et.srcId = edgePartition.srcIds(pos)
    // assert(vmap.containsKey(e.src.id))
    et.srcAttr = vmap.get(et.srcId)
    et.dstId = edgePartition.dstIds(pos)
    // assert(vmap.containsKey(e.dst.id))
    et.dstAttr = vmap.get(et.dstId)
    //println("Iter called: " + pos)
    et.attr = edgePartition.data(pos)
    pos += 1
    et
  }

  override def toList: List[EdgeTriplet[VD, ED]] = {
    val lb = new mutable.ListBuffer[EdgeTriplet[VD,ED]]
    val currentEdge = new EdgeTriplet[VD, ED]
    for (i <- (0 until edgePartition.size)) {
      currentEdge.srcId = edgePartition.srcIds(i)
      // assert(vmap.containsKey(e.src.id))
      currentEdge.srcAttr = vmap.get(currentEdge.srcId)
      currentEdge.dstId = edgePartition.dstIds(i)
      // assert(vmap.containsKey(e.dst.id))
      currentEdge.dstAttr = vmap.get(currentEdge.dstId)
      currentEdge.attr = edgePartition.data(i)
      lb += currentEdge
    }
    lb.toList
  }
} // end of Edge Triplet Iterator

object EdgeTripletBuilder {
  def makeTriplets[VD: ClassManifest, ED: ClassManifest]( 
    vTableReplicated: IndexedRDD[Pid, VertexHashMap[VD]],
    eTable: IndexedRDD[Pid, EdgePartition[ED]]): RDD[EdgeTriplet[VD, ED]] = {
    val iterFun = (iter: Iterator[(Pid, (VertexHashMap[VD], EdgePartition[ED]))]) => {
      val (pid, (vmap, edgePartition)) = iter.next()
      //assert(iter.hasNext == false)
      // Return an iterator that looks up the hash map to find matching 
      // vertices for each edge.
      new EdgeTripletIterator(vmap, edgePartition)
    }
    ClosureCleaner.clean(iterFun) 
    vTableReplicated.zipJoinRDD(eTable)
      .mapPartitions( iterFun ) // end of map partition
  }

}


/**
 * A Graph RDD that supports computation on graphs.
 */
class GraphImpl[VD: ClassManifest, ED: ClassManifest] protected (
    @transient val vTable: IndexedRDD[Vid, VD],
    @transient val vid2pid: IndexedRDD[Vid, Array[Pid]],
    @transient val eTable: IndexedRDD[Pid, EdgePartition[ED]])
  extends Graph[VD, ED] {

//  def this() = this(null,null,null)


  /**
   * The vTableReplicated is a version of the vertex data after it is 
   * replicated.
   */
  @transient val vTableReplicated: IndexedRDD[Pid, VertexHashMap[VD]] = 
    createVTableReplicated(vTable, vid2pid, eTable) 




  /** Return a RDD of vertices. */
  @transient override val vertices: RDD[(Vid, VD)] = vTable


  /** Return a RDD of edges. */
  @transient override val edges: RDD[Edge[ED]] = {
    eTable.mapPartitions { iter => iter.next()._2.iterator }
  }


  /** Return a RDD that brings edges with its source and destination vertices together. */
  @transient override val triplets: RDD[EdgeTriplet[VD, ED]] = 
    EdgeTripletBuilder.makeTriplets(vTableReplicated, eTable)


  // {
  //   val iterFun = (iter: Iterator[(Pid, (VertexHashMap[VD], EdgePartition[ED]))]) => {
  //     val (pid, (vmap, edgePartition)) = iter.next()
  //     //assert(iter.hasNext == false)
  //     // Return an iterator that looks up the hash map to find matching 
  //     // vertices for each edge.
  //     new EdgeTripletIterator(vmap, edgePartition)
  //   }
  //   ClosureCleaner.clean(iterFun) 
  //   vTableReplicated.join(eTable).mapPartitions( iterFun ) // end of map partition
  // }




  override def cache(): Graph[VD, ED] = {
    eTable.cache()
    vid2pid.cache()
    vTable.cache()
    /** @todo should we cache the replicated data? */
    vTableReplicated.cache()
    this
  }


  override def statistics: Map[String, Any] = {
    val numVertices = this.numVertices
    val numEdges = this.numEdges
    val replicationRatio = 
      vid2pid.map(kv => kv._2.size).sum / vTable.count
    val loadArray = 
      eTable.map{ case (pid, epart) => epart.data.size }.collect.map(x => x.toDouble / numEdges)
    val minLoad = loadArray.min
    val maxLoad = loadArray.max
    Map(
      "Num Vertices" -> numVertices, "Num Edges" -> numEdges,
      "Replication" -> replicationRatio, "Load Array" -> loadArray, 
      "Min Load" -> minLoad, "Max Load" -> maxLoad) 
  }


  override def reverse: Graph[VD, ED] = {
    val etable = eTable.mapValues( _.reverse ).asInstanceOf[IndexedRDD[Pid, EdgePartition[ED]]] 
    new GraphImpl(vTable, vid2pid, etable)
  }


  override def mapVertices[VD2: ClassManifest](f: (Vid, VD) => VD2): Graph[VD2, ED] = {
    val newVTable = vTable.mapValuesWithKeys((vid, data) => f(vid, data))
      .asInstanceOf[IndexedRDD[Vid, VD2]]
    new GraphImpl(newVTable, vid2pid, eTable)
  }

  override def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2] = {
    val newETable = eTable.mapValues(eBlock => eBlock.map(f))
      .asInstanceOf[IndexedRDD[Pid, EdgePartition[ED2]]]
    new GraphImpl(vTable, vid2pid, newETable)
  }


  override def mapTriplets[ED2: ClassManifest](f: EdgeTriplet[VD, ED] => ED2):
    Graph[VD, ED2] = {
    val newETable = eTable.join(vTableReplicated).mapValues{ 
      case (edgePartition, vmap) =>
      val et = new EdgeTriplet[VD, ED]    
      edgePartition.map{e => 
        et.set(e)
        et.srcAttr = vmap(e.srcId)
        et.dstAttr = vmap(e.dstId)
        f(et)
      }
    }.asInstanceOf[IndexedRDD[Pid, EdgePartition[ED2]]]
    new GraphImpl(vTable, vid2pid, newETable)
  }

  // override def correctEdges(): Graph[VD, ED] = {
  //   val sc = vertices.context
  //   val vset = sc.broadcast(vertices.map(_.id).collect().toSet)
  //   val newEdges = edges.filter(e => vset.value.contains(e.src) && vset.value.contains(e.dst))
  //   Graph(vertices, newEdges)
  // }


  override def subgraph(epred: EdgeTriplet[VD,ED] => Boolean = (x => true), 
    vpred: (Vid, VD) => Boolean = ((a,b) => true) ): Graph[VD, ED] = {

    /** @todo The following code behaves deterministically on each
     * vertex predicate but uses additional space.  Should we swithc to
     * this version
     */
    // val predGraph = mapVertices(v => (v.data, vpred(v)))
    // val newETable = predGraph.triplets.filter(t => 
    //   if(v.src.data._2 && v.dst.data._2) {
    //     val src = Vertex(t.src.id, t.src.data._1)
    //     val dst = Vertex(t.dst.id, t.dst.data._1)
    //     epred(new EdgeTriplet[VD, ED](src, dst, t.data))
    //   } else { false })

    // val newVTable = predGraph.vertices.filter(v => v.data._1)
    //   .map(v => (v.id, v.data._1)).indexed()

    // Reuse the partitioner (but not the index) from this graph
    val newVTable = vertices.filter(v => vpred(v._1, v._2)).indexed(vTable.index.partitioner)


    // Restrict the set of edges to those that satisfy the vertex and the edge predicate.
    val newETable = createETable(
      triplets.filter(
        t => vpred( t.srcId, t.srcAttr ) && vpred( t.dstId, t.dstAttr ) && epred(t)
        )
        .map( t => Edge(t.srcId, t.dstId, t.attr) ),
      eTable.index.partitioner.numPartitions
      )

    // Construct the Vid2Pid map. Here we assume that the filter operation 
    // behaves deterministically.  
    // @todo reindex the vertex and edge tables 
    val newVid2Pid = createVid2Pid(newETable, newVTable.index)

    new GraphImpl(newVTable, newVid2Pid, newETable)
  }


  // Because of the edgepartitioner, we know that all edges with the same src and dst
  // will be in the same partition

  // We will want to keep the same partitioning scheme. Use newGraph() rather than
  // new GraphImpl()
  // TODO(crankshaw) is there a better way to do this using RDD.groupBy()
  // functions?

  override def groupEdgeTriplets[ED2: ClassManifest](
    f: Iterator[EdgeTriplet[VD,ED]] => ED2 ): Graph[VD,ED2] = {
  //override def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ):
   
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
            (t.srcId, t.dstId) }
        //.groupBy { e => (e.src, e.dst) }
        // Apply the user supplied supplied edge group function to
        // each group of edges
        // The result of this line is Map[(Long, Long, ED2]
        .mapValues { ts: List[EdgeTriplet[VD, ED]] => f(ts.toIterator) }
        // convert the resulting map back to a list of tuples
        .toList
        // TODO(crankshaw) needs an iterator over the tuples? 
        // Why can't I map over the list?
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

      // @todo eliminate the need to call createETable
      val newETable = createETable(newEdges, 
        eTable.index.partitioner.numPartitions)

      new GraphImpl(vTable, vid2pid, newETable)

  }


  override def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ):
    Graph[VD,ED2] = {

      val newEdges: RDD[Edge[ED2]] = edges.mapPartitions { partIter =>
        partIter.toList
        .groupBy { e: Edge[ED] => 
            println(e.srcId + " " + e.dstId)
            (e.srcId, e.dstId) }
        .mapValues { ts => f(ts.toIterator) }
        .toList
        .toIterator
        .map { case ((src, dst), data) => Edge(src, dst, data) }
      }
      // @todo eliminate the need to call createETable
      val newETable = createETable(newEdges, 
        eTable.index.partitioner.numPartitions)

      new GraphImpl(vTable, vid2pid, newETable)
  }



  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassManifest](
      mapFunc: EdgeTriplet[VD, ED] => Array[(Vid, A)],
      reduceFunc: (A, A) => A)
    : RDD[(Vid, A)] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // Map and preaggregate 
    val preAgg = vTableReplicated.zipJoinRDD(eTable).flatMap{
      case (pid, (vmap, edgePartition)) => 
        val aggMap = new VertexHashMap[A]
        val et = new EdgeTriplet[VD, ED]
        edgePartition.foreach{e => 
          et.set(e)
          et.srcAttr = vmap(e.srcId)
          et.dstAttr = vmap(e.dstId)
          mapFunc(et).foreach{case (vid, a) => 
            if(aggMap.containsKey(vid)) {
              aggMap.put(vid, reduceFunc(aggMap.get(vid), a))             
              } else { aggMap.put(vid, a) }
            }
          }
        // Return the aggregate map
        aggMap.long2ObjectEntrySet().fastIterator().map{ 
          entry => (entry.getLongKey(), entry.getValue())
        }
      }.partitionBy(vTable.index.rdd.partitioner.get)

    // do the final reduction reusing the index map
    IndexedRDD(preAgg, vTable.index, reduceFunc)
  }


  override def outerJoinVertices[U: ClassManifest, VD2: ClassManifest]
    (updates: RDD[(Vid, U)])(updateF: (Vid, VD, Option[U]) => VD2)
    : Graph[VD2, ED] = {
    ClosureCleaner.clean(updateF)
    val newVTable = vTable.zipJoinLeftWithKeys(updates)(updateF)
    new GraphImpl(newVTable, vid2pid, eTable)
  }


} // end of class GraphImpl
















object GraphImpl {

  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)], edges: RDD[Edge[ED]]): 
  GraphImpl[VD,ED] = {

    apply(vertices, edges, 
      vertices.context.defaultParallelism, edges.context.defaultParallelism)
  }


  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)], edges: RDD[Edge[ED]],
    numVPart: Int, numEPart: Int): GraphImpl[VD,ED] = {

    val vtable = vertices.indexed(numVPart)
    val etable = createETable(edges, numEPart)
    val vid2pid = createVid2Pid(etable, vtable.index)

    new GraphImpl(vtable, vid2pid, etable)
  }



  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  protected def createETable[ED: ClassManifest](
    edges: RDD[Edge[ED]], numPartitions: Int)
    : IndexedRDD[Pid, EdgePartition[ED]] = {
      val ceilSqrt: Pid = math.ceil(math.sqrt(numPartitions)).toInt 
    edges
      .map { e =>
        // Random partitioning based on the source vertex id.
        // val part: Pid = edgePartitionFunction1D(e.srcId, e.dstId, numPartitions)
        // val part: Pid = edgePartitionFunction2D(e.srcId, e.dstId, numPartitions, ceilSqrt)
        val part: Pid = randomVertexCut(e.srcId, e.dstId, numPartitions)
        //val part: Pid = canonicalEdgePartitionFunction2D(e.srcId, e.dstId, numPartitions, ceilSqrt)

        // Should we be using 3-tuple or an optimized class
        MessageToPartition(part, (e.srcId, e.dstId, e.attr))
      }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex({ (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED]
        iter.foreach { message =>
          val data = message.data
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true).indexed()
  }


  protected def createVid2Pid[ED: ClassManifest](
    eTable: IndexedRDD[Pid, EdgePartition[ED]],
    vTableIndex: RDDIndex[Vid]): IndexedRDD[Vid, Array[Pid]] = {
    val preAgg = eTable.mapPartitions { iter =>
      val (pid, edgePartition) = iter.next()
      val vSet = new VertexSet
      edgePartition.foreach(e => {vSet.add(e.srcId); vSet.add(e.dstId)})
      vSet.iterator.map { vid => (vid.toLong, pid) }
    }
    IndexedRDD[Vid, Pid, ArrayBuffer[Pid]](preAgg, vTableIndex, 
      (p: Pid) => ArrayBuffer(p),
      (ab: ArrayBuffer[Pid], p:Pid) => {ab.append(p); ab},
      (a: ArrayBuffer[Pid], b: ArrayBuffer[Pid]) => a ++ b)
      .mapValues(a => a.toArray).asInstanceOf[IndexedRDD[Vid, Array[Pid]]]
  }


  protected def createVTableReplicated[VD: ClassManifest, ED: ClassManifest](
    vTable: IndexedRDD[Vid, VD], vid2pid: IndexedRDD[Vid, Array[Pid]],
    eTable: IndexedRDD[Pid, EdgePartition[ED]]): 
    IndexedRDD[Pid, VertexHashMap[VD]] = {
    // Join vid2pid and vTable, generate a shuffle dependency on the joined 
    // result, and get the shuffle id so we can use it on the slave.
    vTable.zipJoinRDD(vid2pid)
      .flatMap { case (vid, (vdata, pids)) => 
        pids.iterator.map { pid => MessageToPartition(pid, (vid, vdata)) }
      }
      .partitionBy(eTable.partitioner.get) //@todo assert edge table has partitioner
      .mapPartitionsWithIndex( (pid, iter) => {
        // Build the hashmap for each partition
        val vmap = new VertexHashMap[VD]
        for( msg <- iter ) { vmap.put(msg.data._1, msg.data._2) }
        Array((pid, vmap)).iterator
      }, preservesPartitioning = true)
      .indexed(eTable.index)  
  }


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

} // end of object GraphImpl

