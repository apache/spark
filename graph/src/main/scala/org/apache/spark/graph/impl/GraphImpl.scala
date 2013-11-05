package org.apache.spark.graph.impl

import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuilder


import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner 
import org.apache.spark.util.ClosureCleaner

import org.apache.spark.rdd
import org.apache.spark.rdd.RDD


import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._
import org.apache.spark.graph.impl.MessageToPartitionRDDFunctions._

import org.apache.spark.util.hash.BitSet
import org.apache.spark.util.hash.OpenHashSet
import org.apache.spark.util.hash.PrimitiveKeyOpenHashMap



/**
 * The Iterator type returned when constructing edge triplets
 */
class EdgeTripletIterator[VD: ClassManifest, ED: ClassManifest](
  val vidToIndex: VertexIdToIndexMap,
  val vertexArray: Array[VD],
  val edgePartition: EdgePartition[ED]) extends Iterator[EdgeTriplet[VD, ED]] {

  private var pos = 0
  private val et = new EdgeTriplet[VD, ED]
  private val vmap = new PrimitiveKeyOpenHashMap[Vid, VD](vidToIndex, vertexArray)
  
  override def hasNext: Boolean = pos < edgePartition.size
  override def next() = {
    et.srcId = edgePartition.srcIds(pos)
    // assert(vmap.containsKey(e.src.id))
    et.srcAttr = vmap(et.srcId)
    et.dstId = edgePartition.dstIds(pos)
    // assert(vmap.containsKey(e.dst.id))
    et.dstAttr = vmap(et.dstId)
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
      currentEdge.srcAttr = vmap(currentEdge.srcId)
      currentEdge.dstId = edgePartition.dstIds(i)
      // assert(vmap.containsKey(e.dst.id))
      currentEdge.dstAttr = vmap(currentEdge.dstId)
      currentEdge.attr = edgePartition.data(i)
      lb += currentEdge
    }
    lb.toList
  }
} // end of Edge Triplet Iterator


/**
 * A Graph RDD that supports computation on graphs.
 */
class GraphImpl[VD: ClassManifest, ED: ClassManifest] protected (
    @transient val vTable: VertexSetRDD[VD],
    @transient val vid2pid: VertexSetRDD[Array[Pid]],
    @transient val localVidMap: RDD[(Pid, VertexIdToIndexMap)],
    @transient val eTable: RDD[(Pid, EdgePartition[ED])] )
  extends Graph[VD, ED] {

  def this() = this(null, null, null, null)



  /**
   * (localVidMap: VertexSetRDD[Pid, VertexIdToIndexMap]) is a version of the
   * vertex data after it is replicated. Within each partition, it holds a map
   * from vertex ID to the index where that vertex's attribute is stored. This
   * index refers to an array in the same partition in vTableReplicatedValues.
   *
   * (vTableReplicatedValues: VertexSetRDD[Pid, Array[VD]]) holds the vertex data
   * and is arranged as described above.
   */
  @transient val vTableReplicatedValues: RDD[(Pid, Array[VD])] =
    createVTableReplicated(vTable, vid2pid, localVidMap)


  /** Return a RDD of vertices. */
  @transient override val vertices = vTable


  /** Return a RDD of edges. */
  @transient override val edges: RDD[Edge[ED]] = {
    eTable.mapPartitions( iter => iter.next()._2.iterator , true )
  }


  /** Return a RDD that brings edges with its source and destination vertices together. */
  @transient override val triplets: RDD[EdgeTriplet[VD, ED]] =
    makeTriplets(localVidMap, vTableReplicatedValues, eTable)


  override def cache(): Graph[VD, ED] = {
    eTable.cache()
    vid2pid.cache()
    vTable.cache()
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


  /**
   * Display the lineage information for this graph.
   */
  def printLineage() = {

    def traverseLineage(rdd: RDD[_], indent: String = "", visited: Map[Int, String] = Map.empty[Int, String]) {
      if(visited.contains(rdd.id)) {
        println(indent + visited(rdd.id))
        println(indent)
      } else {
        val locs = rdd.partitions.map( p => rdd.preferredLocations(p) )
        val cacheLevel = rdd.getStorageLevel
        val name = rdd.id
        val deps = rdd.dependencies
        val partitioner = rdd.partitioner
        val numparts = partitioner match { case Some(p) => p.numPartitions; case None => 0}
        println(indent + name + ": " + cacheLevel.description + " (partitioner: " + partitioner + ", " + numparts +")")
        println(indent + " |--->  Deps:    " + deps.map(d => (d, d.rdd.id) ).toString)
        println(indent + " |--->  PrefLoc: " + locs.map(x=> x.toString).mkString(", "))
        deps.foreach(d => traverseLineage(d.rdd, indent + " | ", visited))        
      }
    }
 
    println("eTable ------------------------------------------")
    traverseLineage(eTable, "  ")
    var visited = Map(eTable.id -> "eTable")

    println("\n\nvTable.index ------------------------------------")
    traverseLineage(vTable.index.rdd, "  ", visited)
    visited += (vTable.index.rdd.id -> "vTable.index")

    println("\n\nvTable.values ------------------------------------")
    traverseLineage(vTable.valuesRDD, "  ", visited)
    visited += (vTable.valuesRDD.id -> "vTable.values")

    println("\n\nvTable ------------------------------------------")
    traverseLineage(vTable, "  ", visited)
    visited += (vTable.id -> "vTable")

    println("\n\nvid2pid -----------------------------------------")
    traverseLineage(vid2pid, "  ", visited)
    visited += (vid2pid.id -> "vid2pid")
    visited += (vid2pid.valuesRDD.id -> "vid2pid.values")
    
    println("\n\nlocalVidMap -------------------------------------")
    traverseLineage(localVidMap, "  ", visited)
    visited += (localVidMap.id -> "localVidMap")
    
    println("\n\nvTableReplicatedValues --------------------------")
    traverseLineage(vTableReplicatedValues, "  ", visited)
    visited += (vTableReplicatedValues.id -> "vTableReplicatedValues")

    println("\n\ntriplets ----------------------------------------")
    traverseLineage(triplets, "  ", visited)
    println(visited)
  } // end of print lineage


  override def reverse: Graph[VD, ED] = {
    val newEtable = eTable.mapPartitions( _.map{ case (pid, epart) => (pid, epart.reverse) }, 
      preservesPartitioning = true)
    new GraphImpl(vTable, vid2pid, localVidMap, newEtable)
  }


  override def mapVertices[VD2: ClassManifest](f: (Vid, VD) => VD2): Graph[VD2, ED] = {
    val newVTable = vTable.mapValuesWithKeys((vid, data) => f(vid, data))
    new GraphImpl(newVTable, vid2pid, localVidMap, eTable)
  }

  override def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2] = {
    val newETable = eTable.mapPartitions(_.map{ case (pid, epart) => (pid, epart.map(f)) },
      preservesPartitioning = true)
    new GraphImpl(vTable, vid2pid, localVidMap, newETable)
  }


  override def mapTriplets[ED2: ClassManifest](f: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] =
    GraphImpl.mapTriplets(this, f)


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
    val newVTable = 
      VertexSetRDD(vertices.filter(v => vpred(v._1, v._2)).partitionBy(vTable.index.partitioner))


    // Restrict the set of edges to those that satisfy the vertex and the edge predicate.
    val newETable = createETable(
      triplets.filter(
        t => vpred( t.srcId, t.srcAttr ) && vpred( t.dstId, t.dstAttr ) && epred(t)
        )
        .map( t => Edge(t.srcId, t.dstId, t.attr) ))

    // Construct the Vid2Pid map. Here we assume that the filter operation 
    // behaves deterministically.  
    // @todo reindex the vertex and edge tables 
    val newVid2Pid = createVid2Pid(newETable, newVTable.index)
    val newVidMap = createLocalVidMap(newETable)

    new GraphImpl(newVTable, newVid2Pid, localVidMap, newETable)
  }


  override def groupEdgeTriplets[ED2: ClassManifest](
    f: Iterator[EdgeTriplet[VD,ED]] => ED2 ): Graph[VD,ED2] = {
      val newEdges: RDD[Edge[ED2]] = triplets.mapPartitions { partIter =>
        partIter
        // TODO(crankshaw) toList requires that the entire edge partition
        // can fit in memory right now.
        .toList
        // groups all ETs in this partition that have the same src and dst
        // Because all ETs with the same src and dst will live on the same
        // partition due to the EdgePartitioner, this guarantees that these
        // ET groups will be complete.
        .groupBy { t: EdgeTriplet[VD, ED] =>  (t.srcId, t.dstId) }
        .mapValues { ts: List[EdgeTriplet[VD, ED]] => f(ts.toIterator) }
        .toList
        .toIterator
        .map { case ((src, dst), data) => Edge(src, dst, data) }
        .toIterator
      }

      //TODO(crankshaw) eliminate the need to call createETable
      val newETable = createETable(newEdges)
      new GraphImpl(vTable, vid2pid, localVidMap, newETable)
  }


  override def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ):
    Graph[VD,ED2] = {

      val newEdges: RDD[Edge[ED2]] = edges.mapPartitions { partIter =>
        partIter.toList
        .groupBy { e: Edge[ED] => (e.srcId, e.dstId) }
        .mapValues { ts => f(ts.toIterator) }
        .toList
        .toIterator
        .map { case ((src, dst), data) => Edge(src, dst, data) }
      }
      // TODO(crankshaw) eliminate the need to call createETable
      val newETable = createETable(newEdges)

      new GraphImpl(vTable, vid2pid, localVidMap, newETable)
  }



  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassManifest](
      mapFunc: EdgeTriplet[VD, ED] => Array[(Vid, A)],
      reduceFunc: (A, A) => A)
    : VertexSetRDD[A] = 
    GraphImpl.mapReduceTriplets(this, mapFunc, reduceFunc)


  override def outerJoinVertices[U: ClassManifest, VD2: ClassManifest]
    (updates: RDD[(Vid, U)])(updateF: (Vid, VD, Option[U]) => VD2)
    : Graph[VD2, ED] = {
    ClosureCleaner.clean(updateF)
    val newVTable = vTable.leftJoin(updates)(updateF)
    new GraphImpl(newVTable, vid2pid, localVidMap, eTable)
  }


} // end of class GraphImpl






object GraphImpl {

  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)], edges: RDD[Edge[ED]],
    defaultVertexAttr: VD): 
  GraphImpl[VD,ED] = {
    apply(vertices, edges, defaultVertexAttr, (a:VD, b:VD) => a)
  }


  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)], 
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD,
    mergeFunc: (VD, VD) => VD): GraphImpl[VD,ED] = {

    val vtable = VertexSetRDD(vertices, mergeFunc) 
    /** 
     * @todo Verify that there are no edges that contain vertices 
     * that are not in vTable.  This should probably be resolved:
     *
     *  edges.flatMap{ e => Array((e.srcId, null), (e.dstId, null)) }
     *       .cogroup(vertices).map{
     *         case (vid, _, attr) => 
     *           if (attr.isEmpty) (vid, defaultValue)
     *           else (vid, attr)
     *        }
     * 
     */
    val etable = createETable(edges)
    val vid2pid = createVid2Pid(etable, vtable.index)
    val localVidMap = createLocalVidMap(etable)
    new GraphImpl(vtable, vid2pid, localVidMap, etable)
  }


  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  protected def createETable[ED: ClassManifest](edges: RDD[Edge[ED]])
    : RDD[(Pid, EdgePartition[ED])] = {
    // Get the number of partitions
    val numPartitions = edges.partitions.size
    val ceilSqrt: Pid = math.ceil(math.sqrt(numPartitions)).toInt 
    edges.map { e =>
      // Random partitioning based on the source vertex id.
      // val part: Pid = edgePartitionFunction1D(e.srcId, e.dstId, numPartitions)
      // val part: Pid = edgePartitionFunction2D(e.srcId, e.dstId, numPartitions, ceilSqrt)
      val part: Pid = randomVertexCut(e.srcId, e.dstId, numPartitions)
      //val part: Pid = canonicalEdgePartitionFunction2D(e.srcId, e.dstId, numPartitions, ceilSqrt)

      // Should we be using 3-tuple or an optimized class
      MessageToPartition(part, (e.srcId, e.dstId, e.attr))
    }
    .partitionBy(new HashPartitioner(numPartitions))
    .mapPartitionsWithIndex( (pid, iter) => {
      val builder = new EdgePartitionBuilder[ED]
      iter.foreach { message =>
        val data = message.data
        builder.add(data._1, data._2, data._3)
      }
      val edgePartition = builder.toEdgePartition
      Iterator((pid, edgePartition))
    }, preservesPartitioning = true).cache()
  }


  protected def createVid2Pid[ED: ClassManifest](
    eTable: RDD[(Pid, EdgePartition[ED])],
    vTableIndex: VertexSetIndex): VertexSetRDD[Array[Pid]] = {
    val preAgg = eTable.mapPartitions { iter =>
      val (pid, edgePartition) = iter.next()
      val vSet = new VertexSet
      edgePartition.foreach(e => {vSet.add(e.srcId); vSet.add(e.dstId)})
      vSet.iterator.map { vid => (vid.toLong, pid) }
    }
    VertexSetRDD[Pid, ArrayBuffer[Pid]](preAgg, vTableIndex, 
      (p: Pid) => ArrayBuffer(p),
      (ab: ArrayBuffer[Pid], p:Pid) => {ab.append(p); ab},
      (a: ArrayBuffer[Pid], b: ArrayBuffer[Pid]) => a ++ b)
      .mapValues(a => a.toArray).cache()
  }


  protected def createLocalVidMap[ED: ClassManifest](eTable: RDD[(Pid, EdgePartition[ED])]): 
    RDD[(Pid, VertexIdToIndexMap)] = {
    eTable.mapPartitions( _.map{ case (pid, epart) =>
      val vidToIndex = new VertexIdToIndexMap
      epart.foreach{ e => 
        vidToIndex.add(e.srcId)
        vidToIndex.add(e.dstId)
      }
      (pid, vidToIndex)
    }, preservesPartitioning = true).cache()
  }


  protected def createVTableReplicated[VD: ClassManifest](
      vTable: VertexSetRDD[VD], 
      vid2pid: VertexSetRDD[Array[Pid]],
      replicationMap: RDD[(Pid, VertexIdToIndexMap)]): 
    RDD[(Pid, Array[VD])] = {
    // Join vid2pid and vTable, generate a shuffle dependency on the joined 
    // result, and get the shuffle id so we can use it on the slave.
    val msgsByPartition = vTable.zipJoinFlatMap(vid2pid) { (vid, vdata, pids) =>
      pids.iterator.map { pid => MessageToPartition(pid, (vid, vdata)) }
    }.partitionBy(replicationMap.partitioner.get).cache()
   
    replicationMap.zipPartitions(msgsByPartition){ 
      (mapIter, msgsIter) =>
      val (pid, vidToIndex) = mapIter.next()
      assert(!mapIter.hasNext)
      // Populate the vertex array using the vidToIndex map
      val vertexArray = new Array[VD](vidToIndex.capacity)
      for (msg <- msgsIter) {
        val ind = vidToIndex.getPos(msg.data._1) & OpenHashSet.POSITION_MASK
        vertexArray(ind) = msg.data._2
      }
      Iterator((pid, vertexArray))
    }.cache()

    // @todo assert edge table has partitioner
  }


  def makeTriplets[VD: ClassManifest, ED: ClassManifest]( 
    localVidMap: RDD[(Pid, VertexIdToIndexMap)],
    vTableReplicatedValues: RDD[(Pid, Array[VD]) ],
    eTable: RDD[(Pid, EdgePartition[ED])]): RDD[EdgeTriplet[VD, ED]] = {
    localVidMap.zipPartitions(vTableReplicatedValues, eTable) {
      (vidMapIter, replicatedValuesIter, eTableIter) =>
      val (_, vidToIndex) = vidMapIter.next()
      val (_, vertexArray) = replicatedValuesIter.next()
      val (_, edgePartition) = eTableIter.next()
      new EdgeTripletIterator(vidToIndex, vertexArray, edgePartition)
    }
  }


  def mapTriplets[VD: ClassManifest, ED: ClassManifest, ED2: ClassManifest](
    g: GraphImpl[VD, ED],   
    f: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] = {
    val newETable = g.eTable.zipPartitions(g.localVidMap, g.vTableReplicatedValues){ 
      (edgePartitionIter, vidToIndexIter, vertexArrayIter) =>
      val (pid, edgePartition) = edgePartitionIter.next()
      val (_, vidToIndex) = vidToIndexIter.next()
      val (_, vertexArray) = vertexArrayIter.next()
      val et = new EdgeTriplet[VD, ED]
      val vmap = new PrimitiveKeyOpenHashMap[Vid, VD](vidToIndex, vertexArray)
      val newEdgePartition = edgePartition.map{e =>
        et.set(e)
        et.srcAttr = vmap(e.srcId)
        et.dstAttr = vmap(e.dstId)
        f(et)
      }
      Iterator((pid, newEdgePartition))
    }
    new GraphImpl(g.vTable, g.vid2pid, g.localVidMap, newETable)
  }


  def mapReduceTriplets[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
    g: GraphImpl[VD, ED],
    mapFunc: EdgeTriplet[VD, ED] => Array[(Vid, A)],
    reduceFunc: (A, A) => A): VertexSetRDD[A] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // Map and preaggregate 
    val preAgg = g.eTable.zipPartitions(g.localVidMap, g.vTableReplicatedValues){ 
      (edgePartitionIter, vidToIndexIter, vertexArrayIter) =>
      val (pid, edgePartition) = edgePartitionIter.next()
      val (_, vidToIndex) = vidToIndexIter.next()
      val (_, vertexArray) = vertexArrayIter.next()
      assert(!edgePartitionIter.hasNext)
      assert(!vidToIndexIter.hasNext)
      assert(!vertexArrayIter.hasNext)
      assert(vidToIndex.capacity == vertexArray.size)
      val vmap = new PrimitiveKeyOpenHashMap[Vid, VD](vidToIndex, vertexArray)
      // We can reuse the vidToIndex map for aggregation here as well.
      /** @todo Since this has the downside of not allowing "messages" to arbitrary
       * vertices we should consider just using a fresh map.
       */
      val msgArray = new Array[A](vertexArray.size)
      val msgBS = new BitSet(vertexArray.size)
      // Iterate over the partition
      val et = new EdgeTriplet[VD, ED]
      edgePartition.foreach{e => 
        et.set(e)
        et.srcAttr = vmap(e.srcId)
        et.dstAttr = vmap(e.dstId)
        mapFunc(et).foreach{ case (vid, msg) =>
          // verify that the vid is valid
          assert(vid == et.srcId || vid == et.dstId)
          // Get the index of the key
          val ind = vidToIndex.getPos(vid) & OpenHashSet.POSITION_MASK
          // Populate the aggregator map
          if(msgBS.get(ind)) {
            msgArray(ind) = reduceFunc(msgArray(ind), msg)
          } else { 
            msgArray(ind) = msg
            msgBS.set(ind)
          }
        }
      }
      // construct an iterator of tuples Iterator[(Vid, A)]
      msgBS.iterator.map( ind => (vidToIndex.getValue(ind), msgArray(ind)) )
    }.partitionBy(g.vTable.index.rdd.partitioner.get)
    // do the final reduction reusing the index map
    VertexSetRDD(preAgg, g.vTable.index, reduceFunc)
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
   * @todo This will only partition edges to the upper diagonal
   * of the 2D processor space.
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

