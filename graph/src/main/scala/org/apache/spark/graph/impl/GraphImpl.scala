package org.apache.spark.graph.impl

import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.util.ClosureCleaner

import org.apache.spark.Partitioner
import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._
import org.apache.spark.graph.impl.MsgRDDFunctions._
import org.apache.spark.graph.util.BytecodeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveKeyOpenHashMap}


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
 *
 * @param localVidMap Stores the location of vertex attributes after they are
 * replicated. Within each partition, localVidMap holds a map from vertex ID to
 * the index where that vertex's attribute is stored. This index refers to the
 * arrays in the same partition in the variants of
 * [[VTableReplicatedValues]]. Therefore, localVidMap can be reused across
 * changes to the vertex attributes.
 */
class GraphImpl[VD: ClassManifest, ED: ClassManifest] protected (
    @transient val vTable: VertexSetRDD[VD],
    @transient val vid2pid: Vid2Pid,
    @transient val localVidMap: RDD[(Pid, VertexIdToIndexMap)],
    @transient val eTable: RDD[(Pid, EdgePartition[ED])] )
  extends Graph[VD, ED] {

  def this() = this(null, null, null, null)

  @transient val vTableReplicatedValues: VTableReplicatedValues[VD] =
    new VTableReplicatedValues(vTable, vid2pid, localVidMap)

  /** Return a RDD of vertices. */
  @transient override val vertices = vTable

  /** Return a RDD of edges. */
  @transient override val edges: RDD[Edge[ED]] = {
    eTable.mapPartitions( iter => iter.next()._2.iterator , true )
  }

  /** Return a RDD that brings edges with its source and destination vertices together. */
  @transient override val triplets: RDD[EdgeTriplet[VD, ED]] =
    makeTriplets(localVidMap, vTableReplicatedValues.bothAttrs, eTable)

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    eTable.persist(newLevel)
    vid2pid.persist(newLevel)
    vTable.persist(newLevel)
    localVidMap.persist(newLevel)
    // vTableReplicatedValues.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = persist(StorageLevel.MEMORY_ONLY)

  override def statistics: Map[String, Any] = {
    val numVertices = this.numVertices
    val numEdges = this.numEdges
    val replicationRatioBothAttrs =
      vid2pid.bothAttrs.map(kv => kv._2.size).sum / numVertices
    val replicationRatioSrcAttrOnly =
      vid2pid.srcAttrOnly.map(kv => kv._2.size).sum / numVertices
    val replicationRatioDstAttrOnly =
      vid2pid.dstAttrOnly.map(kv => kv._2.size).sum / numVertices
    val loadArray =
      eTable.map{ case (pid, epart) => epart.data.size }.collect.map(x => x.toDouble / numEdges)
    val minLoad = loadArray.min
    val maxLoad = loadArray.max
    Map(
      "Num Vertices" -> numVertices, "Num Edges" -> numEdges,
      "Replication (both)" -> replicationRatioBothAttrs,
      "Replication (src only)" -> replicationRatioSrcAttrOnly,
      "Replication (dest only)" -> replicationRatioDstAttrOnly,
      "Load Array" -> loadArray,
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

    println("\n\nvid2pid.bothAttrs -------------------------------")
    traverseLineage(vid2pid.bothAttrs, "  ", visited)
    visited += (vid2pid.bothAttrs.id -> "vid2pid")
    visited += (vid2pid.bothAttrs.valuesRDD.id -> "vid2pid.bothAttrs")

    println("\n\nlocalVidMap -------------------------------------")
    traverseLineage(localVidMap, "  ", visited)
    visited += (localVidMap.id -> "localVidMap")

    println("\n\nvTableReplicatedValues.bothAttrs ----------------")
    traverseLineage(vTableReplicatedValues.bothAttrs, "  ", visited)
    visited += (vTableReplicatedValues.bothAttrs.id -> "vTableReplicatedValues.bothAttrs")

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
    val newVid2Pid = new Vid2Pid(newETable, newVTable.index)
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

    vertices.cache
    val etable = createETable(edges).cache
    // Get the set of all vids, preserving partitions
    val partitioner = Partitioner.defaultPartitioner(vertices)
    val implicitVids = etable.flatMap {
      case (pid, partition) => Array.concat(partition.srcIds, partition.dstIds)
    }.map(vid => (vid, ())).partitionBy(partitioner)
    val allVids = vertices.zipPartitions(implicitVids) {
      (a, b) => a.map(_._1) ++ b.map(_._1)
    }
    // Index the set of all vids
    val index = VertexSetRDD.makeIndex(allVids, Some(partitioner))
    // Index the vertices and fill in missing attributes with the default
    val vtable = VertexSetRDD(vertices, index, mergeFunc).fillMissing(defaultVertexAttr)

    val vid2pid = new Vid2Pid(etable, vtable.index)
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
      new MessageToPartition(part, (e.srcId, e.dstId, e.attr))
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

  private def createLocalVidMap(
      eTable: RDD[(Pid, EdgePartition[ED])] forSome { type ED }
    ): RDD[(Pid, VertexIdToIndexMap)] = {
    eTable.mapPartitions( _.map{ case (pid, epart) =>
      val vidToIndex = new VertexIdToIndexMap
      epart.foreach{ e =>
        vidToIndex.add(e.srcId)
        vidToIndex.add(e.dstId)
      }
      (pid, vidToIndex)
    }, preservesPartitioning = true).cache()
  }

  def makeTriplets[VD: ClassManifest, ED: ClassManifest](
    localVidMap: RDD[(Pid, VertexIdToIndexMap)],
    vTableReplicatedValues: RDD[(Pid, Array[VD]) ],
    eTable: RDD[(Pid, EdgePartition[ED])]): RDD[EdgeTriplet[VD, ED]] = {
    eTable.zipPartitions(localVidMap, vTableReplicatedValues) {
      (eTableIter, vidMapIter, replicatedValuesIter) =>
      val (_, vidToIndex) = vidMapIter.next()
      val (_, vertexArray) = replicatedValuesIter.next()
      val (_, edgePartition) = eTableIter.next()
      new EdgeTripletIterator(vidToIndex, vertexArray, edgePartition)
    }
  }

  def mapTriplets[VD: ClassManifest, ED: ClassManifest, ED2: ClassManifest](
    g: GraphImpl[VD, ED],
    f: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] = {
    val newETable = g.eTable.zipPartitions(g.localVidMap, g.vTableReplicatedValues.bothAttrs){
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

    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    val mapFuncUsesSrcAttr = accessesVertexAttr[VD, ED](mapFunc, "srcAttr")
    val mapFuncUsesDstAttr = accessesVertexAttr[VD, ED](mapFunc, "dstAttr")

    // Map and preaggregate
    val preAgg = g.eTable.zipPartitions(
      g.localVidMap,
      g.vTableReplicatedValues.get(mapFuncUsesSrcAttr, mapFuncUsesDstAttr)
    ){
      (edgePartitionIter, vidToIndexIter, vertexArrayIter) =>
      val (_, edgePartition) = edgePartitionIter.next()
      val (_, vidToIndex) = vidToIndexIter.next()
      val (_, vertexArray) = vertexArrayIter.next()
      assert(!edgePartitionIter.hasNext)
      assert(!vidToIndexIter.hasNext)
      assert(!vertexArrayIter.hasNext)
      assert(vidToIndex.capacity == vertexArray.size)
      // Reuse the vidToIndex map to run aggregation.
      val vmap = new PrimitiveKeyOpenHashMap[Vid, VD](vidToIndex, vertexArray)
      // TODO(jegonzal): This doesn't allow users to send messages to arbitrary vertices.
      val msgArray = new Array[A](vertexArray.size)
      val msgBS = new BitSet(vertexArray.size)
      // Iterate over the partition
      val et = new EdgeTriplet[VD, ED]

      edgePartition.foreach { e =>
        et.set(e)
        if (mapFuncUsesSrcAttr) {
          et.srcAttr = vmap(e.srcId)
        }
        if (mapFuncUsesDstAttr) {
          et.dstAttr = vmap(e.dstId)
        }
        // TODO(rxin): rewrite the foreach using a simple while loop to speed things up.
        // Also given we are only allowing zero, one, or two messages, we can completely unroll
        // the for loop.
        mapFunc(et).foreach { case (vid, msg) =>
          // verify that the vid is valid
          assert(vid == et.srcId || vid == et.dstId)
          // Get the index of the key
          val ind = vidToIndex.getPos(vid) & OpenHashSet.POSITION_MASK
          // Populate the aggregator map
          if (msgBS.get(ind)) {
            msgArray(ind) = reduceFunc(msgArray(ind), msg)
          } else {
            msgArray(ind) = msg
            msgBS.set(ind)
          }
        }
      }
      // construct an iterator of tuples Iterator[(Vid, A)]
      msgBS.iterator.map { ind =>
        new AggregationMsg[A](vidToIndex.getValue(ind), msgArray(ind))
      }
    }.partitionBy(g.vTable.index.rdd.partitioner.get)
    // do the final reduction reusing the index map
    VertexSetRDD.aggregate(preAgg, g.vTable.index, reduceFunc)
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

  private def accessesVertexAttr[VD: ClassManifest, ED: ClassManifest](
      closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }

} // end of object GraphImpl
