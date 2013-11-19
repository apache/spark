package org.apache.spark.graph.impl

import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.util.ClosureCleaner
import org.apache.spark.SparkException

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
    @transient val eTable: RDD[(Pid, EdgePartition[ED])],
    @transient val partitioner: PartitionStrategy)
  extends Graph[VD, ED] {

  def this() = this(null, null, null, null, null)

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
    def traverseLineage(
        rdd: RDD[_],
        indent: String = "",
        visited: Map[Int, String] = Map.empty[Int, String]) {
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
        println(indent + name + ": " + cacheLevel.description + " (partitioner: " + partitioner +
          ", " + numparts +")")
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
    new GraphImpl(vTable, vid2pid, localVidMap, newEtable, partitioner)
  }

  override def mapVertices[VD2: ClassManifest](f: (Vid, VD) => VD2): Graph[VD2, ED] = {
    val newVTable = vTable.mapValuesWithKeys((vid, data) => f(vid, data))
    new GraphImpl(newVTable, vid2pid, localVidMap, eTable, partitioner)
  }

  override def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2] = {
    val newETable = eTable.mapPartitions(_.map{ case (pid, epart) => (pid, epart.map(f)) },
      preservesPartitioning = true)
    new GraphImpl(vTable, vid2pid, localVidMap, newETable, partitioner)
  }

  override def mapTriplets[ED2: ClassManifest](f: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] =
    GraphImpl.mapTriplets(this, f)

  override def subgraph(epred: EdgeTriplet[VD,ED] => Boolean = (x => true),
    vpred: (Vid, VD) => Boolean = ((a,b) => true) ): Graph[VD, ED] = {
    // Reuse the partitioner (but not the index) from this graph
    val newVTable =
      VertexSetRDD(vertices.filter(v => vpred(v._1, v._2)).partitionBy(vTable.index.partitioner))
    // Restrict the set of edges to those that satisfy the vertex and the edge predicate.
    val newETable = createETable(
      triplets.filter(
        t => vpred( t.srcId, t.srcAttr ) && vpred( t.dstId, t.dstAttr ) && epred(t)
        ).map( t => Edge(t.srcId, t.dstId, t.attr) ), partitioner)
    // Construct the Vid2Pid map. Here we assume that the filter operation
    // behaves deterministically.
    // @todo reindex the vertex and edge tables
    val newVid2Pid = new Vid2Pid(newETable, newVTable.index)
    val newVidMap = createLocalVidMap(newETable)
    new GraphImpl(newVTable, newVid2Pid, localVidMap, newETable, partitioner)
  } // end of subgraph

  override def groupEdges(merge: (ED, ED) => ED ): Graph[VD,ED] = {
    ClosureCleaner.clean(merge)
    val newETable =
      eTable.mapPartitions { _.map(p => (p._1, p._2.groupEdges(merge))) }
    new GraphImpl(vTable, vid2pid, localVidMap, newETable, partitioner)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassManifest](
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(Vid, A)],
      reduceFunc: (A, A) => A)
    : VertexSetRDD[A] =
    GraphImpl.mapReduceTriplets(this, mapFunc, reduceFunc)

  override def outerJoinVertices[U: ClassManifest, VD2: ClassManifest]
    (updates: RDD[(Vid, U)])(updateF: (Vid, VD, Option[U]) => VD2)
    : Graph[VD2, ED] = {
    ClosureCleaner.clean(updateF)
    val newVTable = vTable.leftJoin(updates)(updateF)
    new GraphImpl(newVTable, vid2pid, localVidMap, eTable, partitioner)
  }
} // end of class GraphImpl


object GraphImpl {

  def apply[VD: ClassManifest, ED: ClassManifest](
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      partitionStrategy: PartitionStrategy):
    GraphImpl[VD, ED] = {
    val etable = createETable(edges).cache
    // Get the set of all vids
    val vids = etable.mapPartitions( iter => {
      val (pid, epart) = iter.next()
      assert(!iter.hasNext)
      epart.iterator.flatMap(e => Iterator(e.srcId, e.dstId))
    }, true)
    // Index the set of all vids
    val index = VertexSetRDD.makeIndex(vids)
    // Index the vertices and fill in missing attributes with the default
    val vtable = VertexSetRDD(index, defaultValue)
    val vid2pid = new Vid2Pid(etable, vtable.index)
    val localVidMap = createLocalVidMap(etable)
    new GraphImpl(vtable, vid2pid, localVidMap, etable, partitionStrategy)
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)],
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD): GraphImpl[VD,ED] = {
    apply(vertices, edges, defaultVertexAttr, (a:VD, b:VD) => a, RandomVertexCut())
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)],
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD,
    partitionStrategy: PartitionStrategy): GraphImpl[VD,ED] = {
    apply(vertices, edges, defaultVertexAttr, (a:VD, b:VD) => a, partitionStrategy)
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)],
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD,
    mergeFunc: (VD, VD) => VD): GraphImpl[VD,ED] = {
    apply(vertices, edges, defaultVertexAttr, mergeFunc, RandomVertexCut())
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
    vertices: RDD[(Vid, VD)],
    edges: RDD[Edge[ED]],
    defaultVertexAttr: VD,
    mergeFunc: (VD, VD) => VD,
    partitionStrategy: PartitionStrategy): GraphImpl[VD,ED] = {

    vertices.cache
    val etable = createETable(edges, partitionStrategy).cache
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
    new GraphImpl(vtable, vid2pid, localVidMap, etable, partitionStrategy)
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
    edges: RDD[Edge[ED]],
    partitionStrategy: PartitionStrategy): RDD[(Pid, EdgePartition[ED])] = {
    // Get the number of partitions
    val numPartitions = edges.partitions.size

    edges.map { e =>
      val part: Pid = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)

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
    val newETable = g.eTable.zipPartitions(g.localVidMap, g.vTableReplicatedValues.bothAttrs) {
      (edgePartitionIter, vidToIndexIter, vertexArrayIter) =>
      val (pid, edgePartition) = edgePartitionIter.next()
      val (_, vidToIndex) = vidToIndexIter.next()
      val (_, vertexArray) = vertexArrayIter.next()
      val et = new EdgeTriplet[VD, ED]
      val vmap = new PrimitiveKeyOpenHashMap[Vid, VD](vidToIndex, vertexArray)
      val newEdgePartition = edgePartition.map { e =>
        et.set(e)
        et.srcAttr = vmap(e.srcId)
        et.dstAttr = vmap(e.dstId)
        f(et)
      }
      Iterator((pid, newEdgePartition))
    }
    new GraphImpl(g.vTable, g.vid2pid, g.localVidMap, newETable, g.partitioner)
  }

  def mapReduceTriplets[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
      g: GraphImpl[VD, ED],
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(Vid, A)],
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
    ) {
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

  private def accessesVertexAttr[VD: ClassManifest, ED: ClassManifest](
      closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }

} // end of object GraphImpl
