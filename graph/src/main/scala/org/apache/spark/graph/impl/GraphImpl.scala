package org.apache.spark.graph.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.impl.GraphImpl._
import org.apache.spark.graph.impl.MsgRDDFunctions._
import org.apache.spark.graph.util.BytecodeUtils
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ClosureCleaner


/**
 * A Graph RDD that supports computation on graphs.
 *
 * Graphs are represented using two classes of data: vertex-partitioned and
 * edge-partitioned. `vertices` contains vertex attributes, which are vertex-partitioned. `edges`
 * contains edge attributes, which are edge-partitioned. For operations on vertex neighborhoods,
 * vertex attributes are replicated to the edge partitions where they appear as sources or
 * destinations. `routingTable` stores the routing information for shipping vertex attributes to
 * edge partitions. `replicatedVertexView` stores a view of the replicated vertex attributes created
 * using the routing table.
 */
class GraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val vertices: VertexRDD[VD],
    @transient val edges: EdgeRDD[ED],
    @transient val routingTable: RoutingTable,
    @transient val replicatedVertexView: ReplicatedVertexView[VD])
  extends Graph[VD, ED] {

  def this(
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED],
      routingTable: RoutingTable) = {
    this(vertices, edges, routingTable, new ReplicatedVertexView(vertices, edges, routingTable))
  }

  def this(
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]) = {
    this(vertices, edges, new RoutingTable(edges, vertices))
  }

  /** Return a RDD that brings edges together with their source and destination vertices. */
  @transient override val triplets: RDD[EdgeTriplet[VD, ED]] = {
    val vdTag = classTag[VD]
    val edTag = classTag[ED]
    edges.partitionsRDD.zipPartitions(
      replicatedVertexView.get(true, true), true) { (ePartIter, vPartIter) =>
      val (pid, ePart) = ePartIter.next()
      val (_, vPart) = vPartIter.next()
      new EdgeTripletIterator(vPart.index, vPart.values, ePart)(vdTag, edTag)
    }
  }

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = persist(StorageLevel.MEMORY_ONLY)

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    val numPartitions = edges.partitions.size
    val edTag = classTag[ED]
    val newEdges = new EdgeRDD(edges.map { e =>
      val part: Pid = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)

      // Should we be using 3-tuple or an optimized class
      new MessageToPartition(part, (e.srcId, e.dstId, e.attr))
    }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex( { (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED]()(edTag)
        iter.foreach { message =>
          val data = message.data
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true).cache())
    new GraphImpl(vertices, newEdges)
  }

  override def statistics: Map[String, Any] = {
    // Get the total number of vertices after replication, used to compute the replication ratio.
    def numReplicatedVertices(vid2pids: RDD[Array[Array[Vid]]]): Double = {
      vid2pids.map(_.map(_.size).sum.toLong).reduce(_ + _).toDouble
    }

    val numVertices = this.ops.numVertices
    val numEdges = this.ops.numEdges
    val replicationRatioBoth = numReplicatedVertices(routingTable.bothAttrs) / numVertices
    val replicationRatioSrcOnly = numReplicatedVertices(routingTable.srcAttrOnly) / numVertices
    val replicationRatioDstOnly = numReplicatedVertices(routingTable.dstAttrOnly) / numVertices
    // One entry for each partition, indicate the total number of edges on that partition.
    val loadArray = edges.partitionsRDD.map(_._2.size).collect().map(_.toDouble / numEdges)
    val minLoad = loadArray.min
    val maxLoad = loadArray.max
    Map(
      "Num Vertices" -> numVertices,
      "Num Edges" -> numEdges,
      "Replication (both)" -> replicationRatioBoth,
      "Replication (src only)" -> replicationRatioSrcOnly,
      "Replication (dest only)" -> replicationRatioDstOnly,
      "Load Array" -> loadArray,
      "Min Load" -> minLoad,
      "Max Load" -> maxLoad)
  }

  /**
   * Display the lineage information for this graph.
   */
  def printLineage() = {
    def traverseLineage(
        rdd: RDD[_],
        indent: String = "",
        visited: Map[Int, String] = Map.empty[Int, String]) {
      if (visited.contains(rdd.id)) {
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
    println("edges ------------------------------------------")
    traverseLineage(edges, "  ")
    var visited = Map(edges.id -> "edges")
    println("\n\nvertices ------------------------------------------")
    traverseLineage(vertices, "  ", visited)
    visited += (vertices.id -> "vertices")
    println("\n\nroutingTable.bothAttrs -------------------------------")
    traverseLineage(routingTable.bothAttrs, "  ", visited)
    visited += (routingTable.bothAttrs.id -> "routingTable.bothAttrs")
    println("\n\ntriplets ----------------------------------------")
    traverseLineage(triplets, "  ", visited)
    println(visited)
  } // end of printLineage

  override def reverse: Graph[VD, ED] = {
    val newETable = edges.mapEdgePartitions((pid, part) => part.reverse)
    new GraphImpl(vertices, newETable, routingTable, replicatedVertexView)
  }

  override def mapVertices[VD2: ClassTag](f: (Vid, VD) => VD2): Graph[VD2, ED] = {
    if (classTag[VD] equals classTag[VD2]) {
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f))
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = new ReplicatedVertexView[VD2](
        changedVerts, edges, routingTable,
        Some(replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2]]))
      new GraphImpl(newVerts, edges, routingTable, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      new GraphImpl(vertices.mapVertexPartitions(_.map(f)), edges, routingTable)
    }
  }

  override def mapEdges[ED2: ClassTag](
      f: (Pid, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newETable = edges.mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, newETable , routingTable, replicatedVertexView)
  }

  override def mapTriplets[ED2: ClassTag](
      f: (Pid, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    // Use an explicit manifest in PrimitiveKeyOpenHashMap init so we don't pull in the implicit
    // manifest from GraphImpl (which would require serializing GraphImpl).
    val vdTag = classTag[VD]
    val newEdgePartitions =
      edges.partitionsRDD.zipPartitions(replicatedVertexView.get(true, true), true) {
        (ePartIter, vTableReplicatedIter) =>
        val (ePid, edgePartition) = ePartIter.next()
        val (vPid, vPart) = vTableReplicatedIter.next()
        assert(!vTableReplicatedIter.hasNext)
        assert(ePid == vPid)
        val et = new EdgeTriplet[VD, ED]
        val inputIterator = edgePartition.iterator.map { e =>
          et.set(e)
          et.srcAttr = vPart(e.srcId)
          et.dstAttr = vPart(e.dstId)
          et
        }
        // Apply the user function to the vertex partition
        val outputIter = f(ePid, inputIterator)
        // Consume the iterator to update the edge attributes
        val newEdgePartition = edgePartition.map(outputIter)
        Iterator((ePid, newEdgePartition))
      }
    new GraphImpl(vertices, new EdgeRDD(newEdgePartitions), routingTable, replicatedVertexView)
  }

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = x => true,
      vpred: (Vid, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))

    // Filter the edges
    val edTag = classTag[ED]
    val newEdges = new EdgeRDD[ED](triplets.filter { et =>
      vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)
    }.mapPartitionsWithIndex( { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED]()(edTag)
      iter.foreach { et => builder.add(et.srcId, et.dstId, et.attr) }
      val edgePartition = builder.toEdgePartition
      Iterator((pid, edgePartition))
    }, preservesPartitioning = true)).cache()

    // Reuse the previous ReplicatedVertexView unmodified. The replicated vertices that have been
    // removed will be ignored, since we only refer to replicated vertices when they are adjacent to
    // an edge.
    new GraphImpl(newVerts, newEdges, new RoutingTable(newEdges, newVerts), replicatedVertexView)
  } // end of subgraph

  override def mask[VD2: ClassTag, ED2: ClassTag] (
      other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    // Reuse the previous ReplicatedVertexView unmodified. The replicated vertices that have been
    // removed will be ignored, since we only refer to replicated vertices when they are adjacent to
    // an edge.
    new GraphImpl(newVerts, newEdges, routingTable, replicatedVertexView)
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    ClosureCleaner.clean(merge)
    val newETable = edges.mapEdgePartitions((pid, part) => part.groupEdges(merge))
    new GraphImpl(vertices, newETable, routingTable, replicatedVertexView)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassTag](
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(Vid, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None) = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    val mapUsesSrcAttr = accessesVertexAttr[VD, ED](mapFunc, "srcAttr")
    val mapUsesDstAttr = accessesVertexAttr[VD, ED](mapFunc, "dstAttr")
    val vs = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.get(mapUsesSrcAttr, mapUsesDstAttr, activeSet)
      case None =>
        replicatedVertexView.get(mapUsesSrcAttr, mapUsesDstAttr)
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = edges.partitionsRDD.zipPartitions(vs, true) { (ePartIter, vPartIter) =>
      val (ePid, edgePartition) = ePartIter.next()
      val (vPid, vPart) = vPartIter.next()
      assert(!vPartIter.hasNext)
      assert(ePid == vPid)
      // Choose scan method
      val activeFraction = vPart.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
      val edgeIter = activeDirectionOpt match {
        case Some(EdgeDirection.Both) =>
          if (activeFraction < 0.8) {
            edgePartition.indexIterator(srcVid => vPart.isActive(srcVid))
              .filter(e => vPart.isActive(e.dstId))
          } else {
            edgePartition.iterator.filter(e => vPart.isActive(e.srcId) && vPart.isActive(e.dstId))
          }
        case Some(EdgeDirection.Out) =>
          if (activeFraction < 0.8) {
            edgePartition.indexIterator(srcVid => vPart.isActive(srcVid))
          } else {
            edgePartition.iterator.filter(e => vPart.isActive(e.srcId))
          }
        case Some(EdgeDirection.In) =>
          edgePartition.iterator.filter(e => vPart.isActive(e.dstId))
        case None =>
          edgePartition.iterator
      }

      // Scan edges and run the map function
      val et = new EdgeTriplet[VD, ED]
      val mapOutputs = edgeIter.flatMap { e =>
        et.set(e)
        if (mapUsesSrcAttr) {
          et.srcAttr = vPart(e.srcId)
        }
        if (mapUsesDstAttr) {
          et.dstAttr = vPart(e.dstId)
        }
        mapFunc(et)
      }
      // Note: This doesn't allow users to send messages to arbitrary vertices.
      vPart.aggregateUsingIndex(mapOutputs, reduceFunc).iterator
    }

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, reduceFunc)
  } // end of mapReduceTriplets

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
      (updates: RDD[(Vid, U)])(updateF: (Vid, VD, Option[U]) => VD2): Graph[VD2, ED] = {
    if (classTag[VD] equals classTag[VD2]) {
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(updates)(updateF)
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = new ReplicatedVertexView[VD2](
        changedVerts, edges, routingTable,
        Some(replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2]]))
      new GraphImpl(newVerts, edges, routingTable, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(updates)(updateF)
      new GraphImpl(newVerts, edges, routingTable)
    }
  }

  private def accessesVertexAttr[VD, ED](closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }
} // end of class GraphImpl


object GraphImpl {

  def apply[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] =
  {
    fromEdgeRDD(createEdgeRDD(edges), defaultVertexAttr)
  }

  def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(Pid, EdgePartition[ED])],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    fromEdgeRDD(new EdgeRDD(edgePartitions), defaultVertexAttr)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(Vid, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] =
  {
    val edgeRDD = createEdgeRDD(edges).cache()

    // Get the set of all vids
    val partitioner = Partitioner.defaultPartitioner(vertices)
    val vPartitioned = vertices.partitionBy(partitioner)
    val vidsFromEdges = collectVidsFromEdges(edgeRDD, partitioner)
    val vids = vPartitioned.zipPartitions(vidsFromEdges) { (vertexIter, vidsFromEdgesIter) =>
      vertexIter.map(_._1) ++ vidsFromEdgesIter.map(_._1)
    }

    val vertexRDD = VertexRDD(vids, vPartitioned, defaultVertexAttr)

    new GraphImpl(vertexRDD, edgeRDD)
  }

  /**
   * Create the edge RDD, which is much more efficient for Java heap storage than the normal edges
   * data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge RDD contains multiple partitions, and each partition contains only one RDD key-value
   * pair: the key is the partition id, and the value is an EdgePartition object containing all the
   * edges in a partition.
   */
  private def createEdgeRDD[ED: ClassTag](
      edges: RDD[Edge[ED]]): EdgeRDD[ED] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    new EdgeRDD(edgePartitions)
  }

  private def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edges: EdgeRDD[ED],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    edges.cache()
    // Get the set of all vids
    val vids = collectVidsFromEdges(edges, new HashPartitioner(edges.partitions.size))
    // Create the VertexRDD.
    val vertices = VertexRDD(vids.mapValues(x => defaultVertexAttr))
    new GraphImpl(vertices, edges)
  }

  /** Collects all vids mentioned in edges and partitions them by partitioner. */
  private def collectVidsFromEdges(
      edges: EdgeRDD[_],
      partitioner: Partitioner): RDD[(Vid, Int)] = {
    // TODO: Consider doing map side distinct before shuffle.
    new ShuffledRDD[Vid, Int, (Vid, Int)](
      edges.collectVids.map(vid => (vid, 0)), partitioner)
      .setSerializer(classOf[VidMsgSerializer].getName)
  }
} // end of object GraphImpl
