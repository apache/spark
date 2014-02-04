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

package org.apache.spark.graphx.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl._
import org.apache.spark.graphx.impl.MsgRDDFunctions._
import org.apache.spark.graphx.util.BytecodeUtils
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ClosureCleaner


/**
 * A graph that supports computation on graphs.
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
  extends Graph[VD, ED] with Serializable {

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null, null, null)

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

  override def unpersistVertices(blocking: Boolean = true): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    replicatedVertexView.unpersist(blocking)
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    val numPartitions = edges.partitions.size
    val edTag = classTag[ED]
    val newEdges = new EdgeRDD(edges.map { e =>
      val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)

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
    GraphImpl(vertices, newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    val newETable = edges.mapEdgePartitions((pid, part) => part.reverse)
    new GraphImpl(vertices, newETable, routingTable, replicatedVertexView)
  }

  override def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2): Graph[VD2, ED] = {
    if (classTag[VD] equals classTag[VD2]) {
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = new ReplicatedVertexView[VD2](
        changedVerts, edges, routingTable,
        Some(replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2]]))
      new GraphImpl(newVerts, edges, routingTable, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), edges, routingTable)
    }
  }

  override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newETable = edges.mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, newETable , routingTable, replicatedVertexView)
  }

  override def mapTriplets[ED2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
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
      vpred: (VertexId, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
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
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None) = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    val mapUsesSrcAttr = accessesVertexAttr(mapFunc, "srcAttr")
    val mapUsesDstAttr = accessesVertexAttr(mapFunc, "dstAttr")
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
            edgePartition.indexIterator(srcVertexId => vPart.isActive(srcVertexId))
              .filter(e => vPart.isActive(e.dstId))
          } else {
            edgePartition.iterator.filter(e => vPart.isActive(e.srcId) && vPart.isActive(e.dstId))
          }
        case Some(EdgeDirection.Either) =>
          // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
          // the index here. Instead we have to scan all edges and then do the filter.
          edgePartition.iterator.filter(e => vPart.isActive(e.srcId) || vPart.isActive(e.dstId))
        case Some(EdgeDirection.Out) =>
          if (activeFraction < 0.8) {
            edgePartition.indexIterator(srcVertexId => vPart.isActive(srcVertexId))
          } else {
            edgePartition.iterator.filter(e => vPart.isActive(e.srcId))
          }
        case Some(EdgeDirection.In) =>
          edgePartition.iterator.filter(e => vPart.isActive(e.dstId))
        case _ => // None
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
      (other: RDD[(VertexId, U)])
      (updateF: (VertexId, VD, Option[U]) => VD2): Graph[VD2, ED] =
  {
    if (classTag[VD] equals classTag[VD2]) {
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF)
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = new ReplicatedVertexView[VD2](
        changedVerts, edges, routingTable,
        Some(replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2]]))
      new GraphImpl(newVerts, edges, routingTable, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      GraphImpl(newVerts, edges, routingTable)
    }
  }

  /** Test whether the closure accesses the the attribute with name `attrName`. */
  private def accessesVertexAttr(closure: AnyRef, attrName: String): Boolean = {
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
      edgePartitions: RDD[(PartitionID, EdgePartition[ED])],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    fromEdgeRDD(new EdgeRDD(edgePartitions), defaultVertexAttr)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] =
  {
    val edgeRDD = createEdgeRDD(edges).cache()

    // Get the set of all vids
    val partitioner = Partitioner.defaultPartitioner(vertices)
    val vPartitioned = vertices.partitionBy(partitioner)
    val vidsFromEdges = collectVertexIdsFromEdges(edgeRDD, partitioner)
    val vids = vPartitioned.zipPartitions(vidsFromEdges) { (vertexIter, vidsFromEdgesIter) =>
      vertexIter.map(_._1) ++ vidsFromEdgesIter.map(_._1)
    }

    val vertexRDD = VertexRDD(vids, vPartitioned, defaultVertexAttr)

    GraphImpl(vertexRDD, edgeRDD)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] = {
    // Cache RDDs that are referenced multiple times
    edges.cache()

    GraphImpl(vertices, edges, new RoutingTable(edges, vertices))
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED],
      routingTable: RoutingTable): GraphImpl[VD, ED] = {
    // Cache RDDs that are referenced multiple times. `routingTable` is cached by default, so we
    // don't cache it explicitly.
    vertices.cache()
    edges.cache()

    new GraphImpl(
      vertices, edges, routingTable, new ReplicatedVertexView(vertices, edges, routingTable))
  }

  /**
   * Create the edge RDD, which is much more efficient for Java heap storage than the normal edges
   * data structure (RDD[(VertexId, VertexId, ED)]).
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
    val vids = collectVertexIdsFromEdges(edges, new HashPartitioner(edges.partitions.size))
    // Create the VertexRDD.
    val vertices = VertexRDD(vids.mapValues(x => defaultVertexAttr))
    GraphImpl(vertices, edges)
  }

  /** Collects all vids mentioned in edges and partitions them by partitioner. */
  private def collectVertexIdsFromEdges(
      edges: EdgeRDD[_],
      partitioner: Partitioner): RDD[(VertexId, Int)] = {
    // TODO: Consider doing map side distinct before shuffle.
    new ShuffledRDD[VertexId, Int, (VertexId, Int)](
      edges.collectVertexIds.map(vid => (vid, 0)), partitioner)
      .setSerializer(classOf[VertexIdMsgSerializer].getName)
  }
} // end of object GraphImpl
