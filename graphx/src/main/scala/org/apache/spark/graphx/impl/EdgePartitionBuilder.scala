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

import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{SortDataFormat, Sorter, PrimitiveVector}

/** Constructs an EdgePartition from scratch. */
private[graphx]
class EdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    size: Int = 64) {
  private[this] val edges = new PrimitiveVector[Edge[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    edges += Edge(src, dst, d)
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    new Sorter(Edge.edgeArraySortDataFormat[ED])
      .sort(edgeArray, 0, edgeArray.length, Edge.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.size)
    val localDstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index. Also populate a map from vertex id to a sequential local offset.
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var currLocalId = -1
      var i = 0
      while (i < edgeArray.size) {
        val srcId = edgeArray(i).srcId
        val dstId = edgeArray(i).dstId
        localSrcIds(i) = global2local.changeValue(srcId,
          { currLocalId += 1; local2global += srcId; currLocalId }, identity)
        localDstIds(i) = global2local.changeValue(dstId,
          { currLocalId += 1; local2global += dstId; currLocalId }, identity)
        data(i) = edgeArray(i).attr
        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }

        i += 1
      }
      vertexAttrs = new Array[VD](currLocalId + 1)
    }
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global.trim().array, vertexAttrs,
      None)
  }
}

/**
 * Constructs an EdgePartition from an existing EdgePartition with the same vertex set. This enables
 * reuse of the local vertex ids. Intended for internal use in EdgePartition only.
 */
private[impl]
class ExistingEdgePartitionBuilder[
    @specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    vertexAttrs: Array[VD],
    activeSet: Option[VertexSet],
    size: Int = 64) {
  private[this] val edges = new PrimitiveVector[EdgeWithLocalIds[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, localSrc: Int, localDst: Int, d: ED) {
    edges += EdgeWithLocalIds(src, dst, localSrc, localDst, d)
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val edgeArray = edges.trim().array
    new Sorter(EdgeWithLocalIds.edgeArraySortDataFormat[ED])
      .sort(edgeArray, 0, edgeArray.length, EdgeWithLocalIds.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.size)
    val localDstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var i = 0
      while (i < edgeArray.size) {
        localSrcIds(i) = edgeArray(i).localSrcId
        localDstIds(i) = edgeArray(i).localDstId
        data(i) = edgeArray(i).attr
        if (edgeArray(i).srcId != currSrcId) {
          currSrcId = edgeArray(i).srcId
          index.update(currSrcId, i)
        }
        i += 1
      }
    }

    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet)
  }
}

private[impl] case class EdgeWithLocalIds[@specialized ED](
    srcId: VertexId, dstId: VertexId, localSrcId: Int, localDstId: Int, attr: ED)

private[impl] object EdgeWithLocalIds {
  implicit def lexicographicOrdering[ED]: Ordering[EdgeWithLocalIds[ED]] =
    new Ordering[EdgeWithLocalIds[ED]] {
      override def compare(a: EdgeWithLocalIds[ED], b: EdgeWithLocalIds[ED]): Int = {
        if (a.srcId == b.srcId) {
          if (a.dstId == b.dstId) 0
          else if (a.dstId < b.dstId) -1
          else 1
        } else if (a.srcId < b.srcId) -1
        else 1
      }
    }

  private[graphx] def edgeArraySortDataFormat[ED] = {
    new SortDataFormat[EdgeWithLocalIds[ED], Array[EdgeWithLocalIds[ED]]] {
      override def getKey(data: Array[EdgeWithLocalIds[ED]], pos: Int): EdgeWithLocalIds[ED] = {
        data(pos)
      }

      override def swap(data: Array[EdgeWithLocalIds[ED]], pos0: Int, pos1: Int): Unit = {
        val tmp = data(pos0)
        data(pos0) = data(pos1)
        data(pos1) = tmp
      }

      override def copyElement(
          src: Array[EdgeWithLocalIds[ED]], srcPos: Int,
          dst: Array[EdgeWithLocalIds[ED]], dstPos: Int) {
        dst(dstPos) = src(srcPos)
      }

      override def copyRange(
          src: Array[EdgeWithLocalIds[ED]], srcPos: Int,
          dst: Array[EdgeWithLocalIds[ED]], dstPos: Int, length: Int) {
        System.arraycopy(src, srcPos, dst, dstPos, length)
      }

      override def allocate(length: Int): Array[EdgeWithLocalIds[ED]] = {
        new Array[EdgeWithLocalIds[ED]](length)
      }
    }
  }
}
