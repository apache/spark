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

import org.mockito.cglib.util.ParallelSorter

import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

private[graphx]
class EdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    size: Int = 64) {
  var srcIds = new PrimitiveVector[VertexId](size)
  var dstIds = new PrimitiveVector[VertexId](size)
  var data = new PrimitiveVector[ED](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    srcIds += src
    dstIds += dst
    data += d
  }

  def toEdgePartition: EdgePartition[ED, VD] = {
    val srcIdsTrim = srcIds.trim().array
    val dstIdsTrim = dstIds.trim().array
    val dataTrim = data.trim().array

    // Sort the three arrays in parallel by (srcId, dstId)
    val arrays = Array[Object](srcIdsTrim, dstIdsTrim, dataTrim)
    val sorter = ParallelSorter.create(arrays)
    sorter.quickSort(1, 0, srcIdsTrim.length) // necessary for groupEdges
    sorter.mergeSort(0, 0, srcIdsTrim.length) // preserves dstId sort order

    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    if (srcIdsTrim.length > 0) {
      index.update(srcIds(0), 0)
      var currSrcId: VertexId = srcIds(0)
      var i = 0
      while (i < srcIdsTrim.size) {
        if (srcIdsTrim(i) != currSrcId) {
          currSrcId = srcIdsTrim(i)
          index.update(currSrcId, i)
        }
        i += 1
      }
    }

    // Create and populate a VertexPartition with vids from the edges, but no attributes
    val vidsIter = srcIdsTrim.iterator ++ dstIdsTrim.iterator
    val vertexIds = new OpenHashSet[VertexId]
    vidsIter.foreach(vid => vertexIds.add(vid))
    val vertices = new VertexPartition(
      vertexIds, new Array[VD](vertexIds.capacity), vertexIds.getBitSet)

    new EdgePartition(srcIdsTrim, dstIdsTrim, dataTrim, index, vertices)
  }
}
