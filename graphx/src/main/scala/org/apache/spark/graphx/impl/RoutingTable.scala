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

import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.PrimitiveVector

/**
 * Stores the locations of edge-partition join sites for each vertex attribute; that is, the routing
 * information for shipping vertex attributes to edge partitions. This is always cached because it
 * may be used multiple times in ReplicatedVertexView -- once to ship the vertex attributes and
 * (possibly) once to ship the active-set information.
 */
private[impl]
class RoutingTable(edges: EdgeRDD[_], vertices: VertexRDD[_]) {

  val bothAttrs: RDD[Array[Array[VertexId]]] = createPid2Vid(true, true)
  val srcAttrOnly: RDD[Array[Array[VertexId]]] = createPid2Vid(true, false)
  val dstAttrOnly: RDD[Array[Array[VertexId]]] = createPid2Vid(false, true)
  val noAttrs: RDD[Array[Array[VertexId]]] = createPid2Vid(false, false)

  def get(includeSrcAttr: Boolean, includeDstAttr: Boolean): RDD[Array[Array[VertexId]]] =
    (includeSrcAttr, includeDstAttr) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }

  private def createPid2Vid(
      includeSrcAttr: Boolean, includeDstAttr: Boolean): RDD[Array[Array[VertexId]]] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val vid2pid: RDD[(VertexId, PartitionID)] = edges.partitionsRDD.mapPartitions { iter =>
      val (pid: PartitionID, edgePartition: EdgePartition[_]) = iter.next()
      val numEdges = edgePartition.size
      val vSet = new VertexSet
      if (includeSrcAttr) {  // Add src vertices to the set.
        var i = 0
        while (i < numEdges) {
          vSet.add(edgePartition.srcIds(i))
          i += 1
        }
      }
      if (includeDstAttr) {  // Add dst vertices to the set.
      var i = 0
        while (i < numEdges) {
          vSet.add(edgePartition.dstIds(i))
          i += 1
        }
      }
      vSet.iterator.map { vid => (vid, pid) }
    }

    val numPartitions = vertices.partitions.size
    vid2pid.partitionBy(vertices.partitioner.get).mapPartitions { iter =>
      val pid2vid = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
      for ((vid, pid) <- iter) {
        pid2vid(pid) += vid
      }

      Iterator(pid2vid.map(_.trim().array))
    }.cache().setName("RoutingTable %s %s".format(includeSrcAttr, includeDstAttr))
  }
}
