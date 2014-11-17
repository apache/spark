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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._

/**
 * Manages shipping vertex attributes to the edge partitions of an
 * [[org.apache.spark.graphx.EdgeRDD]]. Vertex attributes may be partially shipped to construct a
 * triplet view with vertex attributes on only one side, and they may be updated. An active vertex
 * set may additionally be shipped to the edge partitions. Be careful not to store a reference to
 * `edges`, since it may be modified when the attribute shipping level is upgraded.
 */
private[impl]
class ReplicatedVertexView[VD: ClassTag, ED: ClassTag](
    var edges: EdgeRDDImpl[ED, VD],
    var hasSrcId: Boolean = false,
    var hasDstId: Boolean = false) {

  /**
   * Return a new `ReplicatedVertexView` with the specified `EdgeRDD`, which must have the same
   * shipping level.
   */
  def withEdges[VD2: ClassTag, ED2: ClassTag](
      edges_ : EdgeRDDImpl[ED2, VD2]): ReplicatedVertexView[VD2, ED2] = {
    new ReplicatedVertexView(edges_, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where edges are reversed and shipping levels are swapped to
   * match.
   */
  def reverse() = {
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    new ReplicatedVertexView(newEdges, hasDstId, hasSrcId)
  }

  /**
   * Upgrade the shipping level in-place to the specified levels by shipping vertex attributes from
   * `vertices`. This operation modifies the `ReplicatedVertexView`, and callers can access `edges`
   * afterwards to obtain the upgraded view.
   */
  def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean, includeDst: Boolean) {
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst)
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get)
      val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) => ePartIter.map {
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }

  /**
   * Return a new `ReplicatedVertexView` where the `activeSet` in each edge partition contains only
   * vertex ids present in `actives`. This ships a vertex id to all edge partitions where it is
   * referenced, ignoring the attribute shipping level.
   */
  def withActiveSet(actives: VertexRDD[_]): ReplicatedVertexView[VD, ED] = {
    val shippedActives = actives.shipVertexIds()
      .setName("ReplicatedVertexView.withActiveSet - shippedActives (broadcast)")
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedActives) {
      (ePartIter, shippedActivesIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.withActiveSet(shippedActivesIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where vertex attributes in edge partition are updated using
   * `updates`. This ships a vertex attribute only to the edge partitions where it is in the
   * position(s) specified by the attribute shipping level.
   */
  def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] = {
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
}
