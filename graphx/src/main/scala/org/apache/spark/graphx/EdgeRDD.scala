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

package org.apache.spark.graphx

import scala.reflect.ClassTag

import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.impl.EdgePartitionBuilder
import org.apache.spark.graphx.impl.EdgeRDDImpl

/**
 * `EdgeRDD[ED, VD]` extends `RDD[Edge[ED]]` by storing the edges in columnar format on each
 * partition for performance. It may additionally store the vertex attributes associated with each
 * edge to provide the triplet view. Shipping of the vertex attributes is managed by
 * `impl.ReplicatedVertexView`.
 */
abstract class EdgeRDD[ED, VD](
    @transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]]) extends RDD[Edge[ED]](sc, deps) {

  private[graphx] def partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])]

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, EdgePartition[ED, VD])].iterator(part, context)
    if (p.hasNext) {
      p.next._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2, VD]

  /**
   * Reverse all the edges in this RDD.
   *
   * @return a new EdgeRDD containing all the edges reversed
   */
  def reverse: EdgeRDD[ED, VD]

  /** Removes all edges but those matching `epred` and where both vertices match `vpred`. */
  def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgeRDD[ED, VD]

  /**
   * Inner joins this EdgeRDD with another EdgeRDD, assuming both are partitioned using the same
   * [[PartitionStrategy]].
   *
   * @param other the EdgeRDD to join with
   * @param f the join function applied to corresponding values of `this` and `other`
   * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
   *         with values supplied by `f`
   */
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgeRDD[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3, VD]

  private[graphx] def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
      f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): EdgeRDD[ED2, VD2]

  /** Replaces the edge partitions while preserving all other properties of the EdgeRDD. */
  private[graphx] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
      partitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): EdgeRDD[ED2, VD2]

  /**
   * Changes the target storage level while preserving all other properties of the
   * EdgeRDD. Operations on the returned EdgeRDD will preserve this storage level.
   *
   * This does not actually trigger a cache; to do this, call
   * [[org.apache.spark.graphx.EdgeRDD#cache]] on the returned EdgeRDD.
   */
  private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel): EdgeRDD[ED, VD]
}

object EdgeRDD {
  /**
   * Creates an EdgeRDD from a set of edges.
   *
   * @tparam ED the edge attribute type
   * @tparam VD the type of the vertex attributes that may be joined with the returned EdgeRDD
   */
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): EdgeRDD[ED, VD] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED, VD]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    EdgeRDD.fromEdgePartitions(edgePartitions)
  }

  /**
   * Creates an EdgeRDD from already-constructed edge partitions.
   *
   * @tparam ED the edge attribute type
   * @tparam VD the type of the vertex attributes that may be joined with the returned EdgeRDD
   */
  def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
      edgePartitions: RDD[(Int, EdgePartition[ED, VD])]): EdgeRDD[ED, VD] = {
    new EdgeRDDImpl(edgePartitions)
  }
}
