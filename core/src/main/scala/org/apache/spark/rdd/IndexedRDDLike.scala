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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import IndexedRDD.Id
import IndexedRDDFunctions.rdd2IndexedRDDFunctions

/**
 * Contains members that are shared among all variants of IndexedRDD (e.g., IndexedRDD,
 * VertexRDD).
 *
 * @tparam V the type of the values stored in the IndexedRDD
 * @tparam P the type of the partitions making up the IndexedRDD
 */
private[spark] trait IndexedRDDLike[
    @specialized(Long, Int, Double) V,
    P[X] <: IndexedRDDPartitionBase[X] with IndexedRDDPartitionOps[X, P]]
  extends RDD[(Id, V)] {

  /** The underlying representation of the IndexedRDD as an RDD of partitions. */
  def partitionsRDD: RDD[P[V]]
  require(partitionsRDD.partitioner.isDefined)

  /** A generator for ClassTags of the partition type P. */
  implicit def pTag[V2]: ClassTag[P[V2]]

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** Provides the `RDD[(Id, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[(Id, V)] = {
    firstParent[P[V]].iterator(part, context).next.iterator
  }
}
