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

package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD

/**
 * The [[Partition]] used by [[CachedRDD]].
 */
case class CachedRDDPartition(
    index: Int,
    originalPartitions: Array[Partition],
    @transient originalPreferredLocations: Seq[String]) extends Partition

/**
 * It wraps the real cached RDD with coalesced partitions.
 *
 * @param prev The real cached RDD
 * @param partitionSpecs the coalesced partitions
 */
class CachedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    partitionSpecs: Seq[CoalescedPartitionSpec])
  extends RDD[T](prev.context, Nil) { // Nil since we implement getDependencies

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      val spec = partitionSpecs(i)
      val originalPartitions = spec.startReducerIndex.until(spec.endReducerIndex)
        .map(prev.partitions).toArray
      val originalPreferredLocations = originalPartitions.flatMap(prev.preferredLocations)
        .distinct.toSeq
      CachedRDDPartition(i, originalPartitions, originalPreferredLocations)
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[CachedRDDPartition].originalPreferredLocations
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[CachedRDDPartition].originalPartitions.iterator.flatMap { partition =>
      firstParent[T].iterator(partition, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CachedRDDPartition].originalPartitions.map(_.index).toSeq
    })
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}
