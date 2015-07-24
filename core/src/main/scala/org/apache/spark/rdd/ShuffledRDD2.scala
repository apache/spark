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

import java.util.Arrays

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
@DeveloperApi
class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {

  // TODO: validate partitionStartIndices

  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.size

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }
}

private[spark] class ShuffledRDD2Partition(
    val index: Int, val startIndexInParent: Int, val endIndexInParent: Int)
  extends Partition {

  override def hashCode(): Int = index
}

/**
 * :: DeveloperApi ::
 * A more flexible ShuffledRDD used to debug dynamic DAG work.
 *
 * @param dependency a ShuffleDependency to use
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD2[K, V, C](
    var dependency: ShuffleDependency[K, V, C],
    val partitionStartIndices: Array[Int])
    extends RDD[(K, C)](dependency.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner = {
    Some(new CoalescedPartitioner(dependency.partitioner, partitionStartIndices))
  }

  override def getPartitions: Array[Partition] = {
    val n = dependency.partitioner.numPartitions
    Array.tabulate[Partition](partitionStartIndices.length) { i =>
      val startIndex = partitionStartIndices(i)
      val endIndex = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      new ShuffledRDD2Partition(i, startIndex, endIndex)
    }
  }

  override def compute(p: Partition, context: TaskContext): Iterator[(K, C)] = {
    val sp = p.asInstanceOf[ShuffledRDD2Partition]
    SparkEnv.get.shuffleManager.getReader(
      dependency.shuffleHandle, sp.startIndexInParent, sp.endIndexInParent, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
