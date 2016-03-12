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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.util.collection.Utils

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val input = firstParent[T].iterator(split, context)
    // Use -1 for shuffle id since we are not in a shuffle.
    val shuffleId = -1
    // Set the ID of the RDD and partition being processed. We need to do this per
    // element since we chain the iterator transformations together
    val data = input.map{x =>
      context.setRDDPartitionInfo(id, shuffleId, split.index)
      x
    }
    val wrappedData = Utils.signalWhenEmpty(data,
      () => context.taskMetrics.markFullyProcessed(id, shuffleId, split.index))
    // We also set it before the first call to the user function in case the user provides a
    // function which access a consistent accumulator before accessing any elements.
    context.setRDDPartitionInfo(id, shuffleId, split.index)
    f(context, split.index, wrappedData)
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
