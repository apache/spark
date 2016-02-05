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

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    // (TaskContext, rdd id, partition index, iterator)
    f: (TaskContext, Int, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  def this(prev: RDD[T],
      // (TaskContext, partition index, iterator)
      f: (TaskContext, Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean) = {
    this(prev, (t, _, p, i) => f(t, p, i), preservesPartitioning)
  }

  def this(prev: RDD[T],
      // (TaskContext, partition index, iterator)
      f: (TaskContext, Int, Iterator[T]) => Iterator[U]) = {
    this(prev, (t, _, p, i) => f(t, p, i), false)
  }

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val input = firstParent[T].iterator(split, context)
    // Set the ID of the RDD and partition being processed. We need to do this per
    // element since we chain the iterator transformations together
    val data = input.map{x => context.setRDDPartitionInfo(id, split.index); x}
    f(context, id, split.index, data)
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
