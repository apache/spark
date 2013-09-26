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

import org.apache.spark.{Partition, TaskContext}


/**
 * A variant of the MapPartitionsRDD that passes the partition index into the
 * closure. This can be used to generate or collect partition specific
 * information such as the number of tuples in a partition.
 */
private[spark]
class MapPartitionsWithIndexRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: (Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean
  ) extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def compute(split: Partition, context: TaskContext) =
    f(split.index, firstParent[T].iterator(split, context))
}
