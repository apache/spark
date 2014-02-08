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

import java.util.Random

import scala.reflect.ClassTag

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.util.random.RandomSampler

private[spark]
class PartitionwiseSampledRDDPartition(val prev: Partition, val seed: Long)
  extends Partition with Serializable {
  override val index: Int = prev.index
}

/**
 * A RDD sampled from its parent RDD partition-wise. For each partition of the parent RDD,
 * a user-specified [[org.apache.spark.util.random.RandomSampler]] instance is used to obtain
 * a random sample of the records in the partition. The random seeds assigned to the samplers
 * are guaranteed to have different values.
 *
 * @param prev RDD to be sampled
 * @param sampler a random sampler
 * @param seed random seed, default to System.nanoTime
 * @tparam T input RDD item type
 * @tparam U sampled RDD item type
 */
class PartitionwiseSampledRDD[T: ClassTag, U: ClassTag](
    prev: RDD[T],
    sampler: RandomSampler[T, U],
    seed: Long = System.nanoTime)
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = {
    val random = new Random(seed)
    firstParent[T].partitions.map(x => new PartitionwiseSampledRDDPartition(x, random.nextLong()))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[PartitionwiseSampledRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[U] = {
    val split = splitIn.asInstanceOf[PartitionwiseSampledRDDPartition]
    val thisSampler = sampler.clone
    thisSampler.setSeed(split.seed)
    thisSampler.sample(firstParent[T].iterator(split.prev, context))
  }
}
