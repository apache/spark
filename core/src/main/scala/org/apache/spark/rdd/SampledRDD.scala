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
import java.util.Random

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import org.apache.spark.{Partition, TaskContext}

@deprecated("Replaced by PartitionwiseSampledRDDPartition", "1.0")
private[spark]
class SampledRDDPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

@deprecated("Replaced by PartitionwiseSampledRDD", "1.0")
class SampledRDD[T: ClassTag](
    prev: RDD[T],
    withReplacement: Boolean,
    frac: Double,
    seed: Int)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new SampledRDDPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SampledRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[SampledRDDPartition]
    if (withReplacement) {
      // For large datasets, the expected number of occurrences of each element in a sample with
      // replacement is Poisson(frac). We use that to get a count for each element.
      val poisson = new Poisson(frac, new DRand(split.seed))
      firstParent[T].iterator(split.prev, context).flatMap { element =>
        val count = poisson.nextInt()
        if (count == 0) {
          Iterator.empty  // Avoid object allocation when we return 0 items, which is quite often
        } else {
          Iterator.fill(count)(element)
        }
      }
    } else { // Sampling without replacement
      val rand = new Random(split.seed)
      firstParent[T].iterator(split.prev, context).filter(x => (rand.nextDouble <= frac))
    }
  }
}
