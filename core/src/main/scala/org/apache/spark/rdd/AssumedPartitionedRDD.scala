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

import org.apache.spark.{SparkException, TaskContext, Partition, Partitioner}

import scala.reflect.ClassTag

private[spark] class AssumedPartitionedRDD[K: ClassTag, V: ClassTag](
    parent: RDD[(K,V)],
    part: Partitioner,
    val verify: Boolean
  ) extends RDD[(K,V)](parent) {

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = firstParent[(K,V)].partitions

  if(verify && getPartitions.size != part.numPartitions) {
    throw new SparkException(s"Assumed Partitioner $part expects ${part.numPartitions} " +
      s"partitions, but there are ${getPartitions.size} partitions.  If you are assuming a" +
      s" partitioner on a HadoopRDD, you might need to disable input splits with a custom input" +
      s" format")
  }

  override def compute(split: Partition, context: TaskContext) = {
    if (verify) {
      firstParent[(K,V)].iterator(split, context).map{ case(k,v) =>
        if (partitioner.get.getPartition(k) != split.index) {
          throw new SparkException(s"key $k in split ${split.index} was not in the assumed " +
            s"partition.  If you are assuming a partitioner on a HadoopRDD, you might need to " +
            s"disable input splits with a custom input format")
        }
        (k,v)
      }
    } else {
      firstParent[(K,V)].iterator(split, context)
    }
  }

}
