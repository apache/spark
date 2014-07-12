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

package org.apache.spark.mllib.rdd

import org.apache.spark.mllib.stat.Distribution
import org.apache.spark.util.Utils
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

private[mllib] class RandomRDDPartition(val idx: Int,
    val size: Long,
    val distribution: Distribution,
    val seed: Long)
  extends Partition with Serializable {

  override val index: Int = idx

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getIterator = {
    val newRng = distribution.copy()
    newRng.setSeed(seed + idx)
    new FixedSizeIterator(size, newRng)
  }
}

private[mllib] class FixedSizeIterator(override val size: Long, val rng: Distribution)
  extends Iterator[Double] {

  private var currentSize = 0

  override def hasNext: Boolean = currentSize < size

  override def next(): Double = {
    currentSize += 1
    rng.nextDouble()
  }
}

private[mllib] class RandomRDD(@transient private var sc: SparkContext,
    numSlices: Int,
    size: Long,
    @transient distribution: Distribution,
    @transient seed: Long = Utils.random.nextLong) extends RDD[Double](sc, Nil) {

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Double] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition]
    split.getIterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partitionSize = size / numSlices
    val firstPartitionSize = numSlices - partitionSize * numSlices
    val partitions = new Array[RandomRDDPartition](numSlices)
    var i = 0
    while (i < numSlices) {
      partitions(i) =  if (i == 0) {
        new RandomRDDPartition(i, firstPartitionSize, distribution, seed)
      } else {
        new RandomRDDPartition(i, partitionSize, distribution, seed)
      }
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }
}
