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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.random.DistributionGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

private[mllib] class RandomRDDPartition(val idx: Int,
    val size: Long,
    val rng: DistributionGenerator,
    val seed: Long) extends Partition {

  override val index: Int = idx

}

// These two classes are necessary since Range objects in Scala cannot have size > Int.MaxValue
private[mllib] class RandomRDD(@transient private var sc: SparkContext,
    size: Long,
    numSlices: Int,
    @transient rng: DistributionGenerator,
    @transient seed: Long = Utils.random.nextLong) extends RDD[Double](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numSlices > 0, "Positive number of partitions required")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Double] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition]
    RandomRDD.getPointIterator(split)
  }

  override def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(size, numSlices, rng, seed)
  }
}

private[mllib] class RandomVectorRDD(@transient private var sc: SparkContext,
    size: Long,
    vectorSize: Int,
    numSlices: Int,
    @transient rng: DistributionGenerator,
    @transient seed: Long = Utils.random.nextLong) extends RDD[Vector](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numSlices > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Vector] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition]
    RandomRDD.getVectorIterator(split, vectorSize)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(size, numSlices, rng, seed)
  }
}

private[mllib] object RandomRDD {

  private[mllib] class FixedSizePointIterator(val numElem: Long, val rng: DistributionGenerator)
    extends Iterator[Double] {

    private var currentSize = 0

    override def hasNext: Boolean = currentSize < numElem

    override def next(): Double = {
      currentSize += 1
      rng.nextValue()
    }
  }

  private[mllib] class FixedSizeVectorIterator(val numElem: Long,
      val vectorSize: Int,
      val rng: DistributionGenerator)
    extends Iterator[Vector] {

    private var currentSize = 0

    override def hasNext: Boolean = currentSize < numElem

    override def next(): Vector = {
      currentSize += 1
      new DenseVector((0 until vectorSize).map { _ => rng.nextValue() }.toArray)
    }
  }

  def getPartitions(size: Long,
      numSlices: Int,
      rng: DistributionGenerator,
      seed: Long): Array[Partition] = {

    val firstPartitionSize = size / numSlices + size % numSlices
    val otherPartitionSize = size / numSlices

    val partitions = new Array[RandomRDDPartition](numSlices)
    var i = 0
    while (i < numSlices) {
      partitions(i) =  if (i == 0) {
        new RandomRDDPartition(i, firstPartitionSize, rng, seed)
      } else {
        new RandomRDDPartition(i, otherPartitionSize, rng.newInstance(), seed)
      }
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getPointIterator(partition: RandomRDDPartition): Iterator[Double] = {
    partition.rng.setSeed(partition.seed + partition.index)
    new FixedSizePointIterator(partition.size, partition.rng)
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getVectorIterator(partition: RandomRDDPartition, vectorSize: Int): Iterator[Vector] = {
    partition.rng.setSeed(partition.seed + partition.index)
    new FixedSizeVectorIterator(partition.size, vectorSize, partition.rng)
  }
}
