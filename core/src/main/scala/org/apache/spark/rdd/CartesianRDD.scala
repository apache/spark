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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, Partitioner, ShuffleDependency, SparkContext,
    SparkEnv, TaskContext}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

private[spark] class CartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var rdd2 : RDD[U])
  extends RDD[(T, U)](sc, Nil)
  with Serializable {

  var rdd1WithPid = rdd1.mapPartitionsWithIndex((pid, iter) => iter.map(x => (pid, x)))
  var rdd2WithPid = rdd2.mapPartitionsWithIndex((pid, iter) => iter.map(x => (pid, x)))

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  private def getParititionIterator[T: ClassTag](
      dependency: ShuffleDependency[Int, T, T], partitionIndex: Int, context: TaskContext) = {
    SparkEnv.get.shuffleManager
      .getReader[Int, T](dependency.shuffleHandle, partitionIndex, partitionIndex + 1, context)
      .read().map(x => x._2)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    for (x <- getParititionIterator[T](dependencies(0).asInstanceOf[ShuffleDependency[Int, T, T]],
          currSplit.s1.index, context);
         y <- getParititionIterator[U](dependencies(1).asInstanceOf[ShuffleDependency[Int, U, U]],
           currSplit.s2.index, context))
      yield (x, y)
  }

  private def getShufflePartitioner(numParts: Int): Partitioner = {
    return new Partitioner {
      require(numPartitions > 0)
      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      override def numPartitions: Int = numParts
    }
  }

  private var serializer: Serializer = SparkEnv.get.serializer

  override def getDependencies: Seq[Dependency[_]] = List(
    new ShuffleDependency[Int, T, T](
      rdd1WithPid.asInstanceOf[RDD[_ <: Product2[Int, T]]],
      getShufflePartitioner(rdd1WithPid.getNumPartitions), serializer),
    new ShuffleDependency[Int, U, U](
      rdd2WithPid.asInstanceOf[RDD[_ <: Product2[Int, U]]],
      getShufflePartitioner(rdd2WithPid.getNumPartitions), serializer)
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd1WithPid = null
    rdd2WithPid = null
  }
}
