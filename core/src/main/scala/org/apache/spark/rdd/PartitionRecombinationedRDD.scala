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

import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}

/**
 * A RDD that just redistribute the dependency RDD's partitions.
 * It provides the capability to reorder, remove, duplicate... any RDD partition level operation.
 */
class RecombinationedRDD[T: ClassTag](
    prev: RDD[T],
    f: (Seq[Int] => Seq[Int])) extends RDD[T](prev) {
  private val recombinationed = {
    val partitionIndexes = prev.partitions.indices
    f(partitionIndexes)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    prev.iterator(split.asInstanceOf[RecombinationedPartition].parentPartition, context)
  }

  override protected def getPartitions: Array[Partition] = {
    recombinationed.zipWithIndex.map { case (parentIndex, index) =>
      RecombinationedPartition(index, prev.partitions(parentIndex))
    }.toArray
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] = List(recombinationed(id))
    }
  )
}

case class RecombinationedPartition(index: Int, parentPartition: Partition) extends Partition
