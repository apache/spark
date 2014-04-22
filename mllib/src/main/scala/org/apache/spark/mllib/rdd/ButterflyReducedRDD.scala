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

import scala.reflect.ClassTag

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

private case class ButterflyReducedRDDPartition(
    override val index: Int,
    _1: Partition,
    _2: Partition) extends Partition

private[spark] class ButterflyReducedRDD[T: ClassTag](
    rdd: RDD[T],
    offset: Int,
    reducer: (T, T) => T) extends RDD[T](rdd) {

  val numPartitions = rdd.partitions.size

  private def targetPartition(i: Int): Int = {
    (i + offset) % numPartitions
  }

  override def getPartitions: Array[Partition] = {
    rdd.partitions.zipWithIndex.map { case (part, i) =>
      ButterflyReducedRDDPartition(i, part, rdd.partitions(targetPartition(i)))
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val pair = s.asInstanceOf[ButterflyReducedRDDPartition]
    Iterator((rdd.iterator(pair._1, context) ++ rdd.iterator(pair._2, context)).reduce(reducer))
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    rdd.preferredLocations(s.asInstanceOf[ButterflyReducedRDDPartition]._1)
  }
}
