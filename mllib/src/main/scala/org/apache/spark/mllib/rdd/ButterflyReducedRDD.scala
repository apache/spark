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

/** A partition in a butterfly-reduced RDD. */
private case class ButterflyReducedRDDPartition(
    override val index: Int,
    source: Partition,
    target: Partition) extends Partition

/**
 * Butterfly-reduced RDD.
 */
private[mllib] class ButterflyReducedRDD[T: ClassTag](
    @transient rdd: RDD[T],
    reducer: (T, T) => T,
    @transient offset: Int) extends RDD[T](rdd) {

  /** Computes the target partition. */
  private def targetPartition(i: Int): Partition = {
    val j = (i + offset) % rdd.partitions.size
    rdd.partitions(j)
  }

  override def getPartitions: Array[Partition] = {
    rdd.partitions.zipWithIndex.map { case (part, i) =>
      ButterflyReducedRDDPartition(i, part, targetPartition(i))
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val pair = s.asInstanceOf[ButterflyReducedRDDPartition]
    Iterator((firstParent[T].iterator(pair.source, context) ++
      firstParent[T].iterator(pair.target, context)).reduce(reducer))
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    rdd.preferredLocations(s.asInstanceOf[ButterflyReducedRDDPartition].source)
  }
}
