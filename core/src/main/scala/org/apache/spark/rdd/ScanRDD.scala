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

import org.apache.spark.{Partition, TaskContext}

private[spark]
class ScanRDDPartition[U: ClassTag](val prev: Partition, val start: U)
  extends Partition with Serializable {
  override val index: Int = prev.index
}

private[spark]
class ScanRDD[T: ClassTag, U: ClassTag](prev: RDD[T],
                                        zero: U,
                                        seqOp: (U, T) => U,
                                        combOp: (U, U) => U) extends RDD[(T, U)](prev) {

  @transient private val startValues = {
    val n = prev.partitions.length
    if (n == 0) {
      Array.empty[U]
    } else if (n == 1) {
      Array(zero)
    } else {
      prev.context.runJob(
        prev,
        (iter : Iterator[T]) => {
          var cum = zero
          iter.foreach(t => cum = seqOp(cum, t))
          cum
        },
        0 until n - 1 // do not need to count the last partition
      ).scanLeft(zero)(combOp)
    }
  }

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(T, U)] = {
    val split = splitIn.asInstanceOf[ScanRDDPartition]
    var cum = split.start
    val parentIter = firstParent[T].iterator(split.prev, context)
    parentIter.map { t =>
      cum = seqOp(cum, t)
      (t, cum)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    firstParent[T].partitions.map(x => new ScanRDDPartition(x, startValues(x.index)))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    firstParent[T].preferredLocations(split.asInstanceOf[ScanRDDPartition].prev)
  }
}
