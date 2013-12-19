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

import org.apache.spark.{OneToOneDependency, SparkContext, Partition, TaskContext}

import java.io.{ObjectOutputStream, IOException}

import scala.reflect.ClassTag

private[spark] class ZippedPartition[T: ClassTag, U: ClassTag](
    idx: Int,
    @transient rdd1: RDD[T],
    @transient rdd2: RDD[U]
  ) extends Partition {

  var partition1 = rdd1.partitions(idx)
  var partition2 = rdd2.partitions(idx)
  override val index: Int = idx

  def partitions = (partition1, partition2)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent partition at the time of task serialization
    partition1 = rdd1.partitions(idx)
    partition2 = rdd2.partitions(idx)
    oos.defaultWriteObject()
  }
}

class ZippedRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1: RDD[T],
    var rdd2: RDD[U])
  extends RDD[(T, U)](sc, List(new OneToOneDependency(rdd1), new OneToOneDependency(rdd2))) {

  override def getPartitions: Array[Partition] = {
    if (rdd1.partitions.size != rdd2.partitions.size) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    val array = new Array[Partition](rdd1.partitions.size)
    for (i <- 0 until rdd1.partitions.size) {
      array(i) = new ZippedPartition(i, rdd1, rdd2)
    }
    array
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(T, U)] = {
    val (partition1, partition2) = s.asInstanceOf[ZippedPartition[T, U]].partitions
    rdd1.iterator(partition1, context).zip(rdd2.iterator(partition2, context))
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    val (partition1, partition2) = s.asInstanceOf[ZippedPartition[T, U]].partitions
    val pref1 = rdd1.preferredLocations(partition1)
    val pref2 = rdd2.preferredLocations(partition2)
    // Check whether there are any hosts that match both RDDs; otherwise return the union
    val exactMatchLocations = pref1.intersect(pref2)
    if (!exactMatchLocations.isEmpty) {
      exactMatchLocations
    } else {
      (pref1 ++ pref2).distinct
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
