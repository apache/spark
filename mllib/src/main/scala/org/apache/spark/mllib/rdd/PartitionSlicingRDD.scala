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

import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition}

/**
 * Represents an RDD obtained from partition slicing of its parent RDD.
 */
private[mllib] class PartitionSlicingRDD[T: ClassTag](
    @transient rdd: RDD[T],
    @transient slice: Seq[Int]) extends RDD[T](rdd) {

  override def getPartitions: Array[Partition] = {
    slice.map(i => rdd.partitions(i)).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(s, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    rdd.preferredLocations(s)
  }
}
