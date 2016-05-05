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

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Partition

/**
 * ::DeveloperApi::
 * A PartitionCoalescer defines how to coalesce the partitions of a given RDD.
 */
@DeveloperApi
trait PartitionCoalescer {

  /**
   * Coalesce the partitions of the given RDD.
   *
   * @param maxPartitions the maximum number of partitions to have after coalescing
   * @param parent the parent RDD whose partitions to coalesce
   * @return an array of [[PartitionGroup]]s, where each element is itself an array of
   * [[Partition]]s and represents a partition after coalescing is performed.
   */
  def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup]
}

/**
 * ::DeveloperApi::
 * A group of [[Partition]]s
 * @param prefLoc preferred location for the partition group
 */
@DeveloperApi
class PartitionGroup(val prefLoc: Option[String] = None) {
  val partitions = mutable.ArrayBuffer[Partition]()
  def numPartitions: Int = partitions.size
}
