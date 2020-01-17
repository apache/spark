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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.rdd._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils

class SizeBasedCoalescer(val maxSize: Long, val schema: Seq[Attribute])
  extends PartitionCoalescer with Serializable {
  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    val rowSize = EstimationUtils.getSizePerRow(schema).toLong
    val partitionIndexToSize = parent.mapPartitionsWithIndexInternal((index, part) => {
      // TODO make it more accurate
      Map(index -> rowSize * part.size).iterator
    }).collectAsMap()

    val partitions = parent.partitions

    val groups = ArrayBuffer[PartitionGroup]()
    var currentGroup = new PartitionGroup()
    var currentSum = 0L
    var totalSum = 0L
    var index = 0

    def updateGroups(): Unit = {
      groups += currentGroup
      currentGroup = new PartitionGroup()
      currentSum = 0
    }

    def addPartition(partition: Partition, splitSize: Long): Unit = {
      currentGroup.partitions += partition
      currentSum += splitSize
      totalSum += splitSize
    }

    while (index < partitions.length) {
      val partition = partitions(index)
      val partitionSize = partitionIndexToSize(index)
      if (currentSum + partitionSize < maxSize) {
        addPartition(partition, partitionSize)
        index += 1
        if (index == partitions.length) {
          updateGroups()
        }
      } else {
        if (currentGroup.partitions.isEmpty) {
          addPartition(partition, partitionSize)
          index += 1
        } else {
          updateGroups()
        }
      }
    }
    groups.toArray
  }
}

