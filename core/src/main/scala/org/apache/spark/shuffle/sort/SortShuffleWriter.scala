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

package org.apache.spark.shuffle.sort

import org.apache.spark.shuffle.{ShuffleWriter, BaseShuffleHandle}
import org.apache.spark.{SparkEnv, Logging, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val numOutputPartitions = dep.partitioner.numPartitions
  private val metrics = context.taskMetrics

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    var sorter: ExternalSorter[K, V, _] = null

    val partitions: Iterator[(Int, Iterator[Product2[K, _]])] = {
      if (dep.mapSideCombine) {
        if (!dep.aggregator.isDefined) {
          throw new IllegalStateException("Aggregator is empty for map-side combine")
        }
        sorter = new ExternalSorter[K, V, C](
          dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
        sorter.write(records)
        sorter.partitionedIterator
      } else {
        sorter = new ExternalSorter[K, V, V](
          None, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
        sorter.write(records)
        sorter.partitionedIterator
      }
    }

    for ((id, elements) <- partitions) {

    }

    ???
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = ???
}
