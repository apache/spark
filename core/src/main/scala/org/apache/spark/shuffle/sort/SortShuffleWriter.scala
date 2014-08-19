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

import java.io.{BufferedOutputStream, File, FileOutputStream, DataOutputStream}

import org.apache.spark.{MapOutputTracker, SparkEnv, Logging, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleWriter, BaseShuffleHandle}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val numPartitions = dep.partitioner.numPartitions

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer.orNull)

  private val conf = SparkEnv.get.conf
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  private var sorter: ExternalSorter[K, V, _] = null
  private var outputFile: File = null
  private var indexFile: File = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    if (dep.mapSideCombine) {
      if (!dep.aggregator.isDefined) {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      }
      sorter = new ExternalSorter[K, V, C](
        dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
      sorter.insertAll(records)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      sorter = new ExternalSorter[K, V, V](
        None, Some(dep.partitioner), None, dep.serializer)
      sorter.insertAll(records)
    }

    // Create a single shuffle file with reduce ID 0 that we'll write all results to. We'll later
    // serve different ranges of this file using an index file that we create at the end.
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, 0)

    outputFile = blockManager.diskBlockManager.getFile(blockId)
    indexFile = blockManager.diskBlockManager.getFile(blockId.name + ".index")

    val partitionLengths = sorter.writePartitionedFile(blockId, context)

    // Register our map output with the ShuffleBlockManager, which handles cleaning it over time
    blockManager.shuffleBlockManager.addCompletedMap(dep.shuffleId, mapId, numPartitions)

    mapStatus = new MapStatus(blockManager.blockManagerId,
      partitionLengths.map(MapOutputTracker.compressSize))
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output file if we created one
        if (outputFile != null) {
          outputFile.delete()
        }
        if (indexFile != null) {
          indexFile.delete()
        }
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        sorter.stop()
        sorter = null
      }
    }
  }
}
