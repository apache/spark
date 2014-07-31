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
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024

  private var sorter: ExternalSorter[K, V, _] = null
  private var outputFile: File = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    // Get an iterator with the elements for each partition ID
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
        // In this case we pass neither an aggregator nor an ordering to the sorter, because we
        // don't care whether the keys get sorted in each partition; that will be done on the
        // reduce side if the operation being run is sortByKey.
        sorter = new ExternalSorter[K, V, V](
          None, Some(dep.partitioner), None, dep.serializer)
        sorter.write(records)
        sorter.partitionedIterator
      }
    }

    // Create a single shuffle file with reduce ID 0 that we'll write all results to. We'll later
    // serve different ranges of this file using an index file that we create at the end.
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, 0)
    outputFile = blockManager.diskBlockManager.getFile(blockId)

    // Track location of each range in the output file
    val offsets = new Array[Long](numPartitions + 1)
    val lengths = new Array[Long](numPartitions)

    // Statistics
    var totalBytes = 0L
    var totalTime = 0L

    for ((id, elements) <- partitions) {
      if (elements.hasNext) {
        val writer = blockManager.getDiskWriter(blockId, outputFile, ser, fileBufferSize)
        for (elem <- elements) {
          writer.write(elem)
        }
        writer.commit()
        writer.close()
        val segment = writer.fileSegment()
        offsets(id + 1) = segment.offset + segment.length
        lengths(id) = segment.length
        totalTime += writer.timeWriting()
        totalBytes += segment.length
      } else {
        // The partition is empty; don't create a new writer to avoid writing headers, etc
        offsets(id + 1) = offsets(id)
      }
    }

    val shuffleMetrics = new ShuffleWriteMetrics
    shuffleMetrics.shuffleBytesWritten = totalBytes
    shuffleMetrics.shuffleWriteTime = totalTime
    context.taskMetrics.shuffleWriteMetrics = Some(shuffleMetrics)
    context.taskMetrics.memoryBytesSpilled += sorter.memoryBytesSpilled
    context.taskMetrics.diskBytesSpilled += sorter.diskBytesSpilled

    // Write an index file with the offsets of each block, plus a final offset at the end for the
    // end of the output file. This will be used by SortShuffleManager.getBlockLocation to figure
    // out where each block begins and ends.

    val diskBlockManager = blockManager.diskBlockManager
    val indexFile = diskBlockManager.getFile(blockId.name + ".index")
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    try {
      var i = 0
      while (i < numPartitions + 1) {
        out.writeLong(offsets(i))
        i += 1
      }
    } finally {
      out.close()
    }

    // Register our map output with the ShuffleBlockManager, which handles cleaning it over time
    blockManager.shuffleBlockManager.addCompletedMap(dep.shuffleId, mapId, numPartitions)

    mapStatus = new MapStatus(blockManager.blockManagerId,
      lengths.map(MapOutputTracker.compressSize))
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
