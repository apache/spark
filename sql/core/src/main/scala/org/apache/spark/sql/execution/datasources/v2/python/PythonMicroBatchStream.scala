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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{AcceptsLatestSeenOffset, MicroBatchStream, Offset}
import org.apache.spark.sql.execution.datasources.v2.python.PythonMicroBatchStream.nextStreamId
import org.apache.spark.sql.execution.python.PythonStreamingSourceRunner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{PythonStreamBlockId, StorageLevel}

case class PythonStreamingSourceOffset(json: String) extends Offset

class PythonMicroBatchStream(
    ds: PythonDataSourceV2,
    shortName: String,
    outputSchema: StructType,
    options: CaseInsensitiveStringMap
  )
  extends MicroBatchStream
  with Logging
  with AcceptsLatestSeenOffset {
  private def createDataSourceFunc =
    ds.source.createPythonFunction(
      ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema)).dataSource)

  private val streamId = nextStreamId
  private var nextBlockId = 0L

  // planInputPartitions() maybe be called multiple times for the current microbatch.
  // Cache the result of planInputPartitions() because it may involve sending data
  // from python to JVM.
  private var cachedInputPartition: Option[(String, String, PythonStreamingInputPartition)] = None

  private val runner: PythonStreamingSourceRunner =
    new PythonStreamingSourceRunner(createDataSourceFunc, outputSchema)
  runner.init()

  override def initialOffset(): Offset = PythonStreamingSourceOffset(runner.initialOffset())

  override def latestOffset(): Offset = PythonStreamingSourceOffset(runner.latestOffset())

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffsetJson = start.asInstanceOf[PythonStreamingSourceOffset].json
    val endOffsetJson = end.asInstanceOf[PythonStreamingSourceOffset].json

    if (cachedInputPartition.exists(p => p._1 == startOffsetJson && p._2 == endOffsetJson)) {
      return Array(cachedInputPartition.get._3)
    }

    val (partitions, rows) = runner.partitions(startOffsetJson, endOffsetJson)
    if (rows.isDefined) {
      // Only SimpleStreamReader without partitioning prefetch data.
      assert(partitions.length == 1)
      nextBlockId = nextBlockId + 1
      val blockId = PythonStreamBlockId(streamId, nextBlockId)
      SparkEnv.get.blockManager.putIterator(
        blockId, rows.get, StorageLevel.MEMORY_AND_DISK_SER, true)
      val partition = PythonStreamingInputPartition(0, partitions.head, Some(blockId))
      cachedInputPartition.foreach(_._3.dropCache())
      cachedInputPartition = Some((startOffsetJson, endOffsetJson, partition))
      Array(partition)
    } else {
      partitions.zipWithIndex
        .map(p => PythonStreamingInputPartition(p._2, p._1, None))
    }
  }

  override def setLatestSeenOffset(offset: Offset): Unit = {
    // Call planPartition on python with an empty offset range to initialize the start offset
    // for the prefetching of simple reader.
    runner.partitions(offset.json(), offset.json())
  }

  private lazy val readInfo: PythonDataSourceReadInfo = {
    ds.source.createReadInfoInPython(
      ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema)),
      outputSchema,
      isStreaming = true)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PythonStreamingPartitionReaderFactory(
      ds.source, readInfo.func, outputSchema, None)
  }

  override def commit(end: Offset): Unit = {
    runner.commit(end.asInstanceOf[PythonStreamingSourceOffset].json)
  }

  override def stop(): Unit = {
    cachedInputPartition.foreach(_._3.dropCache())
    runner.stop()
  }

  override def deserializeOffset(json: String): Offset = PythonStreamingSourceOffset(json)
}

object PythonMicroBatchStream {
  private var currentId = 0
  def nextStreamId: Int = synchronized {
    currentId = currentId + 1
    currentId
  }
}

