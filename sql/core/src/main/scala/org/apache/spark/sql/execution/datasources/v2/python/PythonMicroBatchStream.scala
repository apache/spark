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
import org.apache.spark.sql.connector.read.streaming.{AcceptsLatestSeenOffset, MicroBatchStream, Offset, ReadLimit}
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl
import org.apache.spark.sql.execution.datasources.v2.python.PythonMicroBatchStream._
import org.apache.spark.sql.execution.python.streaming.PythonStreamingSourceRunner
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
  with SupportsAdmissionControl
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

  // Store the latest available offset for reporting
  private var latestAvailableOffset: Option[PythonStreamingSourceOffset] = None

  private val runner: PythonStreamingSourceRunner =
    new PythonStreamingSourceRunner(createDataSourceFunc, outputSchema)
  runner.init()

  override def initialOffset(): Offset = PythonStreamingSourceOffset(runner.initialOffset())

  override def getDefaultReadLimit: ReadLimit = {
    import scala.util.Try

    def parseLong(key: String): Long = {
      Try(options.get(key).toLong).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '${options.get(key)}' for option '$key', must be a positive integer")
      }
    }

    def parseInt(key: String): Int = {
      Try(options.get(key).toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '${options.get(key)}' for option '$key', must be a positive integer")
      }
    }

    if (options.containsKey(MAX_RECORDS_PER_BATCH)) {
      val records = parseLong(MAX_RECORDS_PER_BATCH)
      logInfo(s"Admission control: $MAX_RECORDS_PER_BATCH = $records")
      ReadLimit.maxRows(records)
    } else if (options.containsKey(MAX_FILES_PER_BATCH)) {
      val files = parseInt(MAX_FILES_PER_BATCH)
      logInfo(s"Admission control: $MAX_FILES_PER_BATCH = $files")
      ReadLimit.maxFiles(files)
    } else if (options.containsKey(MAX_BYTES_PER_BATCH)) {
      val bytes = parseLong(MAX_BYTES_PER_BATCH)
      logInfo(s"Admission control: $MAX_BYTES_PER_BATCH = $bytes")
      ReadLimit.maxBytes(bytes)
    } else {
      logDebug("No admission control limit configured, using allAvailable")
      ReadLimit.allAvailable()
    }
  }

  override def latestOffset(): Offset = {
    // Required by MicroBatchStream. Since this stream implements SupportsAdmissionControl, the
    // engine is expected to call latestOffset(startOffset, limit), but we delegate for safety.
    latestOffset(null, getDefaultReadLimit)
  }

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    // Admission control is implemented on the Python side using data source options.
    // We still validate configured options via getDefaultReadLimit(), but do not send ReadLimit
    // to Python.
    getDefaultReadLimit
    val startJson = Option(startOffset).map(_.json()).getOrElse("null")
    val (cappedOffsetJson, trueLatestJson) = runner.latestOffsetWithReport(startJson)
    val cappedOffset = PythonStreamingSourceOffset(cappedOffsetJson)
    latestAvailableOffset = Some(PythonStreamingSourceOffset(trueLatestJson))
    cappedOffset
  }

  override def reportLatestOffset(): Offset = {
    latestAvailableOffset.orNull
  }

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
    ds.getOrCreateReadInfo(shortName, options, outputSchema, isStreaming = true)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PythonStreamingPartitionReaderFactory(
      ds.source, readInfo.func, outputSchema, None, None)
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
  private[python] val MAX_RECORDS_PER_BATCH = "maxRecordsPerBatch"
  private[python] val MAX_FILES_PER_BATCH = "maxFilesPerBatch"
  private[python] val MAX_BYTES_PER_BATCH = "maxBytesPerBatch"

  def nextStreamId: Int = synchronized {
    currentId = currentId + 1
    currentId
  }
}
