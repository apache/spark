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
import org.apache.spark.sql.execution.datasources.v2.python.PythonMicroBatchStream.nextStreamId
import org.apache.spark.sql.execution.python.streaming.PythonStreamingSourceRunner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.{PythonStreamBlockId, StorageLevel}

case class PythonStreamingSourceOffset(json: String) extends Offset

/**
 * Micro-batch stream implementation for Python data sources with admission control support.
 *
 * This class bridges JVM Spark streaming with Python-based data sources, supporting:
 *   - Admission control via ReadLimit (maxRecordsPerBatch, maxFilesPerBatch, maxBytesPerBatch)
 *   - Offset tracking and management
 *   - Latest seen offset for prefetching optimization
 *
 * Admission control options:
 *   - `maxRecordsPerBatch`: Maximum number of rows per batch (Long, must be > 0)
 *   - `maxFilesPerBatch`: Maximum number of files per batch (Int, must be > 0)
 *   - `maxBytesPerBatch`: Maximum bytes per batch (Long, must be > 0)
 *
 * @param ds
 *   the Python data source V2 instance
 * @param shortName
 *   short name of the data source
 * @param outputSchema
 *   the output schema
 * @param options
 *   configuration options including admission control settings
 * @since 4.2.0
 */
class PythonMicroBatchStream(
    ds: PythonDataSourceV2,
    shortName: String,
    outputSchema: StructType,
    options: CaseInsensitiveStringMap)
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

  /**
   * Returns the default read limit based on configured options. Supports: maxRecordsPerBatch,
   * maxFilesPerBatch, maxBytesPerBatch. Falls back to allAvailable if no valid limit is
   * configured.
   *
   * @since 4.2.0
   */
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

    if (options.containsKey("maxRecordsPerBatch")) {
      val records = parseLong("maxRecordsPerBatch")
      logInfo(s"Admission control: maxRecordsPerBatch = $records")
      ReadLimit.maxRows(records)
    } else if (options.containsKey("maxFilesPerBatch")) {
      val files = parseInt("maxFilesPerBatch")
      logInfo(s"Admission control: maxFilesPerBatch = $files")
      ReadLimit.maxFiles(files)
    } else if (options.containsKey("maxBytesPerBatch")) {
      val bytes = parseLong("maxBytesPerBatch")
      logInfo(s"Admission control: maxBytesPerBatch = $bytes")
      ReadLimit.maxBytes(bytes)
    } else {
      logDebug("No admission control limit configured, using allAvailable")
      ReadLimit.allAvailable()
    }
  }

  override def latestOffset(): Offset = {
    // Bridge to new signature with default read limit for backward compatibility
    // Pass null as start offset to maintain backward compatibility with old behavior
    latestOffset(null, getDefaultReadLimit)
  }

  /**
   * Returns the latest offset with admission control limit applied. Also updates the true latest
   * offset for reporting purposes.
   *
   * @param startOffset
   *   the starting offset, may be null
   * @param limit
   *   the read limit to apply
   * @return
   *   the capped offset respecting the limit
   * @since 4.2.0
   */
  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    val startJson = Option(startOffset).map(_.json()).orNull
    val (cappedOffsetJson, trueLatestJson) = runner.latestOffsetWithReport(startJson, limit)
    val cappedOffset = PythonStreamingSourceOffset(cappedOffsetJson)
    latestAvailableOffset = Some(PythonStreamingSourceOffset(trueLatestJson))
    cappedOffset
  }

  /**
   * Reports the true latest available offset without admission control limits applied.
   *
   * @return
   *   the uncapped latest offset
   * @since 4.2.0
   */
  override def reportLatestOffset(): Offset = {
    latestAvailableOffset.orNull
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffsetJson = start.asInstanceOf[PythonStreamingSourceOffset].json
    val endOffsetJson = end.asInstanceOf[PythonStreamingSourceOffset].json

    if (cachedInputPartition.exists(p => p._1 == startOffsetJson && p._2 == endOffsetJson)) {
      Array(cachedInputPartition.get._3)
    } else {
      val (partitions, rows) = runner.partitions(startOffsetJson, endOffsetJson)
      if (rows.isDefined) {
        // Only SimpleStreamReader without partitioning prefetch data.
        assert(partitions.length == 1)
        nextBlockId = nextBlockId + 1
        val blockId = PythonStreamBlockId(streamId, nextBlockId)
        SparkEnv.get.blockManager.putIterator(
          blockId,
          rows.get,
          StorageLevel.MEMORY_AND_DISK_SER,
          true)
        val partition = PythonStreamingInputPartition(0, partitions.head, Some(blockId))
        cachedInputPartition.foreach(_._3.dropCache())
        cachedInputPartition = Some((startOffsetJson, endOffsetJson, partition))
        Array(partition)
      } else {
        partitions.zipWithIndex
          .map(p => PythonStreamingInputPartition(p._2, p._1, None))
      }
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
    new PythonStreamingPartitionReaderFactory(ds.source, readInfo.func, outputSchema, None, None)
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
