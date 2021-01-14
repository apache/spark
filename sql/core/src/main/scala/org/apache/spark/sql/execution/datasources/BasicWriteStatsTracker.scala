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

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.SerializableConfiguration


/**
 * Simple metrics collected during an instance of [[FileFormatDataWriter]].
 * These were first introduced in https://github.com/apache/spark/pull/18159 (SPARK-20703).
 */
case class BasicWriteTaskStats(
    partitionsStats: mutable.Map[TablePartitionSpec, PartitionStats],
    totalNumFiles: Int,
    totalNumBytes: Long,
    totalNumRows: Long)
  extends WriteTaskStats

case class PartitionStats(var numFiles: Int = 0, var numBytes: Long = 0, var numRows: Long = 0) {
  def updateNumFiles(num: Int): Unit = numFiles = numFiles + num

  def updateNumBytes(size: Long): Unit = numBytes = numBytes + size

  def updateNumRows(num: Long): Unit = numRows = numRows + num

  def merge(stats: PartitionStats): Unit = {
    updateNumFiles(stats.numFiles)
    updateNumBytes(stats.numBytes)
    updateNumRows(stats.numRows)
  }
}

/**
 * Simple [[WriteTaskStatsTracker]] implementation that produces [[BasicWriteTaskStats]].
 */
class BasicWriteTaskStatsTracker(hadoopConf: Configuration)
  extends WriteTaskStatsTracker with Logging {

  private[this] val partitionsStats: mutable.Map[TablePartitionSpec, PartitionStats] =
    mutable.Map.empty
  private[this] var totalNumFiles: Int = 0
  private[this] var submittedFiles: Int = 0
  private[this] var totalNumBytes: Long = 0L
  private[this] var totalNumRows: Long = 0L

  private[this] var curPartitionValue: Option[TablePartitionSpec] = None
  private[this] var curFile: Option[String] = None

  /**
   * Get the size of the file expected to have been written by a worker.
   * @param filePath path to the file
   * @return the file size or None if the file was not found.
   */
  private def getFileSize(filePath: String): Option[Long] = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    try {
      Some(fs.getFileStatus(path).getLen())
    } catch {
      case e: FileNotFoundException =>
        // may arise against eventually consistent object stores
        logDebug(s"File $path is not yet visible", e)
        None
    }
  }

  override def newPartition(partitionValues: TablePartitionSpec): Unit = {
    curPartitionValue = Some(partitionValues)
    val origin = partitionsStats.getOrElse(partitionValues, PartitionStats())
    partitionsStats.put(partitionValues, origin)
  }

  override def newBucket(bucketId: Int): Unit = {
    // currently unhandled
  }

  override def newFile(filePath: String): Unit = {
    statCurrentFile()
    curFile = Some(filePath)
    submittedFiles += 1
  }

  private def statCurrentFile(): Unit = {
    curFile.foreach { path =>
      getFileSize(path).foreach { len =>
        curPartitionValue.foreach { partitionValue =>
          val partitionStats = partitionsStats.getOrElse(partitionValue, PartitionStats())
          partitionStats.updateNumFiles(1)
          partitionStats.updateNumBytes(len)
          partitionsStats.update(partitionValue, partitionStats)
        }
        totalNumBytes += len
        totalNumFiles += 1
      }
      curFile = None
    }
  }

  override def newRow(row: InternalRow): Unit = {
    curPartitionValue.foreach { partitionValue =>
      val partitionStats = partitionsStats.getOrElse(partitionValue, PartitionStats())
      partitionStats.updateNumRows(1)
      partitionsStats.update(partitionValue, partitionStats)
    }
    totalNumRows += 1
  }

  override def getFinalStats(): WriteTaskStats = {
    statCurrentFile()

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { outputMetrics =>
      outputMetrics.setBytesWritten(totalNumBytes)
      outputMetrics.setRecordsWritten(totalNumRows)
    }

    if (submittedFiles != totalNumFiles) {
      logInfo(s"Expected $submittedFiles files, but only saw $totalNumFiles. " +
        "This could be due to the output format not writing empty files, " +
        "or files being not immediately visible in the filesystem.")
    }
    BasicWriteTaskStats(partitionsStats, totalNumFiles, totalNumBytes, totalNumRows)
  }
}


/**
 * Simple [[WriteJobStatsTracker]] implementation that's serializable, capable of
 * instantiating [[BasicWriteTaskStatsTracker]] on executors and processing the
 * [[BasicWriteTaskStats]] they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class BasicWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient val metrics: Map[String, SQLMetric])
  extends WriteJobStatsTracker {

  @transient val partitionsStats: mutable.Map[TablePartitionSpec, PartitionStats] =
    mutable.Map.empty
  @transient var numFiles: Long = 0L
  @transient var totalNumBytes: Long = 0L
  @transient var totalNumOutput: Long = 0L

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new BasicWriteTaskStatsTracker(serializableHadoopConf.value)
  }

  override def processStats(stats: Seq[WriteTaskStats]): Unit = {
    val sparkContext = SparkContext.getActive.get
    val basicStats = stats.map(_.asInstanceOf[BasicWriteTaskStats])

    basicStats.foreach { summary =>
      summary.partitionsStats.foreach { case (partitionValue, stats) =>
        val currentStats = partitionsStats.getOrElse(partitionValue, PartitionStats())
        currentStats.merge(stats)
        partitionsStats.put(partitionValue, currentStats)
      }
      numFiles += summary.totalNumFiles
      totalNumBytes += summary.totalNumBytes
      totalNumOutput += summary.totalNumRows
    }

    metrics(BasicWriteJobStatsTracker.NUM_FILES_KEY).add(numFiles)
    metrics(BasicWriteJobStatsTracker.NUM_OUTPUT_BYTES_KEY).add(totalNumBytes)
    metrics(BasicWriteJobStatsTracker.NUM_OUTPUT_ROWS_KEY).add(totalNumOutput)
    metrics(BasicWriteJobStatsTracker.NUM_PARTS_KEY).add(partitionsStats.keys.size)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toList)
  }
}

object BasicWriteJobStatsTracker {
  private val NUM_FILES_KEY = "numFiles"
  private val NUM_OUTPUT_BYTES_KEY = "numOutputBytes"
  private val NUM_OUTPUT_ROWS_KEY = "numOutputRows"
  private val NUM_PARTS_KEY = "numParts"

  def metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      NUM_FILES_KEY -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      NUM_OUTPUT_BYTES_KEY -> SQLMetrics.createSizeMetric(sparkContext, "written output"),
      NUM_OUTPUT_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      NUM_PARTS_KEY -> SQLMetrics.createMetric(sparkContext, "number of dynamic part")
    )
  }
}
