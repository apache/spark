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
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.SerializableConfiguration

// TODO: Support ColumnStatistics
case class SinglePartitionWriteStats(
    partition: InternalRow,
    var numFiles: Int,
    var numBytes: Long,
    var numRows: Long)
  extends WriteTaskStats

case class PartitionedWriteTaskStats(partitions: Seq[SinglePartitionWriteStats])
  extends WriteTaskStats

class PartitionedWriteTaskStatsTracker(
    hadoopConf: Configuration,
    taskCommitTimeMetric: Option[SQLMetric] = None)
  extends WriteTaskStatsTracker with Logging {

  private[this] val partitions: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer.empty

  private[this] val partitionStats: mutable.ArrayBuffer[SinglePartitionWriteStats] =
    mutable.ArrayBuffer.empty
  private[this] var currentPartition: InternalRow = null
  private[this] var currentPartitionStats: SinglePartitionWriteStats = null
  private[this] var numFiles: Int = 0
  private[this] var numBytes: Long = 0
  private[this] var numRows: Long = 0
  private[this] var numSubmittedFiles: Int = 0

  private[this] val submittedFiles =
    mutable.HashMap.empty[InternalRow, mutable.HashSet[String]]

  /**
   * Get the size of the file expected to have been written by a worker.
   * @param filePath path to the file
   * @return the file size or None if the file was not found.
   */
  private def getFileSize(filePath: String): Option[Long] = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    getFileSize(fs, path)
  }

  /**
   * Get the size of the file expected to have been written by a worker.
   * This supports the XAttr in HADOOP-17414 when the "magic committer" adds
   * a custom HTTP header to the a zero byte marker.
   * If the output file as returned by getFileStatus > 0 then the length if
   * returned. For zero-byte files, the (optional) Hadoop FS API getXAttr() is
   * invoked. If a parseable, non-negative length can be retrieved, this
   * is returned instead of the length.
   * @return the file size or None if the file was not found.
   */
  private [datasources] def getFileSize(fs: FileSystem, path: Path): Option[Long] = {
    // the normal file status probe.
    try {
      val len = fs.getFileStatus(path).getLen
      if (len > 0) {
        return Some(len)
      }
    } catch {
      case e: FileNotFoundException =>
        // may arise against eventually consistent object stores.
        logDebug(s"File $path is not yet visible", e)
        return None
    }

    // Output File Size is 0. Look to see if it has an attribute
    // declaring a future-file-length.
    // Failure of API call, parsing, invalid value all return the
    // 0 byte length.

    var len = 0L
    try {
      val attr = fs.getXAttr(path, BasicWriteJobStatsTracker.FILE_LENGTH_XATTR)
      if (attr != null && attr.nonEmpty) {
        val str = new String(attr, StandardCharsets.UTF_8)
        logDebug(s"File Length statistics for $path retrieved from XAttr: $str")
        // a non-empty header was found. parse to a long via the java class
        val l = java.lang.Long.parseLong(str)
        if (l > 0) {
          len = l
        } else {
          logDebug("Ignoring negative value in XAttr file length")
        }
      }
    } catch {
      case e: NumberFormatException =>
        // warn but don't dump the whole stack
        logInfo(s"Failed to parse" +
          s" ${BasicWriteJobStatsTracker.FILE_LENGTH_XATTR}:$e;" +
          s" bytes written may be under-reported");
      case e: UnsupportedOperationException =>
        // this is not unusual; ignore
        logDebug(s"XAttr not supported on path $path", e);
      case e: Exception =>
        // Something else. Log at debug and continue.
        logDebug(s"XAttr processing failure on $path", e);
    }
    Some(len)
  }

  override def newPartition(partitionValues: InternalRow): Unit = {
    partitions.append(partitionValues)
    currentPartition = partitionValues
    currentPartitionStats = SinglePartitionWriteStats(partitionValues, 0, 0L, 0L)
    partitionStats.append(currentPartitionStats)
  }

  override def newFile(filePath: String): Unit = {
    if (partitionStats.isEmpty) {
      currentPartitionStats = SinglePartitionWriteStats(InternalRow.empty, 0, 0L, 0L)
      partitionStats.append(currentPartitionStats)
      currentPartition = InternalRow.empty
      partitions.append(currentPartition)
    }
    val partitionSubmittedFiles =
      submittedFiles.getOrElseUpdate(currentPartition, mutable.HashSet.empty[String])
    partitionSubmittedFiles.add(filePath)
    numSubmittedFiles += 1
  }

  override def closeFile(filePath: String): Unit = {
    updateFileStats(currentPartition, filePath)
    submittedFiles.get(currentPartition).map(_.remove(filePath))
  }

  private def updateFileStats(partitionValue: InternalRow, filePath: String): Unit = {
    getFileSize(filePath).foreach { len =>
      partitionStats(partitions.indexOf(partitionValue)).numBytes += len
      partitionStats(partitions.indexOf(partitionValue)).numFiles += 1
      numBytes += 1
      numFiles += 1
    }
  }

  override def newRow(filePath: String, row: InternalRow): Unit = {
    currentPartitionStats.numRows += 1
    numRows += 1
  }

  override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
    submittedFiles.foreach { case (part, files) =>
      files.foreach(file => updateFileStats(part, file))
    }
    submittedFiles.clear()

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { outputMetrics =>
      outputMetrics.setBytesWritten(numBytes)
      outputMetrics.setRecordsWritten(numRows)
    }

    if (numSubmittedFiles != numFiles) {
      logWarning(s"Expected $numSubmittedFiles files, but only saw $numFiles. " +
        "This could be due to the output format not writing empty files, " +
        "or files being not immediately visible in the filesystem.")
    }
    taskCommitTimeMetric.foreach(_ += taskCommitTime)
    PartitionedWriteTaskStats(partitionStats.toSeq)
  }
}


/**
 * Simple [[WriteJobStatsTracker]] implementation that's serializable, capable of
 * instantiating [[BasicWriteTaskStatsTracker]] on executors and processing the
 * [[BasicWriteTaskStats]] they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class PartitionedWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient val driverSideMetrics: Map[String, SQLMetric],
    taskCommitTimeMetric: SQLMetric)
  extends WriteJobStatsTracker {

  def this(
      serializableHadoopConf: SerializableConfiguration,
      metrics: Map[String, SQLMetric]) = {
    this(serializableHadoopConf, metrics - TASK_COMMIT_TIME, metrics(TASK_COMMIT_TIME))
  }

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new PartitionedWriteTaskStatsTracker(serializableHadoopConf.value, Some(taskCommitTimeMetric))
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    val sparkContext = SparkContext.getActive.get
    val partitionsSet: mutable.Set[InternalRow] = mutable.HashSet.empty
    var numFiles: Long = 0L
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L

    val basicStats = stats.map(_.asInstanceOf[PartitionedWriteTaskStats])

    basicStats.foreach { partitionedStats =>
      partitionedStats.partitions.foreach { partitionedSummary =>
        partitionsSet += partitionedSummary.partition
        numFiles += partitionedSummary.numFiles
        totalNumBytes += partitionedSummary.numBytes
        totalNumOutput += partitionedSummary.numRows
      }
    }

    driverSideMetrics(JOB_COMMIT_TIME).add(jobCommitTime)
    driverSideMetrics(NUM_FILES_KEY).add(numFiles)
    driverSideMetrics(NUM_OUTPUT_BYTES_KEY).add(totalNumBytes)
    driverSideMetrics(NUM_OUTPUT_ROWS_KEY).add(totalNumOutput)
    driverSideMetrics(NUM_PARTS_KEY).add(partitionsSet.size)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, driverSideMetrics.values.toList)
  }
}
