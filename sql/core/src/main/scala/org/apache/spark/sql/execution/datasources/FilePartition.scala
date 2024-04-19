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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

import org.apache.spark.Partition
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{CONFIG, DESIRED_PARTITIONS_SIZE, MAX_PARTITIONS_SIZE, PARTITIONS_SIZE}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.ScanFileListing
import org.apache.spark.sql.internal.SQLConf

/**
 * A collection of file blocks that should be read as a single task
 * (possibly from multiple partitioned directories).
 */
case class FilePartition(index: Int, files: Array[PartitionedFile])
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}

object FilePartition extends Logging {

  private def getFilePartitions(
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long,
      openCostInBytes: Long): Seq[FilePartition] = {
    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }

  def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    val openCostBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxPartNum = sparkSession.sessionState.conf.filesMaxPartitionNum
    val partitions = getFilePartitions(partitionedFiles, maxSplitBytes, openCostBytes)
    if (maxPartNum.exists(partitions.size > _)) {
      val totalSizeInBytes =
        partitionedFiles.map(_.length + openCostBytes).map(BigDecimal(_)).sum[BigDecimal]
      val desiredSplitBytes =
        (totalSizeInBytes / BigDecimal(maxPartNum.get)).setScale(0, RoundingMode.UP).longValue
      val desiredPartitions = getFilePartitions(partitionedFiles, desiredSplitBytes, openCostBytes)
      logWarning(log"The number of partitions is ${MDC(PARTITIONS_SIZE, partitions.size)}, " +
        log"which exceeds the maximum number configured: " +
        log"${MDC(MAX_PARTITIONS_SIZE, maxPartNum.get)}. Spark rescales it to " +
        log"${MDC(DESIRED_PARTITIONS_SIZE, desiredPartitions.size)} by ignoring the " +
        log"configuration of ${MDC(CONFIG, SQLConf.FILES_MAX_PARTITION_BYTES.key)}.")
      desiredPartitions
    } else {
      partitions
    }
  }

  /**
   * Returns the max split bytes, given the total number of bytes taken by the selected
   * partitions.
   */
  def maxSplitBytes(sparkSession: SparkSession, calculateTotalBytes: => Long): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = calculateTotalBytes
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }

  /**
   * Returns the max split bytes, given the selected partitions represented using the
   * [[ScanFileListing]] type.
   */
  def maxSplitBytes(sparkSession: SparkSession, selectedPartitions: ScanFileListing): Long = {
    val byteNum = selectedPartitions.calculateTotalPartitionBytes
    maxSplitBytes(sparkSession, byteNum)
  }

  /**
   * Returns the max split bytes, given the selected partitions represented as a sequence of
   * [[PartitionDirectory]]s.
   */
  def maxSplitBytes(
      sparkSession: SparkSession, selectedPartitions: Seq[PartitionDirectory]): Long = {
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val byteNum = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    maxSplitBytes(sparkSession, byteNum)
  }
}
