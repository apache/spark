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
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, DESIRED_NUM_PARTITIONS, MAX_NUM_PARTITIONS, NUM_PARTITIONS}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.ScanFileListing
import org.apache.spark.sql.internal.{SessionStateHelper, SQLConf}

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

object FilePartition extends SessionStateHelper with Logging {

  private def getFilePartitionsBySize(
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
    partitionedFiles.sortBy(_.length)(implicitly[Ordering[Long]].reverse).foreach { file =>
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

  private def getFilePartitionsByFileNum(
      partitionedFiles: Seq[PartitionedFile],
      outputPartitions: Int,
      smallFileThreshold: Double): Seq[FilePartition] = {
    // Flatten and sort descending by file size.
    val filesSorted: Seq[(PartitionedFile, Long)] =
      partitionedFiles
        .map(f => (f, f.length))
        .sortBy(_._2)(Ordering.Long.reverse)

    val partitions = Seq.fill(outputPartitions)(mutable.ArrayBuffer.empty[PartitionedFile])

    def addToBucket(
        heap: mutable.PriorityQueue[(Long, Int, Int)],
        file: PartitionedFile,
        sz: Long): Unit = {
      val (load, numFiles, idx) = heap.dequeue()
      partitions(idx) += file
      heap.enqueue((load + sz, numFiles + 1, idx))
    }

    // First by load, then by numFiles.
    val heapByFileSize =
      mutable.PriorityQueue.empty[(Long, Int, Int)](
        Ordering
          .by[(Long, Int, Int), (Long, Int)] {
            case (load, numFiles, _) =>
              (load, numFiles)
          }
          .reverse
      )

    if (smallFileThreshold > 0) {
      val smallFileTotalSize = filesSorted.map(_._2).sum * smallFileThreshold
      // First by numFiles, then by load.
      val heapByFileNum =
        mutable.PriorityQueue.empty[(Long, Int, Int)](
          Ordering
            .by[(Long, Int, Int), (Int, Long)] {
              case (load, numFiles, _) =>
                (numFiles, load)
            }
            .reverse
        )

      (0 until outputPartitions).foreach(i => heapByFileNum.enqueue((0L, 0, i)))

      var numSmallFiles = 0
      var smallFileSize = 0L
      // Enqueue small files to the least number of files and the least load.
      filesSorted.reverse.takeWhile(f => f._2 + smallFileSize <= smallFileTotalSize).foreach {
        case (file, sz) =>
          addToBucket(heapByFileNum, file, sz)
          numSmallFiles += 1
          smallFileSize += sz
      }

      // Move buckets from heapByFileNum to heapByFileSize.
      while (heapByFileNum.nonEmpty) {
        heapByFileSize.enqueue(heapByFileNum.dequeue())
      }

      // Finally, enqueue remaining files.
      filesSorted.take(filesSorted.size - numSmallFiles).foreach {
        case (file, sz) =>
          addToBucket(heapByFileSize, file, sz)
      }
    } else {
      (0 until outputPartitions).foreach(i => heapByFileSize.enqueue((0L, 0, i)))

      filesSorted.foreach {
        case (file, sz) =>
          addToBucket(heapByFileSize, file, sz)
      }
    }

    partitions.zipWithIndex.map {
      case (p, idx) => FilePartition(idx, p.toArray)
    }
  }

  def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    val conf = getSqlConf(sparkSession)
    val openCostBytes = conf.filesOpenCostInBytes
    val maxPartNum = conf.filesMaxPartitionNum
    val partitions = getFilePartitionsBySize(partitionedFiles, maxSplitBytes, openCostBytes)
    conf.filesPartitionStrategy match {
      case "file_based" =>
        getFilePartitionsByFileNum(partitionedFiles, Math.min(partitions.size,
          maxPartNum.getOrElse(Int.MaxValue)), conf.smallFileThreshold)
      case "size_based" =>
        if (maxPartNum.exists(partitions.size > _)) {
          val totalSizeInBytes =
            partitionedFiles.map(_.length + openCostBytes).map(BigDecimal(_)).sum[BigDecimal]
          val desiredSplitBytes =
            (totalSizeInBytes / BigDecimal(maxPartNum.get)).setScale(0, RoundingMode.UP).longValue
          val desiredPartitions = getFilePartitionsBySize(
            partitionedFiles, desiredSplitBytes, openCostBytes)
          logWarning(log"The number of partitions is ${MDC(NUM_PARTITIONS, partitions.size)}, " +
            log"which exceeds the maximum number configured: " +
            log"${MDC(MAX_NUM_PARTITIONS, maxPartNum.get)}. Spark rescales it to " +
            log"${MDC(DESIRED_NUM_PARTITIONS, desiredPartitions.size)} by ignoring the " +
            log"configuration of ${MDC(CONFIG, SQLConf.FILES_MAX_PARTITION_BYTES.key)}.")
          desiredPartitions
        } else {
          partitions
        }
    }
  }

  /**
   * Returns the max split bytes, given the total number of bytes taken by the selected
   * partitions.
   */
  def maxSplitBytes(sparkSession: SparkSession, calculateTotalBytes: => Long): Long = {
    val conf = getSqlConf(sparkSession)
    val defaultMaxSplitBytes = conf.filesMaxPartitionBytes
    val openCostInBytes = conf.filesOpenCostInBytes
    val minPartitionNum = conf.filesMinPartitionNum
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
    val openCostInBytes = getSqlConf(sparkSession).filesOpenCostInBytes
    val byteNum = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    maxSplitBytes(sparkSession, byteNum)
  }
}
