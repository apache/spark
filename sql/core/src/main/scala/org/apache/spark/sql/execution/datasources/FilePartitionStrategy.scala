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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

/**
 * An (internal) interface that takes in a list of files and partitions them for parallelization.
 */
trait FilePartitionStrategy {
  /**
   * `input` is a list of input files, in the form of (partition column value, file status).
   *
   * The function should return a list of file blocks to read for each partition. The i-th position
   * indicates the list of file blocks to read for task i.
   */
  def partition(
      sparkSession: SparkSession,
      fileFormat: FileFormat,
      options: Map[String, String],
      input: Seq[(InternalRow, FileStatus)])
    : Seq[Seq[PartitionedFile]]
}


/**
 * A default [[FilePartitionStrategy]] that binpacks files roughly into evenly sized partitions.
 */
class DefaultFilePartitionStrategy extends FilePartitionStrategy with Logging {
  import DefaultFilePartitionStrategy._

  override def partition(
      sparkSession: SparkSession,
      fileFormat: FileFormat,
      options: Map[String, String],
      input: Seq[(InternalRow, FileStatus)])
    : Seq[Seq[PartitionedFile]] = {

    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = input.map(_._2.getLen + openCostInBytes).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles: Array[PartitionedFile] = input.flatMap { case (partitionValues, file) =>
      val blockLocations = getBlockLocations(file)
      if (fileFormat.isSplitable(sparkSession, options, file.getPath)) {
        (0L until file.getLen by maxSplitBytes).map { offset =>
          val remaining = file.getLen - offset
          val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
          val hosts = getBlockHosts(blockLocations, offset, size)
          PartitionedFile(partitionValues, file.getPath.toUri.toString, offset, size, hosts)
        }
      } else {
        val hosts = getBlockHosts(blockLocations, 0, file.getLen)
        Seq(PartitionedFile(partitionValues, file.getPath.toUri.toString, 0, file.getLen, hosts))
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = new ArrayBuffer[Seq[PartitionedFile]]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition = currentFiles.toArray.toSeq // Copy to a new Array.
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "First Fit Decreasing" (FFD)
    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()

    partitions
  }
}


object DefaultFilePartitionStrategy {
  def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  /**
   * Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
   * pair that represents a segment of the same file, find out the block that contains the largest
   * fraction the segment, and returns location hosts of that block. If no such block can be found,
   * returns an empty array.
   */
  def getBlockHosts(
    blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}
