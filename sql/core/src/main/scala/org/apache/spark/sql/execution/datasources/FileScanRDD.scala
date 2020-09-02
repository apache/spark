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

import org.apache.spark.{Partition => RDDPartition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

/**
 * A part (i.e. "block") of a single file that should be read, along with partition column values
 * that need to be prepended to each row.
 *
 * @param partitionValues value of partition columns to be prepended to each row.
 * @param filePath URI of the file to read
 * @param start the beginning offset (in bytes) of the block.
 * @param length number of bytes to read.
 * @param locations locality information (list of nodes that have the data).
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long,
    @transient locations: Array[String] = Array.empty) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
 * The mode for scanning a list of file partitions.
 *
 * [[RowMode]]: Scan files sequentially one by one, and scan each file row by row.
 * [[BatchMode]]: Scan files sequentially one by one, and scan each file batch by batch.
 * [[SortedBucketMode]]: Scan files together at same time in a sort-merge way
 *                       (for sorted bucket files only).
 */
sealed abstract class ScanMode

case object RowMode extends ScanMode

case object BatchMode extends ScanMode

case class SortedBucketMode(sortOrdering: Ordering[InternalRow]) extends ScanMode {
  override def toString: String = "SortedBucketMode"
}

/**
 * An RDD that scans a list of file partitions.
 *
 * @param scanMode the mode for scanning files. Based on scan mode, the corresponding
 *                 subclass of [[BaseFileScanIterator]] will be used to scan files.
 */
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition],
    val scanMode: ScanMode)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = scanMode match {
      case RowMode => new FileRowScanIterator(split, context, ignoreCorruptFiles,
        ignoreMissingFiles, readFunction)

      case BatchMode => new FileBatchScanIterator(split, context, ignoreCorruptFiles,
        ignoreMissingFiles, readFunction)

      case SortedBucketMode(sortOrdering) => new FileSortedBucketScanIterator(split, context,
        ignoreCorruptFiles, ignoreMissingFiles, readFunction, sortOrdering)

      case x => throw new IllegalArgumentException(
        s"${getClass.getSimpleName} not take $x as the ScanMode")
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = filePartitions.toArray

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    split.asInstanceOf[FilePartition].preferredLocations()
  }
}
