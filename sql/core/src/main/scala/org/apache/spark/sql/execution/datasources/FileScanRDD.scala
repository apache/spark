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
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{Partition => RDDPartition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileNameHolder, RDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * A single file that should be read, along with partition column values that
 * need to be prepended to each row.  The reading should start at the first
 * valid record found after `start`.
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long,
    locations: Array[String] = Array.empty) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
 * A collection of files that should be read as a single task possibly from multiple partitioned
 * directories.
 *
 * TODO: This currently does not take locality information about the files into account.
 */
case class FilePartition(index: Int, files: Seq[PartitionedFile]) extends RDDPartition

object FileScanRDD {
  private val ioExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("FileScanRDD", 16))
}

class FileScanRDD(
    @transient val sqlContext: SQLContext,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
  extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  /**
   * To get better interleaving of CPU and IO, this RDD will create a future to prepare the next
   * file while the current one is being processed. `currentIterator` is the current file and
   * `nextFile` is the future that will initialize the next file to be read. This includes things
   * such as starting up connections to open the file and any initial buffering. The expectation
   * is that `currentIterator` is CPU intensive and `nextFile` is IO intensive.
   */
  val isAsyncIOEnabled = sqlContext.conf.filesAsyncIO

  case class NextFile(file: PartitionedFile, iter: Iterator[Object])

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[Object] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // apply readFunction, because it might read some bytes.
      private val getBytesReadCallback: Option[() => Long] =
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

      // For Hadoop 2.5+, we get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      // If we can't get the bytes read from the FS stats, fall back to the file size,
      // which may be inaccurate.
      private def updateBytesReadWithFileSize(): Unit = {
        if (getBytesReadCallback.isEmpty && currentFile != null) {
          inputMetrics.incBytesRead(currentFile.length)
        }
      }

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null

      private[this] var nextFile: Future[NextFile] =
        if (isAsyncIOEnabled) prepareNextFile() else null

      def hasNext = (currentIterator != null && currentIterator.hasNext) || nextIterator()
      def next() = {
        val nextElement = currentIterator.next()
        // TODO: we should have a better separation of row based and batch based scan, so that we
        // don't need to run this `if` for every record.
        if (nextElement.isInstanceOf[ColumnarBatch]) {
          inputMetrics.incRecordsRead(nextElement.asInstanceOf[ColumnarBatch].numRows())
        } else {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        nextElement
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        updateBytesReadWithFileSize()
        if (isAsyncIOEnabled) {
          if (nextFile != null) {
            // Wait for the async task to complete
            val file = ThreadUtils.awaitResult(nextFile, Duration.Inf)
            InputFileNameHolder.setInputFileName(file.file.filePath)
            currentIterator = file.iter
            // Asynchronously start the next file.
            nextFile = prepareNextFile()
            hasNext
          } else {
            currentFile = null
            InputFileNameHolder.unsetInputFileName()
            false
          }
        } else {
          if (files.hasNext) {
            currentFile = files.next()
            logInfo(s"Reading File $currentFile")
            InputFileNameHolder.setInputFileName(currentFile.filePath)
            currentIterator = readFunction(currentFile)
            hasNext
          } else {
            currentFile = null
            InputFileNameHolder.unsetInputFileName()
            false
          }
        }
      }

      override def close() = {
        updateBytesRead()
        updateBytesReadWithFileSize()
        InputFileNameHolder.unsetInputFileName()
      }

      def prepareNextFile(): Future[NextFile] = {
        if (files.hasNext) {
          Future {
            val nextFile = files.next()
            val nextFileIter = readFunction(nextFile)
            // Read something from the file to trigger some initial IO.
            nextFileIter.hasNext
            NextFile(nextFile, nextFileIter)
          }(FileScanRDD.ioExecutionContext)
        } else {
          null
        }
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = filePartitions.toArray

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    val files = split.asInstanceOf[FilePartition].files

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
    }
  }
}
