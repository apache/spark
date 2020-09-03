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

import java.io.{FileNotFoundException, IOException}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.{Partition => RDDPartition, SparkUpgradeException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{QueryExecutionException, RowIterator}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

/**
 * Holds common logic for iterators to scan files
 */
abstract class BaseFileScanIterator(
    split: RDDPartition,
    context: TaskContext,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean,
    readFunction: PartitionedFile => Iterator[InternalRow])
  extends Iterator[Object]
  with AutoCloseable
  with Logging {

  protected val inputMetrics: InputMetrics = context.taskMetrics().inputMetrics
  private val existingBytesRead = inputMetrics.bytesRead

  // Find a function that will return the FileSystem bytes read by this thread. Do this before
  // apply readFunction, because it might read some bytes.
  private val getBytesReadCallback =
    SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

  // We get our input bytes from thread-local Hadoop FileSystem statistics.
  // If we do a coalesce, however, we are likely to compute multiple partitions in the same
  // task and in the same thread, in which case we need to avoid override values written by
  // previous partitions (SPARK-13071).
  protected def incTaskInputMetricsBytesRead(): Unit = {
    inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
  }

  private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
  protected[this] var currentFile: PartitionedFile = null
  protected[this] var currentIterator: Iterator[Object] = null

  override def hasNext: Boolean = {
    // Kill the task in case it has been marked as killed. This logic is from
    // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
    // to avoid performance overhead.
    context.killTaskIfInterrupted()
    (currentIterator != null && currentIterator.hasNext) || nextIterator()
  }

  override def next(): Object

  private def readFile(file: PartitionedFile): Iterator[InternalRow] = {
    try {
      readFunction(file)
    } catch {
      case e: FileNotFoundException =>
        throw new FileNotFoundException(
          e.getMessage + "\n" +
            "It is possible the underlying files have been updated. " +
            "You can explicitly invalidate the cache in Spark by " +
            "running 'REFRESH TABLE tableName' command in SQL or " +
            "by recreating the Dataset/DataFrame involved.")
    }
  }

  /** Advances to the next file. Returns true if a new non-empty iterator is available. */
  protected def nextIterator(): Boolean = {
    if (files.hasNext) {
      currentFile = files.next()
      logInfo(s"Reading File $currentFile")
      // Sets InputFileBlockHolder for the file block's information
      InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)

      if (ignoreMissingFiles || ignoreCorruptFiles) {
        currentIterator = new NextIterator[Object] {
          private val file = currentFile
          // The readFunction may read some bytes before consuming the iterator, e.g.,
          // vectorized Parquet reader. Here we use lazy val to delay the creation of
          // iterator so that we will throw exception in `getNext`.
          private lazy val internalIter = readFile(file)

          override def getNext(): AnyRef = {
            try {
              if (internalIter.hasNext) {
                internalIter.next()
              } else {
                finished = true
                null
              }
            } catch {
              case e: FileNotFoundException if ignoreMissingFiles =>
                logWarning(s"Skipped missing file: $currentFile", e)
                finished = true
                null
              // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
              case e: FileNotFoundException if !ignoreMissingFiles => throw e
              case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                logWarning(
                  s"Skipped the rest of the content in the corrupted file: $currentFile", e)
                finished = true
                null
            }
          }

          override def close(): Unit = {}
        }
      } else {
        currentIterator = readFile(currentFile)
      }

      try {
        context.killTaskIfInterrupted()
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      } catch {
        case e: SchemaColumnConvertNotSupportedException =>
          val message = "Parquet column cannot be converted in " +
            s"file ${currentFile.filePath}. Column: ${e.getColumn}, " +
            s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
          throw new QueryExecutionException(message, e)
        case e: ParquetDecodingException =>
          if (e.getCause.isInstanceOf[SparkUpgradeException]) {
            throw e.getCause
          } else if (e.getMessage.contains("Can not read value at")) {
            val message = "Encounter error while reading parquet files. " +
              "One possible cause: Parquet column cannot be converted in the " +
              "corresponding files. Details: "
            throw new QueryExecutionException(message, e)
          }
          throw e
      }
    } else {
      currentFile = null
      InputFileBlockHolder.unset()
      false
    }
  }

  override def close(): Unit = {
    incTaskInputMetricsBytesRead()
    InputFileBlockHolder.unset()
  }
}

/**
 * Iterator to scan files row by row
 */
class FileRowScanIterator(
    split: RDDPartition,
    context: TaskContext,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean,
    readFunction: PartitionedFile => Iterator[InternalRow])
  extends BaseFileScanIterator(split, context, ignoreCorruptFiles, ignoreMissingFiles,
    readFunction) {

  override def next(): Object = {
    val nextRow = currentIterator.next()

    // Too costly to update every record
    if (inputMetrics.recordsRead %
        SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
      incTaskInputMetricsBytesRead()
    }
    inputMetrics.incRecordsRead(1)
    nextRow
  }
}

/**
 * Iterator to scan files batch by batch
 */
class FileBatchScanIterator(
    split: RDDPartition,
    context: TaskContext,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean,
    readFunction: PartitionedFile => Iterator[InternalRow])
  extends BaseFileScanIterator(split, context, ignoreCorruptFiles, ignoreMissingFiles,
    readFunction) {

  override def next(): Object = {
    val nextBatch = currentIterator.next()
    incTaskInputMetricsBytesRead()
    inputMetrics.incRecordsRead(nextBatch.asInstanceOf[ColumnarBatch].numRows())
    nextBatch
  }
}

/**
 * Iterator to scan files all together at the same time,
 * and read row by row based on `sortOrdering` in sort-merge way.
 * It uses standard scala priority queue to decide read order.
 * This iterator is used for reading sorted bucketed table only.
 *
 * @param sortOrdering The order to read rows in multiple files
 */
class FileSortedBucketScanIterator(
    split: RDDPartition,
    context: TaskContext,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean,
    readFunction: PartitionedFile => Iterator[InternalRow],
    sortOrdering: Ordering[InternalRow])
  extends BaseFileScanIterator(split, context, ignoreCorruptFiles, ignoreMissingFiles,
    readFunction) {

  // The priority queue to keep the latest row from each file
  private val rowHeap = new mutable.PriorityQueue[IteratorWithRow]()(
    // Reverse the order as priority queue de-queues the highest priority one
    Ordering.by[IteratorWithRow, InternalRow](_.getRow)(sortOrdering).reverse)
  private var heapInitialized: Boolean = false
  protected var currentIteratorWithRow: IteratorWithRow = _

  override def hasNext: Boolean = {
    // Kill the task in case it has been marked as killed. This logic is from
    // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
    // to avoid performance overhead. This is the same as `BaseFileScanIterator.hasNext()`.
    context.killTaskIfInterrupted()

    if (!heapInitialized) {
      initializeHeapWithFirstRows()
      heapInitialized = true
    }
    rowHeap.nonEmpty
  }

  override def next(): Object = {
    currentIteratorWithRow = rowHeap.dequeue()

    // Make a copy of row because we need to enqueue next row (if there any) to the heap
    val nextRow = currentIteratorWithRow.getRow.copy()
    if (currentIteratorWithRow.advanceNext()) {
      rowHeap.enqueue(currentIteratorWithRow)
    }

    // Too costly to update every record
    if (inputMetrics.recordsRead %
      SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
      incTaskInputMetricsBytesRead()
    }
    inputMetrics.incRecordsRead(1)

    // Set InputFileBlockHolder for the file block's information
    currentFile = currentIteratorWithRow.getFile
    InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)

    nextRow
  }

  private def initializeHeapWithFirstRows(): Unit = {
    while (nextIterator()) {
      require(currentIterator != null && currentFile != null,
        "currentIterator and currentFile should not be null if nextIterator() returns true")
      // In case of columnar batch, read row by row in one batch
      val convertedIter: Iterator[InternalRow] = currentIterator.flatMap {
        case batch: ColumnarBatch => batch.rowIterator().asScala
        case row => Iterator.single(row.asInstanceOf[InternalRow])
      }
      currentIteratorWithRow = new IteratorWithRow(convertedIter, currentFile)
      if (currentIteratorWithRow.advanceNext()) {
        rowHeap.enqueue(currentIteratorWithRow)
      }
    }
  }
}

/**
 * A wrapper for iterator, its file and its current latest row.
 * Designed to be instantiated once per each file in one thread, and reused.
 */
private[execution] class IteratorWithRow(
    iterator: Iterator[InternalRow],
    file: PartitionedFile) extends RowIterator {
  private var row: InternalRow = _

  override def advanceNext(): Boolean = {
    if (iterator.hasNext) {
      row = iterator.next()
      true
    } else {
      row = null
      false
    }
  }

  override def getRow: InternalRow = row

  def getFile: PartitionedFile = file
}
