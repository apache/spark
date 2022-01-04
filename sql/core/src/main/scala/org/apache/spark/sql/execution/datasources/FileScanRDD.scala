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

import java.io.{Closeable, FileNotFoundException, IOException}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition => RDDPartition, SparkUpgradeException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.FileFormat._
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator

/**
 * A part (i.e. "block") of a single file that should be read, along with partition column values
 * that need to be prepended to each row.
 *
 * @param partitionValues value of partition columns to be prepended to each row.
 * @param filePath URI of the file to read
 * @param start the beginning offset (in bytes) of the block.
 * @param length number of bytes to read.
 * @param modificationTime The modification time of the input file, in milliseconds.
 * @param fileSize The length of the input file (not the block), in bytes.
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long,
    @transient locations: Array[String] = Array.empty,
    modificationTime: Long = 0L,
    fileSize: Long = 0L) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
 * An RDD that scans a list of file partitions.
 */
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition],
    val readDataSchema: StructType,
    val metadataColumns: Seq[AttributeReference] = Seq.empty)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[Object] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // apply readFunction, because it might read some bytes.
      private val getBytesReadCallback =
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def incTaskInputMetricsBytesRead(): Unit = {
        inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
      }

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null

      private def resetCurrentIterator(): Unit = {
        currentIterator match {
          case iter: NextIterator[_] =>
            iter.closeIfNeeded()
          case iter: Closeable =>
            iter.close()
          case _ => // do nothing
        }
        currentIterator = null
      }

      def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }

      ///////////////////////////
      // FILE METADATA METHODS //
      ///////////////////////////

      // a metadata internal row, will only be updated when the current file is changed
      val metadataRow: InternalRow = new GenericInternalRow(metadataColumns.length)

      // an unsafe projection to convert a joined internal row to an unsafe row
      private lazy val projection = {
        val joinedExpressions =
          readDataSchema.fields.map(_.dataType) ++ metadataColumns.map(_.dataType)
        UnsafeProjection.create(joinedExpressions)
      }

      /**
       * For each partitioned file, metadata columns for each record in the file are exactly same.
       * Only update metadata row when `currentFile` is changed.
       */
      private def updateMetadataRow(): Unit = {
        if (metadataColumns.nonEmpty && currentFile != null) {
          val path = new Path(currentFile.filePath)
          metadataColumns.zipWithIndex.foreach { case (attr, i) =>
            attr.name match {
              case FILE_PATH => metadataRow.update(i, UTF8String.fromString(path.toString))
              case FILE_NAME => metadataRow.update(i, UTF8String.fromString(path.getName))
              case FILE_SIZE => metadataRow.update(i, currentFile.fileSize)
              case FILE_MODIFICATION_TIME =>
                // the modificationTime from the file is in millisecond,
                // while internally, the TimestampType is stored in microsecond
                metadataRow.update(i, currentFile.modificationTime * 1000L)
            }
          }
        }
      }

      /**
       * Create a writable column vector containing all required metadata columns
       */
      private def createMetadataColumnVector(c: ColumnarBatch): Array[WritableColumnVector] = {
        val path = new Path(currentFile.filePath)
        val filePathBytes = path.toString.getBytes
        val fileNameBytes = path.getName.getBytes
        var rowId = 0
        metadataColumns.map(_.name).map {
          case FILE_PATH =>
            val columnVector = new OnHeapColumnVector(c.numRows(), StringType)
            rowId = 0
            // use a tight-loop for better performance
            while (rowId < c.numRows()) {
              columnVector.putByteArray(rowId, filePathBytes)
              rowId += 1
            }
            columnVector
          case FILE_NAME =>
            val columnVector = new OnHeapColumnVector(c.numRows(), StringType)
            rowId = 0
            // use a tight-loop for better performance
            while (rowId < c.numRows()) {
              columnVector.putByteArray(rowId, fileNameBytes)
              rowId += 1
            }
            columnVector
          case FILE_SIZE =>
            val columnVector = new OnHeapColumnVector(c.numRows(), LongType)
            columnVector.putLongs(0, c.numRows(), currentFile.fileSize)
            columnVector
          case FILE_MODIFICATION_TIME =>
            val columnVector = new OnHeapColumnVector(c.numRows(), LongType)
            // the modificationTime from the file is in millisecond,
            // while internally, the TimestampType is stored in microsecond
            columnVector.putLongs(0, c.numRows(), currentFile.modificationTime * 1000L)
            columnVector
        }.toArray
      }

      /**
       * Add metadata columns at the end of nextElement if needed.
       * For different row implementations, use different methods to update and append.
       */
      private def addMetadataColumnsIfNeeded(nextElement: Object): Object = {
        if (metadataColumns.nonEmpty) {
          nextElement match {
            case c: ColumnarBatch =>
              new ColumnarBatch(
                Array.tabulate(c.numCols())(c.column) ++ createMetadataColumnVector(c),
                c.numRows())
            case u: UnsafeRow => projection.apply(new JoinedRow(u, metadataRow))
            case i: InternalRow => new JoinedRow(i, metadataRow)
          }
        } else {
          nextElement
        }
      }

      def next(): Object = {
        val nextElement = currentIterator.next()
        // TODO: we should have a better separation of row based and batch based scan, so that we
        // don't need to run this `if` for every record.
        if (nextElement.isInstanceOf[ColumnarBatch]) {
          incTaskInputMetricsBytesRead()
          inputMetrics.incRecordsRead(nextElement.asInstanceOf[ColumnarBatch].numRows())
        } else {
          // too costly to update every record
          if (inputMetrics.recordsRead %
              SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
            incTaskInputMetricsBytesRead()
          }
          inputMetrics.incRecordsRead(1)
        }
        addMetadataColumnsIfNeeded(nextElement)
      }

      private def readCurrentFile(): Iterator[InternalRow] = {
        try {
          readFunction(currentFile)
        } catch {
          case e: FileNotFoundException =>
            throw QueryExecutionErrors.readCurrentFileNotFoundError(e)
        }
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          updateMetadataRow()
          logInfo(s"Reading File $currentFile")
          // Sets InputFileBlockHolder for the file block's information
          InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)

          resetCurrentIterator()
          if (ignoreMissingFiles || ignoreCorruptFiles) {
            currentIterator = new NextIterator[Object] {
              // The readFunction may read some bytes before consuming the iterator, e.g.,
              // vectorized Parquet reader. Here we use a lazily initialized variable to delay the
              // creation of iterator so that we will throw exception in `getNext`.
              private var internalIter: Iterator[InternalRow] = null

              override def getNext(): AnyRef = {
                try {
                  // Initialize `internalIter` lazily.
                  if (internalIter == null) {
                    internalIter = readCurrentFile()
                  }

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

              override def close(): Unit = {
                internalIter match {
                  case iter: Closeable =>
                    iter.close()
                  case _ => // do nothing
                }
              }
            }
          } else {
            currentIterator = readCurrentFile()
          }

          try {
            hasNext
          } catch {
            case e: SchemaColumnConvertNotSupportedException =>
              throw QueryExecutionErrors.unsupportedSchemaColumnConvertError(
                currentFile.filePath, e.getColumn, e.getLogicalType, e.getPhysicalType, e)
            case sue: SparkUpgradeException => throw sue
            case NonFatal(e) =>
              e.getCause match {
                case sue: SparkUpgradeException => throw sue
                case _ => throw QueryExecutionErrors.cannotReadFilesError(e, currentFile.filePath)
              }
          }
        } else {
          currentFile = null
          updateMetadataRow()
          InputFileBlockHolder.unset()
          false
        }
      }

      override def close(): Unit = {
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
        resetCurrentIterator()
      }
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
