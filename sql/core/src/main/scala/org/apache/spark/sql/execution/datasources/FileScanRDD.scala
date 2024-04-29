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
import java.net.URI

import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition => RDDPartition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.internal.MDC
import org.apache.spark.paths.SparkPath
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow, JoinedRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.FileFormat._
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, ConstantColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.ArrayImplicits._
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
 * @param otherConstantMetadataColumnValues The values of any additional constant metadata columns.
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: SparkPath,
    start: Long,
    length: Long,
    @transient locations: Array[String] = Array.empty,
    modificationTime: Long = 0L,
    fileSize: Long = 0L,
    otherConstantMetadataColumnValues: Map[String, Any] = Map.empty) {

  def pathUri: URI = filePath.toUri
  def toPath: Path = filePath.toPath
  def urlEncodedPath: String = filePath.urlEncoded

  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
 * An RDD that scans a list of file partitions.
 * @param metadataColumns File-constant metadata columns to append to end of schema.
 */
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition],
    val readSchema: StructType,
    val metadataColumns: Seq[AttributeReference] = Seq.empty,
    metadataExtractors: Map[String, PartitionedFile => Any] = Map.empty,
    options: FileSourceOptions = new FileSourceOptions(CaseInsensitiveMap(Map.empty)))
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = options.ignoreCorruptFiles
  private val ignoreMissingFiles = options.ignoreMissingFiles

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

      private[this] val files = split.asInstanceOf[FilePartition].files.iterator
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

      // This is called by both `hasNext` and `nextIterator`. We do not merge it into `hasNext`,
      // to make the error enhancement in `hasNext` only apply once in the recursion (`nextIterator`
      // calls `hasNext0`).
      private def hasNext0: Boolean = {
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }

      def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
        try {
          hasNext0
        } catch {
          case e: Throwable => throw FileDataSourceV2.attachFilePath(currentFile.urlEncodedPath, e)
        }
      }

      ///////////////////////////
      // FILE METADATA METHODS //
      ///////////////////////////

      // a metadata internal row, will only be updated when the current file is changed
      val metadataRow: InternalRow = new GenericInternalRow(metadataColumns.length)

      // an unsafe projection to convert a joined internal row to an unsafe row
      private lazy val projection = {
        val joinedExpressions =
          readSchema.fields.map(_.dataType) ++ metadataColumns.map(_.dataType)
        UnsafeProjection.create(joinedExpressions)
      }

      /**
       * The value of some of the metadata columns remains exactly the same for each record of
       * a partitioned file. Only need to update their values in the metadata row when `currentFile`
       * is changed.
       */
      private def updateMetadataRow(): Unit =
        if (metadataColumns.nonEmpty && currentFile != null) {
          updateMetadataInternalRow(
            metadataRow, metadataColumns.map(_.name), currentFile, metadataExtractors)
        }

      /**
       * Create an array of constant column vectors containing all required metadata columns
       */
      private def createMetadataColumnVector(c: ColumnarBatch): Array[ColumnVector] = {
        val tmpRow = new GenericInternalRow(1)
        metadataColumns.map { attr =>
          // Populate each metadata column by passing the resulting value through `tmpRow`.
          getFileConstantMetadataColumnValue(attr.name, currentFile, metadataExtractors) match {
            case Literal(null, _) =>
              tmpRow.setNullAt(0)
            case literal =>
              require(PhysicalDataType(attr.dataType) == PhysicalDataType(literal.dataType))
              tmpRow.update(0, literal.value)
          }

          val columnVector = new ConstantColumnVector(c.numRows(), attr.dataType)
          ColumnVectorUtils.populate(columnVector, tmpRow, 0)
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
            case c: ColumnarBatch => new ColumnarBatch(
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
        nextElement match {
          case batch: ColumnarBatch =>
            incTaskInputMetricsBytesRead()
            inputMetrics.incRecordsRead(batch.numRows())
          case _ =>
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
        readFunction(currentFile)
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          updateMetadataRow()
          logInfo(s"Reading File $currentFile")
          // Sets InputFileBlockHolder for the file block's information
          InputFileBlockHolder
            .set(currentFile.urlEncodedPath, currentFile.start, currentFile.length)

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
                    logWarning(log"Skipped missing file: ${MDC(PATH, currentFile)}", e)
                    finished = true
                    null
                  // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
                  case e: FileNotFoundException if !ignoreMissingFiles => throw e
                  case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                    logWarning(log"Skipped the rest of the content in the corrupted file: " +
                      log"${MDC(PATH, currentFile)}", e)
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

          hasNext0
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
    split.asInstanceOf[FilePartition].preferredLocations().toImmutableArraySeq
  }
}
