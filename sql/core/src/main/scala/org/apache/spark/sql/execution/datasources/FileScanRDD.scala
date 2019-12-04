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
import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.{Partition => RDDPartition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{NextIterator, SerializableConfiguration}

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
 * An RDD that scans a list of file partitions.
 */
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new PartitionScanIterator(split, context)
    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = filePartitions.toArray

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    split.asInstanceOf[FilePartition].preferredLocations()
  }

  class PartitionScanIterator(
      split: RDDPartition,
      context: TaskContext) extends Iterator[Object] with AutoCloseable {
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

    protected var files = split.asInstanceOf[FilePartition].files.toIterator
    protected var currentFile: PartitionedFile = null
    private var currentIterator: Iterator[Object] = null

    def hasNext: Boolean = {
      // Kill the task in case it has been marked as killed. This logic is from
      // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
      // to avoid performance overhead.
      context.killTaskIfInterrupted()
      (currentIterator != null && currentIterator.hasNext) || nextIterator()
    }
    def next(): Object = {
      val nextElement = currentIterator.next()
      // TODO: we should have a better separation of row based and batch based scan, so that we
      // don't need to run this `if` for every record.
      val preNumRecordsRead = inputMetrics.recordsRead
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
      nextElement
    }

    private def readCurrentFile(): Iterator[InternalRow] = {
      try {
        readFunction(currentFile)
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
            // The readFunction may read some bytes before consuming the iterator, e.g.,
            // vectorized Parquet reader. Here we use lazy val to delay the creation of
            // iterator so that we will throw exception in `getNext`.
            private lazy val internalIter = readCurrentFile()

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
          currentIterator = readCurrentFile()
        }

        try {
          hasNext
        } catch {
          case e: SchemaColumnConvertNotSupportedException =>
            val message = "Parquet column cannot be converted in " +
              s"file ${currentFile.filePath}. Column: ${e.getColumn}, " +
              s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
            throw new QueryExecutionException(message, e)
          case e: ParquetDecodingException =>
            if (e.getMessage.contains("Can not read value at")) {
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
}

class SinglePartitionFileScanRDD(
    sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    filePartitions: Seq[FilePartition],
    rootPaths: Map[String, InternalRow])
  extends FileScanRDD(sparkSession, readFunction, filePartitions) {

  val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(
    new SerializableConfiguration(sparkSession.sessionState.newHadoopConf()))

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new PartitionAndRootFilesScanIterator(split, context, rootPaths)
    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())
    iterator.asInstanceOf[Iterator[InternalRow]]
  }

  class PartitionAndRootFilesScanIterator(
      split: RDDPartition,
      context: TaskContext,
      rootPaths: Map[String, InternalRow]) extends PartitionScanIterator(split, context) {

    private val rootDirs = rootPaths.toIterator
    private val fileSet = split.asInstanceOf[FilePartition].files.map(_.filePath).toSet

    override def nextIterator(): Boolean = {
      if (super.nextIterator()) {
        true
      } else {
        if (rootDirs.hasNext) {
          val nextPath = rootDirs.next()
          val parentPath = nextPath._1
          logInfo(s"list root path ${parentPath} to future out more files to scan.")
          val leafFiles = listLeafFiles(new Path(new URI(parentPath)), fileSet)
          files = leafFiles.map { file =>
            PartitionedFile(nextPath._2, file.getPath.toUri.toString, 0, file.getLen)}.toIterator

          nextIterator
        } else {
          currentFile = null
          InputFileBlockHolder.unset()
          false
        }
      }
    }

    /**
     * Lists a single filesystem path recursively.
     * @return all children of path that match the specified filter.
     */
    private def listLeafFiles(path: Path, excludedFileSet: Set[String]): Seq[FileStatus] = {
      val hadoopConf = broadcastedHadoopConf.value.value
      val fs = path.getFileSystem(hadoopConf)
      val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))

      val statuses: Array[FileStatus] =
        try {
          fs.listStatus(path)
        } catch {
          case _: FileNotFoundException =>
            logWarning(s"The directory $path was not found. Was it deleted very recently?")
            Array.empty[FileStatus]
        }
      val filteredStatuses = statuses.filterNot(status =>
        shouldFilterOut(status.getPath, excludedFileSet))
      val allLeafStatuses = {
        val (dirs, topLevelFiles) = filteredStatuses.partition(_.isDirectory)
        val nestedFiles: Seq[FileStatus] = dirs.flatMap(dir =>
          listLeafFiles(dir.getPath, excludedFileSet))

        val allFiles = topLevelFiles ++ nestedFiles
        if (filter != null) allFiles.filter(f => filter.accept(f.getPath)) else allFiles
      }
      allLeafStatuses.filterNot(status => shouldFilterOut(status.getPath, excludedFileSet))
    }

    /** Checks if we should filter out this path. */
    def shouldFilterOut(path: Path, excludedFileSet: Set[String]): Boolean = {
      if (excludedFileSet.contains(path.toUri.toString)) {
        true
      } else {
        val pathName = path.getName
        // We filter follow paths:
        // 1. everything that starts with _ and ., except _common_metadata and _metadata
        // because Parquet needs to find those metadata files from leaf files returned
        // by this method.
        // We should refactor this logic to not mix metadata files with data files.
        // 2. everything that ends with `._COPYING_`, because this is a intermediate
        // state of file. we should skip this file in case of double reading.
        val exclude = (pathName.startsWith("_") && !pathName.contains("=")) ||
          pathName.startsWith(".") || pathName.endsWith("._COPYING_")
        val include = pathName.startsWith("_common_metadata") || pathName.startsWith("_metadata")
        exclude && !include
      }
    }
  }
}
