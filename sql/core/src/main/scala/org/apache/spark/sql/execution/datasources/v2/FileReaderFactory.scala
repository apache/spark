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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.TaskContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FilePartitionUtil, PartitionedFile}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class FileReaderFactory[T](
    file: FilePartition,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    ignoreCorruptFiles: Boolean = false,
    ignoreMissingFiles: Boolean = false)
  extends DataReaderFactory[T] {
  override def createDataReader(): DataReader[T] = {
    val taskContext = TaskContext.get()
    val iter = FilePartitionUtil.compute(file, taskContext, readFunction,
      ignoreCorruptFiles, ignoreMissingFiles)
    InternalRowDataReader[T](iter)
  }

  override def preferredLocations(): Array[String] = {
    FilePartitionUtil.getPreferredLocations(file)
  }
}

case class InternalRowDataReader[T](iter: Iterator[InternalRow])
  extends DataReader[T] {
  override def next(): Boolean = iter.hasNext

  override def get(): T = iter.next().asInstanceOf[T]

  override def close(): Unit = {}
}

abstract class FileDataReader[T] extends DataReader[T] {
  def file: PartitionedFile
}

case class FileReaderFoobar[T](
    context: TaskContext,
    readers: Iterator[FileDataReader[T]],
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean)
  extends DataReader[T] {
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
    private def updateBytesRead(): Unit = {
      inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
    }

    // If we can't get the bytes read from the FS stats, fall back to the file size,
    // which may be inaccurate.
    private def updateBytesReadWithFileSize(): Unit = {
      if (currentFile != null) {
        inputMetrics.incBytesRead(currentFile.file.length)
      }
    }

    private[this] var currentFile: FileDataReader[T] = null

  override def next(): Boolean = {
    // Kill the task in case it has been marked as killed. This logic is from
    // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
    // to avoid performance overhead.
    context.killTaskIfInterrupted()

    if (currentFile != null && currentFile.next()) {
      return true
    }
    while (readers.hasNext) {
      currentFile = readers.next()
      if (currentFile.next()) {
        return true
      }
    }

    return false
  }

  override def get(): T = {
    val nextElement = currentFile.get()
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

  override def close(): Unit = {
    updateBytesRead()
    updateBytesReadWithFileSize()
    InputFileBlockHolder.unset()
    if (currentFile != null) {
      currentFile.close()
    }
  }
}
