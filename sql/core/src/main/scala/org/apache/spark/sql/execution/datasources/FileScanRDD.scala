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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{RDD, SqlNewHadoopRDDState}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.ThreadUtils

/**
 * A single file that should be read, along with partition column values that
 * need to be prepended to each row.  The reading should start at the first
 * valid record found after `offset`.
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long) {
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
case class FilePartition(index: Int, files: Seq[PartitionedFile]) extends Partition

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
  val asyncIO = sqlContext.conf.filesAsyncIO

  case class NextFile(file: PartitionedFile, iter: Iterator[Object])

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[Object] with AutoCloseable {
      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      // TODO: do we need to close this?
      private[this] var currentIterator: Iterator[Object] = null

      private[this] var nextFile: Future[NextFile] = if (asyncIO) prepareNextFile() else null

      def hasNext = (currentIterator != null && currentIterator.hasNext) || nextIterator()
      def next() = currentIterator.next()

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        val file = if (asyncIO) {
          if (nextFile == null) return false
          // Wait for the async task to complete
          Await.result(nextFile, Duration.Inf)
        } else {
          if (!files.hasNext) return false
          val f = files.next()
          NextFile(f, readFunction(f))
        }

        // This is only used to evaluate the rest of the execution so we can safely set it here.
        SqlNewHadoopRDDState.setInputFileName(file.file.filePath)
        currentIterator = file.iter

        if (asyncIO) {
          // Asynchronously start the next file.
          nextFile = prepareNextFile()
        }

        hasNext
      }

      override def close() = {
        SqlNewHadoopRDDState.unsetInputFileName()
      }

      def prepareNextFile() = {
        if (files.hasNext) {
          Future {
            val file = files.next()
            val it = readFunction(file)
            // Read something from the file to trigger some initial IO.
            it.hasNext
            NextFile(file, it)
          }(FileScanRDD.ioExecutionContext)
        } else {
          null
        }
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[Partition] = filePartitions.toArray
}
