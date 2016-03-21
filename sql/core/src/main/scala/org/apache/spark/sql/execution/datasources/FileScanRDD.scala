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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{RDD, SqlNewHadoopRDDState}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow

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
  override def toString(): String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}


/**
 * A collection of files that should be read as a single task possibly from multiple partitioned
 * directories.
 *
 * TODO: This currently does not take locality information about the files into account.
 */
case class FilePartition(val index: Int, files: Seq[PartitionedFile]) extends Partition

class FileScanRDD(
    @transient val sqlContext: SQLContext,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
    extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iterator = new Iterator[Object] with AutoCloseable {
      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentIterator: Iterator[Object] = null

      def hasNext = (currentIterator != null && currentIterator.hasNext) || nextIterator()
      def next() = currentIterator.next()

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          val nextFile = files.next()
          logInfo(s"Reading File $nextFile")
          SqlNewHadoopRDDState.setInputFileName(nextFile.filePath)
          currentIterator = readFunction(nextFile)
          hasNext
        } else {
          SqlNewHadoopRDDState.unsetInputFileName()
          false
        }
      }

      override def close() = {
        SqlNewHadoopRDDState.unsetInputFileName()
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[Partition] = filePartitions.toArray
}
