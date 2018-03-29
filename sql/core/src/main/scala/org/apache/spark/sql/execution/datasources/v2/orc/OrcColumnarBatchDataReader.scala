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
package org.apache.spark.sql.execution.datasources.v2.orc

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OrcColumnarBatchReader, OrcUtils}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class OrcBatchDataReaderFactory(
    file: FilePartition,
    dataSchema: StructType,
    partitionSchema: StructType,
    readSchema: StructType,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readerConf: OrcDataReaderFactoryConf)
  extends DataReaderFactory[ColumnarBatch] {
  private val readFunction = (file: PartitionedFile) => {
    val conf = broadcastedConf.value.value
    val filePath = new Path(new URI(file.filePath))
    val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val reader = OrcFile.createReader(filePath, readerOptions)
    val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
      readerConf.isCaseSensitive, dataSchema, readSchema, reader, conf)
    if (requestedColIdsOrEmptyFile.isEmpty) {
      Iterator.empty
    } else {
      val requestedColIds = requestedColIdsOrEmptyFile.get
      assert(requestedColIds.length == readSchema.length,
        "[BUG] requested column IDs do not match required schema")
      val taskContext = Option(TaskContext.get())
      val batchReader = new OrcColumnarBatchReader(
        readerConf.enableOffHeapColumnVector && taskContext.isDefined,
        readerConf.copyToSpark, readerConf.capacity)

      // SPARK-23399 Register a task completion listener first to call `close()` in all cases.
      // There is a possibility that `initialize` and `initBatch` hit some errors (like OOM)
      // after opening a file.
      val iter = new RecordReaderIterator(batchReader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => batchReader.close()))

      val taskConf = new Configuration(conf)
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
        requestedColIds.filter(_ != -1).sorted.mkString(","))
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)
      batchReader.initialize(fileSplit, taskAttemptContext)

      val partitionColIds = PartitioningUtils.requestedPartitionColumnIds(
        partitionSchema, readSchema, readerConf.isCaseSensitive)
      batchReader.initBatch(
        reader.getSchema,
        readSchema.fields,
        requestedColIds,
        partitionColIds,
        file.partitionValues)
      iter.asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def createDataReader(): DataReader[ColumnarBatch] = {
    val taskContext = TaskContext.get()
    val iter = FilePartitionUtil.compute(file, taskContext, readFunction)
    OrcColumnarBatchDataReader(iter)
  }

  override def preferredLocations(): Array[String] = {
    FilePartitionUtil.getPreferredLocations(file)
  }
}

case class OrcColumnarBatchDataReader(iter: Iterator[InternalRow])
  extends DataReader[ColumnarBatch] {
  override def next(): Boolean = iter.hasNext

  override def get(): ColumnarBatch = iter.next().asInstanceOf[ColumnarBatch]

  override def close(): Unit = {}
}
