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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OrcDeserializer, OrcUtils}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class OrcUnsafeRowReaderFactory(
    file: FilePartition,
    dataSchema: StructType,
    partitionSchema: StructType,
    readSchema: StructType,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readerConf: OrcDataReaderFactoryConf)
  extends DataReaderFactory[UnsafeRow] {
  private val readFunction = (file: PartitionedFile) => {
    val conf = broadcastedConf.value.value

    val filePath = new Path(new URI(file.filePath))

    val fs = filePath.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    val reader = OrcFile.createReader(filePath, readerOptions)

    val requiredSchema =
      PartitioningUtils.subtractSchema(readSchema, partitionSchema, readerConf.isCaseSensitive)
    val requestedColIdsOrEmptyFile = OrcUtils.requestedColumnIds(
      readerConf.isCaseSensitive, dataSchema, requiredSchema, reader, conf)
    if (requestedColIdsOrEmptyFile.isEmpty) {
      Iterator.empty
    } else {
      val requestedColIds = requestedColIdsOrEmptyFile.get
      assert(requestedColIds.length == requiredSchema.length,
        "[BUG] requested column IDs o not match required schema")
      val taskConf = new Configuration(conf)
      taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,
        requestedColIds.filter(_ != -1).sorted.mkString(","))

      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      val taskContext = Option(TaskContext.get())
      val orcRecordReader = new OrcInputFormat[OrcStruct]
        .createRecordReader(fileSplit, taskAttemptContext)
      val iter = new RecordReaderIterator[OrcStruct](orcRecordReader)
      taskContext.foreach(_.addTaskCompletionListener(_ => iter.close()))

      val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
      val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
      val deserializer = new OrcDeserializer(dataSchema, requiredSchema, requestedColIds)

      if (partitionSchema.length == 0) {
        iter.map(value => unsafeProjection(deserializer.deserialize(value)))
      } else {
        val joinedRow = new JoinedRow()
        iter.map(value =>
          unsafeProjection(joinedRow(deserializer.deserialize(value), file.partitionValues)))
      }
    }
  }

  override def createDataReader(): DataReader[UnsafeRow] = {
    val taskContext = TaskContext.get()
    val iter = FilePartitionUtil.compute(file, taskContext, readFunction,
      readerConf.ignoreCorruptFiles, readerConf.ignoreMissingFiles)
    OrcUnsafeRowDataReader(iter)
  }

  override def preferredLocations(): Array[String] = {
    FilePartitionUtil.getPreferredLocations(file)
  }
}

case class OrcUnsafeRowDataReader(iter: Iterator[InternalRow])
  extends DataReader[UnsafeRow] {
  override def next(): Boolean = iter.hasNext

  override def get(): UnsafeRow = iter.next().asInstanceOf[UnsafeRow]

  override def close(): Unit = {}
}
