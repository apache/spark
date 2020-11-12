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

import java.util.Date

import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.execution.datasources.{DynamicPartitionDataWriter, SingleDirectoryDataWriter, WriteJobDescription}

case class FileWriterFactory (
    description: WriteJobDescription,
    committer: FileCommitProtocol) extends DataWriterFactory {
  override def createWriter(partitionId: Int, realTaskId: Long): DataWriter[InternalRow] = {
    val taskAttemptContext = createTaskAttemptContext(partitionId)
    committer.setupTask(taskAttemptContext)
    if (description.partitionColumns.isEmpty) {
      new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
    } else {
      new DynamicPartitionDataWriter(description, taskAttemptContext, committer)
    }
  }

  private def createTaskAttemptContext(partitionId: Int): TaskAttemptContextImpl = {
    val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, partitionId)
    val taskAttemptId = new TaskAttemptID(taskId, 0)
    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskId.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }
}
