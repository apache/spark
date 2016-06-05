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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{OutputCommitter, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.sql.types.StructType

private[datasources] abstract class StreamingOutputWriterFactory extends OutputWriterFactory {

  /** Create a [[RecordWriter]] that writes the given path without using OutputCommitter */
  private[datasources] def createNoCommitterTextRecordWriter(
      path: String,
      hadoopAttemptContext: TaskAttemptContext,
      defaultWorkingFileFuc: (TaskAttemptContext, String) => Path
    ): RecordWriter[NullWritable, Text] = {
    // Custom OutputFormat that disable use of committer and writes to the given path
    val outputFormat = new TextOutputFormat[NullWritable, Text]() {
      override def getOutputCommitter(c: TaskAttemptContext): OutputCommitter = { null }
      override def getDefaultWorkFile(c: TaskAttemptContext, ext: String): Path = {
        defaultWorkingFileFuc(c, ext)
      }
    }
    outputFormat.getRecordWriter(hadoopAttemptContext)
  }

  /** Disable the use of the older API. */
  override def newInstance(
      path: String,
      bucketId: Option[Int],
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} does not support this version of newInstance")
  }
}
