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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.JobArtifactSet
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, _}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite


class PythonWrite(
    ds: PythonDataSourceV2,
    shortName: String,
    info: LogicalWriteInfo,
    isTruncate: Boolean
  ) extends Write {

  override def toString: String = shortName

  override def toBatch: BatchWrite = new PythonBatchWrite(ds, shortName, info, isTruncate)

  override def toStreaming: StreamingWrite =
    new PythonStreamingWrite(ds, shortName, info, isTruncate)

  override def description: String = "(Python)"

  override def supportedCustomMetrics(): Array[CustomMetric] =
    ds.source.createPythonMetrics()
}

/**
 * A [[BatchWrite]] for python data source writing. Responsible for generating the writer factory.
 * */
class PythonBatchWrite(
    ds: PythonDataSourceV2,
    shortName: String,
    info: LogicalWriteInfo,
    isTruncate: Boolean
  ) extends BatchWrite {

  // Store the pickled data source writer instance.
  private var pythonDataSourceWriter: Array[Byte] = _

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def createBatchWriterFactory(physicalInfo: PhysicalWriteInfo): DataWriterFactory =
  {
    val writeInfo = ds.source.createWriteInfoInPython(
      shortName,
      info.schema(),
      info.options(),
      isTruncate,
      isStreaming = false)

    pythonDataSourceWriter = writeInfo.writer

    PythonBatchWriterFactory(ds.source, writeInfo.func, info.schema(), jobArtifactUUID)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    ds.source.commitWriteInPython(pythonDataSourceWriter, messages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    ds.source.commitWriteInPython(pythonDataSourceWriter, messages, abort = true)
  }
}
