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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType

/**
 * A [[streamingWrite]] for python data source writing.
 * Responsible for generating the writer factory, committing or aborting a microbatch.
 * */
class PythonStreamingWrite(
    ds: PythonDataSourceV2,
    shortName: String,
    info: LogicalWriteInfo,
    isTruncate: Boolean) extends StreamingWrite {

  // Store the pickled data source writer instance.
  private var pythonDataSourceWriter: Array[Byte] = _

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  private def createDataSourceFunc =
    ds.source.createPythonFunction(
      ds.getOrCreateDataSourceInPython(shortName, info.options(), Some(info.schema())).dataSource
    )

  private lazy val pythonStreamingSinkCommitRunner =
    new PythonStreamingSinkCommitRunner(createDataSourceFunc, info.schema(), isTruncate)

  override def createStreamingWriterFactory(
       physicalInfo: PhysicalWriteInfo): StreamingDataWriterFactory = {
    val writeInfo = ds.source.createWriteInfoInPython(
      shortName,
      info.schema(),
      info.options(),
      isTruncate,
      isStreaming = true)

    pythonDataSourceWriter = writeInfo.writer

    new PythonStreamingWriterFactory(ds.source, writeInfo.func, info.schema(), jobArtifactUUID)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    pythonStreamingSinkCommitRunner.commitOrAbort(messages, epochId, false)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    pythonStreamingSinkCommitRunner.commitOrAbort(messages, epochId, true)
  }
}

class PythonStreamingWriterFactory(
    source: UserDefinedPythonDataSource,
    pickledWriteFunc: Array[Byte],
    inputSchema: StructType,
    jobArtifactUUID: Option[String])
  extends PythonBatchWriterFactory(source, pickledWriteFunc, inputSchema, jobArtifactUUID)
    with StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    createWriter(partitionId, taskId)
  }
}
