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
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, _}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.internal.SQLConf


class PythonWrite(
    ds: PythonDataSourceV2,
    shortName: String,
    info: LogicalWriteInfo,
    isTruncate: Boolean
  ) extends Write with RequiresDistributionAndOrdering {

  override def toString: String = shortName

  override def toBatch: BatchWrite = new PythonBatchWrite(ds, shortName, info, isTruncate)

  override def toStreaming: StreamingWrite =
    new PythonStreamingWrite(ds, shortName, info, isTruncate)

  override def description: String = "(Python)"

  override def supportedCustomMetrics(): Array[CustomMetric] =
    ds.source.createPythonMetrics()

  /**
   * The distribution and ordering the Python writer asks for, fetched from Python once per write.
   *
   * `V2Writes` calls each `RequiresDistributionAndOrdering` method separately, so the result is
   * cached to keep this to a single Python worker invocation. When the feature is disabled, or
   * for a streaming write, we skip the worker and report the API defaults, leaving planning
   * unchanged.
   */
  private lazy val requirements: PythonWriteRequirements = {
    if (SQLConf.get.pythonWriteDistributionAndOrdering && !isStreamingWrite) {
      ds.source.writeDistributionAndOrderingInPython(info.schema(), info.options(), isTruncate)
    } else {
      PythonWriteRequirements.none
    }
  }

  /**
   * Whether a streaming query builds this write. The requirements come from the batch
   * `DataSourceWriter`, which a streaming sink need not implement. A batch write, even one inside
   * `foreachBatch`, is not marked.
   */
  private def isStreamingWrite: Boolean = info match {
    case i: LogicalWriteInfoImpl => i.isStreaming
    case _ => false
  }

  override def requiredDistribution(): Distribution = requirements.distribution

  override def distributionStrictlyRequired(): Boolean = requirements.distributionStrictlyRequired

  override def requiredNumPartitions(): Int = requirements.requiredNumPartitions

  override def advisoryPartitionSizeInBytes(): Long = requirements.advisoryPartitionSizeInBytes

  override def requiredOrdering(): Array[SortOrder] = requirements.ordering
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
  private val sessionUUID = {
    SparkSession.getActiveSession.collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override def createBatchWriterFactory(physicalInfo: PhysicalWriteInfo): DataWriterFactory =
  {
    val writeInfo = ds.source.createWriteInfoInPython(
      shortName,
      info.schema(),
      info.options(),
      isTruncate,
      isStreaming = false)

    pythonDataSourceWriter = writeInfo.writer

    PythonBatchWriterFactory(ds.source, writeInfo.func, info.schema(),
      jobArtifactUUID, sessionUUID)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    ds.source.commitWriteInPython(pythonDataSourceWriter, messages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    ds.source.commitWriteInPython(pythonDataSourceWriter, messages, abort = true)
  }
}
