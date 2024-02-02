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

import scala.jdk.CollectionConverters.IteratorHasAsScala

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

case class PythonWriterCommitMessage(pickledMessage: Array[Byte]) extends WriterCommitMessage

case class PythonBatchWriterFactory(
    source: UserDefinedPythonDataSource,
    pickledWriteFunc: Array[Byte],
    inputSchema: StructType,
    jobArtifactUUID: Option[String]) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new DataWriter[InternalRow] {

      private[this] val metrics: Map[String, SQLMetric] = PythonCustomMetric.pythonMetrics

      private var commitMessage: PythonWriterCommitMessage = _

      override def writeAll(records: java.util.Iterator[InternalRow]): Unit = {
        val evaluatorFactory = source.createMapInBatchEvaluatorFactory(
          pickledWriteFunc,
          "write_to_data_source",
          inputSchema,
          UserDefinedPythonDataSource.writeOutputSchema,
          metrics,
          jobArtifactUUID)
        val outputIter = evaluatorFactory.createEvaluator().eval(partitionId, records.asScala)
        outputIter.foreach { row =>
          if (commitMessage == null) {
            commitMessage = PythonWriterCommitMessage(row.getBinary(0))
          } else {
            throw QueryExecutionErrors.invalidWriterCommitMessageError(details = "more than one")
          }
        }
        if (commitMessage == null) {
          throw QueryExecutionErrors.invalidWriterCommitMessageError(details = "zero")
        }
      }

      override def write(record: InternalRow): Unit =
        SparkException.internalError("write method for Python data source should not be called.")

      override def commit(): WriterCommitMessage = {
        commitMessage.asInstanceOf[WriterCommitMessage]
      }

      override def abort(): Unit = {}

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        source.createPythonTaskMetrics(metrics.map { case (k, v) => k -> v.value })
      }
    }
  }
}
