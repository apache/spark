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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * A physical operator specialized to execute the run method of a `WriteOutFileCommand`,
 * save the result to prevent multiple executions, and record necessary metrics for UI.
 */
case class WrittenFileCommandExec(
    cmd: WriteOutFileCommand,
    children: Seq[SparkPlan]) extends CommandExec {

  override lazy val metrics = cmd.metrics(sqlContext.sparkContext)

  /**
   * The callback function used to update metrics returned from the operation of writing data out.
   */
  private def updateDriverMetrics(writeSummaries: Seq[ExecutedWriteSummary]): Unit = {
    var numPartitions = 0
    var numFiles = 0
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L

    writeSummaries.foreach { summary =>
      numPartitions += summary.updatedPartitions.size
      numFiles += summary.numOutputFile
      totalNumBytes += summary.numOutputBytes
      totalNumOutput += summary.numOutputRows
    }

    // Metrics of writing time in ms.
    // We only count for the non-zero writing time when computing average.
    val writingTimePerFile: Seq[Long] = writeSummaries.flatMap(_.writingTimePerFile)
    val nonZeroCount = writingTimePerFile.filter(_ > 0).size
    val avgWritingTime = if (nonZeroCount == 0) {
      0
    } else {
      writingTimePerFile.sum / nonZeroCount
    }

    // Updates metrics.
    val numDynamicPartsMetric = metrics("numDynamicParts")
    val fileNumMetric = metrics("numFiles")
    val numBytesMetric = metrics("numOutputBytes")
    val numOutputRowsMetric = metrics("numOutputRows")
    val writingTimeMetric = metrics("writingTime")

    numDynamicPartsMetric.add(numPartitions)
    fileNumMetric.add(numFiles)
    numBytesMetric.add(totalNumBytes)
    numOutputRowsMetric.add(totalNumOutput)
    writingTimeMetric.add(avgWritingTime)

    val generalMetrics = Seq(numDynamicPartsMetric, fileNumMetric, numBytesMetric,
      numOutputRowsMetric, writingTimeMetric)

    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId, generalMetrics)
  }

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(sqlContext.sparkSession, children, updateDriverMetrics)
    rows.map(converter(_).asInstanceOf[InternalRow])
  }
}
