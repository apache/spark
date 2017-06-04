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

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A logical command specialized for writing data out. `WriteOutFileCommand`s are
 * wrapped in `WrittenFileCommandExec` during execution.
 */
trait WriteOutFileCommand extends logical.Command {

  /**
   * Those metrics will be updated once the command finishes writing data out. Those metrics will
   * be taken by `WrittenFileCommandExe` as its metrics when showing in UI.
   */
  def metrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      // General metrics.
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numParts" -> SQLMetrics.createMetric(sparkContext, "number of dynamic part"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "bytes of written output"),
      "avgTime" -> SQLMetrics.createMetric(sparkContext, "average writing time (ms)")
    )

  def run(
      sparkSession: SparkSession,
      children: Seq[SparkPlan],
      metricsCallback: (Seq[ExecutedWriteSummary]) => Unit): Seq[Row] = {
    throw new NotImplementedError
  }
}

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

    val times = writeSummaries.flatMap(_.writingTimePerFile.filter(_ > 0))
    val avgWritingTime = if (times.size > 0) {
      times.sum / times.size
    } else {
      0
    }

    val metricsNames = Seq("numParts", "numFiles", "numOutputBytes", "numOutputRows", "avgTime")
    val metricsValues = Seq(numPartitions, numFiles, totalNumBytes, totalNumOutput, avgWritingTime)
    metricsNames.zip(metricsValues).foreach(x => metrics(x._1).add(x._2))

    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId,
      metricsNames.map(metrics(_)))
  }

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(sqlContext.sparkSession, children, updateDriverMetrics)
    rows.map(converter(_).asInstanceOf[InternalRow])
  }
}
