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
import org.apache.spark.util.Utils

/**
 * A logical command specialized for writing data out. `FileWritingCommand`s are
 * wrapped in `FileWritingCommandExec` during execution.
 */
trait FileWritingCommand extends logical.Command {
  def run(
    sparkSession: SparkSession,
    children: Seq[SparkPlan],
    fileCommandExec: FileWritingCommandExec): Seq[Row]
}

/**
 * A physical operator specialized to execute the run method of a `FileWritingCommand`,
 * save the result to prevent multiple executions, and record necessary metrics for UI.
 */
case class FileWritingCommandExec(
    cmd: FileWritingCommand,
    children: Seq[SparkPlan],
    givenMetrics: Option[Map[String, SQLMetric]] = None) extends CommandExec {

  override val metrics = givenMetrics.getOrElse {
    val sparkContext = sqlContext.sparkContext
    Map(
      // General metrics.
      "avgTime" -> SQLMetrics.createMetric(sparkContext, "average writing time (ms)"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "bytes of written output"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numParts" -> SQLMetrics.createMetric(sparkContext, "number of dynamic part")
    )
  }

  /**
   * Callback function that update metrics collected from the writing operation.
   */
  private[sql] def postDriverMetrics(writeSummaries: Seq[ExecutedWriteSummary]): Unit = {
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

    // The time for writing individual file can be zero if it's less than 1 ms. Zero values can
    // lower actual time of writing to zero when calculating average, so excluding them.
    val avgWritingTime =
      Utils.average(writeSummaries.flatMap(_.writingTimePerFile.filter(_ > 0))).toLong
    // Note: for simplifying metric values assignment, we put the values as the alphabetically
    // sorted of the metric keys.
    val metricsNames = metrics.keys.toSeq.sorted
    val metricsValues = Seq(avgWritingTime, numFiles, totalNumBytes, totalNumOutput, numPartitions)
    metricsNames.zip(metricsValues).foreach(x => metrics(x._1).add(x._2))

    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId,
      metricsNames.map(metrics(_)))
  }

  protected[sql] lazy val invokeCommand: Seq[Row] = cmd.run(sqlContext.sparkSession, children, this)
}
