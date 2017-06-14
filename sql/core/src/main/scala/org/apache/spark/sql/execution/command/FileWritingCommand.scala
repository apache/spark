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
  // Whether this command will update the wrapping `FileWritingCommandExec`'s metrics.
  protected[sql] val canUpdateMetrics: Boolean = true

  def run(
    sparkSession: SparkSession,
    children: Seq[SparkPlan],
    fileCommandExec: Option[FileWritingCommandExec]): Seq[Row]
}

/**
 * A physical operator specialized to execute the run method of a `FileWritingCommand`,
 * save the result to prevent multiple executions, and record necessary metrics for UI.
 */
case class FileWritingCommandExec(
    cmd: FileWritingCommand,
    children: Seq[SparkPlan],
    parentCommandExec: Option[FileWritingCommandExec] = None) extends CommandExec {

  override val metrics: Map[String, SQLMetric] =
    if (parentCommandExec.isDefined && cmd.canUpdateMetrics) {
      // When this operator is invoked by another `FileWritingCommandExec`, we need to update
      // the caller's metrics.
      parentCommandExec.get.metrics
    } else if (cmd.canUpdateMetrics) {
      val sparkContext = sqlContext.sparkContext
      Map(
        "avgTime" -> SQLMetrics.createMetric(sparkContext, "average writing time (ms)"),
        "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
        "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "bytes of written output"),
        "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
        "numParts" -> SQLMetrics.createMetric(sparkContext, "number of dynamic part")
      )
    } else {
      // The command declares it won't update metrics. Just use an empty map.
      Map.empty
    }

  /**
   * Callback function that update metrics collected from the writing operation.
   */
  private[sql] def postDriverMetrics(writeSummaries: Seq[ExecutedWriteSummary]): Unit = {
    assert(cmd.canUpdateMetrics, s"This $cmd shouldn't update metrics")
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

    metrics("avgTime").add(avgWritingTime)
    metrics("numFiles").add(numFiles)
    metrics("numOutputBytes").add(totalNumBytes)
    metrics("numOutputRows").add(totalNumOutput)
    metrics("numParts").add(numPartitions)

    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId, metrics.values.toList)
  }

  protected[sql] def invokeCommand(): Seq[Row] = {
    val exec = if (cmd.canUpdateMetrics) Some(this) else None
    cmd.run(sqlContext.sparkSession, children, exec)
  }
}
