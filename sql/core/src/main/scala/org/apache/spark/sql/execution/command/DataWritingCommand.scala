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
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * A special `RunnableCommand` which writes data out and updates metrics.
 */
trait DataWritingCommand extends RunnableCommand {

  override lazy val metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
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
  protected def updateWritingMetrics(writeSummaries: Seq[ExecutedWriteSummary]): Unit = {
    val sparkContext = SparkContext.getActive.get
    var numPartitions = 0
    var numFiles = 0
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L
    var totalWritingTime: Long = 0L

    writeSummaries.foreach { summary =>
      numPartitions += summary.updatedPartitions.size
      numFiles += summary.numOutputFile
      totalNumBytes += summary.numOutputBytes
      totalNumOutput += summary.numOutputRows
      totalWritingTime += summary.totalWritingTime
    }

    val avgWritingTime = if (numFiles > 0) {
      (totalWritingTime / numFiles).toLong
    } else {
      0L
    }

    metrics("avgTime").add(avgWritingTime)
    metrics("numFiles").add(numFiles)
    metrics("numOutputBytes").add(totalNumBytes)
    metrics("numOutputRows").add(totalNumOutput)
    metrics("numParts").add(numPartitions)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toList)
  }
}
