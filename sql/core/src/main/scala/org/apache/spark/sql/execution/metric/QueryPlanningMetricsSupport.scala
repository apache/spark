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

package org.apache.spark.sql.execution.metric

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.QueryPlanningTracker

/**
 * It is a helper object for the metrics which is traced by [[QueryPlanningTracker]].
 */
object QueryPlanningMetricsSupport {
  import QueryPlanningTracker._

  val FILE_LISTING_TIME = FILE_LISTING + "Time"
  private def startTimestampMetric(prefix: String) = prefix + "Start"
  private def endTimestampMetric(prefix: String) = prefix + "End"

  /**
   * Create all file listing relative metrics and return the Map.
   */
  def createFileListingMetrics(sc: SparkContext): Map[String, SQLMetric] = Map(
    FILE_LISTING_TIME -> SQLMetrics.createMetric(sc, "total file listing time (ms)"),
    startTimestampMetric(FILE_LISTING) ->
      SQLMetrics.createTimestampMetric(sc, "file listing start"),
    startTimestampMetric(PARTITION_PRUNING) ->
      SQLMetrics.createTimestampMetric(sc, "partition pruning start"),
    endTimestampMetric(PARTITION_PRUNING) ->
      SQLMetrics.createTimestampMetric(sc, "partition pruning end"),
    endTimestampMetric(FILE_LISTING) ->
      SQLMetrics.createTimestampMetric(sc, "file listing end"))

  /**
   * Get updated file listing relative metrics from QueryPlanningTracker phases.
   */
  def getUpdatedFileListingMetrics(metrics: Map[String, SQLMetric]): Seq[SQLMetric] = {
    val updatedMetrics = new ArrayBuffer[SQLMetric]()

    // Update all metric relative with file listing phase.
    def phaseMetricsUpdate(phase: String): Unit = {
      val phaseSummary = QueryPlanningTracker.get.phases.get(phase)
      if (phaseSummary.isDefined) {
        metrics(FILE_LISTING_TIME).add(phaseSummary.get.durationMs)
        metrics(startTimestampMetric(phase)).set(phaseSummary.get.startTimeMs)
        metrics(endTimestampMetric(phase)).set(phaseSummary.get.endTimeMs)

        updatedMetrics.append(
          metrics(FILE_LISTING_TIME),
          metrics(startTimestampMetric(phase)),
          metrics(endTimestampMetric(phase)))
      }
    }
    phaseMetricsUpdate(FILE_LISTING)
    phaseMetricsUpdate(PARTITION_PRUNING)

    updatedMetrics.toSeq
  }
}
