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

package org.apache.spark.util

import java.text.NumberFormat
import java.util.{Arrays, Locale}

import scala.concurrent.duration._

import org.apache.spark.SparkException
import org.apache.spark.util.Utils

private[spark] object MetricUtils {

  val SUM_METRIC: String = "sum"
  val SIZE_METRIC: String = "size"
  val TIMING_METRIC: String = "timing"
  val NS_TIMING_METRIC: String = "nsTiming"
  val AVERAGE_METRIC: String = "average"
  private val baseForAvgMetric: Int = 10
  private val METRICS_NAME_SUFFIX = "(min, med, max (stageId: taskId))"

  private def toNumberFormat(value: Long): String = {
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    numberFormat.format(value.toDouble / baseForAvgMetric)
  }

  def metricNeedsMax(metricsType: String): Boolean = {
    metricsType != SUM_METRIC
  }

/**
   * A function that defines how we aggregate the final accumulator results among all tasks,
   * and represent it in string for a SQL physical operator.
    */
  def stringValue(metricsType: String, values: Array[Long], maxMetrics: Array[Long]): String = {
    // taskInfo = "(driver)" OR (stage ${stageId}.${attemptId}: task $taskId)
    val taskInfo = if (maxMetrics.isEmpty) {
      "(driver)"
    } else {
      s"(stage ${maxMetrics(1)}.${maxMetrics(2)}: task ${maxMetrics(3)})"
    }
    if (metricsType == SUM_METRIC) {
      val numberFormat = NumberFormat.getIntegerInstance(Locale.US)
      numberFormat.format(values.sum)
    } else if (metricsType == AVERAGE_METRIC) {
      val validValues = values.filter(_ > 0)
      // When there are only 1 metrics value (or None), no need to display max/min/median. This is
      // common for driver-side SQL metrics.
      if (validValues.length <= 1) {
        toNumberFormat(validValues.headOption.getOrElse(0))
      } else {
        val Seq(min, med, max) = {
          Arrays.sort(validValues)
          Seq(
            toNumberFormat(validValues(0)),
            toNumberFormat(validValues(validValues.length / 2)),
            toNumberFormat(validValues(validValues.length - 1)))
        }
        s"$METRICS_NAME_SUFFIX:\n($min, $med, $max $taskInfo)"
      }
    } else {
      val strFormat: Long => String = if (metricsType == SIZE_METRIC) {
        Utils.bytesToString
      } else if (metricsType == TIMING_METRIC) {
        Utils.msDurationToString
      } else if (metricsType == NS_TIMING_METRIC) {
        duration => Utils.msDurationToString(duration.nanos.toMillis)
      } else {
        throw SparkException.internalError(s"unexpected metrics type: $metricsType")
      }

      val validValues = values.filter(_ >= 0)
      // When there are only 1 metrics value (or None), no need to display max/min/median. This is
      // common for driver-side SQL metrics.
      if (validValues.length <= 1) {
        strFormat(validValues.headOption.getOrElse(0))
      } else {
        val Seq(sum, min, med, max) = {
          Arrays.sort(validValues)
          Seq(
            strFormat(validValues.sum),
            strFormat(validValues(0)),
            strFormat(validValues(validValues.length / 2)),
            strFormat(validValues(validValues.length - 1)))
        }
        s"total $METRICS_NAME_SUFFIX\n$sum ($min, $med, $max $taskInfo)"
      }
    }
  }
}
