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

import java.text.NumberFormat
import java.util.{Arrays, Locale}

import scala.concurrent.duration._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, Utils}
import org.apache.spark.util.AccumulatorContext.internOption

/**
 * A metric used in a SQL query plan. This is implemented as an [[AccumulatorV2]]. Updates on
 * the executor side are automatically propagated and shown in the SQL UI through metrics. Updates
 * on the driver side must be explicitly posted using [[SQLMetrics.postDriverMetricUpdates()]].
 */
class SQLMetric(
    val metricType: String,
    initValue: Long = 0L) extends AccumulatorV2[Long, Long] {
  // initValue defines the initial value of the metric. 0 is the lowest value considered valid.
  // If a SQLMetric is invalid, it is set to 0 upon receiving any updates, and it also reports
  // 0 as its value to avoid exposing it to the user programmatically.
  //
  // For many SQLMetrics, we use initValue = -1 to indicate that the metric is by default invalid.
  // At the end of a task, we will update the metric making it valid, and the invalid metrics will
  // be filtered out when calculating min, max, etc. as a workaround
  // for SPARK-11013.
  assert(initValue <= 0)
  // _value will always be either initValue or non-negative.
  private var _value = initValue

  override def copy(): SQLMetric = {
    val newAcc = new SQLMetric(metricType, initValue)
    newAcc._value = _value
    newAcc
  }

  override def reset(): Unit = _value = initValue

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: SQLMetric =>
      if (!o.isZero) {
        if (isZero) _value = 0
        _value += o.value
      }
    case _ => throw QueryExecutionErrors.cannotMergeClassWithOtherClassError(
      this.getClass.getName, other.getClass.getName)
  }

  // This is used to filter out metrics. Metrics with value equal to initValue should
  // be filtered out, since they are either invalid or safe to filter without changing
  // the aggregation defined in [[SQLMetrics.stringValue]].
  // Note that we don't use 0 here since we may want to collect 0 metrics for
  // calculating min, max, etc. See SPARK-11013.
  override def isZero: Boolean = _value == initValue

  override def add(v: Long): Unit = {
    if (v >= 0) {
      if (isZero) _value = 0
      _value += v
    }
  }

  // We can set a double value to `SQLMetric` which stores only long value, if it is
  // average metrics.
  def set(v: Double): Unit = if (v >= 0) {
    SQLMetrics.setDoubleForAverageMetrics(this, v)
  }

  def set(v: Long): Unit = if (v >= 0) {
    _value = v
  }

  def +=(v: Long): Unit = add(v)

  // _value may be uninitialized, in many cases being -1. We should not expose it to the user
  // and instead return 0.
  override def value: Long = if (isZero) 0 else _value

  // Provide special identifier as metadata so we can tell that this is a `SQLMetric` later
  override def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    AccumulableInfo(id, name, internOption(update), internOption(value), true, true,
      SQLMetrics.cachedSQLAccumIdentifier)
  }

  // We should provide the raw value which can be -1, so that `SQLMetrics.stringValue` can correctly
  // filter out the invalid -1 values.
  override def toInfoUpdate: AccumulableInfo = {
    AccumulableInfo(id, name, internOption(Some(_value)), None, true, true,
      SQLMetrics.cachedSQLAccumIdentifier)
  }
}

object SQLMetrics {
  private val SUM_METRIC = "sum"
  private val SIZE_METRIC = "size"
  private val TIMING_METRIC = "timing"
  private val NS_TIMING_METRIC = "nsTiming"
  private val AVERAGE_METRIC = "average"

  private val baseForAvgMetric: Int = 10

  val cachedSQLAccumIdentifier = Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER)

  private val metricsCache: LoadingCache[String, Option[String]] =
    CacheBuilder.newBuilder().maximumSize(10000)
    .build(new CacheLoader[String, Option[String]] {
      override def load(name: String): Option[String] = {
        Option(name)
      }
    })

  /**
   * Converts a double value to long value by multiplying a base integer, so we can store it in
   * `SQLMetrics`. It only works for average metrics. When showing the metrics on UI, we restore
   * it back to a double value up to the decimal places bound by the base integer.
   */
  private[sql] def setDoubleForAverageMetrics(metric: SQLMetric, v: Double): Unit = {
    assert(metric.metricType == AVERAGE_METRIC,
      s"Can't set a double to a metric of metrics type: ${metric.metricType}")
    metric.set((v * baseForAvgMetric).toLong)
  }

  def createMetric(sc: SparkContext, name: String): SQLMetric = {
    val acc = new SQLMetric(SUM_METRIC)
    acc.register(sc, name = metricsCache.get(name), countFailedValues = false)
    acc
  }

  /**
   * Create a metric to report data source v2 custom metric.
   */
  def createV2CustomMetric(sc: SparkContext, customMetric: CustomMetric): SQLMetric = {
    val acc = new SQLMetric(CustomMetrics.buildV2CustomMetricTypeName(customMetric))
    acc.register(sc, name = metricsCache.get(customMetric.description()), countFailedValues = false)
    acc
  }

  /**
   * Create a metric to report the size information (including total, min, med, max) like data size,
   * spill size, etc.
   */
  def createSizeMetric(sc: SparkContext, name: String, initValue: Long = -1): SQLMetric = {
    // The final result of this metric in physical operator UI may look like:
    // data size total (min, med, max):
    // 100GB (100MB, 1GB, 10GB)
    val acc = new SQLMetric(SIZE_METRIC, initValue)
    acc.register(sc, name = metricsCache.get(name), countFailedValues = false)
    acc
  }

  def createTimingMetric(sc: SparkContext, name: String, initValue: Long = -1): SQLMetric = {
    // The final result of this metric in physical operator UI may looks like:
    // duration total (min, med, max):
    // 5s (800ms, 1s, 2s)
    val acc = new SQLMetric(TIMING_METRIC, initValue)
    acc.register(sc, name = metricsCache.get(name), countFailedValues = false)
    acc
  }

  def createNanoTimingMetric(sc: SparkContext, name: String, initValue: Long = -1): SQLMetric = {
    // Same with createTimingMetric, just normalize the unit of time to millisecond.
    val acc = new SQLMetric(NS_TIMING_METRIC, initValue)
    acc.register(sc, name = metricsCache.get(name), countFailedValues = false)
    acc
  }

  /**
   * Create a metric to report the average information (including min, med, max) like
   * avg hash probe. As average metrics are double values, this kind of metrics should be
   * only set with `SQLMetric.set` method instead of other methods like `SQLMetric.add`.
   * The initial values (zeros) of this metrics will be excluded after.
   */
  def createAverageMetric(sc: SparkContext, name: String): SQLMetric = {
    // The final result of this metric in physical operator UI may looks like:
    // probe avg (min, med, max):
    // (1.2, 2.2, 6.3)
    val acc = new SQLMetric(AVERAGE_METRIC)
    acc.register(sc, name = metricsCache.get(name), countFailedValues = false)
    acc
  }

  private def toNumberFormat(value: Long): String = {
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    numberFormat.format(value.toDouble / baseForAvgMetric)
  }

  def metricNeedsMax(metricsType: String): Boolean = {
    metricsType != SUM_METRIC
  }

  private val METRICS_NAME_SUFFIX = "(min, med, max (stageId: taskId))"

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

  def postDriverMetricsUpdatedByValue(
      sc: SparkContext,
      executionId: String,
      accumUpdates: Seq[(Long, Long)]): Unit = {
    if (executionId != null) {
      sc.listenerBus.post(
        SparkListenerDriverAccumUpdates(executionId.toLong, accumUpdates))
    }
  }

  /**
   * Updates metrics based on the driver side value. This is useful for certain metrics that
   * are only updated on the driver, e.g. subquery execution time, or number of files.
   */
  def postDriverMetricUpdates(
      sc: SparkContext, executionId: String, metrics: Seq[SQLMetric]): Unit = {
    // There are some cases we don't care about the metrics and call `SparkPlan.doExecute`
    // directly without setting an execution id. We should be tolerant to it.
    if (executionId != null) {
      sc.listenerBus.post(
        SparkListenerDriverAccumUpdates(executionId.toLong, metrics.map(m => m.id -> m.value)))
    }
  }
}
