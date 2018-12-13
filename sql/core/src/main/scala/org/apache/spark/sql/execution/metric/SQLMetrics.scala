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
import java.util.Locale

import scala.concurrent.duration._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, Utils}


/**
 * A metric used in a SQL query plan. This is implemented as an [[AccumulatorV2]]. Updates on
 * the executor side are automatically propagated and shown in the SQL UI through metrics. Updates
 * on the driver side must be explicitly posted using [[SQLMetrics.postDriverMetricUpdates()]].
 */
class SQLMetric(val metricType: String, initValue: Long = 0L) extends AccumulatorV2[Long, Long] {
  // This is a workaround for SPARK-11013.
  // We may use -1 as initial value of the accumulator, if the accumulator is valid, we will
  // update it at the end of task and the value will be at least 0. Then we can filter out the -1
  // values before calculate max, min, etc.
  private[this] var _value = initValue
  private var _zeroValue = initValue

  override def copy(): SQLMetric = {
    val newAcc = new SQLMetric(metricType, _value)
    newAcc._zeroValue = initValue
    newAcc
  }

  override def reset(): Unit = _value = _zeroValue

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: SQLMetric => _value += o.value
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def isZero(): Boolean = _value == _zeroValue

  override def add(v: Long): Unit = _value += v

  // We can set a double value to `SQLMetric` which stores only long value, if it is
  // average metrics.
  def set(v: Double): Unit = SQLMetrics.setDoubleForAverageMetrics(this, v)

  def set(v: Long): Unit = _value = v

  def +=(v: Long): Unit = _value += v

  override def value: Long = _value

  // Provide special identifier as metadata so we can tell that this is a `SQLMetric` later
  override def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    new AccumulableInfo(
      id, name, update, value, true, true, Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
  }
}

object SQLMetrics {
  private val SUM_METRIC = "sum"
  private val SIZE_METRIC = "size"
  private val TIMING_METRIC = "timing"
  private val NS_TIMING_METRIC = "nsTiming"
  private val AVERAGE_METRIC = "average"

  private val baseForAvgMetric: Int = 10

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
    acc.register(sc, name = Some(name), countFailedValues = false)
    acc
  }

  /**
   * Create a metric to report the size information (including total, min, med, max) like data size,
   * spill size, etc.
   */
  def createSizeMetric(sc: SparkContext, name: String): SQLMetric = {
    // The final result of this metric in physical operator UI may look like:
    // data size total (min, med, max):
    // 100GB (100MB, 1GB, 10GB)
    val acc = new SQLMetric(SIZE_METRIC, -1)
    acc.register(sc, name = Some(s"$name total (min, med, max)"), countFailedValues = false)
    acc
  }

  def createTimingMetric(sc: SparkContext, name: String): SQLMetric = {
    // The final result of this metric in physical operator UI may looks like:
    // duration(min, med, max):
    // 5s (800ms, 1s, 2s)
    val acc = new SQLMetric(TIMING_METRIC, -1)
    acc.register(sc, name = Some(s"$name total (min, med, max)"), countFailedValues = false)
    acc
  }

  def createNanoTimingMetric(sc: SparkContext, name: String): SQLMetric = {
    // Same with createTimingMetric, just normalize the unit of time to millisecond.
    val acc = new SQLMetric(NS_TIMING_METRIC, -1)
    acc.register(sc, name = Some(s"$name total (min, med, max)"), countFailedValues = false)
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
    acc.register(sc, name = Some(s"$name (min, med, max)"), countFailedValues = false)
    acc
  }

  /**
   * A function that defines how we aggregate the final accumulator results among all tasks,
   * and represent it in string for a SQL physical operator.
   */
  def stringValue(metricsType: String, values: Seq[Long]): String = {
    if (metricsType == SUM_METRIC) {
      val numberFormat = NumberFormat.getIntegerInstance(Locale.US)
      numberFormat.format(values.sum)
    } else if (metricsType == AVERAGE_METRIC) {
      val numberFormat = NumberFormat.getNumberInstance(Locale.US)

      val validValues = values.filter(_ > 0)
      val Seq(min, med, max) = {
        val metric = if (validValues.isEmpty) {
          Seq.fill(3)(0L)
        } else {
          val sorted = validValues.sorted
          Seq(sorted(0), sorted(validValues.length / 2), sorted(validValues.length - 1))
        }
        metric.map(v => numberFormat.format(v.toDouble / baseForAvgMetric))
      }
      s"\n($min, $med, $max)"
    } else {
      val strFormat: Long => String = if (metricsType == SIZE_METRIC) {
        Utils.bytesToString
      } else if (metricsType == TIMING_METRIC) {
        Utils.msDurationToString
      } else if (metricsType == NS_TIMING_METRIC) {
        duration => Utils.msDurationToString(duration.nanos.toMillis)
      } else {
        throw new IllegalStateException("unexpected metrics type: " + metricsType)
      }

      val validValues = values.filter(_ >= 0)
      val Seq(sum, min, med, max) = {
        val metric = if (validValues.isEmpty) {
          Seq.fill(4)(0L)
        } else {
          val sorted = validValues.sorted
          Seq(sorted.sum, sorted(0), sorted(validValues.length / 2), sorted(validValues.length - 1))
        }
        metric.map(strFormat)
      }
      s"\n$sum ($min, $med, $max)"
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
