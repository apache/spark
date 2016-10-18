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

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, Utils}


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

  override def mergeImpl(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: SQLMetric => _value += o.value
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def isZero(): Boolean = _value == _zeroValue

  override def addImpl(v: Long): Unit = _value += v

  def +=(v: Long): Unit = add(v)

  override def value: Long = _value

  // Provide special identifier as metadata so we can tell that this is a `SQLMetric` later
  override def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    new AccumulableInfo(
      id, name, update, value, internal = true, countFailedValues = true, dataProperty = false,
      Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
  }
}


object SQLMetrics {
  private val SUM_METRIC = "sum"
  private val SIZE_METRIC = "size"
  private val TIMING_METRIC = "timing"

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
    // The final result of this metric in physical operator UI may looks like:
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

  /**
   * A function that defines how we aggregate the final accumulator results among all tasks,
   * and represent it in string for a SQL physical operator.
   */
  def stringValue(metricsType: String, values: Seq[Long]): String = {
    if (metricsType == SUM_METRIC) {
      val numberFormat = NumberFormat.getIntegerInstance(Locale.ENGLISH)
      numberFormat.format(values.sum)
    } else {
      val strFormat: Long => String = if (metricsType == SIZE_METRIC) {
        Utils.bytesToString
      } else if (metricsType == TIMING_METRIC) {
        Utils.msDurationToString
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
}
