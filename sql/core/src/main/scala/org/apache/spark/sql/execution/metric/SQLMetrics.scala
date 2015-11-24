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

import org.apache.spark.util.Utils
import org.apache.spark.{Accumulable, AccumulableParam, SparkContext}

/**
 * Create a layer for specialized metric. We cannot add `@specialized` to
 * `Accumulable/AccumulableParam` because it will break Java source compatibility.
 *
 * An implementation of SQLMetric should override `+=` and `add` to avoid boxing.
 */
private[sql] abstract class SQLMetric[R <: SQLMetricValue[T], T](
    name: String, val param: SQLMetricParam[R, T])
  extends Accumulable[R, T](param.zero, param, Some(name), true) {

  def reset(): Unit = {
    this.value = param.zero
  }
}

/**
 * Create a layer for specialized metric. We cannot add `@specialized` to
 * `Accumulable/AccumulableParam` because it will break Java source compatibility.
 */
private[sql] trait SQLMetricParam[R <: SQLMetricValue[T], T] extends AccumulableParam[R, T] {

  /**
   * A function that defines how we aggregate the final accumulator results among all tasks,
   * and represent it in string for a SQL physical operator.
   */
  val stringValue: Seq[T] => String

  def zero: R
}

/**
 * Create a layer for specialized metric. We cannot add `@specialized` to
 * `Accumulable/AccumulableParam` because it will break Java source compatibility.
 */
private[sql] trait SQLMetricValue[T] extends Serializable {

  def value: T

  override def toString: String = value.toString
}

/**
 * A wrapper of Long to avoid boxing and unboxing when using Accumulator
 */
private[sql] class LongSQLMetricValue(private var _value : Long) extends SQLMetricValue[Long] {

  def add(incr: Long): LongSQLMetricValue = {
    _value += incr
    this
  }

  // Although there is a boxing here, it's fine because it's only called in SQLListener
  override def value: Long = _value
}

/**
 * A specialized long Accumulable to avoid boxing and unboxing when using Accumulator's
 * `+=` and `add`.
 */
private[sql] class LongSQLMetric private[metric](name: String, param: LongSQLMetricParam)
  extends SQLMetric[LongSQLMetricValue, Long](name, param) {

  override def +=(term: Long): Unit = {
    localValue.add(term)
  }

  override def add(term: Long): Unit = {
    localValue.add(term)
  }
}

private class LongSQLMetricParam(val stringValue: Seq[Long] => String, initialValue: Long)
  extends SQLMetricParam[LongSQLMetricValue, Long] {

  override def addAccumulator(r: LongSQLMetricValue, t: Long): LongSQLMetricValue = r.add(t)

  override def addInPlace(r1: LongSQLMetricValue, r2: LongSQLMetricValue): LongSQLMetricValue =
    r1.add(r2.value)

  override def zero(initialValue: LongSQLMetricValue): LongSQLMetricValue = zero

  override def zero: LongSQLMetricValue = new LongSQLMetricValue(initialValue)
}

private[sql] object SQLMetrics {

  private def createLongMetric(
      sc: SparkContext,
      name: String,
      stringValue: Seq[Long] => String,
      initialValue: Long): LongSQLMetric = {
    val param = new LongSQLMetricParam(stringValue, initialValue)
    val acc = new LongSQLMetric(name, param)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  def createLongMetric(sc: SparkContext, name: String): LongSQLMetric = {
    createLongMetric(sc, name, _.sum.toString, 0L)
  }

  /**
   * Create a metric to report the size information (including total, min, med, max) like data size,
   * spill size, etc.
   */
  def createSizeMetric(sc: SparkContext, name: String): LongSQLMetric = {
    val stringValue = (values: Seq[Long]) => {
      // This is a workaround for SPARK-11013.
      // We use -1 as initial value of the accumulator, if the accumulator is valid, we will update
      // it at the end of task and the value will be at least 0.
      val validValues = values.filter(_ >= 0)
      val Seq(sum, min, med, max) = {
        val metric = if (validValues.length == 0) {
          Seq.fill(4)(0L)
        } else {
          val sorted = validValues.sorted
          Seq(sorted.sum, sorted(0), sorted(validValues.length / 2), sorted(validValues.length - 1))
        }
        metric.map(Utils.bytesToString)
      }
      s"\n$sum ($min, $med, $max)"
    }
    // The final result of this metric in physical operator UI may looks like:
    // data size total (min, med, max):
    // 100GB (100MB, 1GB, 10GB)
    createLongMetric(sc, s"$name total (min, med, max)", stringValue, -1L)
  }

  /**
   * A metric that its value will be ignored. Use this one when we need a metric parameter but don't
   * care about the value.
   */
  val nullLongMetric = new LongSQLMetric("null", new LongSQLMetricParam(_.sum.toString, 0L))
}
