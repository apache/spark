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

import org.apache.spark.{Accumulable, AccumulableParam, SparkContext}

/**
 * Create a layer for specialized metric. We cannot add `@specialized` to
 * `Accumulable/AccumulableParam` because it will break Java source compatibility.
 *
 * An implementation of SQLMetric should override `+=` and `add` to avoid boxing.
 */
private[sql] abstract class SQLMetric[R <: SQLMetricValue[T], T](
    name: String, val param: SQLMetricParam[R, T])
  extends Accumulable[R, T](param.zero, param, Some(name), true)

/**
 * Create a layer for specialized metric. We cannot add `@specialized` to
 * `Accumulable/AccumulableParam` because it will break Java source compatibility.
 */
private[sql] trait SQLMetricParam[R <: SQLMetricValue[T], T] extends AccumulableParam[R, T] {

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
 * A wrapper of Int to avoid boxing and unboxing when using Accumulator
 */
private[sql] class IntSQLMetricValue(private var _value: Int) extends SQLMetricValue[Int] {

  def add(term: Int): IntSQLMetricValue = {
    _value += term
    this
  }

  // Although there is a boxing here, it's fine because it's only called in SQLListener
  override def value: Int = _value
}

/**
 * A specialized long Accumulable to avoid boxing and unboxing when using Accumulator's
 * `+=` and `add`.
 */
private[sql] class LongSQLMetric private[metric](name: String)
  extends SQLMetric[LongSQLMetricValue, Long](name, LongSQLMetricParam) {

  override def +=(term: Long): Unit = {
    localValue.add(term)
  }

  override def add(term: Long): Unit = {
    localValue.add(term)
  }
}

private object LongSQLMetricParam extends SQLMetricParam[LongSQLMetricValue, Long] {

  override def addAccumulator(r: LongSQLMetricValue, t: Long): LongSQLMetricValue = r.add(t)

  override def addInPlace(r1: LongSQLMetricValue, r2: LongSQLMetricValue): LongSQLMetricValue =
    r1.add(r2.value)

  override def zero(initialValue: LongSQLMetricValue): LongSQLMetricValue = zero

  override def zero: LongSQLMetricValue = new LongSQLMetricValue(0L)
}

private[sql] object SQLMetrics {

  def createLongMetric(sc: SparkContext, name: String): LongSQLMetric = {
    val acc = new LongSQLMetric(name)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(acc))
    acc
  }

  /**
   * A metric that its value will be ignored. Use this one when we need a metric parameter but don't
   * care about the value.
   */
  val nullLongMetric = new LongSQLMetric("null")
}
