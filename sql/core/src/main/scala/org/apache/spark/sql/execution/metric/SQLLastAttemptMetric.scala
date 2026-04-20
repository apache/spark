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

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

class SQLLastAttemptMetric(
    metricType: String,
    initValue: Long = 0L)
  extends SQLMetric(metricType, initValue)
  with SQLLastAttemptAccumulator[Long, Long, Long, SQLMetric] {

  override protected def partialMergeVal: Long = _value

  override protected def partialMerge(value: Long): Unit = {
    // For SQLLastAttemptMetric, this is just add to the underlying SQLMetric.
    super.add(value)
  }

  override protected def isMergeable(other: AccumulatorV2[_, _]): Boolean = other match {
    case o: SQLLastAttemptMetric => o.metricType == metricType
    case _ => false
  }

  // SQLLastAttemptMetric is used internally to aggregate system metrics (counters) such as
  // number of rows processed, and it should not store user data.
  protected def accumulatorStoresUserData: Boolean = false

  override protected def newDriverQueryExecutionAcc(): SQLMetric =
    new SQLMetric(metricType, initValue)
  override protected def addToDriverAcc(acc: SQLMetric, value: Long): Unit = acc.add(value)
  override protected def setDriverAcc(acc: SQLMetric, value: Long): Unit = acc.set(value)
  override protected def driverAccValue(acc: SQLMetric): Long = acc.value

  override def copy(): SQLLastAttemptMetric = {
    val newAcc = new SQLLastAttemptMetric(metricType, initValue)
    newAcc._value = _value
    newAcc
  }

  override def add(v: Long): Unit = {
    super.add(v)
    if (v >= 0) {
      // set value of SQLMetric after the add.
      setValueIfOnDriverSide(value)
      addQueryExecutionValueIfOnDriverSide(v)
    }
  }

  override def set(v: Long): Unit = {
    super.set(v)
    if (v >= 0) {
      // set value of SQLMetric after the set.
      setValueIfOnDriverSide(value)
      setQueryExecutionValueIfOnDriverSide(value)
    }
  }

}

object SQLLastAttemptMetrics {
  /**
   * Create a metric to report the value aggregated from the last attempt of each task. These
   * would be the values for the tasks that actually contributed to the final output of the
   * execution.
   */
  def createMetric(sc: SparkContext, name: String): SQLLastAttemptMetric = {
    val acc = new SQLLastAttemptMetric(SQLMetrics.SUM_METRIC)
    acc.register(sc, name = SQLMetrics.metricsCache.get(name), countFailedValues = false)
    acc.initializeLastAttemptAccumulator()
    acc
  }
}
