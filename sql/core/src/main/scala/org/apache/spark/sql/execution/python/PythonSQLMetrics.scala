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

package org.apache.spark.sql.execution.python

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics

private[python] trait PythonSQLMetrics { self: SparkPlan =>

  override val metrics = Map(
    "pythonExecTime" ->
      SQLMetrics.createNanoTimingMetric(sparkContext, "time spent executing"),
    "pythonDataSerializeTime" ->
      SQLMetrics.createNanoTimingMetric(sparkContext, "time spent sending data"),
    "pythonCodeSerializeTime" -> SQLMetrics.createNanoTimingMetric(sparkContext,
      "time spent sending code"),
    "pythonCodeSent" ->
      SQLMetrics.createSizeMetric(sparkContext, "bytes of code sent"),
    "pythonDataReceived" ->
      SQLMetrics.createSizeMetric(sparkContext, "bytes of data returned"),
    "pythonDataSent" ->
      SQLMetrics.createSizeMetric(sparkContext, "bytes of data sent"),
    "pythonNumBatchesReceived" ->
      SQLMetrics.createMetric(sparkContext, "number of batches returned"),
    "pythonNumBatchesSent" ->
      SQLMetrics.createMetric(sparkContext, "number of batches processed"),
    "pythonNumRowsReceived" ->
      SQLMetrics.createMetric(sparkContext, "number of rows returned"),
    "pythonNumRowsSent" ->
      SQLMetrics.createMetric(sparkContext, "number of rows processed")
  )

  val pythonNumRowsReceived = longMetric("pythonNumRowsReceived")
  val pythonNumRowsSent = longMetric("pythonNumRowsSent")
  val pythonExecTime = longMetric("pythonExecTime")
  val pythonDataSerializeTime = longMetric("pythonDataSerializeTime")
  val pythonCodeSerializeTime = longMetric("pythonCodeSerializeTime")
  val pythonCodeSent = longMetric("pythonCodeSent")
  val pythonDataReceived = longMetric("pythonDataReceived")
  val pythonDataSent = longMetric("pythonDataSent")
  val pythonNumBatchesReceived = longMetric("pythonNumBatchesReceived")
  val pythonNumBatchesSent = longMetric("pythonNumBatchesSent")
}
