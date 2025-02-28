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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.PythonSQLMetrics
import org.apache.spark.util.MetricUtils


class PythonCustomMetric(
    override val name: String,
    override val description: String) extends CustomMetric {
  // To allow the aggregation can be called. See `SQLAppStatusListener.aggregateMetrics`
  def this() = this(null, null)

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    MetricUtils.stringValue("size", taskMetrics, Array.empty[Long])
  }
}

class PythonCustomTaskMetric(
    override val name: String,
    override val value: Long) extends CustomTaskMetric


object PythonCustomMetric {
  val pythonMetrics: Map[String, SQLMetric] = {
    // Dummy SQLMetrics. The result is manually reported via DSv2 interface
    // via passing the value to `CustomTaskMetric`. Note that `pythonOtherMetricsDesc`
    // is not used when it is reported. It is to reuse existing Python runner.
    // See also `UserDefinedPythonDataSource.createPythonMetrics`.
    PythonSQLMetrics.pythonSizeMetricsDesc.keys
      .map(_ -> new SQLMetric("size", -1)).toMap ++
      PythonSQLMetrics.pythonTimingMetricsDesc.keys
        .map(_ -> new SQLMetric("timing", -1)).toMap ++
      PythonSQLMetrics.pythonOtherMetricsDesc.keys
        .map(_ -> new SQLMetric("sum", -1)).toMap
  }
}
