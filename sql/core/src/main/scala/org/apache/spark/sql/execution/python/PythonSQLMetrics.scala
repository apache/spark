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
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

trait PythonSQLMetrics { self: SparkPlan =>
  protected val pythonMetrics: Map[String, SQLMetric] = {
    PythonSQLMetrics.pythonSizeMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createSizeMetric(sparkContext, v)
    } ++ PythonSQLMetrics.pythonTimingMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createTimingMetric(sparkContext, v)
    } ++ PythonSQLMetrics.pythonOtherMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createMetric(sparkContext, v)
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics
}

object PythonSQLMetrics {
  val pythonSizeMetricsDesc: Map[String, String] = {
    Map(
      "pythonDataSent" -> "data sent to Python workers",
      "pythonDataReceived" -> "data returned from Python workers"
    )
  }

  val pythonTimingMetricsDesc: Map[String, String] = {
    Map(
      "pythonBootTime" -> "time to start Python workers",
      "pythonInitTime" -> "time to initialize Python workers",
      "pythonTotalTime" -> "time to run Python workers"
    )
  }

  val pythonOtherMetricsDesc: Map[String, String] = {
    Map("pythonNumRowsReceived" -> "number of output rows")
  }
}
