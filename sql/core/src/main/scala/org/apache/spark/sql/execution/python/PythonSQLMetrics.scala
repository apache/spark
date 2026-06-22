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
    } ++ PythonSQLMetrics.pythonBatchSizeMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createSizeMetric(sparkContext, v)
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
      "pythonTotalTime" -> "time to run Python workers",
      "pythonProcessingTime" -> "time to execute Python code"
    )
  }

  val pythonOtherMetricsDesc: Map[String, String] = {
    Map(
      "pythonNumRowsReceived" -> "number of output rows",
      // Input batches cut at the byte cap, counted once per cut batch. Only populated when a
      // finite spark.sql.execution.python.udf.maxBytesPerBatch is set.
      "pythonOversizedBatchCount" -> "number of batches cut at the byte limit")
  }

  // Size metrics for the regular (pickle) Python UDF input batching path (BatchEvalPythonExec).
  // pythonPeakPickledBatchBytes is the peak per-batch pickled size (the primary contiguous heap
  // allocation on this path; reported as the cross-task max via the UI's min/med/max breakdown,
  // like peakMemory). pythonEstimatedInputBytes is the running sum of the per-row size estimates
  // the byte cap uses, compared against the measured pythonDataSent to gauge estimator accuracy
  // (only populated when a byte cap is configured). Kept out of pythonSizeMetricsDesc, which also
  // feeds the Python data source DSv2 metric declarations
  // (UserDefinedPythonDataSource.createPythonMetrics) that never populate these.
  val pythonBatchSizeMetricsDesc: Map[String, String] = {
    Map(
      "pythonPeakPickledBatchBytes" -> "peak pickled batch size in bytes",
      "pythonEstimatedInputBytes" -> "estimated pickled input size in bytes")
  }
}
