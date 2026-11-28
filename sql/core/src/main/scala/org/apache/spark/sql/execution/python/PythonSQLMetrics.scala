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
  // PythonPhaseMetrics supplies the phase schema; Python decides whether to collect timing.
  // A zero pythonNumTimingReports means detailed timing data is unavailable.
  protected val pythonMetrics: Map[String, SQLMetric] = {
    PythonSQLMetrics.pythonSizeMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createSizeMetric(sparkContext, v)
    } ++ PythonSQLMetrics.pythonTimingMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createNanoTimingMetric(sparkContext, v)
    } ++ PythonSQLMetrics.pythonOtherMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createMetric(sparkContext, v)
    } ++ additionalMetrics ++ phaseMetrics
  }

  // Hook for subtraits to add operator-specific metrics on top of the shared ones above. Default
  // empty so the shared trait stays minimal; see [[PythonPickleBatchMetrics]].
  protected def additionalMetrics: Map[String, SQLMetric] = Map.empty

  protected def phaseMetrics: Map[String, SQLMetric] = Map.empty

  protected def createPythonPhaseMetrics(): Map[String, SQLMetric] = {
    PythonSQLMetrics.pythonPhaseTimingMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createNanoTimingMetric(sparkContext, v)
    } ++ PythonSQLMetrics.pythonPhaseCountMetricsDesc.map { case (k, v) =>
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

  // Timings are in nanoseconds. The unit is part of each key and name, so that they differ
  // from the millisecond metrics of earlier versions wherever metrics are stored by name.
  val pythonTimingMetricsDesc: Map[String, String] = {
    Map(
      "pythonBootTimeNs" -> "time to start Python workers (ns)",
      "pythonInitTimeNs" -> "time to initialize Python workers (ns)",
      "pythonTotalTimeNs" -> "time to run Python workers (ns)",
      "pythonProcessingTimeNs" -> "time to execute Python code (ns)"
    )
  }

  val pythonOtherMetricsDesc: Map[String, String] = {
    Map(
      "pythonNumRowsReceived" -> "number of output rows",
      "pythonNumPipelinedTasks" -> "number of pipelined Python worker tasks")
  }

  // In nanoseconds, with the unit in each key and name, like pythonTimingMetricsDesc.
  val pythonPhaseTimingMetricsDesc: Map[String, String] = {
    Map(
      "pythonDataReadTimeNs" -> "time to read and deserialize Arrow data (ns)",
      "pythonInputPreparationTimeNs" -> "time to prepare Python UDF input (ns)",
      "pythonUDFExecutionTimeNs" -> "time to evaluate Python UDFs (ns)",
      "pythonOutputPreparationTimeNs" -> "time to prepare Python UDF output (ns)",
      "pythonDataWriteTimeNs" -> "time to serialize and write Arrow data (ns)")
  }

  val pythonPhaseCountMetricsDesc: Map[String, String] = {
    Map(
      "pythonNumTimingReports" -> "number of Python tasks with detailed timing",
      "pythonNumTimedBatches" -> "number of Python batches with detailed timing")
  }
}

/** Shared detailed timing schema for Python execution operators. */
trait PythonPhaseMetrics extends PythonSQLMetrics { self: SparkPlan =>
  override protected def phaseMetrics: Map[String, SQLMetric] = createPythonPhaseMetrics()
}

/**
 * Metrics specific to the regular (pickle-serialized) Python UDF input batching path in
 * [[BatchEvalPythonExec.getInputIterator]]. Mixed in only by the operators that pickle their
 * input ([[BatchEvalPythonExec]] and [[BatchEvalPythonUDTFExec]]); kept out of the shared
 * [[PythonSQLMetrics]] so they do not surface as always-zero rows on the Arrow / streaming Python
 * operators that mix in PythonSQLMetrics but never pickle through getInputIterator.
 *
 *   - pythonPeakPickledBatchBytes: peak per-batch pickled size (the primary contiguous heap
 *     allocation on this path). Like peakMemory it is a SIZE metric, so the figure to read is the
 *     per-partition max in the UI's min/med/max breakdown; the aggregated total (the sum of the
 *     per-task peaks) is not meaningful on its own. Always recorded on the pickle path.
 *   - pythonEstimatedInputBytes: running sum of the per-row size estimates the byte cap uses,
 *     compared against the measured pythonDataSent to gauge estimator accuracy. Only populated
 *     when a byte cap is configured.
 *   - pythonOversizedBatchCount: input batches cut at the byte cap, counted once per cut batch.
 *     Only populated when a finite spark.sql.execution.python.udf.maxBytesPerBatch is set.
 *
 * Also kept out of pythonSizeMetricsDesc, which feeds the Python data source DSv2 metric
 * declarations (UserDefinedPythonDataSource.createPythonMetrics) that never populate these.
 */
trait PythonPickleBatchMetrics extends PythonPhaseMetrics { self: SparkPlan =>
  override protected def additionalMetrics: Map[String, SQLMetric] =
    PythonPickleBatchMetrics.pickleBatchSizeMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createSizeMetric(sparkContext, v)
    } ++ PythonPickleBatchMetrics.pickleBatchCountMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createMetric(sparkContext, v)
    }
}

object PythonPickleBatchMetrics {
  // Peak pickled batch size and estimated input size, reported as SIZE metrics.
  val pickleBatchSizeMetricsDesc: Map[String, String] = {
    Map(
      "pythonPeakPickledBatchBytes" -> "peak pickled batch size in bytes",
      "pythonEstimatedInputBytes" -> "estimated pickled input size in bytes")
  }

  // Count of batches cut at the byte cap, reported as a SUM metric.
  val pickleBatchCountMetricsDesc: Map[String, String] = {
    Map("pythonOversizedBatchCount" -> "number of batches cut at the byte limit")
  }
}
