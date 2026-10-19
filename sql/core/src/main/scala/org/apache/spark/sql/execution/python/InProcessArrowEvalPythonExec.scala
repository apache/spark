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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf

/** Row-based CDI execution, sharing the standard Python UDF evaluator contracts. */
case class InProcessArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan) extends EvalPythonExec with PythonSQLMetrics {

  // The keys of `PythonSQLMetrics` without `pythonBootTime`, since there are no workers to
  // start, and with descriptions of in-process execution. Times are summed in nanoseconds
  // across tasks, since a registration or a batch can take less than a millisecond.
  override protected def sizeMetricsDesc: Map[String, String] =
    InProcessArrowEvalPythonExec.sizeMetricsDesc
  override protected def timingMetricsDesc: Map[String, String] = Map.empty
  override protected def otherMetricsDesc: Map[String, String] =
    InProcessArrowEvalPythonExec.otherMetricsDesc
  override protected def additionalMetrics: Map[String, SQLMetric] =
    InProcessArrowEvalPythonExec.nanoTimingMetricsDesc.map { case (k, v) =>
      k -> SQLMetrics.createNanoTimingMetric(sparkContext, v)
    }

  override protected def doExecute(): RDD[InternalRow] = {
    InProcessPythonUDFBuilder.checkConfiguration(conf)
    super.doExecute()
  }

  // The evaluator consumes each input row before pulling the next one, copying it into its
  // queue and writing its arguments to Arrow, so the rows need not be copied first.
  override protected def evaluatorInput: RDD[InternalRow] = child.execute()

  override protected def evaluatorFactory: EvalPythonEvaluatorFactory = {
    new InProcessArrowEvalPythonEvaluatorFactory(
      child.output, udfs, output, conf.arrowMaxRecordsPerBatch, conf.arrowMaxBytesPerBatch,
      conf.sessionLocalTimeZone, conf.arrowUseLargeVarTypes, conf.pysparkHideTraceback,
      conf.pysparkSimplifiedTraceback, conf.pysparkTracebackWithLocals,
      conf.getConf(SQLConf.PYTHON_UDF_IN_PROCESS_FULL_VALIDATION), pythonMetrics)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object InProcessArrowEvalPythonExec {
  private[python] val sizeMetricsDesc: Map[String, String] = Map(
    "pythonDataSent" -> "data exported to in-process Python",
    "pythonDataReceived" -> "data returned from in-process Python")

  private[python] val nanoTimingMetricsDesc: Map[String, String] = Map(
    "pythonInitTime" -> "time to register UDFs in in-process Python",
    "pythonTotalTime" -> "total time of in-process UDF evaluation, including reading its input",
    "pythonProcessingTime" -> "time to execute Python code")

  private[python] val otherMetricsDesc: Map[String, String] = Map(
    "pythonNumRowsReceived" -> "number of output rows")
}
