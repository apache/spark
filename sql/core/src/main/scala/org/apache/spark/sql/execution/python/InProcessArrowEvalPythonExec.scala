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
import org.apache.spark.sql.internal.SQLConf

/** Row-based CDI execution, sharing the standard Python UDF evaluator contracts. */
case class InProcessArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan) extends EvalPythonExec with PythonSQLMetrics {

  override protected def doExecute(): RDD[InternalRow] = {
    InProcessPythonUDFBuilder.checkConfiguration(conf)
    // Unlike EvalPythonExec, do not copy every input row: the evaluator consumes each row
    // before pulling the next one, copying it into the queue and writing its arguments to
    // Arrow on the same thread.
    val inputRDD = child.execute()
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }

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
