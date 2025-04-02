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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.window._

/**
 * This class calculates and outputs windowed aggregates over the rows in a single partition.
 *
 * This is similar to [[WindowExec]]. The main difference is that this node does not compute
 * any window aggregation values. Instead, it computes the lower and upper bound for each window
 * (i.e. window bounds) and pass the data and indices to Python worker to do the actual window
 * aggregation.
 *
 * It currently materializes all data associated with the same partition key and passes them to
 * Python worker. This is not strictly necessary for sliding windows and can be improved (by
 * possibly slicing data into overlapping chunks and stitching them together).
 *
 * This class groups window expressions by their window boundaries so that window expressions
 * with the same window boundaries can share the same window bounds. The window bounds are
 * prepended to the data passed to the python worker.
 *
 * For example, if we have:
 *     avg(v) over specifiedwindowframe(RowFrame, -5, 5),
 *     avg(v) over specifiedwindowframe(RowFrame, UnboundedPreceding, UnboundedFollowing),
 *     avg(v) over specifiedwindowframe(RowFrame, -3, 3),
 *     max(v) over specifiedwindowframe(RowFrame, -3, 3)
 *
 * The python input will look like:
 * (lower_bound_w1, upper_bound_w1, lower_bound_w3, upper_bound_w3, v)
 *
 * where w1 is specifiedwindowframe(RowFrame, -5, 5)
 *       w2 is specifiedwindowframe(RowFrame, UnboundedPreceding, UnboundedFollowing)
 *       w3 is specifiedwindowframe(RowFrame, -3, 3)
 *
 * Note that w2 doesn't have bound indices in the python input because it's unbounded window
 * so it's bound indices will always be the same.
 *
 * Bounded window and Unbounded window are evaluated differently in Python worker:
 * (1) Bounded window takes the window bound indices in addition to the input columns.
 *     Unbounded window takes only input columns.
 * (2) Bounded window evaluates the udf once per input row.
 *     Unbounded window evaluates the udf once per window partition.
 * This is controlled by Python runner conf "pandas_window_bound_types"
 *
 * The logic to compute window bounds is delegated to [[WindowFunctionFrame]] and shared with
 * [[WindowExec]]
 *
 * Note this doesn't support partial aggregation and all aggregation is computed from the entire
 * window.
 */
case class WindowInPandasExec(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExecBase with PythonSQLMetrics {
  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size")
  )

  protected override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory =
      new WindowInPandasEvaluatorFactory(
        windowExpression,
        partitionSpec,
        orderSpec,
        child.output,
        longMetric("spillSize"),
        pythonMetrics,
        conf.pythonUDFProfiler)

    // Start processing.
    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndex { (index, rowIterator) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, rowIterator)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowInPandasExec =
    copy(child = newChild)
}
