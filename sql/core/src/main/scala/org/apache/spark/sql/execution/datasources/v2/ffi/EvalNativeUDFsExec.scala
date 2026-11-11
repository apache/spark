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
package org.apache.spark.sql.execution.datasources.v2.ffi

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{EvalNativeUDFs, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/** Plans the evaluation of native functions. */
object NativeUDFEvals extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case EvalNativeUDFs(udfs, resultAttrs, child) =>
      EvalNativeUDFsExec(udfs.asInstanceOf[Seq[NativeUDF]], resultAttrs, planLater(child)) :: Nil
    case _ => Nil
  }
}

/**
 * Evaluates calls of native functions in the tasks of its child, with the libraries of their
 * packages, as `spark_udf.h` says. See [[NativeUDFEvaluator]].
 */
case class EvalNativeUDFsExec(udfs: Seq[NativeUDF], resultAttrs: Seq[Attribute], child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numBatches" -> SQLMetrics.createMetric(sparkContext, "number of batches"),
    "evalTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in native functions"))

  override protected def doExecute(): RDD[InternalRow] = {
    // The executors get the packages as artifacts of the session that runs the query.
    val packages = udfs.map { udf =>
      NativeDataSourceRegistry.distribute(udf.pkg).asInstanceOf[NativeDataSourcePackage]
    }
    val evaluator = new NativeUDFEvaluator(child.output, udfs, packages,
      conf.arrowMaxRecordsPerBatch, conf.arrowMaxBytesPerBatch, conf.sessionLocalTimeZone,
      metrics)
    child.execute().mapPartitionsWithIndexInternal { (index, rows) =>
      evaluator.evaluate(TaskContext.get(), index, rows)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): EvalNativeUDFsExec =
    copy(child = newChild)
}
