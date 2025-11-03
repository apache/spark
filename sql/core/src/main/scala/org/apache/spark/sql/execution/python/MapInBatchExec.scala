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

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.UnaryExecNode

/**
 * A relation produced by applying a function that takes an iterator of batches
 * such as pandas DataFrame or PyArrow's record batches, and outputs an iterator of them.
 *
 * This is somewhat similar with [[FlatMapGroupsInPandasExec]] and
 * `org.apache.spark.sql.catalyst.plans.logical.MapPartitionsInRWithArrow`
 */
trait MapInBatchExec extends UnaryExecNode with PythonSQLMetrics {
  protected val func: Expression
  protected val pythonEvalType: Int

  protected val isBarrier: Boolean

  protected val profile: Option[ResourceProfile]

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val pythonUDF = func.asInstanceOf[PythonUDF]
    val pythonFunction = pythonUDF.func
    val chainedFunc = Seq((ChainedPythonFunctions(Seq(pythonFunction)), pythonUDF.resultId.id))
    val evaluatorFactory = new MapInBatchEvaluatorFactory(
      output,
      chainedFunc,
      child.schema,
      pythonUDF.dataType,
      conf.arrowMaxRecordsPerBatch,
      pythonEvalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID)

    val rdd = if (isBarrier) {
      val rddBarrier = child.execute().barrier()
      if (conf.usePartitionEvaluator) {
        rddBarrier.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        rddBarrier.mapPartitionsWithIndex { (index, iter) =>
          evaluatorFactory.createEvaluator().eval(index, iter)
        }
      }
    } else {
      val inputRdd = child.execute()
      if (conf.usePartitionEvaluator) {
        inputRdd.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        inputRdd.mapPartitionsWithIndexInternal { (index, iter) =>
          evaluatorFactory.createEvaluator().eval(index, iter)
        }
      }
    }
    profile.map(rp => rdd.withResources(rp)).getOrElse(rdd)
  }
}
