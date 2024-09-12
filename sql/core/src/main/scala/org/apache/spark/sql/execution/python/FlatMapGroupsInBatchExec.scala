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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.PandasGroupUtils._
import org.apache.spark.sql.types.StructType


/**
 * Base class for Python-based FlatMapGroupsIn*Exec.
 */
trait FlatMapGroupsInBatchExec extends SparkPlan with UnaryExecNode with PythonSQLMetrics {
  val groupingAttributes: Seq[Attribute]
  val func: Expression
  val output: Seq[Attribute]
  val child: SparkPlan

  protected val pythonEvalType: Int

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val largeVarTypes = conf.arrowUseLargeVarTypes
  private val arrowMaxRecordsPerBatch = conf.arrowMaxRecordsPerBatch
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private val pythonUDF = func.asInstanceOf[PythonUDF]
  private val pythonFunction = pythonUDF.func
  private val chainedFunc =
    Seq((ChainedPythonFunctions(Seq(pythonFunction)), pythonUDF.resultId.id))
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  protected def groupedData(iter: Iterator[InternalRow], attrs: Seq[Attribute]):
      Iterator[Iterator[InternalRow]] =
    groupAndProject(iter, groupingAttributes, child.output, attrs)
      .map { case (_, x) => x }

  protected def groupedSchema(attrs: Seq[Attribute]): StructType =
    DataTypeUtils.fromAttributes(attrs)

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val (dedupAttributes, argOffsets) = resolveArgOffsets(child.output, groupingAttributes)

    // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty) iter else {

      val data = groupedData(iter, dedupAttributes)

      val runner = new GroupedArrowPythonRunner(
        chainedFunc,
        pythonEvalType,
        Array(argOffsets),
        groupedSchema(dedupAttributes),
        sessionLocalTimeZone,
        arrowMaxRecordsPerBatch,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        conf.pythonUDFProfiler)

      executePython(data, output, runner)
    }}
  }
}
