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
import org.apache.spark.sql.execution.{BinaryExecNode, CoGroupedIterator, SparkPlan}
import org.apache.spark.sql.execution.python.PandasGroupUtils._


/**
 * Base class for Python-based FlatMapCoGroupsIn*Exec.
 */
trait FlatMapCoGroupsInBatchExec extends SparkPlan with BinaryExecNode with PythonSQLMetrics {
  val leftGroup: Seq[Attribute]
  val rightGroup: Seq[Attribute]
  val func: Expression
  val output: Seq[Attribute]
  val left: SparkPlan
  val right: SparkPlan

  protected val pythonEvalType: Int

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val arrowMaxRecordsPerBatch = conf.arrowMaxRecordsPerBatch
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private val pythonUDF = func.asInstanceOf[PythonUDF]
  private val pandasFunction = pythonUDF.func
  private val chainedFunc =
    Seq((ChainedPythonFunctions(Seq(pandasFunction)), pythonUDF.resultId.id))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    val leftDist = if (leftGroup.isEmpty) AllTuples else ClusteredDistribution(leftGroup)
    val rightDist = if (rightGroup.isEmpty) AllTuples else ClusteredDistribution(rightGroup)
    leftDist :: rightDist :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    leftGroup
      .map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val (leftDedup, leftArgOffsets) = resolveArgOffsets(left.output, leftGroup)
    val (rightDedup, rightArgOffsets) = resolveArgOffsets(right.output, rightGroup)
    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

    // Map cogrouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    left.execute().zipPartitions(right.execute())  { (leftData, rightData) =>
      if (leftData.isEmpty && rightData.isEmpty) Iterator.empty else {

        val leftGrouped = groupAndProject(leftData, leftGroup, left.output, leftDedup)
        val rightGrouped = groupAndProject(rightData, rightGroup, right.output, rightDedup)
        val data = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup)
          .map { case (_, l, r) => (l, r) }

        val runner = new CoGroupedArrowPythonRunner(
          chainedFunc,
          pythonEvalType,
          Array(leftArgOffsets ++ rightArgOffsets),
          DataTypeUtils.fromAttributes(leftDedup),
          DataTypeUtils.fromAttributes(rightDedup),
          sessionLocalTimeZone,
          arrowMaxRecordsPerBatch,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID,
          conf.pythonUDFProfiler)

        executePython(data, output, runner)
      }
    }
  }
}
