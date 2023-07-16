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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.CoGroupedIterator
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.PandasGroupUtils.{executePython, groupAndProject}
import org.apache.spark.sql.types.StructType

case class FlatMapCoGroupsInPandasEvaluatorFactory(
    output: Seq[Attribute],
    leftGroup: Seq[Attribute],
    leftOutput: Seq[Attribute],
    leftDedup: Seq[Attribute],
    leftArgOffsets: Array[Int],
    rightGroup: Seq[Attribute],
    rightOutput: Seq[Attribute],
    rightDedup: Seq[Attribute],
    rightArgOffsets: Array[Int],
    chainedFunc: Seq[ChainedPythonFunctions],
    sessionLocalTimeZone: String,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new FlatMapCoGroupsInPandasEvaluator

  private class FlatMapCoGroupsInPandasEvaluator
      extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 2)
      val leftData = inputs(0)
      val rightData = inputs(1)
      if (leftData.isEmpty && rightData.isEmpty) Iterator.empty
      else {

        val leftGrouped = groupAndProject(leftData, leftGroup, leftOutput, leftDedup)
        val rightGrouped = groupAndProject(rightData, rightGroup, rightOutput, rightDedup)
        val data = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup)
          .map { case (_, l, r) => (l, r) }

        val runner = new CoGroupedArrowPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
          Array(leftArgOffsets ++ rightArgOffsets),
          StructType.fromAttributes(leftDedup),
          StructType.fromAttributes(rightDedup),
          sessionLocalTimeZone,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID)

        executePython(data, output, runner)
      }
    }
  }
}
