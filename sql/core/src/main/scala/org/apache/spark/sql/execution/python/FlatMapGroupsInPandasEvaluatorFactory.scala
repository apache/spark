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
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.PandasGroupUtils.{executePython, groupAndProject}
import org.apache.spark.sql.types.StructType

class FlatMapGroupsInPandasEvaluatorFactory(
    childOutput: Seq[Attribute],
    output: Seq[Attribute],
    groupingAttributes: Seq[Attribute],
    dedupAttributes: Seq[Attribute],
    argOffsets: Array[Int],
    chainedFunc: Seq[ChainedPythonFunctions],
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new FlatMapGroupsInPandasEvaluator

  private class FlatMapGroupsInPandasEvaluator
      extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val iter = inputs.head
      // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
      if (iter.isEmpty) iter
      else {

        val data = groupAndProject(iter, groupingAttributes, childOutput, dedupAttributes)
          .map { case (_, x) => x }

        val runner = new ArrowPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
          Array(argOffsets),
          StructType.fromAttributes(dedupAttributes),
          sessionLocalTimeZone,
          largeVarTypes,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID)

        executePython(data, output, runner)
      }
    }
  }
}
