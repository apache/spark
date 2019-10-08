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

import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{BinaryExecNode, CoGroupedIterator, SparkPlan}
import org.apache.spark.sql.execution.python.PandasGroupUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils


/**
 * Physical node for [[org.apache.spark.sql.catalyst.plans.logical.FlatMapCoGroupsInPandas]]
 *
 * The input dataframes are first Cogrouped.  Rows from each side of the cogroup are passed to the
 * Python worker via Arrow.  As each side of the cogroup may have a different schema we send every
 * group in its own Arrow stream.
 * The Python worker turns the resulting record batches to `pandas.DataFrame`s, invokes the
 * user-defined function, and passes the resulting `pandas.DataFrame`
 * as an Arrow record batch. Finally, each record batch is turned to
 * Iterator[InternalRow] using ColumnarBatch.
 *
 * Note on memory usage:
 * Both the Python worker and the Java executor need to have enough memory to
 * hold the largest cogroup. The memory on the Java side is used to construct the
 * record batches (off heap memory). The memory on the Python side is used for
 * holding the `pandas.DataFrame`. It's possible to further split one group into
 * multiple record batches to reduce the memory footprint on the Java side, this
 * is left as future work.
 */
case class FlatMapCoGroupsInPandasExec(
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan)
  extends SparkPlan with BinaryExecNode {

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
  private val pandasFunction = func.asInstanceOf[PythonUDF].func
  private val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))

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

    val (leftDedup, leftArgOffsets) = resolveArgOffsets(left, leftGroup)
    val (rightDedup, rightArgOffsets) = resolveArgOffsets(right, rightGroup)

    // Map cogrouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    left.execute().zipPartitions(right.execute())  { (leftData, rightData) =>
      if (leftData.isEmpty && rightData.isEmpty) Iterator.empty else {

        val leftGrouped = groupAndProject(leftData, leftGroup, left.output, leftDedup)
        val rightGrouped = groupAndProject(rightData, rightGroup, right.output, rightDedup)
        val data = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup)
          .map { case (_, l, r) => (l, r) }

        val runner = new CoGroupedArrowPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
          Array(leftArgOffsets ++ rightArgOffsets),
          StructType.fromAttributes(leftDedup),
          StructType.fromAttributes(rightDedup),
          sessionLocalTimeZone,
          pythonRunnerConf)

        executePython(data, output, runner)
      }
    }
  }
}
