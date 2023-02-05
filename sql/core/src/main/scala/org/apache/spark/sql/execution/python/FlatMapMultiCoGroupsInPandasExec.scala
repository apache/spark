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
import org.apache.spark.sql.catalyst.plans.physical.{
  AllTuples,
  ClusteredDistribution,
  Distribution,
  Partitioning
}
import org.apache.spark.sql.execution.{MultiCoGroupedIterator, NaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.python.PandasGroupUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

/**
 * Physical node for [[org.apache.spark.sql.catalyst.plans.logical.FlatMapMultiCoGroupsInPandas]]
 *
 * The input dataframes are first Cogrouped. Rows from each side of the cogroup are passed to the
 * Python worker via Arrow. As each side of the cogroup may have a different schema we send every
 * group in its own Arrow stream. The Python worker turns the resulting record batches to
 * `pandas.DataFrame`s, invokes the user-defined function, and passes the resulting
 * `pandas.DataFrame` as an Arrow record batch. Finally, each record batch is turned to
 * Iterator[InternalRow] using ColumnarBatch.
 *
 * Note on memory usage: Both the Python worker and the Java executor need to have enough memory
 * to hold the largest cogroup. The memory on the Java side is used to construct the record
 * batches (off heap memory). The memory on the Python side is used for holding the
 * `pandas.DataFrame`. It's possible to further split one group into multiple record batches to
 * reduce the memory footprint on the Java side, this is left as future work.
 */
case class FlatMapMultiCoGroupsInPandasExec(
    groups: List[Seq[Attribute]],
    func: Expression,
    output: Seq[Attribute],
    plans: List[SparkPlan],
    passKey: Boolean)
  extends SparkPlan with NaryExecNode with PythonSQLMetrics{

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pandasFunction = func.asInstanceOf[PythonUDF].func
  private val pythonRunnerConf = (ArrowUtils.getPythonRunnerConfMap(conf) +
    ("pass_key" -> passKey.toString))
  private val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = plans.head.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    groups.map(group =>
      if (group.isEmpty) AllTuples else ClusteredDistribution(group)
    )
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    groups.map(rightgroup => rightgroup.map(SortOrder(_, Ascending)))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val resolvedArgOffsets = plans
      .zip(groups)
      .map({ case (plan: SparkPlan, group) =>
        resolveArgOffsets(plan.output, group)
      })
    // Map cogrouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    plans.head
      .execute()
      .zipPartitions(plans.tail.map(_.execute()))( datas =>
        if (datas.forall(_.isEmpty)) Iterator.empty
        else {
          val internalRowDatas: Seq[Iterator[InternalRow]] = datas
            .map(data => data.map(_.asInstanceOf[InternalRow]))

          val grouped = (internalRowDatas, groups, plans.map(_.output)).zipped.toList
            .zip(resolvedArgOffsets.map(_._1))
            .map({
              case (
                (
                  internalRowData: Iterator[InternalRow],
                  group: Seq[Attribute],
                  output: Seq[Attribute]
                ),
                dedup: Seq[Attribute]) =>
                groupAndProject(internalRowData, group, output, dedup)
            })

          val data = new MultiCoGroupedIterator(grouped, groups.head)
            .map { case (_, b) => b }

          val runner = new MultiCoGroupedArrowPythonRunner(
            chainedFunc,
            PythonEvalType.SQL_MULTICOGROUPED_MAP_PANDAS_UDF,
            Array(resolvedArgOffsets.flatMap(_._2).toArray),
            resolvedArgOffsets.map(_._1).map(StructType.fromAttributes),
            sessionLocalTimeZone,
            pythonRunnerConf,
            pythonMetrics)

          executePython(data, output, runner)
        }
      )
  }

  override protected def withNewChildrenInternal(
      plans: List[SparkPlan]): FlatMapMultiCoGroupsInPandasExec = {
    copy(groups = groups, func = func, output = output, plans = plans, passKey)
  }

  override def childrenNodes: Seq[SparkPlan] = plans
}
