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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{BinaryExecNode, CoGroupedIterator, SparkPlan}

case class FlatMapCoGroupsInPandasExec(
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan)
  extends BasePandasGroupExec(func, output) with BinaryExecNode{

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    leftGroup
      .map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {

    val (schemaLeft, leftDedup, _) = createSchema(left, leftGroup)
    val (schemaRight, rightDedup, _) = createSchema(right, rightGroup)

    left.execute().zipPartitions(right.execute())  { (leftData, rightData) =>

      val leftGrouped = groupAndDedup(leftData, leftGroup, left.output, leftDedup)
      val rightGrouped = groupAndDedup(rightData, rightGroup, right.output, rightDedup)
      val data = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup)
        .map{case (k, l, r) => (l, r)}

      val runner = new InterleavedArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        Array(Array.empty),
        schemaLeft,
        schemaRight,
        sessionLocalTimeZone,
        pythonRunnerConf)

      executePython(data, runner)

    }

  }

}
