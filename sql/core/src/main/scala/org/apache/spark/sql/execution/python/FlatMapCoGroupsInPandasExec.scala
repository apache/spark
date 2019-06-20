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

import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{BinaryExecNode, CoGroupedIterator, GroupedIterator, SparkPlan}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

case class FlatMapCoGroupsInPandasExec(
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode {

  private val pandasFunction = func.asInstanceOf[PythonUDF].func

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = {
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    leftGroup
      .map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil
  }


  override protected def doExecute(): RDD[InternalRow] = {

    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    left.execute().zipPartitions(right.execute())  { (leftData, rightData) =>
      val leftGrouped = GroupedIterator(leftData, leftGroup, left.output)
      val rightGrouped = GroupedIterator(rightData, rightGroup, right.output)
      val cogroup = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup)
        .map{case (k, l, r) => (l, r)}
      val context = TaskContext.get()
      val columnarBatchIter = new InterleavedArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
        Array(Array.empty),
        left.schema,
        right.schema,
        sessionLocalTimeZone,
        pythonRunnerConf).compute(cogroup, context.partitionId(), context)


        val unsafeProj = UnsafeProjection.create(output, output)

        columnarBatchIter.flatMap { batch =>
          // Grouped Map UDF returns a StructType column in ColumnarBatch, select the children here
          val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
          val outputVectors = output.indices.map(structVector.getChild)
          val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
          flattenedBatch.setNumRows(batch.numRows())
          flattenedBatch.rowIterator.asScala
        }.map(unsafeProj)
    }

  }

}
