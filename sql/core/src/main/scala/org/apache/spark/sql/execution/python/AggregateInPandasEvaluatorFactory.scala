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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.GroupedIterator
import org.apache.spark.sql.execution.aggregate.UpdatingSessionsIterator
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class AggregateInPandasEvaluatorFactory(
    childOutput: Seq[Attribute],
    aggExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    windowExecBufferInMemoryThreshold: Int,
    windowExecBufferSpillThreshold: Int,
    sessionWindowOption: Option[NamedExpression],
    groupingWithoutSessionExpressions: Seq[NamedExpression],
    allInputs: ArrayBuffer[Expression],
    groupingExpressions: Seq[NamedExpression],
    pyFuncs: Seq[ChainedPythonFunctions],
    argOffsets: Array[Array[Int]],
    aggInputSchema: StructType,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new AggregateInPandasEvaluator

  private class AggregateInPandasEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {

    private def mayAppendUpdatingSessionIterator(
        iter: Iterator[InternalRow]): Iterator[InternalRow] = {
      val newIter = sessionWindowOption match {
        case Some(sessionExpression) =>
          new UpdatingSessionsIterator(
            iter,
            groupingWithoutSessionExpressions,
            sessionExpression,
            childOutput,
            windowExecBufferInMemoryThreshold,
            windowExecBufferSpillThreshold)

        case None => iter
      }
      newIter
    }

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val iter = inputs.head
      // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
      if (iter.isEmpty) iter else {
        // If we have session window expression in aggregation, we wrap iterator with
        // UpdatingSessionIterator to calculate sessions for input rows and update
        // rows' session column, so that further aggregations can aggregate input rows
        // for the same session.
        val newIter: Iterator[InternalRow] = mayAppendUpdatingSessionIterator(iter)
        val prunedProj = UnsafeProjection.create(allInputs.toSeq, childOutput)

        val groupedItr = if (groupingExpressions.isEmpty) {
          // Use an empty unsafe row as a place holder for the grouping key
          Iterator((new UnsafeRow(), newIter))
        } else {
          GroupedIterator(newIter, groupingExpressions, childOutput)
        }
        val grouped = groupedItr.map { case (key, rows) =>
          (key, rows.map(prunedProj))
        }

        val context = TaskContext.get()

        // The queue used to buffer input rows so we can drain it to
        // combine input with output from Python.
        val queue = HybridRowQueue(
          context.taskMemoryManager(),
          new File(Utils.getLocalDir(SparkEnv.get.conf)),
          groupingExpressions.length)
        context.addTaskCompletionListener[Unit] { _ =>
          queue.close()
        }

        // Add rows to queue to join later with the result.
        val projectedRowIter = grouped.map { case (groupingKey, rows) =>
          queue.add(groupingKey.asInstanceOf[UnsafeRow])
          rows
        }

        val columnarBatchIter = new ArrowPythonRunner(
          pyFuncs,
          PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
          argOffsets,
          aggInputSchema,
          sessionLocalTimeZone,
          largeVarTypes,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID).compute(projectedRowIter, context.partitionId(), context)

        val joinedAttributes =
          groupingExpressions.map(_.toAttribute) ++ aggExpressions.map(_.resultAttribute)
        val joined = new JoinedRow
        val resultProj = UnsafeProjection.create(resultExpressions, joinedAttributes)

        columnarBatchIter.map(_.rowIterator.next()).map { aggOutputRow =>
          val leftRow = queue.remove()
          val joinedRow = joined(leftRow, aggOutputRow)
          resultProj(joinedRow)
        }
      }
    }
  }
}
