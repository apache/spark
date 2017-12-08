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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Expression, JoinedRow, NamedExpression, PythonUDF, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

case class AggregateInPandasExec(
    groupingExpressions: Seq[Expression],
    udfExpressions: Seq[PythonUDF],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }
  }

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingExpressions.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pandasRespectSessionTimeZone = conf.pandasRespectSessionTimeZone

    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip

    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]

    allInputs.appendAll(groupingExpressions)

    val argOffsets = inputs.map { input =>
      input.map { e =>
          allInputs += e
          dataTypes += e.dataType
          allInputs.length - 1 - groupingExpressions.length
      }.toArray
    }.toArray

    val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    })

    inputRDD.mapPartitionsInternal { iter =>
      val grouped = if (groupingExpressions.isEmpty) {
        Iterator((null, iter))
      } else {
        val groupedIter = GroupedIterator(iter, groupingExpressions, child.output)

        val dropGrouping =
          UnsafeProjection.create(allInputs.drop(groupingExpressions.length), child.output)

        groupedIter.map {
          case (k, groupedRowIter) => (k, groupedRowIter.map(dropGrouping))
        }
      }

      val context = TaskContext.get()

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), groupingExpressions.length)
      context.addTaskCompletionListener { _ =>
        queue.close()
      }

      // Add rows to queue to join later with the result.
      val projectedRowIter = grouped.map { case (groupingKey, rows) =>
        queue.add(groupingKey.asInstanceOf[UnsafeRow])
        rows
      }

      val columnarBatchIter = new ArrowPythonRunner(
        pyFuncs, bufferSize, reuseWorker,
        PythonEvalType.SQL_PANDAS_GROUP_AGG_UDF, argOffsets, schema,
        sessionLocalTimeZone, pandasRespectSessionTimeZone)
        .compute(projectedRowIter, context.partitionId(), context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      columnarBatchIter.map(_.rowIterator.next()).map{ outputRow =>
        val leftRow = queue.remove()
        val joinedRow = joined(leftRow, outputRow)
        resultProj(joinedRow)
      }
    }
  }
}
