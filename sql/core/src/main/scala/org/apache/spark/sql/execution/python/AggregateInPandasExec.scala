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

import org.apache.spark.{JobArtifactSet, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.UpdatingSessionsIterator
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Physical node for aggregation with group aggregate Pandas UDF.
 *
 * This plan works by sending the necessary (projected) input grouped data as Arrow record batches
 * to the python worker, the python worker invokes the UDF and sends the results to the executor,
 * finally the executor evaluates any post-aggregation expressions and join the result with the
 * grouped key.
 */
case class AggregateInPandasExec(
    groupingExpressions: Seq[NamedExpression],
    aggExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with PythonSQLMetrics {

  override val output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  val sessionWindowOption = groupingExpressions.find { p =>
    p.metadata.contains(SessionWindow.marker)
  }

  val groupingWithoutSessionExpressions = sessionWindowOption match {
    case Some(sessionExpression) =>
      groupingExpressions.filterNot { p => p.semanticEquals(sessionExpression) }

    case None => groupingExpressions
  }

  val udfExpressions = aggExpressions.map(_.aggregateFunction.asInstanceOf[PythonUDAF])

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = sessionWindowOption match {
    case Some(sessionExpression) =>
      Seq((groupingWithoutSessionExpressions ++ Seq(sessionExpression))
        .map(SortOrder(_, Ascending)))

    case None => Seq(groupingExpressions.map(SortOrder(_, Ascending)))
  }

  private def collectFunctions(
      udf: PythonFuncExpression): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonFuncExpression) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(!_.exists(_.isInstanceOf[PythonFuncExpression])))
        ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)

    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs.
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argMetas = inputs.map { input =>
      input.map { e =>
        val (key, value) = e match {
          case NamedArgumentExpression(key, value) =>
            (Some(key), value)
          case _ =>
            (None, e)
        }
        if (allInputs.exists(_.semanticEquals(value))) {
          ArgumentMetadata(allInputs.indexWhere(_.semanticEquals(value)), key)
        } else {
          allInputs += value
          dataTypes += value.dataType
          ArgumentMetadata(allInputs.length - 1, key)
        }
      }.toArray
    }.toArray

    // Schema of input rows to the python runner
    val aggInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    }.toArray)


    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

    // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty) iter else {
      // If we have session window expression in aggregation, we wrap iterator with
      // UpdatingSessionIterator to calculate sessions for input rows and update
      // rows' session column, so that further aggregations can aggregate input rows
      // for the same session.
      val newIter: Iterator[InternalRow] = mayAppendUpdatingSessionIterator(iter)
      val prunedProj = UnsafeProjection.create(allInputs.toSeq, child.output)

      val groupedItr = if (groupingExpressions.isEmpty) {
        // Use an empty unsafe row as a place holder for the grouping key
        Iterator((new UnsafeRow(), newIter))
      } else {
        GroupedIterator(newIter, groupingExpressions, child.output)
      }
      val grouped = groupedItr.map { case (key, rows) =>
        (key, rows.map(prunedProj))
      }

      val context = TaskContext.get()

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), groupingExpressions.length)
      context.addTaskCompletionListener[Unit] { _ =>
        queue.close()
      }

      // Add rows to queue to join later with the result.
      val projectedRowIter = grouped.map { case (groupingKey, rows) =>
        queue.add(groupingKey.asInstanceOf[UnsafeRow])
        rows
      }

      val columnarBatchIter = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
        argMetas,
        aggInputSchema,
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        conf.pythonUDFProfiler).compute(projectedRowIter, context.partitionId(), context)

      val joinedAttributes =
        groupingExpressions.map(_.toAttribute) ++ aggExpressions.map(_.resultAttribute)
      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(resultExpressions, joinedAttributes)

      columnarBatchIter.map(_.rowIterator.next()).map { aggOutputRow =>
        val leftRow = queue.remove()
        val joinedRow = joined(leftRow, aggOutputRow)
        resultProj(joinedRow)
      }
    }}
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)


  private def mayAppendUpdatingSessionIterator(
      iter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val newIter = sessionWindowOption match {
      case Some(sessionExpression) =>
        val inMemoryThreshold = conf.windowExecBufferInMemoryThreshold
        val spillThreshold = conf.windowExecBufferSpillThreshold

        new UpdatingSessionsIterator(iter, groupingWithoutSessionExpressions, sessionExpression,
          child.output, inMemoryThreshold, spillThreshold)

      case None => iter
    }

    newIter
  }
}
