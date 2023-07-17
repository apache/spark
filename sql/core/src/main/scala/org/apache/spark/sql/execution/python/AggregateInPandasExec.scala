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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.{ChainedPythonFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils

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
      udf: PythonFuncExpression): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonFuncExpression) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(!_.exists(_.isInstanceOf[PythonFuncExpression])))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs.
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (allInputs.exists(_.semanticEquals(e))) {
          allInputs.indexWhere(_.semanticEquals(e))
        } else {
          allInputs += e
          dataTypes += e.dataType
          allInputs.length - 1
        }
      }.toArray
    }.toArray

    // Schema of input rows to the python runner
    val aggInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    }.toArray)


    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

    val evaluatorFactory = new AggregateInPandasEvaluatorFactory(
      child.output,
      aggExpressions,
      resultExpressions,
      conf.windowExecBufferInMemoryThreshold,
      conf.windowExecBufferSpillThreshold,
      sessionWindowOption,
      groupingWithoutSessionExpressions,
      allInputs,
      groupingExpressions,
      pyFuncs,
      argOffsets,
      aggInputSchema,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID)

    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
