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

import scala.jdk.CollectionConverters._

import org.apache.spark.{JobArtifactSet, SparkException, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Grouped a iterator into batches.
 * This is similar to iter.grouped but returns Iterator[T] instead of Seq[T].
 * This is necessary because sometimes we cannot hold reference of input rows
 * because the some input rows are mutable and can be reused.
 */
private[spark] class BatchIterator[T](iter: Iterator[T], batchSize: Int)
  extends Iterator[Iterator[T]] {

  override def hasNext: Boolean = iter.hasNext

  override def next(): Iterator[T] = {
    new Iterator[T] {
      var count = 0

      override def hasNext: Boolean = iter.hasNext && count < batchSize

      override def next(): T = {
        if (!hasNext) {
          Iterator.empty.next()
        } else {
          count += 1
          iter.next()
        }
      }
    }
  }
}

/**
 * A physical plan that evaluates a vectorized UDF.
 * Following eval types are supported:
 *
 * <ul>
 *   <li> SQL_ARROW_BATCHED_UDF for Arrow Optimized Python UDF
 *   <li> SQL_SCALAR_ARROW_UDF for Scalar Arrow UDF
 *   <li> SQL_SCALAR_ARROW_ITER_UDF for Scalar Iterator Arrow UDF
 *   <li> SQL_SCALAR_PANDAS_UDF for Scalar Pandas UDF
 *   <li> SQL_SCALAR_PANDAS_ITER_UDF for Scalar Iterator Pandas UDF
 *   <li> SQL_ARROW_ELEMENTWISE_UDF for a row-at-a-time UDF lifted out of a higher-order function's
 *        lambda (see ExtractPythonUDFFromLambda)
 *   <li> SQL_SCALAR_PANDAS_ELEMENTWISE_UDF for a Scalar Pandas UDF lifted out of such a lambda
 *   <li> SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF for a Scalar Iterator Pandas UDF lifted out of one
 *   <li> SQL_SCALAR_ARROW_ELEMENTWISE_UDF for a Scalar Arrow UDF lifted out of such a lambda
 *   <li> SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF for a Scalar Iterator Arrow UDF lifted out of one
 * </ul>
 *
 */
case class ArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)(
    val columnarInputEnabled: Boolean) extends EvalPythonExec with PythonSQLMetrics {
  if (!supportedPythonEvalTypes.contains(evalType)) {
    throw SparkException.internalError(s"Unexpected eval type $evalType")
  }

  override protected def additionalMetrics: Map[String, SQLMetric] = {
    if (evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF &&
        !conf.legacyPandasConversionUDF && conf.pythonUDFDetailedTimingEnabled) {
      createPythonPhaseMetrics()
    } else {
      Map.empty
    }
  }

  // SQL metrics are fixed with the plan; use that decision if the session setting changes later.
  private def pythonRunnerConfMap: Map[String, String] =
    ArrowPythonRunner.getPythonRunnerConfMap(conf).updated(
      SQLConf.PYTHON_UDF_DETAILED_TIMING_ENABLED.key,
      pythonMetrics.contains("pythonNumTimingReports").toString)

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
  private[this] val sessionUUID = {
    Option(session).collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  // When the child supports columnar output (e.g., Arrow-backed DSv2 connectors)
  // and columnar input is enabled, accept columnar input: the UDF inputs of a
  // batch whose columns are ArrowColumnVectors are serialized to IPC directly,
  // bypassing the ColumnarToRow -> ArrowWriter round-trip. Each input batch is
  // released once it has been written to Python. Without pass-through columns,
  // the result batches from Python are the output batches, which a row-based
  // parent reads through a ColumnarToRowExec, fused into its whole-stage codegen
  // stage. Otherwise the pass-through columns are copied as rows and joined with
  // the results, and this node also supports row-based output: a row-based parent
  // gets the joined rows as they are, with no ColumnarToRowExec in between, and a
  // columnar parent, i.e. a stacked evaluation, gets them converted back to
  // columnar batches.
  // Otherwise it reads rows from its child, even from one that also supports
  // columnar, such as the scan of the Arrow cache.
  // Whether columnar input is enabled is fixed when the node is planned, and
  // carried over to its copies, so that any copy, including one that a rule makes
  // after the transitions are inserted, reads its child in the format the planner
  // prepared it for, even if the configuration changes before the plan is executed.
  override def supportsColumnar: Boolean = columnarInputEnabled && child.supportsColumnar

  override def supportsRowBased: Boolean = !supportsColumnar || requiredChildOutput.nonEmpty

  override protected def doExecute(): RDD[InternalRow] = {
    if (supportsColumnar) {
      val inputRDD = child.executeColumnar()
      val rowEvaluatorFactory = columnarEvaluatorFactory.rowOutput
      if (conf.usePartitionEvaluator) {
        inputRDD.mapPartitionsWithEvaluator(rowEvaluatorFactory)
      } else {
        inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
          rowEvaluatorFactory.createEvaluator().eval(index, iter)
        }
      }
    } else {
      super.doExecute()
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val inputRDD = child.executeColumnar()
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(columnarEvaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        columnarEvaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }

  private lazy val columnarEvaluatorFactory =
    new ColumnarArrowEvalPythonEvaluatorFactory(
      child.output,
      udfs,
      requiredChildOutput,
      resultAttrs,
      conf.arrowMaxRecordsPerBatch,
      conf.arrowMaxBytesPerBatch,
      evalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConfMap,
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID)

  override protected def evaluatorFactory: EvalPythonEvaluatorFactory = {
    new ArrowEvalPythonEvaluatorFactory(
      child.output,
      udfs,
      requiredChildOutput,
      resultAttrs,
      conf.arrowMaxRecordsPerBatch,
      evalType,
      conf.sessionLocalTimeZone,
      conf.arrowUseLargeVarTypes,
      pythonRunnerConfMap,
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID)
  }

  override def withRequiredChildOutput(
      requiredChildOutput: Seq[Attribute]): ArrowEvalPythonExec =
    copy(requiredChildOutput = requiredChildOutput)(columnarInputEnabled)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)(columnarInputEnabled)

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(columnarInputEnabled.asInstanceOf[java.lang.Boolean])

  private def supportedPythonEvalTypes: Array[Int] =
    Array(
      PythonEvalType.SQL_ARROW_BATCHED_UDF,
      PythonEvalType.SQL_ARROW_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ITER_ELEMENTWISE_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)
}

class ArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    passThroughOutput: Seq[Attribute],
    resultOutput: Seq[Attribute],
    batchSize: Int,
    evalType: Int,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
  extends EvalPythonEvaluatorFactory(childOutput, udfs, passThroughOutput, resultOutput) {

  override def evaluate(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {

    val outputTypes = resultOutput.map(_.dataType.transformRecursively {
      case udt: UserDefinedType[_] => udt.sqlType
    })

    val batchIter = Iterator(iter)

    val pyRunner = new ArrowPythonWithNamedArgumentRunner(
      funcs,
      evalType,
      argMetas,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID,
      // Parallel to `funcs` (both come from `udfs` in order); tells the worker how many `array`
      // levels each element-wise UDF flattens/re-nests (see `PythonUDF.elementwiseNestingDepth`).
      udfs.map(_.elementwiseNestingDepth)) with BatchedPythonArrowInput
    val columnarBatchIter = pyRunner.compute(batchIter, context.partitionId(), context)

    columnarBatchIter.flatMap { batch =>
      val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
      if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
        throw QueryExecutionErrors.arrowDataTypeMismatchError(
          "pandas_udf()", outputTypes, actualDataTypes)
      }
      batch.rowIterator.asScala
    }
  }
}
