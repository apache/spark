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

import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{RowToColumnarEvaluatorFactory, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * A physical plan that evaluates a [[PythonUDTF]] using Apache Arrow.
 * This is similar to [[ArrowEvalPythonExec]].
 *
 * @param udtf the user-defined Python function.
 * @param requiredChildOutput the required output of the child plan. It's used for omitting data
 *                            generation that will be discarded next by a projection.
 * @param resultAttrs the output schema of the Python UDTF.
 * @param child the child plan.
 * @param evalType the Python eval type.
 */
case class ArrowEvalPythonUDTFExec(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends EvalPythonUDTFExec with PythonSQLMetrics {

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val largeVarTypes = conf.arrowUseLargeVarTypes
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
  private[this] val sessionUUID = {
    Option(session).collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"))

  /**
   * The ordinals of the UDTF arguments in the child's output, if all the arguments are columns of
   * the child.
   */
  @transient private lazy val argumentOrdinals: Option[Array[Int]] = {
    val ordinals = flattenArguments()._2.map {
      case a: Attribute => child.output.indexWhere(_.exprId == a.exprId)
      case _ => -1
    }
    if (ordinals.forall(_ >= 0)) Some(ordinals.toArray) else None
  }

  // When the child supports columnar output (e.g., Arrow-backed DSv2 connectors) and all the UDTF
  // arguments are columns of the child, accept columnar input to avoid the ColumnarToRow ->
  // ArrowWriter round-trip of the arguments. Their Arrow FieldVectors are serialized to the Python
  // worker directly. The output rows are still joined row by row with the input rows, as a UDTF
  // returns any number of rows per input row.
  override def supportsColumnar: Boolean =
    child.supportsColumnar && conf.arrowPySparkUDTFColumnarInputEnabled &&
      argumentOrdinals.isDefined
  override def supportsRowBased: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    if (child.supportsColumnar && argumentOrdinals.isDefined) {
      executeWithColumnarInput()
    } else {
      super.doExecute()
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val evaluatorFactory = new RowToColumnarEvaluatorFactory(
      conf.offHeapColumnVectorEnabled,
      conf.columnBatchSize,
      schema,
      longMetric("numOutputRows"),
      longMetric("numOutputBatches"))
    executeWithColumnarInput().mapPartitionsWithIndexInternal { (index, rowIterator) =>
      evaluatorFactory.createEvaluator().eval(index, rowIterator)
    }
  }

  private def executeWithColumnarInput(): RDD[InternalRow] = {
    val ordinals = argumentOrdinals.get
    child.executeColumnar().mapPartitionsInternal { batchIter =>
      val context = TaskContext.get()
      // Only the child columns in the output are buffered to join with the result.
      val buffer = new UDTFInputBuffer(context, requiredChildOutput.length)
      val toBufferedRow = UnsafeProjection.create(requiredChildOutput, child.output)
      val (argMetas, allInputs) = flattenArguments()

      val bufferedBatchIter = batchIter.map { batch =>
        batch.rowIterator().asScala.foreach(row => buffer.add(toBufferedRow(row)))
        batch
      }

      val columnarBatchIter = new ColumnarArrowPythonUDTFRunner(
        udtf,
        evalType,
        argMetas,
        argumentSchema(allInputs),
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        sessionUUID,
        ordinals).compute(bufferedBatchIter, context.partitionId(), context)

      joinWithInput(toOutputRows(columnarBatchIter), buffer, requiredChildOutput)
    }
  }

  override protected def evaluate(
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]] = {

    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val columnarBatchIter = new ArrowPythonUDTFRunner(
      udtf,
      evalType,
      argMetas,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID,
      sessionUUID).compute(batchIter, context.partitionId(), context)

    toOutputRows(columnarBatchIter)
  }

  private def toOutputRows(
      columnarBatchIter: Iterator[ColumnarBatch]): Iterator[Iterator[InternalRow]] = {
    val outputTypes = resultAttrs.map(_.dataType.transformRecursively {
      case udt: UserDefinedType[_] => udt.sqlType
    })

    columnarBatchIter.map { batch =>
      // UDTF returns a StructType column in ColumnarBatch. Flatten the columnar batch here.
      val columnVector = batch.column(0).asInstanceOf[ArrowColumnVector]
      val outputVectors = resultAttrs.indices.map(columnVector.getChild)
      val flattenedBatch = new ColumnarBatch(outputVectors.toArray)

      val actualDataTypes = (0 until flattenedBatch.numCols()).map(
        i => flattenedBatch.column(i).dataType())
      if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
        throw QueryExecutionErrors.arrowDataTypeMismatchError(
          "Python UDTF", outputTypes, actualDataTypes)
      }

      flattenedBatch.setNumRows(batch.numRows())
      flattenedBatch.rowIterator().asScala
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
