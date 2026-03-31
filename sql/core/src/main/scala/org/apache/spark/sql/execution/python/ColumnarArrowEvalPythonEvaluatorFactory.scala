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
import scala.jdk.CollectionConverters._

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Evaluator factory for Arrow Python UDFs that processes [[ColumnarBatch]] input directly.
 *
 * When UDF inputs are simple column references and the batch is Arrow-backed,
 * this avoids the columnar-to-row-to-columnar round-trip by extracting Arrow
 * FieldVectors directly from [[ArrowColumnVector]] columns.
 *
 * When UDF inputs contain complex expressions (not simple column references),
 * falls back to converting ColumnarBatch to rows and delegating to the existing
 * row-based [[ArrowEvalPythonEvaluatorFactory]].
 */
private[python] class ColumnarArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    output: Seq[Attribute],
    batchSize: Int,
    evalType: Int,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
  extends PartitionEvaluatorFactory[ColumnarBatch, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[ColumnarBatch, InternalRow] =
    new ColumnarArrowEvalPythonPartitionEvaluator

  private class ColumnarArrowEvalPythonPartitionEvaluator
      extends PartitionEvaluator[ColumnarBatch, InternalRow] {

    /** Collect chained Python functions and their children (same as EvalPythonEvaluatorFactory). */
    private def collectFunctions(
        udf: PythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
      udf.children match {
        case Seq(u: PythonUDF) =>
          val ((chained, _), children) = collectFunctions(u)
          ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
        case children =>
          assert(children.forall(!_.exists(_.isInstanceOf[PythonUDF])))
          ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
      }
    }

    override def eval(
        partitionIndex: Int,
        iters: Iterator[ColumnarBatch]*): Iterator[InternalRow] = {
      val inputIter = iters.head
      val context = TaskContext.get()

      // Buffer input rows to combine with UDF output later (same as EvalPythonEvaluatorFactory).
      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        childOutput.length)
      context.addTaskCompletionListener[Unit] { _ => queue.close() }

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // Flatten all UDF arguments and build argMetas (same as EvalPythonEvaluatorFactory).
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argMetas = inputs.map { input =>
        input.map { e =>
          val (key, value) = e match {
            case NamedArgumentExpression(key, value) => (Some(key), value)
            case _ => (None, e)
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

      val udfInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toArray)

      // Try to resolve all UDF inputs as simple column references.
      val inputColumnIndices = resolveColumnIndices(allInputs.toSeq)

      if (inputColumnIndices.isDefined) {
        // Columnar path: all UDF inputs are simple column references.
        evalColumnar(
          inputIter, context, queue, pyFuncs, argMetas,
          udfInputSchema, inputColumnIndices.get)
      } else {
        // Fallback: complex expressions exist, convert to rows.
        evalRowFallback(
          inputIter, context, queue, pyFuncs, argMetas, allInputs.toSeq,
          udfInputSchema)
      }
    }

    /**
     * Try to resolve all UDF input expressions as simple column references in childOutput.
     * Returns Some(indices) if all are AttributeReferences; None otherwise.
     */
    private def resolveColumnIndices(allInputs: Seq[Expression]): Option[Array[Int]] = {
      val indices = allInputs.map {
        case attr: AttributeReference =>
          val idx = childOutput.indexWhere(_.exprId == attr.exprId)
          if (idx >= 0) idx else return None
        case _ =>
          return None
      }
      Some(indices.toArray)
    }

    /**
     * Columnar path: extract Arrow vectors directly from ColumnarBatch and send to Python.
     */
    private def evalColumnar(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        queue: HybridRowQueue,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        udfInputSchema: StructType,
        columnIndices: Array[Int]): Iterator[InternalRow] = {

      val unsafeProj = UnsafeProjection.create(childOutput, childOutput)

      // Wrap iterator: buffer rows in queue, then pass batch to runner.
      val bufferedIter = inputIter.map { batch =>
        val rowIter = batch.rowIterator().asScala
        while (rowIter.hasNext) {
          queue.add(unsafeProj(rowIter.next()).copy())
        }
        batch
      }

      val pyRunner = new ColumnarArrowPythonWithNamedArgumentRunner(
        pyFuncs,
        evalType,
        argMetas,
        udfInputSchema,
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        sessionUUID,
        columnIndices)

      val outputTypes = output.drop(childOutput.length).map(_.dataType.transformRecursively {
        case udt: UserDefinedType[_] => udt.sqlType
      })

      val columnarBatchIter = pyRunner.compute(bufferedIter, context.partitionId(), context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
          throw QueryExecutionErrors.arrowDataTypeMismatchError(
            "pandas_udf()", outputTypes, actualDataTypes)
        }
        batch.rowIterator.asScala
      }.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }

    /**
     * Fallback path: convert ColumnarBatch to rows and use existing row-based evaluation.
     * Used when UDF inputs contain complex expressions that cannot be resolved to
     * simple column indices.
     */
    private def evalRowFallback(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        queue: HybridRowQueue,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        allInputs: Seq[Expression],
        udfInputSchema: StructType): Iterator[InternalRow] = {

      val projection = MutableProjection.create(allInputs, childOutput)
      projection.initialize(context.partitionId())

      val outputTypes = output.drop(childOutput.length).map(_.dataType.transformRecursively {
        case udt: UserDefinedType[_] => udt.sqlType
      })

      // Convert ColumnarBatch to rows, buffer + project.
      val unsafeProj = UnsafeProjection.create(childOutput, childOutput)
      val projectedRowIter = inputIter.flatMap { batch =>
        batch.rowIterator().asScala
      }.map { inputRow =>
        queue.add(unsafeProj(inputRow).copy())
        projection(inputRow)
      }

      val batchIter = Iterator(projectedRowIter)

      val pyRunner = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs,
        evalType,
        argMetas,
        udfInputSchema,
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        sessionUUID) with BatchedPythonArrowInput
      val columnarBatchIter = pyRunner.compute(batchIter, context.partitionId(), context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
          throw QueryExecutionErrors.arrowDataTypeMismatchError(
            "pandas_udf()", outputTypes, actualDataTypes)
        }
        batch.rowIterator.asScala
      }.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }
  }
}
