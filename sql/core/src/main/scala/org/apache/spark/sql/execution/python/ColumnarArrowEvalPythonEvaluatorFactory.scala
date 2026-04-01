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
import java.util.ArrayDeque

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
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluator factory for Arrow Python UDFs that processes [[ColumnarBatch]] input.
 *
 * Two execution paths:
 *
 * 1. '''Columnar path''' (all UDF inputs are simple column references):
 *    UDF input columns are extracted as Arrow FieldVectors and sent directly
 *    via IPC. Pass-through columns are kept as [[ColumnVector]] references and
 *    combined with UDF results at the columnar level. This works because each
 *    input [[ColumnarBatch]] is sent as one Arrow IPC RecordBatch, the Python
 *    worker processes scalar UDFs batch-by-batch preserving 1:1 correspondence,
 *    and the default output batch config does not do slicing (both
 *    maxRecordsPerOutputBatch and maxBytesPerOutputBatch default to -1).
 *
 * 2. '''Row fallback path''' (UDF inputs contain complex expressions):
 *    Falls back to row-based ArrowWriter for UDF input serialization and
 *    [[HybridRowQueue]] for pass-through column buffering, since
 *    [[BatchedPythonArrowInput]] re-batches rows and breaks batch boundaries.
 *
 * TODO: Add a physical plan rule that inserts a ProjectExec before
 *   ArrowEvalPythonExec to pre-evaluate complex UDF input expressions into
 *   simple column references. This would eliminate the fallback path,
 *   allowing the direct Arrow path to always be used when the child
 *   supports columnar output.
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

    private def collectFunctions(
        udf: PythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
      udf.children match {
        case Seq(u: PythonUDF) =>
          val ((chained, _), children) = collectFunctions(u)
          ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id),
            children)
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

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // Flatten all UDF arguments and build argMetas.
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

      val outputTypes = output.drop(childOutput.length).map(
        _.dataType.transformRecursively {
          case udt: UserDefinedType[_] => udt.sqlType
        })

      // Try to resolve all UDF inputs as simple column references.
      val inputColumnIndices = resolveColumnIndices(allInputs.toSeq)

      if (inputColumnIndices.isDefined) {
        evalColumnar(inputIter, context, pyFuncs, argMetas,
          udfInputSchema, outputTypes, inputColumnIndices.get)
      } else {
        evalRowFallback(inputIter, context, pyFuncs, argMetas,
          allInputs.toSeq, udfInputSchema, outputTypes)
      }
    }

    /**
     * Try to resolve all UDF input expressions as simple column references
     * in childOutput. Returns Some(indices) if all are AttributeReferences;
     * None otherwise.
     */
    private def resolveColumnIndices(
        allInputs: Seq[Expression]): Option[Array[Int]] = {
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
     * Columnar path: send Arrow FieldVectors directly to Python.
     * Pass-through columns are kept as ColumnVector references and combined
     * with UDF results at the columnar level (1:1 batch correspondence).
     *
     * This works because:
     * - ColumnarArrowPythonInput sends one IPC RecordBatch per ColumnarBatch
     * - Python scalar UDF worker processes batch-by-batch (one in, one out)
     * - Default output batch config does not do slicing (both defaults = -1)
     */
    private def evalColumnar(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        udfInputSchema: StructType,
        outputTypes: Seq[DataType],
        columnIndices: Array[Int]): Iterator[InternalRow] = {

      // Queue pass-through columns per batch for columnar combining.
      val passThruQueue = new ArrayDeque[(Array[ColumnVector], Int)]()

      val bufferedIter = inputIter.map { batch =>
        val passThruCols = childOutput.indices.map(
          i => batch.column(i)).toArray
        passThruQueue.add((passThruCols, batch.numRows()))
        batch
      }

      val pyRunner = new ColumnarArrowPythonWithNamedArgumentRunner(
        pyFuncs, evalType, argMetas, udfInputSchema,
        sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
        pythonMetrics, jobArtifactUUID, sessionUUID, columnIndices)

      val resultIter = pyRunner.compute(
        bufferedIter, context.partitionId(), context)

      // Combine pass-through columns with UDF results at columnar level,
      // then flatten to rows.
      resultIter.flatMap { resultBatch =>
        validateOutputTypes(resultBatch, outputTypes)

        val numRows = resultBatch.numRows()
        val resultCols = (0 until resultBatch.numCols()).map(
          i => resultBatch.column(i)).toArray

        val (passThruCols, passThruRows) = passThruQueue.poll()
        assert(passThruRows == numRows,
          s"Batch size mismatch: pass-through has $passThruRows rows " +
          s"but UDF result has $numRows rows.")

        // Combine: [pass-through columns | UDF result columns]
        val combined = new ColumnarBatch(
          passThruCols ++ resultCols, numRows)
        combined.rowIterator().asScala
      }
    }

    /**
     * Fallback path: UDF inputs contain complex expressions that cannot be
     * resolved to simple column indices. Uses row-based ArrowWriter for UDF
     * input serialization and HybridRowQueue for pass-through buffering.
     *
     * This path uses BatchedPythonArrowInput which re-batches rows according
     * to maxRecordsPerBatch, breaking the original ColumnarBatch boundaries.
     * Therefore columnar pass-through combining is not possible.
     *
     * TODO: Add a physical plan rule that inserts a ProjectExec before
     *   ArrowEvalPythonExec to pre-evaluate complex UDF input expressions
     *   into simple column references. This would eliminate this fallback
     *   entirely.
     */
    private def evalRowFallback(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        allInputs: Seq[Expression],
        udfInputSchema: StructType,
        outputTypes: Seq[DataType]): Iterator[InternalRow] = {

      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        childOutput.length)
      context.addTaskCompletionListener[Unit] { _ => queue.close() }

      val projection = MutableProjection.create(allInputs, childOutput)
      projection.initialize(context.partitionId())

      val unsafeProj = UnsafeProjection.create(childOutput, childOutput)

      val projectedRowIter = inputIter.flatMap { batch =>
        batch.rowIterator().asScala
      }.map { inputRow =>
        queue.add(unsafeProj(inputRow).copy())
        projection(inputRow)
      }

      val batchIter = Iterator(projectedRowIter)
      val pyRunner = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs, evalType, argMetas, udfInputSchema,
        sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
        pythonMetrics, jobArtifactUUID, sessionUUID
      ) with BatchedPythonArrowInput

      val resultIter = pyRunner.compute(
        batchIter, context.partitionId(), context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      resultIter.flatMap { batch =>
        validateOutputTypes(batch, outputTypes)
        batch.rowIterator.asScala
      }.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }

    private def validateOutputTypes(
        batch: ColumnarBatch, outputTypes: Seq[DataType]): Unit = {
      val actualDataTypes = (0 until batch.numCols()).map(
        i => batch.column(i).dataType())
      if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
        throw QueryExecutionErrors.arrowDataTypeMismatchError(
          "pandas_udf()", outputTypes, actualDataTypes)
      }
    }
  }
}
