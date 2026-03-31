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

import java.util.ArrayDeque

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Evaluator factory for Arrow Python UDFs that processes [[ColumnarBatch]] input
 * and produces [[ColumnarBatch]] output, staying fully in columnar format.
 *
 * When UDF inputs are simple column references and the batch is Arrow-backed,
 * this avoids the columnar-to-row-to-columnar round-trip entirely:
 * - UDF input columns: extracted as Arrow FieldVectors and sent directly via IPC.
 * - Pass-through columns: kept as ColumnVector references (no row conversion).
 * - Output: pass-through ColumnVectors + UDF result ColumnVectors combined into
 *   a new [[ColumnarBatch]].
 *
 * When UDF inputs contain complex expressions (not simple column references),
 * the UDF inputs are serialized via the row-based ArrowWriter fallback, but
 * pass-through columns are still kept in columnar format.
 *
 * Note: DBR handles complex expressions by inserting a Project node upstream
 * (via PhotonStageCompiler.extractUDFInputsIntoProject) to pre-evaluate them.
 * A similar physical plan rule could be added to OSS Spark in the future to
 * eliminate the fallback path.
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
  extends PartitionEvaluatorFactory[ColumnarBatch, ColumnarBatch] {

  override def createEvaluator(): PartitionEvaluator[ColumnarBatch, ColumnarBatch] =
    new ColumnarArrowEvalPythonPartitionEvaluator

  private class ColumnarArrowEvalPythonPartitionEvaluator
      extends PartitionEvaluator[ColumnarBatch, ColumnarBatch] {

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
        iters: Iterator[ColumnarBatch]*): Iterator[ColumnarBatch] = {
      val inputIter = iters.head
      val context = TaskContext.get()

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

      val outputTypes = output.drop(childOutput.length).map(_.dataType.transformRecursively {
        case udt: UserDefinedType[_] => udt.sqlType
      })

      // Queue to buffer pass-through columns from input batches.
      // Each entry is (ColumnVector[], numRows) corresponding to one input ColumnarBatch.
      val passThruQueue = new ArrayDeque[(Array[ColumnVector], Int)]()

      // Try to resolve all UDF inputs as simple column references.
      val inputColumnIndices = resolveColumnIndices(allInputs.toSeq)

      val resultIter = if (inputColumnIndices.isDefined) {
        evalColumnar(
          inputIter, context, pyFuncs, argMetas, udfInputSchema,
          inputColumnIndices.get, passThruQueue)
      } else {
        evalRowFallback(
          inputIter, context, pyFuncs, argMetas, allInputs.toSeq,
          udfInputSchema, passThruQueue)
      }

      combineResults(resultIter, outputTypes, passThruQueue)
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
     * Pass-through columns are kept as ColumnVector references -- no row conversion.
     */
    private def evalColumnar(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        udfInputSchema: StructType,
        columnIndices: Array[Int],
        passThruQueue: ArrayDeque[(Array[ColumnVector], Int)]): Iterator[ColumnarBatch] = {

      // Wrap iterator: save pass-through columns, then pass batch to runner.
      val bufferedIter = inputIter.map { batch =>
        val passThruCols = childOutput.indices.map(i => batch.column(i)).toArray
        passThruQueue.add((passThruCols, batch.numRows()))
        batch
      }

      val pyRunner = new ColumnarArrowPythonWithNamedArgumentRunner(
        pyFuncs, evalType, argMetas, udfInputSchema, sessionLocalTimeZone,
        largeVarTypes, pythonRunnerConf, pythonMetrics, jobArtifactUUID,
        sessionUUID, columnIndices)

      pyRunner.compute(bufferedIter, context.partitionId(), context)
    }

    /**
     * Fallback path: UDF inputs contain complex expressions. Convert to rows for
     * UDF input serialization, but still keep pass-through columns in columnar format.
     */
    private def evalRowFallback(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        allInputs: Seq[Expression],
        udfInputSchema: StructType,
        passThruQueue: ArrayDeque[(Array[ColumnVector], Int)]): Iterator[ColumnarBatch] = {

      val projection = MutableProjection.create(allInputs, childOutput)
      projection.initialize(context.partitionId())

      // Convert ColumnarBatch to rows for UDF input, save pass-through columns.
      val projectedRowIter = inputIter.flatMap { batch =>
        val passThruCols = childOutput.indices.map(i => batch.column(i)).toArray
        passThruQueue.add((passThruCols, batch.numRows()))
        batch.rowIterator().asScala.map(projection)
      }

      val batchIter = Iterator(projectedRowIter)
      val pyRunner = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs, evalType, argMetas, udfInputSchema, sessionLocalTimeZone,
        largeVarTypes, pythonRunnerConf, pythonMetrics, jobArtifactUUID,
        sessionUUID) with BatchedPythonArrowInput

      pyRunner.compute(batchIter, context.partitionId(), context)
    }

    /**
     * Combine pass-through columns with UDF result columns into output ColumnarBatches.
     *
     * For scalar Arrow UDFs, Python preserves batch boundaries: one input batch
     * produces one output batch with the same number of rows. This method pops
     * pass-through columns from the queue and combines them with UDF result columns.
     */
    private def combineResults(
        resultIter: Iterator[ColumnarBatch],
        outputTypes: Seq[DataType],
        passThruQueue: ArrayDeque[(Array[ColumnVector], Int)]): Iterator[ColumnarBatch] = {

      resultIter.map { resultBatch =>
        val actualDataTypes = (0 until resultBatch.numCols()).map(
          i => resultBatch.column(i).dataType())
        if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
          throw QueryExecutionErrors.arrowDataTypeMismatchError(
            "pandas_udf()", outputTypes, actualDataTypes)
        }

        val numResultRows = resultBatch.numRows()
        val resultCols = (0 until resultBatch.numCols()).map(
          i => resultBatch.column(i)).toArray

        // Pop the corresponding pass-through columns.
        val (passThruCols, passThruRows) = passThruQueue.poll()
        assert(passThruRows == numResultRows,
          s"Batch size mismatch: pass-through has $passThruRows rows " +
          s"but UDF result has $numResultRows rows. " +
          s"This can happen when Arrow output batch slicing is enabled.")

        // Combine: [pass-through columns | UDF result columns]
        val allCols = passThruCols ++ resultCols
        new ColumnarBatch(allCols, numResultRows)
      }
    }
  }
}
