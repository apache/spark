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
 * Evaluator factory for Arrow Python UDFs that processes [[ColumnarBatch]] input.
 *
 * When UDF inputs are simple column references and the batch is Arrow-backed,
 * the UDF input columns are extracted as Arrow FieldVectors and sent directly
 * via IPC, bypassing the ArrowWriter row-by-row conversion.
 *
 * Pass-through columns (child columns that are not UDF inputs) are buffered
 * as rows in [[HybridRowQueue]] and joined with UDF results row by row, since
 * the Python worker does not preserve input batch boundaries.
 *
 * When UDF inputs contain complex expressions (not simple column references),
 * the UDF inputs are serialized via the row-based ArrowWriter fallback.
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

      // Buffer input rows for joining with UDF output (same as EvalPythonEvaluatorFactory).
      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        childOutput.length)
      context.addTaskCompletionListener[Unit] { _ => queue.close() }

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

      val unsafeProj = UnsafeProjection.create(childOutput, childOutput)

      val resultIter = if (inputColumnIndices.isDefined) {
        // Columnar path: send Arrow FieldVectors directly to Python.
        // Buffer rows in queue for result joining.
        val bufferedIter = inputIter.map { batch =>
          batch.rowIterator().asScala.foreach { row =>
            queue.add(unsafeProj(row).copy())
          }
          batch
        }

        val pyRunner = new ColumnarArrowPythonWithNamedArgumentRunner(
          pyFuncs, evalType, argMetas, udfInputSchema,
          sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
          pythonMetrics, jobArtifactUUID, sessionUUID,
          inputColumnIndices.get)

        pyRunner.compute(bufferedIter, context.partitionId(), context)
      } else {
        // Fallback: complex expressions, convert to rows for UDF input.
        val projection = MutableProjection.create(allInputs.toSeq, childOutput)
        projection.initialize(context.partitionId())

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

        pyRunner.compute(batchIter, context.partitionId(), context)
      }

      // Join UDF results with buffered input rows.
      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      resultIter.flatMap { batch =>
        val actualDataTypes = (0 until batch.numCols()).map(
          i => batch.column(i).dataType())
        if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
          throw QueryExecutionErrors.arrowDataTypeMismatchError(
            "pandas_udf()", outputTypes, actualDataTypes)
        }
        batch.rowIterator.asScala
      }.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }

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
  }
}
