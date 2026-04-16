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
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluator factory for Arrow Python UDFs: ColumnarBatch in, ColumnarBatch out.
 *
 * Three execution paths based on input characteristics:
 *
 * 1. '''Arrow columnar path''' (UDF inputs are simple column refs AND
 *    columns are [[ArrowColumnVector]]): Arrow FieldVectors are extracted
 *    directly and serialized to IPC. Pass-through columns are kept as
 *    [[ColumnVector]] references (safe because Arrow vectors are
 *    independently allocated per batch). Output is produced by columnar
 *    combining: passThruCols ++ resultCols -> ColumnarBatch.
 *
 * 2. '''Non-Arrow columnar path''' (UDF inputs are simple column refs
 *    BUT columns are NOT [[ArrowColumnVector]]): Non-Arrow columnar
 *    readers (Parquet vectorized, InMemoryTableScan) reuse ColumnVector
 *    objects across batches. Uses [[HybridRowQueue]] for pass-through
 *    buffering and converts joined rows back to [[ColumnarBatch]].
 *
 * 3. '''Row fallback path''' (UDF inputs contain complex expressions):
 *    Rows are extracted via [[MutableProjection]] and sent using
 *    [[BasicPythonArrowInput]]. Uses [[HybridRowQueue]] for pass-through
 *    and converts joined rows back to [[ColumnarBatch]].
 *
 * TODO: Add a physical plan rule that inserts a ProjectExec before
 *   ArrowEvalPythonExec to pre-evaluate complex UDF input expressions
 *   into simple column references. This would eliminate path 3.
 */
private[python] class ColumnarArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    output: Seq[Attribute],
    outputSchema: StructType,
    batchSize: Int,
    evalType: Int,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
  extends PartitionEvaluatorFactory[ColumnarBatch, ColumnarBatch] {

  override def createEvaluator()
      : PartitionEvaluator[ColumnarBatch, ColumnarBatch] =
    new ColumnarArrowEvalPythonPartitionEvaluator

  private class ColumnarArrowEvalPythonPartitionEvaluator
      extends PartitionEvaluator[ColumnarBatch, ColumnarBatch] {

    private def collectFunctions(
        udf: PythonUDF
    ): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
      udf.children match {
        case Seq(u: PythonUDF) =>
          val ((chained, _), children) = collectFunctions(u)
          ((ChainedPythonFunctions(
            chained.funcs ++ Seq(udf.func)),
            udf.resultId.id), children)
        case children =>
          assert(children.forall(
            !_.exists(_.isInstanceOf[PythonUDF])))
          ((ChainedPythonFunctions(Seq(udf.func)),
            udf.resultId.id), udf.children)
      }
    }

    override def eval(
        partitionIndex: Int,
        iters: Iterator[ColumnarBatch]*
    ): Iterator[ColumnarBatch] = {
      val inputIter = iters.head
      val context = TaskContext.get()

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argMetas = inputs.map { input =>
        input.map { e =>
          val (key, value) = e match {
            case NamedArgumentExpression(key, value) =>
              (Some(key), value)
            case _ => (None, e)
          }
          if (allInputs.exists(_.semanticEquals(value))) {
            ArgumentMetadata(
              allInputs.indexWhere(_.semanticEquals(value)), key)
          } else {
            allInputs += value
            dataTypes += value.dataType
            ArgumentMetadata(allInputs.length - 1, key)
          }
        }.toArray
      }.toArray

      val udfInputSchema = StructType(
        dataTypes.zipWithIndex.map { case (dt, i) =>
          StructField(s"_$i", dt)
        }.toArray)

      val outputTypes = output.drop(childOutput.length).map(
        _.dataType.transformRecursively {
          case udt: UserDefinedType[_] => udt.sqlType
        })

      val inputColumnIndices = resolveColumnIndices(allInputs.toSeq)

      // Peek at first batch to check if Arrow-backed.
      val peekIter = new PeekableIterator(inputIter)
      val isArrow = peekIter.peek().exists { batch =>
        batch.numCols() > 0 &&
          batch.column(0).isInstanceOf[ArrowColumnVector]
      }

      if (inputColumnIndices.isDefined && isArrow) {
        // Path 1: Arrow columnar -- full optimization.
        evalArrowColumnar(peekIter, context, pyFuncs, argMetas,
          udfInputSchema, outputTypes, inputColumnIndices.get)
      } else {
        // Path 2 & 3: non-Arrow or complex expressions.
        evalWithRowQueue(peekIter, context, pyFuncs, argMetas,
          allInputs.toSeq, udfInputSchema, outputTypes,
          inputColumnIndices)
      }
    }

    private def resolveColumnIndices(
        allInputs: Seq[Expression]): Option[Array[Int]] = {
      val indices = allInputs.map {
        case attr: AttributeReference =>
          val idx = childOutput.indexWhere(_.exprId == attr.exprId)
          if (idx >= 0) idx else return None
        case _ => return None
      }
      Some(indices.toArray)
    }

    /**
     * Path 1: Arrow columnar. ColumnVector references are safe
     * because Arrow vectors are independently allocated per batch.
     * Combines pass-through + UDF result columns into ColumnarBatch.
     */
    private def evalArrowColumnar(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        udfInputSchema: StructType,
        outputTypes: Seq[DataType],
        columnIndices: Array[Int]): Iterator[ColumnarBatch] = {

      val passThruQueue =
        new ArrayDeque[(Array[ColumnVector], Int)]()

      val bufferedIter = inputIter.map { batch =>
        val passThruCols = childOutput.indices.map(
          i => batch.column(i)).toArray
        passThruQueue.add((passThruCols, batch.numRows()))
        batch
      }

      val pyRunner = new ColumnarArrowPythonWithNamedArgumentRunner(
        pyFuncs, evalType, argMetas, udfInputSchema,
        sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
        pythonMetrics, jobArtifactUUID, sessionUUID,
        columnIndices)

      val resultIter = pyRunner.compute(
        bufferedIter, context.partitionId(), context)

      resultIter.map { resultBatch =>
        validateOutputTypes(resultBatch, outputTypes)
        val numRows = resultBatch.numRows()
        val resultCols = (0 until resultBatch.numCols()).map(
          i => resultBatch.column(i)).toArray
        val (passThruCols, passThruRows) = passThruQueue.poll()
        assert(passThruRows == numRows,
          s"Batch size mismatch: pass-through has " +
            s"$passThruRows rows but UDF result has $numRows rows.")
        new ColumnarBatch(passThruCols ++ resultCols, numRows)
      }
    }

    /**
     * Paths 2 & 3: non-Arrow or complex expressions.
     * Uses HybridRowQueue for pass-through, joins rows, then
     * converts back to ColumnarBatch via RowToColumnConverter.
     */
    private def evalWithRowQueue(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        allInputs: Seq[Expression],
        udfInputSchema: StructType,
        outputTypes: Seq[DataType],
        inputColumnIndices: Option[Array[Int]]
    ): Iterator[ColumnarBatch] = {

      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        childOutput.length)
      context.addTaskCompletionListener[Unit] { _ => queue.close() }

      val unsafeProj = UnsafeProjection.create(
        childOutput, childOutput)

      val resultIter = if (inputColumnIndices.isDefined) {
        // Path 2: simple column refs, non-Arrow vectors.
        val bufferedIter = inputIter.map { batch =>
          batch.rowIterator().asScala.foreach { row =>
            queue.add(unsafeProj(row).copy())
          }
          batch
        }
        val pyRunner =
          new ColumnarArrowPythonWithNamedArgumentRunner(
            pyFuncs, evalType, argMetas, udfInputSchema,
            sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
            pythonMetrics, jobArtifactUUID, sessionUUID,
            inputColumnIndices.get)
        pyRunner.compute(
          bufferedIter, context.partitionId(), context)
      } else {
        // Path 3: complex expressions.
        val projection = MutableProjection.create(
          allInputs, childOutput)
        projection.initialize(context.partitionId())
        val batchIter = inputIter.map { batch =>
          batch.rowIterator().asScala.map { row =>
            queue.add(unsafeProj(row).copy())
            projection(row)
          }
        }
        val pyRunner = new ArrowPythonWithNamedArgumentRunner(
          pyFuncs, evalType, argMetas, udfInputSchema,
          sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
          pythonMetrics, jobArtifactUUID, sessionUUID
        ) with BasicPythonArrowInput
        pyRunner.compute(
          batchIter, context.partitionId(), context)
      }

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      val rowIter = resultIter.flatMap { batch =>
        validateOutputTypes(batch, outputTypes)
        batch.rowIterator.asScala
      }.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }

      // Convert joined rows back to ColumnarBatch.
      rowsToColumnarBatches(rowIter, context)
    }

    private def validateOutputTypes(
        batch: ColumnarBatch,
        outputTypes: Seq[DataType]): Unit = {
      val actual = (0 until batch.numCols()).map(
        i => batch.column(i).dataType())
      if (!equalsIgnoreCompatibleCollation(outputTypes, actual)) {
        throw QueryExecutionErrors.arrowDataTypeMismatchError(
          "pandas_udf()", outputTypes, actual)
      }
    }

    /** Convert Iterator[InternalRow] to Iterator[ColumnarBatch]. */
    private def rowsToColumnarBatches(
        rowIter: Iterator[InternalRow],
        context: TaskContext): Iterator[ColumnarBatch] = {
      val converters = new RowToColumnConverter(outputSchema)
      val vectors = OnHeapColumnVector
        .allocateColumns(batchSize, outputSchema).toSeq
      val cb = new ColumnarBatch(vectors.toArray)
      context.addTaskCompletionListener[Unit] { _ => cb.close() }

      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = rowIter.hasNext
        override def next(): ColumnarBatch = {
          cb.setNumRows(0)
          vectors.foreach(_.reset())
          var count = 0
          while (count < batchSize && rowIter.hasNext) {
            converters.convert(rowIter.next(), vectors.toArray)
            count += 1
          }
          cb.setNumRows(count)
          cb
        }
      }
    }
  }
}

/** Iterator wrapper that allows peeking at the first element. */
private[python] class PeekableIterator[T](underlying: Iterator[T])
    extends Iterator[T] {
  private var peeked: Option[T] = None
  private var peekedDone = false

  def peek(): Option[T] = {
    if (!peekedDone) {
      peeked = if (underlying.hasNext) Some(underlying.next())
               else None
      peekedDone = true
    }
    peeked
  }

  override def hasNext: Boolean =
    peeked.isDefined || underlying.hasNext

  override def next(): T = {
    if (peeked.isDefined) {
      val v = peeked.get
      peeked = None
      v
    } else {
      underlying.next()
    }
  }
}
