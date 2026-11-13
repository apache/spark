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
import scala.util.{Left, Right}

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{CompletionIterator, Utils}

/**
 * Evaluator factory for Arrow Python UDFs: ColumnarBatch in, ColumnarBatch out, or rows out
 * through [[rowOutput]] for a row-based parent.
 *
 * Two execution paths based on the UDF inputs:
 *
 * 1. '''Row queue columnar path''' (UDF inputs are simple column refs):
 *    The UDF inputs of Arrow-backed batches are serialized to IPC directly,
 *    and those of other batches row by row.
 *
 * 2. '''Row fallback path''' (UDF inputs contain complex expressions):
 *    Rows are extracted via [[MutableProjection]] and sent using
 *    [[BasicPythonArrowInput]].
 *
 * The results of a batch come back from Python after the writer has asked
 * for the next batches, and a producer may overwrite or free the vectors of
 * a batch once the next one is requested, e.g. Parquet vectorized readers
 * reuse `WritableColumnVector`s across batches, the Arrow cache releases
 * the vectors of a batch when the next one is read, and the Python output
 * of a stacked [[ArrowEvalPythonExec]] is loaded into the same vectors for
 * every batch. So both paths release a batch with closeIfFreeable once it
 * has been written to Python. If no column of the child is passed through,
 * the result batches from Python are the output batches. Otherwise the
 * pass-through columns are copied into a [[HybridRowQueue]], joined with the
 * results by row, and the joined rows are converted back to
 * [[ColumnarBatch]]. Input batches without rows are released and skipped, so
 * that the UDFs are not called with empty input, as on the row path.
 *
 * With [[rowOutput]], the input is handled in the same way, but the joined
 * rows are returned as they are, so that a row-based parent gets them without
 * the round trip through columnar batches. Without pass-through columns, it
 * returns the rows of the result batches, each of which is released with
 * closeIfFreeable once its rows have been returned, as
 * [[org.apache.spark.sql.execution.ColumnarToRowExec]] does.
 *
 * TODO: Add a physical plan rule that inserts a ProjectExec before
 *   ArrowEvalPythonExec to pre-evaluate complex UDF input expressions
 *   into simple column references. This would eliminate the row fallback
 *   path.
 */
private[python] class ColumnarArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    passThroughOutput: Seq[Attribute],
    resultOutput: Seq[Attribute],
    batchSize: Int,
    maxBytesPerBatch: Long,
    evalType: Int,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
  extends PartitionEvaluatorFactory[ColumnarBatch, ColumnarBatch] {

  // The output is the pass-through columns of the child followed by the results of the UDFs.
  private val output = passThroughOutput ++ resultOutput
  private val outputSchema = output.toStructType

  override def createEvaluator(): PartitionEvaluator[ColumnarBatch, ColumnarBatch] =
    new ColumnarArrowEvalPythonPartitionEvaluator

  /** A factory of the same evaluation, which returns the output as unsafe rows. */
  def rowOutput: PartitionEvaluatorFactory[ColumnarBatch, InternalRow] =
    new RowOutputEvaluatorFactory

  private class RowOutputEvaluatorFactory
    extends PartitionEvaluatorFactory[ColumnarBatch, InternalRow] {
    override def createEvaluator(): PartitionEvaluator[ColumnarBatch, InternalRow] =
      new RowOutputPartitionEvaluator
  }

  private class ColumnarArrowEvalPythonPartitionEvaluator
    extends PartitionEvaluator[ColumnarBatch, ColumnarBatch] {
    override def eval(
        partitionIndex: Int,
        iters: Iterator[ColumnarBatch]*): Iterator[ColumnarBatch] = {
      new ColumnarArrowEvalPython().evaluate(iters.head) match {
        case Left(resultBatches) => resultBatches
        case Right(joinedRows) => rowsToColumnarBatches(joinedRows, TaskContext.get())
      }
    }
  }

  private class RowOutputPartitionEvaluator
    extends PartitionEvaluator[ColumnarBatch, InternalRow] {
    override def eval(
        partitionIndex: Int,
        iters: Iterator[ColumnarBatch]*): Iterator[InternalRow] = {
      new ColumnarArrowEvalPython().evaluate(iters.head) match {
        case Left(resultBatches) =>
          val toUnsafe = UnsafeProjection.create(resultOutput, resultOutput)
          rowsOf(resultBatches).map(toUnsafe)
        case Right(joinedRows) => joinedRows
      }
    }
  }

  /**
   * Returns the rows of the batches, releasing each batch with closeIfFreeable once all its
   * rows have been returned, before the next batch is requested, as ColumnarToRowExec does.
   */
  private def rowsOf(batches: Iterator[ColumnarBatch]): Iterator[InternalRow] =
    batches.flatMap { batch =>
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        batch.rowIterator().asScala, batch.closeIfFreeable())
    }

  /**
   * Converts rows back to ColumnarBatches, which stop at `batchSize` rows, unless it is not
   * positive, or once their rows reach `maxBytesPerBatch`. Each has at least one row.
   */
  private def rowsToColumnarBatches(
      rowIter: Iterator[UnsafeRow],
      context: TaskContext): Iterator[ColumnarBatch] = {
    val converters = new RowToColumnConverter(outputSchema)
    // The vectors grow as rows are appended, so that a large or no limit is not allocated.
    val initialCapacity = if (batchSize > 0) math.min(batchSize, 4096) else 4096
    val vectors = OnHeapColumnVector
      .allocateColumns(initialCapacity, outputSchema).toSeq
    val cb = new ColumnarBatch(vectors.toArray)
    context.addTaskCompletionListener[Unit] { _ => cb.close() }

    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = rowIter.hasNext
      override def next(): ColumnarBatch = {
        cb.setNumRows(0)
        vectors.foreach(_.reset())
        var count = 0
        var bytes = 0L
        while (rowIter.hasNext && (count == 0 ||
            ((batchSize <= 0 || count < batchSize) && bytes < maxBytesPerBatch))) {
          val row = rowIter.next()
          converters.convert(row, vectors.toArray)
          bytes += row.getSizeInBytes
          count += 1
        }
        cb.setNumRows(count)
        cb
      }
    }
  }

  private class ColumnarArrowEvalPython {

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

    /**
     * Evaluates the UDFs over the input batches. Without pass-through columns, returns the
     * result batches from Python, otherwise the joined rows as unsafe rows, each valid until
     * the next one is requested.
     */
    def evaluate(
        inputIter: Iterator[ColumnarBatch]
    ): Either[Iterator[ColumnarBatch], Iterator[UnsafeRow]] = {
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

      val outputTypes = resultOutput.map(
        _.dataType.transformRecursively {
          case udt: UserDefinedType[_] => udt.sqlType
        })

      val nonEmptyIter = inputIter.filter { batch =>
        if (batch.numRows() == 0) batch.closeIfFreeable()
        batch.numRows() > 0
      }

      val inputColumnIndices = resolveColumnIndices(allInputs.toSeq)
      if (passThroughOutput.isEmpty) {
        // The vectors of a result batch are valid until the next one is requested, which a
        // consumer respecting the contract of ColumnVector does not rely on beyond that.
        // The result batches are as large as the ones Python returns, unless
        // maxRecordsPerOutputBatch or maxBytesPerOutputBatch slices them, and are not cut by
        // maxRecordsPerBatch or maxBytesPerBatch as the joined rows are when they are converted
        // back for a columnar parent. So a parent that takes columnar input, i.e. a stacked
        // evaluation, gets them at that size, as it gets the batches of any other columnar
        // child. A stacked evaluation does not split its input batches yet either.
        Left(evalPython(nonEmptyIter, context, pyFuncs, argMetas, allInputs.toSeq,
          udfInputSchema, inputColumnIndices, onInputRow = None).map { batch =>
          validateOutputTypes(batch, outputTypes)
          batch
        })
      } else {
        Right(evalWithRowQueue(nonEmptyIter, context, pyFuncs, argMetas,
          allInputs.toSeq, udfInputSchema, outputTypes, inputColumnIndices))
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
     * Sends the UDF inputs of the batches to Python and returns the result batches, calling
     * `onInputRow` with each input row before its batch is released.
     */
    private def evalPython(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        allInputs: Seq[Expression],
        udfInputSchema: StructType,
        inputColumnIndices: Option[Array[Int]],
        onInputRow: Option[InternalRow => Unit]
    ): Iterator[ColumnarBatch] = {
      if (inputColumnIndices.isDefined) {
        // Simple column refs: the writer sends Arrow-backed batches directly.
        val bufferedIter = new WrittenBatchReleasingIterator(inputIter, { batch =>
          onInputRow.foreach(f => batch.rowIterator().asScala.foreach(f))
          true
        })
        val pyRunner =
          new ColumnarArrowPythonWithNamedArgumentRunner(
            pyFuncs, evalType, argMetas, udfInputSchema,
            sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
            pythonMetrics, jobArtifactUUID, sessionUUID,
            inputColumnIndices.get, udfs.map(_.elementwiseNestingDepth))
        pyRunner.compute(
          bufferedIter, context.partitionId(), context)
      } else {
        // Complex expressions.
        val projection = MutableProjection.create(
          allInputs, childOutput)
        projection.initialize(context.partitionId())
        // The rows of a batch are written to Python before the writer asks
        // for the next batch, which releases it.
        val batchIter = new WrittenBatchReleasingIterator(inputIter, _ => true).map { batch =>
          batch.rowIterator().asScala.map { row =>
            onInputRow.foreach(_(row))
            projection(row)
          }
        }
        val pyRunner = new ArrowPythonWithNamedArgumentRunner(
          pyFuncs, evalType, argMetas, udfInputSchema,
          sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
          pythonMetrics, jobArtifactUUID, sessionUUID,
          udfs.map(_.elementwiseNestingDepth)
        ) with BasicPythonArrowInput
        pyRunner.compute(
          batchIter, context.partitionId(), context)
      }
    }

    /**
     * Uses HybridRowQueue for pass-through and joins the pass-through columns of each input
     * row with its results.
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
    ): Iterator[UnsafeRow] = {

      // In pipelined mode the queue's add() runs in the writer thread and remove() runs in
      // the task thread; use lock-free mode to skip per-row synchronization.
      val pipelined = SparkEnv.get.conf.get(PYTHON_UDF_PIPELINED_EXECUTION)
      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        passThroughOutput.length,
        lockFree = pipelined)
      context.addTaskCompletionListener[Unit] { _ => queue.close() }

      // Copies the pass-through columns of an input row before its batch is released.
      val unsafeProj = UnsafeProjection.create(passThroughOutput, childOutput)

      val resultIter = evalPython(inputIter, context, pyFuncs, argMetas, allInputs,
        udfInputSchema, inputColumnIndices,
        onInputRow = Some(row => queue.add(unsafeProj(row).copy())))

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      rowsOf(resultIter.map { batch =>
        validateOutputTypes(batch, outputTypes)
        batch
      }).map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
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
  }
}

/**
 * Hands the input batches to the Python writer. `enqueue` is called on each
 * batch before the writer gets it, and returns whether the batch should be
 * released with closeIfFreeable once it has been written: the writer is done
 * with a batch when it asks for the next one or reaches the end.
 */
private[python] class WrittenBatchReleasingIterator(
    input: Iterator[ColumnarBatch],
    enqueue: ColumnarBatch => Boolean)
  extends Iterator[ColumnarBatch] {

  private var written: ColumnarBatch = _

  private def releaseWritten(): Unit = {
    if (written != null) {
      written.closeIfFreeable()
      written = null
    }
  }

  override def hasNext: Boolean = {
    releaseWritten()
    input.hasNext
  }

  override def next(): ColumnarBatch = {
    releaseWritten()
    val batch = input.next()
    if (enqueue(batch)) {
      written = batch
    }
    batch
  }
}
