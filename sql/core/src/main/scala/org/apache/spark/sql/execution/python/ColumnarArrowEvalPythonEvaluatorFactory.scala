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
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, ValueVector}
import org.apache.arrow.vector.complex.{LargeListViewVector, ListViewVector}
import org.apache.arrow.vector.util.VectorAppender

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, SparkException, TaskContext}
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
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluator factory for Arrow Python UDFs: ColumnarBatch in, ColumnarBatch out.
 *
 * Three execution paths based on input characteristics:
 *
 * 1. '''Arrow columnar path''' (UDF inputs are simple column refs AND
 *    columns are [[ArrowColumnVector]]): Arrow FieldVectors are extracted
 *    directly and serialized to IPC. Pass-through columns are copied into
 *    vectors of the task: like other columnar readers, an Arrow-backed
 *    reader may reuse or release the vectors of a batch once it is asked
 *    for the next one, while a pass-through column is needed until the
 *    result of its batch comes back from Python. Output is produced by
 *    columnar combining: passThruCols ++ resultCols -> ColumnarBatch.
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

      // Peek at first batch to check if Arrow-backed, with vectors that the Arrow columnar path
      // can copy.
      val peekIter = new PeekableIterator(inputIter)
      val isArrow = peekIter.peek().exists { batch =>
        batch.numCols() > 0 && (0 until batch.numCols()).forall { i =>
          batch.column(i) match {
            case arrow: ArrowColumnVector => isCopyable(arrow.getValueVector)
            case _ => false
          }
        }
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
     * Path 1: Arrow columnar. Copies the pass-through columns of each
     * batch, and combines them with the UDF result columns into
     * ColumnarBatch.
     */
    private def evalArrowColumnar(
        inputIter: Iterator[ColumnarBatch],
        context: TaskContext,
        pyFuncs: Seq[(ChainedPythonFunctions, Long)],
        argMetas: Array[Array[ArgumentMetadata]],
        udfInputSchema: StructType,
        outputTypes: Seq[DataType],
        columnIndices: Array[Int]): Iterator[ColumnarBatch] = {

      // The pass-through columns of a batch are needed until its result comes back from Python,
      // which can be many batches later, but the producer may reuse or release the vectors of a
      // batch once it is asked for the next one, as ColumnarBatch allows. So they are copied, and
      // released once the output batch that holds them is no longer used: when the next output
      // batch is produced, or at the end of the task. In pipelined mode, add() runs in the writer
      // thread and poll() in the task thread. The copies are not counted by the task memory
      // manager, and cannot spill: their memory grows with how far the input runs ahead of the
      // results, as the memory of a producer that allocates new vectors for every batch would.
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"pass-through columns of Python UDFs in task ${context.taskAttemptId()}",
        0, Long.MaxValue)
      val passThruQueue = new ConcurrentLinkedQueue[(Array[ColumnVector], Int)]()
      var outputPassThruCols = Array.empty[ColumnVector]
      // This listener must be registered before `compute`, so that it runs after the listener of
      // the runner, which stops the writer thread in pipelined mode. Until the runner waits for
      // the writer to exit (SPARK-60113), the task can still complete while the writer copies a
      // batch: a copy made once the task completed is released right away, and the allocator is
      // closed by whichever of the two ends last.
      val lock = new Object
      var completed = false
      var copying = false
      context.addTaskCompletionListener[Unit] { _ =>
        val closeAllocator = lock.synchronized {
          completed = true
          outputPassThruCols.foreach(_.close())
          passThruQueue.forEach(_._1.foreach(_.close()))
          passThruQueue.clear()
          !copying
        }
        if (closeAllocator) {
          allocator.close()
        }
      }

      val bufferedIter = inputIter.map { batch =>
        val copy = lock.synchronized {
          copying = !completed
          copying
        }
        if (copy) {
          var copies: Array[ColumnVector] = null
          try {
            copies = copyColumns(batch, allocator)
          } finally {
            val closeAllocator = lock.synchronized {
              copying = false
              if (completed) {
                Option(copies).foreach(_.foreach(_.close()))
              } else if (copies != null) {
                passThruQueue.add((copies, batch.numRows()))
              }
              completed
            }
            if (closeAllocator) {
              allocator.close()
            }
          }
        }
        batch
      }

      val pyRunner = new ColumnarArrowPythonWithNamedArgumentRunner(
        pyFuncs, evalType, argMetas, udfInputSchema,
        sessionLocalTimeZone, largeVarTypes, pythonRunnerConf,
        pythonMetrics, jobArtifactUUID, sessionUUID,
        columnIndices, udfs.map(_.elementwiseNestingDepth))

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
        // The previous output batch is no longer used once the next one is asked for. A columnar
        // parent, such as the Arrow cache, may already have closed it with `closeIfFreeable`:
        // closing the columns again has no effect.
        outputPassThruCols.foreach(_.close())
        outputPassThruCols = passThruCols
        new ColumnarBatch(passThruCols ++ resultCols, numRows)
      }
    }

    /** Copies the pass-through columns of a batch into vectors allocated from `allocator`. */
    private def copyColumns(
        batch: ColumnarBatch,
        allocator: BufferAllocator): Array[ColumnVector] = {
      val copies = ArrayBuffer.empty[ColumnVector]
      try {
        childOutput.indices.foreach { i =>
          copies += copyColumn(batch.column(i), childOutput(i).name, batch.numRows(), allocator)
        }
        copies.toArray
      } catch {
        case e: Throwable =>
          copies.foreach(_.close())
          throw e
      }
    }

    private def copyColumn(
        column: ColumnVector,
        name: String,
        numRows: Int,
        allocator: BufferAllocator): ColumnVector = {
      column match {
        case arrow: ArrowColumnVector =>
          val source = arrow.getValueVector
          // The copy has the values of the vector, which must cover the rows of the batch.
          if (source.getValueCount < numRows) {
            throw SparkException.internalError(s"The Arrow vector of the column $name has " +
              s"${source.getValueCount} values, but its batch has $numRows rows.")
          }
          val copy = source.getField.createVector(allocator)
          try {
            copy.setInitialCapacity(source.getValueCount)
            copy.allocateNew()
            // Appends the values of the source to the empty copy, copying the buffers.
            source.accept(new VectorAppender(copy), null)
            new ArrowColumnVector(copy)
          } catch {
            case e: Throwable =>
              copy.close()
              throw e
          }
        case other =>
          throw SparkException.internalError(
            s"Expected an Arrow-backed pass-through column, but got ${other.getClass.getName}.")
      }
    }

    /** Whether `VectorAppender` can copy the vector: it cannot copy list views. */
    private def isCopyable(vector: ValueVector): Boolean = vector match {
      case _: ListViewVector | _: LargeListViewVector => false
      case v: FieldVector => v.getChildrenFromFields.asScala.forall(isCopyable)
      case _ => true
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

      // In pipelined mode the queue's add() runs in the writer thread and remove() runs in
      // the task thread; use lock-free mode to skip per-row synchronization.
      val pipelined = SparkEnv.get.conf.get(PYTHON_UDF_PIPELINED_EXECUTION)
      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        childOutput.length,
        lockFree = pipelined)
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
            inputColumnIndices.get, udfs.map(_.elementwiseNestingDepth))
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
          pythonMetrics, jobArtifactUUID, sessionUUID,
          udfs.map(_.elementwiseNestingDepth)
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
