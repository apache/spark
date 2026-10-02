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
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, PythonUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluates scalar Python UDFs using Arrow CDI in the executor process. Only UDF arguments
 * are converted to Arrow. Original rows are buffered in a spillable queue and joined with
 * the results, unless all of them are UDF arguments that read back from Arrow unchanged.
 * Each batch owns its Arrow buffers so Python can safely retain input arrays.
 *
 * The evaluator owns its queue, so that cleanup at task completion is coordinated with a
 * consumer on another thread, such as a pipelined Python writer or a TRANSFORM feed thread.
 */
class InProcessArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    output: Seq[Attribute],
    batchSize: Int,
    maxBytes: Long,
    timeZoneId: String,
    largeVarTypes: Boolean,
    hideTraceback: Boolean,
    simplifiedTraceback: Boolean,
    tracebackWithLocals: Boolean,
    fullValidation: Boolean,
    metrics: Map[String, SQLMetric])
  extends EvalPythonEvaluatorFactory(childOutput, udfs, output) {

  private[python] def runtimeSession: InProcessPythonRuntime.InterpreterSession =
    InProcessPythonRuntime.currentSession

  /** Evaluates projected arguments and returns only the results. */
  override protected def evaluate(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputSchema: StructType,
      context: TaskContext): Iterator[InternalRow] =
    evaluateBatches(funcs, argMetas, rows, inputSchema, context, joinInput = None)

  override protected def evaluateJoined(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputs: Seq[Expression],
      inputSchema: StructType,
      context: TaskContext): Option[Iterator[InternalRow]] = {
    // If all input columns are UDF arguments, they are written to Arrow regardless. Read them
    // back from the exported input vectors instead of buffering every input row, if their
    // values read back from Arrow exactly as written.
    val readBack = inputs.length == childOutput.length && inputs.zip(childOutput).forall {
      case (a: Attribute, c) => a.exprId == c.exprId
      case _ => false
    } && inputSchema.forall(f => InProcessArrowEvalPythonEvaluatorFactory.readsBack(f.dataType))
    val joinInput = if (readBack) {
      InProcessArrowEvalPythonEvaluatorFactory.ReadBack
    } else {
      // Each projected row is written to Arrow before the next input row is pulled, so the
      // arguments go into a reused buffer rather than being copied value by value.
      val projection = UnsafeProjection.create(inputs, childOutput)
      projection.initialize(context.partitionId())
      InProcessArrowEvalPythonEvaluatorFactory.Buffered(projection)
    }
    Some(evaluateBatches(funcs, argMetas, rows, inputSchema, context, Some(joinInput)))
  }

  private def evaluateBatches(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputSchema: StructType,
      context: TaskContext,
      joinInput: Option[InProcessArrowEvalPythonEvaluatorFactory.JoinInput])
    : Iterator[InternalRow] = {
    import InProcessArrowEvalPythonEvaluatorFactory.{Buffered, ReadBack}
    ArrowUtils.failDuplicatedFieldNames(inputSchema)
    val functions = funcs.map { case (chain, _) =>
      if (chain.funcs.size != 1) {
        throw SparkException.internalError(
          "In-process UDF chains must use separate evaluation nodes")
      }
      chain.funcs.head
    }
    val inputOrdinals = argMetas.map(_.map(_.offset))
    def checkCancellation(): Unit = context.killTaskIfInterrupted()

    val expectedFields = udfs.map { udf =>
      ArrowUtils.toArrowField("result", udf.dataType, true, timeZoneId, largeVarTypes)
    }
    val processingTime = new InProcessArrowEvalPythonEvaluatorFactory.NanosecondTimer(
      metrics("pythonProcessingTime"))
    val initTime = new InProcessArrowEvalPythonEvaluatorFactory.NanosecondTimer(
      metrics("pythonInitTime"))
    val arrowSchema = ArrowUtils.toArrowSchema(inputSchema, timeZoneId, largeVarTypes)
    // Capture before consuming input: an old task must never join a later context's session.
    val runtime = runtimeSession
    // Task completion listeners run on the thread that evaluates this partition. Only a
    // consumer on another thread, such as a pipelined Python writer or a TRANSFORM feed
    // thread, can race with cleanup; it needs IteratorResources and a materialized row.
    val evaluatingThread = Thread.currentThread()
    lazy val materializeResult = UnsafeProjection.create(
      ((if (joinInput.isDefined) childOutput.map(_.dataType) else Nil) ++ udfs.map(_.dataType))
        .toArray)
    val (queue, projection) = joinInput match {
      case Some(Buffered(projection)) =>
        val queue = HybridRowQueue(context.taskMemoryManager(),
          new File(Utils.getLocalDir(SparkEnv.get.conf)), childOutput.length)
        (queue, projection)
      case _ => (null, null)
    }
    val joined = new JoinedRow
    val handles = functions.map(_ => UUID.randomUUID().toString)
    var registered = false
    var writer: ArrowWriter = null
    val results = ArrayBuffer.empty[ArrowColumnVector]
    var startedAt = 0L

    def closeBatch(): Unit = {
      val resources = ArrayBuffer.empty[AutoCloseable]
      resources ++= results
      results.clear()
      if (writer != null) {
        resources += writer.root
        writer = null
      }
      AutoCloseables.close(resources.asJava)
    }

    val resources = new InProcessArrowEvalPythonEvaluatorFactory.IteratorResources(() => {
      if (startedAt != 0L) {
        metrics("pythonTotalTime") += (System.nanoTime() - startedAt) / 1000000
      }
      Utils.tryWithSafeFinally {
        closeBatch()
      } {
        Utils.tryWithSafeFinally {
          if (queue != null) queue.close()
        } {
          if (registered) runtime.release(handles)
        }
      }
    })

    context.addTaskCompletionListener[Unit](_ => resources.close())

    new Iterator[InternalRow] {
      private var batchIter: Iterator[InternalRow] = Iterator.empty

      // A consumer on another thread must not pull input once task completion has started,
      // since listeners that run after this evaluator's free upstream resources. The task
      // thread itself pulls directly, without allocating a closure per row.
      private def hasNextInput(guarded: Boolean): Boolean = {
        if (startedAt == 0L) startedAt = System.nanoTime()
        checkCancellation()
        val available = !resources.isClosed && (batchIter.hasNext ||
          (if (guarded) resources.pull(rows.hasNext) else rows.hasNext))
        if (!available) resources.close()
        available
      }

      override def hasNext: Boolean = {
        if (Thread.currentThread() eq evaluatingThread) {
          hasNextInput(guarded = false)
        } else {
          resources.use(false) { hasNextInput(guarded = true) }
        }
      }

      private def endOfInput: Nothing =
        throw new NoSuchElementException("End of in-process UDF input")

      override def next(): InternalRow = {
        if (Thread.currentThread() eq evaluatingThread) {
          nextRow(guarded = false)
        } else {
          // Do not return a row backed by vectors that task completion can close.
          resources.use[InternalRow](endOfInput) {
            materializeResult(nextRow(guarded = true))
          }
        }
      }

      /** Writes the next input row to the batch, returning false at the end of input. */
      private def pullRow(): Boolean = rows.hasNext && {
        val row = rows.next()
        if (queue != null) {
          queue.add(row.asInstanceOf[UnsafeRow])
          writer.write(projection(row))
        } else {
          writer.write(row)
        }
        true
      }

      private def nextRow(guarded: Boolean): InternalRow = {
        if (!hasNextInput(guarded)) endOfInput
        try {
          if (!batchIter.hasNext) {
            closeBatch()
            if (!registered) {
              // Mark before registering so failure after any registration still cleans up.
              registered = true
              functions.indices.foreach { i =>
                val func = functions(i)
                initTime.add(runtime.register(handles(i), func.command.toArray,
                  expectedFields(i), func.pythonVer, hideTraceback, simplifiedTraceback,
                  tracebackWithLocals, fullValidation))
              }
            }
            val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
            writer = try {
              ArrowWriter.create(root)
            } catch {
              case t: Throwable => Utils.tryWithSafeFinally { throw t } { root.close() }
            }
            var count = 0
            var pulled = true
            while (pulled && (batchSize <= 0 || count < batchSize) &&
                (count == 0 || maxBytes <= 0 || writer.sizeInBytes() < maxBytes)) {
              checkCancellation()
              pulled = if (guarded) resources.pull(pullRow()) else pullRow()
              if (pulled) count += 1
            }
            // Task completion stopped input; do not evaluate a partial batch.
            if (resources.isInputClosed) endOfInput
            writer.finish()
            metrics("pythonDataSent") += writer.sizeInBytes()

            handles.indices.foreach { udfIndex =>
              val handle = handles(udfIndex)
              val ordinals = inputOrdinals(udfIndex)
              checkCancellation()
              // Register each acquired resource immediately, including partially exported
              // inputs and results of earlier UDFs if a later UDF throws.
              val structs = ArrayBuffer.empty[AutoCloseable]
              def array(): ArrowArray = {
                val value = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
                structs += new AutoCloseable {
                  override def close(): Unit =
                    Utils.tryWithSafeFinally {
                      if (value.snapshot().release != 0L) value.release()
                    } { value.close() }
                }
                value
              }
              def schema(): ArrowSchema = {
                val value = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
                structs += new AutoCloseable {
                  override def close(): Unit =
                    Utils.tryWithSafeFinally {
                      if (value.snapshot().release != 0L) value.release()
                    } { value.close() }
                }
                value
              }
              Utils.tryWithSafeFinally {
                val inArrays = ordinals.map(_ => array())
                val inSchemas = ordinals.map(_ => schema())
                val outArray = array()
                val outSchema = schema()
                ordinals.indices.foreach { i =>
                  InProcessArrowBridge.exportColumn(
                    writer.root.getVector(ordinals(i)), inArrays(i), inSchemas(i))
                }
                processingTime.add(runtime.invoke(
                  handle,
                  inArrays.map(_.memoryAddress()).toArray,
                  inSchemas.map(_.memoryAddress()).toArray,
                  outArray.memoryAddress(), outSchema.memoryAddress(),
                  count, argMetas(udfIndex).map(_.name.getOrElse(""))))
                results += InProcessArrowBridge.cdiToColumn(
                  outArray, outSchema, Some(expectedFields(udfIndex)))
                metrics("pythonDataReceived") += results.last.getValueVector.getBufferSize
              } {
                AutoCloseables.close(structs.asJava)
              }
            }

            metrics("pythonNumRowsReceived") += count
            // Input vectors are closed with the writer's root, not with the results.
            val inputs = if (joinInput.contains(ReadBack)) {
              writer.root.getFieldVectors.asScala.map(new ArrowColumnVector(_))
            } else {
              Nil
            }
            val columns = (inputs ++ results).toArray[ColumnVector]
            batchIter = new ColumnarBatch(columns, count).rowIterator().asScala
          }
          val result = batchIter.next()
          if (queue != null) joined(queue.remove(), result) else result
        } catch {
          case t: Throwable => Utils.tryWithSafeFinally { throw t } { resources.close() }
        }
      }
    }
  }
}

private[python] object InProcessArrowEvalPythonEvaluatorFactory {
  /** How the evaluator joins input rows with their results. */
  sealed trait JoinInput
  /** Read the input columns back from the exported Arrow input vectors. */
  case object ReadBack extends JoinInput
  /** Buffer the input rows, writing their projected arguments to Arrow. */
  case class Buffered(projection: UnsafeProjection) extends JoinInput

  /**
   * Whether `ArrowColumnVector` returns exactly the values `ArrowWriter` wrote for this type.
   * Types with derived Arrow representations, such as intervals, nanosecond timestamps, TIME,
   * Variant, geospatial types and UDTs, keep the original rows instead.
   */
  def readsBack(dataType: DataType): Boolean = dataType match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
        FloatType | DoubleType | BinaryType | DateType | TimestampType | TimestampNTZType => true
    case _: DecimalType => true
    case _: StringType => true
    case ArrayType(elementType, _) => readsBack(elementType)
    case MapType(keyType, valueType, _) => readsBack(keyType) && readsBack(valueType)
    case StructType(fields) => fields.forall(f => readsBack(f.dataType))
    case _ => false
  }

  /**
   * A pipelined worker can consume input after task completion has requested cleanup.
   * Defer cleanup until that iterator call returns, without blocking the completion listener
   * on native Python work. Both normal and exceptional returns release deferred resources.
   */
  class IteratorResources(cleanup: () => Unit, inputWaitMillis: Long = 1000L)
    extends AutoCloseable {
    @volatile private var closed = false
    private var inUse = false
    @volatile private var inputClosed = false
    private val inputLock = new ReentrantLock()

    def isClosed: Boolean = closed

    def isInputClosed: Boolean = inputClosed

    /**
     * Pulls input for a consumer on another thread, returning false once closed. Listeners
     * that run after this one, such as the scan's, free upstream resources; `close` waits for
     * a pull in progress, but only briefly, since the pull may itself wait for such a listener.
     */
    def pull(body: => Boolean): Boolean = {
      inputLock.lock()
      try { !inputClosed && body } finally { inputLock.unlock() }
    }

    def use[T](ifClosed: => T)(body: => T): T = {
      synchronized {
        if (closed) return ifClosed
        require(!inUse, "Concurrent consumption of an in-process UDF iterator")
        inUse = true
      }
      var completed = false
      try {
        val result = body
        synchronized {
          inUse = false
          completed = true
          // Do not return a row backed by vectors that deferred cleanup will free.
          if (closed) Utils.tryWithSafeFinally { ifClosed } { cleanup() } else result
        }
      } finally {
        if (!completed) {
          synchronized {
            inUse = false
            if (closed) cleanup()
          }
        }
      }
    }

    override def close(): Unit = {
      inputClosed = true
      if (!inputLock.isHeldByCurrentThread) {
        try {
          if (inputLock.tryLock(inputWaitMillis, TimeUnit.MILLISECONDS)) inputLock.unlock()
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt()
        }
      }
      synchronized {
        if (!closed) {
          closed = true
          if (!inUse) cleanup()
        }
      }
    }
  }

  /** Carry sub-millisecond time between batches instead of dropping it on every invocation. */
  class NanosecondTimer(metric: SQLMetric) {
    private var remainder = 0L

    def add(nanos: Long): Unit = {
      val elapsed = remainder + nanos
      metric += elapsed / 1000000L
      remainder = elapsed % 1000000L
    }
  }
}
