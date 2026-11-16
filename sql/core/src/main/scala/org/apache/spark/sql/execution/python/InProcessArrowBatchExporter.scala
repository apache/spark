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

import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, BaseStruct}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseLargeVariableWidthVector, BaseVariableWidthVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{Field, Schema}

import org.apache.spark.TaskContext
import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils

/**
 * Exports batches of UDF arguments to in-process Python over the Arrow C Data Interface, and
 * imports the results of the UDFs. Each batch owns its Arrow buffers, so that Python can
 * safely retain input arrays.
 *
 * The exporter holds one batch at a time, which is in one of these states:
 *  - none: before the first `startBatch`, and after `close`.
 *  - filling (`startBatch`): `write` adds rows to the batch, counted by `rowCount`.
 *  - evaluated (`evaluate`): the batch was exported and the results of every UDF imported.
 *    The returned rows read the input and result vectors of the batch, and are valid until
 *    the next `startBatch` or `close`, which release those vectors.
 *
 * The UDFs are registered with the session before the first batch is exported, and released
 * by `close` if any registration was attempted. A failure leaves the batch and the
 * registrations to `close`.
 *
 * The exporter is not thread safe. The consumer of the evaluator's iterator calls
 * `startBatch`, `write` and `evaluate`, with the iterator's lock held. `close` is called once,
 * with that lock held too and not while Python runs, by the consumer or by the task
 * completion listener (see [[InProcessArrowEvalPythonEvaluatorFactory.IteratorResources]]).
 *
 * @param session the interpreter session to register and invoke the UDFs with
 * @param functions the Python function of each UDF
 * @param argMetas the arguments of each UDF, as ordinals of `arrowSchema`
 * @param expectedFields the Arrow field that the result of each UDF must have
 * @param arrowSchema the Arrow schema of the UDF arguments, which the written rows have
 * @param readBackInput whether the returned rows start with the input columns, read back from
 *                      the exported input vectors
 * @param metrics the metrics of `InProcessArrowEvalPythonExec`, except `pythonTotalTime`
 * @param context the task context, checked for cancellation before each UDF is invoked
 */
private[python] class InProcessArrowBatchExporter(
    session: InProcessPythonRuntime.InterpreterSession,
    functions: Seq[PythonFunction],
    argMetas: Array[Array[ArgumentMetadata]],
    expectedFields: Seq[Field],
    arrowSchema: Schema,
    readBackInput: Boolean,
    hideTraceback: Boolean,
    simplifiedTraceback: Boolean,
    tracebackWithLocals: Boolean,
    fullValidation: Boolean,
    metrics: Map[String, SQLMetric],
    context: TaskContext) extends AutoCloseable {
  import InProcessArrowBatchExporter.{dataBytes, roundedDensity, PythonCall}

  private val inputOrdinals = argMetas.map(_.map(_.offset))
  private val handles = functions.map(_ => UUID.randomUUID().toString)
  private var registered = false
  private var writer: ArrowWriter = null
  private var numRows = 0
  private val results = ArrayBuffer.empty[ArrowColumnVector]

  /** The number of rows written to the current batch. */
  def rowCount: Int = numRows

  /** The bytes of the rows written to the current batch. */
  def sizeInBytes: Long = writer.sizeInBytes()

  /** Releases the previous batch, if any, and starts an empty one. */
  def startBatch(): Unit = {
    // Each batch needs buffers of its own, which Python may retain, but sizing flat vectors
    // like the previous batch's avoids growing them from Arrow's defaults row by row.
    // Nested vectors keep the defaults, since Arrow sizes their children per record.
    val capacities = if (writer == null) Seq.empty else {
      writer.root.getFieldVectors.asScala.map(v => (v.getValueCount, dataBytes(v))).toSeq
    }
    closeBatch()
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
    capacities.zip(root.getFieldVectors.asScala).foreach { case ((count, bytes), v) =>
      if (count > 0) v match {
        case v: BaseFixedWidthVector => v.setInitialCapacity(count)
        case v: BaseVariableWidthVector if bytes > 0 =>
          v.setInitialCapacity(count, roundedDensity(count, bytes, Int.MaxValue))
        case v: BaseLargeVariableWidthVector if bytes > 0 =>
          v.setInitialCapacity(count, roundedDensity(count, bytes, Long.MaxValue))
        case _ =>
      }
    }
    writer = try {
      ArrowWriter.create(root)
    } catch {
      case t: Throwable => Utils.tryWithSafeFinally { throw t } { root.close() }
    }
    numRows = 0
  }

  /** Adds a row of UDF arguments to the current batch. */
  def write(row: InternalRow): Unit = {
    writer.write(row)
    numRows += 1
  }

  /**
   * Registers the UDFs if this is the first batch, exports the arguments of each UDF,
   * invokes it, and imports its result. Returns the rows of the batch: the input columns if
   * `readBackInput`, followed by the result of each UDF.
   *
   * @param python runs each call into Python
   */
  def evaluate(python: PythonCall): Iterator[InternalRow] = {
    if (!registered) {
      // Mark before registering so failure after any registration still cleans up.
      registered = true
      functions.indices.foreach { i =>
        val func = functions(i)
        python(metrics("pythonInitTime"), () => session.register(handles(i),
          func.command.toArray, expectedFields(i), func.pythonVer, hideTraceback,
          simplifiedTraceback, tracebackWithLocals, fullValidation))
      }
    }
    writer.finish()
    metrics("pythonDataSent") += writer.sizeInBytes()

    handles.indices.foreach { udfIndex =>
      val handle = handles(udfIndex)
      val ordinals = inputOrdinals(udfIndex)
      context.killTaskIfInterrupted()
      // Register each acquired resource immediately, including partially exported
      // inputs and results of earlier UDFs if a later UDF throws.
      val structs = ArrayBuffer.empty[AutoCloseable]
      def track[S <: BaseStruct](struct: S): S = {
        val closer: AutoCloseable = () => InProcessArrowBridge.closeStruct(struct)
        structs += closer
        struct
      }
      def array(): ArrowArray = track(ArrowArray.allocateNew(ArrowUtils.rootAllocator))
      def schema(): ArrowSchema = track(ArrowSchema.allocateNew(ArrowUtils.rootAllocator))
      Utils.tryWithSafeFinally {
        val inArrays = ordinals.map(_ => array())
        val inSchemas = ordinals.map(_ => schema())
        val outArray = array()
        val outSchema = schema()
        ordinals.indices.foreach { i =>
          InProcessArrowBridge.exportColumn(
            writer.root.getVector(ordinals(i)), inArrays(i), inSchemas(i))
        }
        python(metrics("pythonProcessingTime"), () => session.invoke(
          handle,
          inArrays.map(_.memoryAddress()).toArray,
          inSchemas.map(_.memoryAddress()).toArray,
          outArray.memoryAddress(), outSchema.memoryAddress(),
          numRows, argMetas(udfIndex).map(_.name.getOrElse(""))))
        results += InProcessArrowBridge.cdiToColumn(
          outArray, outSchema, Some(expectedFields(udfIndex)))
        metrics("pythonDataReceived") += results.last.getValueVector.getBufferSize
      } {
        AutoCloseables.close(structs.asJava)
      }
    }

    metrics("pythonNumRowsReceived") += numRows
    // Input vectors are closed with the writer's root, not with the results.
    val inputs = if (readBackInput) {
      writer.root.getFieldVectors.asScala.map(new ArrowColumnVector(_))
    } else {
      Nil
    }
    val columns = (inputs ++ results).toArray[ColumnVector]
    new ColumnarBatch(columns, numRows).rowIterator().asScala
  }

  /** Releases the current batch, if any, and the registrations of the UDFs. */
  override def close(): Unit = {
    Utils.tryWithSafeFinally {
      closeBatch()
    } {
      if (registered) session.release(handles)
    }
  }

  private def closeBatch(): Unit = {
    val resources = ArrayBuffer.empty[AutoCloseable]
    resources ++= results
    results.clear()
    if (writer != null) {
      resources += writer.root
      writer = null
    }
    AutoCloseables.close(resources.asJava)
  }
}

private[python] object InProcessArrowBatchExporter {
  /**
   * Runs a call into Python, which returns the nanoseconds it took, and adds them to the
   * metric. It may throw instead of running the call, or after it, to stop the evaluation.
   */
  type PythonCall = (SQLMetric, () => Long) => Unit

  /** The bytes of values in a variable-width vector, or 0 for any other vector. */
  def dataBytes(vector: ValueVector): Long = vector match {
    case v: BaseVariableWidthVector => math.round(v.getDensity * v.getValueCount)
    case v: BaseLargeVariableWidthVector => math.round(v.getDensity * v.getValueCount)
    case _ => 0L
  }

  /**
   * The density for `setInitialCapacity` that allocates `bytes` rounded up to a power of two,
   * as Arrow rounds smaller requests, so that a slightly larger batch does not reallocate and
   * copy its data on its last rows. Arrow truncates `count * density`, hence the half byte.
   */
  def roundedDensity(count: Int, bytes: Long, maxBytes: Long): Double =
    (math.min(ByteArrayMethods.nextPowerOf2(bytes), maxBytes) + 0.5) / count
}
