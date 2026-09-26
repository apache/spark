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

import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluates scalar Python UDFs using Arrow CDI in the executor process. Only UDF arguments
 * are converted to Arrow. Original rows are buffered in a spillable queue and joined with
 * the results. Each batch owns its Arrow buffers so Python can safely retain input arrays.
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
    metrics: Map[String, SQLMetric])
  extends EvalPythonEvaluatorFactory(childOutput, udfs, output) {

  override protected def evaluate(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputSchema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
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
    var runtime: InProcessPythonRuntime.InterpreterSession = null
    val handles = functions.map(_ => UUID.randomUUID().toString)
    var registered = false
    var writer: ArrowWriter = null
    val results = ArrayBuffer.empty[ArrowColumnVector]
    var closed = false
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

    def close(): Unit = {
      if (!closed) {
        closed = true
        if (startedAt != 0L) {
          metrics("pythonTotalTime") += (System.nanoTime() - startedAt) / 1000000
        }
        Utils.tryWithSafeFinally {
          closeBatch()
        } {
          if (registered) runtime.release(handles)
        }
      }
    }

    context.addTaskCompletionListener[Unit](_ => close())

    new Iterator[InternalRow] {
      private var batchIter: Iterator[InternalRow] = Iterator.empty

      override def hasNext: Boolean = {
        if (!closed && startedAt == 0L) startedAt = System.nanoTime()
        checkCancellation()
        val available = !closed && (batchIter.hasNext || rows.hasNext)
        if (!available) close()
        available
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException("End of in-process UDF input")
        try {
          if (!batchIter.hasNext) {
            closeBatch()
            if (!registered) {
              runtime = InProcessPythonRuntime.currentSession
              // Mark before registering so failure after any registration still cleans up.
              registered = true
              functions.indices.foreach { i =>
                val func = functions(i)
                initTime.add(runtime.register(handles(i), func.command.toArray,
                  expectedFields(i), func.pythonVer, hideTraceback, simplifiedTraceback,
                  tracebackWithLocals))
              }
            }
            val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
            writer = try {
              ArrowWriter.create(root)
            } catch {
              case t: Throwable => Utils.tryWithSafeFinally { throw t } { root.close() }
            }
            var count = 0
            while (rows.hasNext && (batchSize <= 0 || count < batchSize) &&
                (count == 0 || maxBytes <= 0 || writer.sizeInBytes() < maxBytes)) {
              checkCancellation()
              writer.write(rows.next())
              count += 1
            }
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
            val columns = results.toArray[ColumnVector]
            batchIter = new ColumnarBatch(columns, count).rowIterator().asScala
          }
          batchIter.next()
        } catch {
          case t: Throwable => Utils.tryWithSafeFinally { throw t } { close() }
        }
      }
    }
  }
}

private[python] object InProcessArrowEvalPythonEvaluatorFactory {
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
