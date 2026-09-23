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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, JoinedRow, PythonUDF, UnsafeProjection}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluates scalar Python UDFs using Arrow CDI in the executor process. Only UDF arguments
 * are converted to Arrow. Original rows are buffered in a spillable queue and joined with
 * the results. Each batch owns its Arrow buffers so Python can safely retain input arrays.
 */
case class InProcessArrowEvalExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override protected def doExecute(): RDD[InternalRow] = {
    val expressions = ArrayBuffer.empty[Expression]
    val inputOrdinals = udfs.map { udf =>
      udf.children.map { expr =>
        val existing = expressions.indexWhere(_.semanticEquals(expr))
        if (existing >= 0) {
          existing
        } else {
          expressions += expr
          expressions.size - 1
        }
      }
    }
    // Synthetic names also allow joins with duplicate output column names.
    val inputSchema = StructType(expressions.zipWithIndex.map { case (expr, i) =>
      StructField(s"_input$i", expr.dataType, expr.nullable)
    }.toSeq)
    val inputExpressions = expressions.toSeq
    val childOutput = child.output
    val resultOutput = output
    val batchSize = conf.arrowMaxRecordsPerBatch
    val maxBytes = conf.arrowMaxBytesPerBatch
    val timeZoneId = conf.sessionLocalTimeZone
    ArrowUtils.failDuplicatedFieldNames(inputSchema)
    val functions = udfs.map(u => (u.func.command.toArray, u.dataType.json, u.func.pythonVer))

    child.execute().mapPartitions { rows =>
      val context = TaskContext.get()
      def checkCancellation(): Unit = context.killTaskIfInterrupted()

      val arrowSchema = ArrowUtils.toArrowSchema(inputSchema, timeZoneId, false)
      val resultProjection = UnsafeProjection.create(resultOutput, resultOutput)
      val projectRow = UnsafeProjection.create(childOutput, childOutput)
      val projectInput = UnsafeProjection.create(inputExpressions, childOutput)
      projectInput.initialize(context.partitionId())
      val joined = new JoinedRow
      val queue = HybridRowQueue(context.taskMemoryManager(), childOutput.length)
      val handles = functions.map(_ => UUID.randomUUID().toString)
      var registered = false
      var writer: ArrowWriter = null
      val results = ArrayBuffer.empty[ArrowColumnVector]
      var closed = false

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
          Utils.tryWithSafeFinally {
            closeBatch()
          } {
            Utils.tryWithSafeFinally { queue.close() } {
              InProcessPythonRuntime.release(handles)
            }
          }
        }
      }

      context.addTaskCompletionListener[Unit](_ => close())

      new Iterator[InternalRow] {
        private var batchIter: Iterator[InternalRow] = Iterator.empty

        override def hasNext: Boolean = {
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
                functions.zip(handles).foreach { case ((command, returnType, version), handle) =>
                  InProcessPythonRuntime.register(handle, command, returnType, timeZoneId, version)
                }
                registered = true
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
                val row = rows.next()
                queue.add(projectRow(row))
                writer.write(projectInput(row))
                count += 1
              }
              writer.finish()

              handles.zip(inputOrdinals).foreach { case (handle, ordinals) =>
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
                  InProcessPythonRuntime.invoke(
                    handle,
                    inArrays.map(_.memoryAddress()).toArray,
                    inSchemas.map(_.memoryAddress()).toArray,
                    outArray.memoryAddress(), outSchema.memoryAddress(),
                    count)
                  results += InProcessArrowBridge.cdiToColumn(outArray, outSchema)
                } {
                  AutoCloseables.close(structs.asJava)
                }
              }

              val columns = results.toArray[ColumnVector]
              batchIter = new ColumnarBatch(columns, count).rowIterator().asScala
            }
            resultProjection(joined(queue.remove(), batchIter.next()))
          } catch {
            case t: Throwable => Utils.tryWithSafeFinally { throw t } { close() }
          }
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
