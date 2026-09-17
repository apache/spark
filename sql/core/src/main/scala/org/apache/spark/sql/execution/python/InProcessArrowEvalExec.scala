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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluates scalar Python UDFs using Arrow CDI in the executor process. Rows (including
 * computed UDF arguments) are written to Arrow once, then input and output buffers cross
 * the JVM/Python boundary without IPC serialization. The runtime owns the JEP thread.
 */
case class InProcessArrowEvalExec(
    udfs: Seq[InProcessPythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override protected def doExecute(): RDD[InternalRow] = {
    val expressions = ArrayBuffer[Expression](child.output: _*)
    val inputOrdinals = udfs.map { udf =>
      udf.children.map {
        case attr: Attribute if child.output.exists(_.exprId == attr.exprId) =>
          child.output.indexWhere(_.exprId == attr.exprId)
        case expr =>
          expressions += expr
          expressions.size - 1
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

    child.execute().mapPartitions { rows =>
      val resultProjection = UnsafeProjection.create(resultOutput, resultOutput)
      val projectInput: InternalRow => InternalRow =
        if (inputExpressions.size == childOutput.size) {
          identity[InternalRow]
        } else {
          val projection = UnsafeProjection.create(inputExpressions, childOutput)
          projection.initialize(TaskContext.getPartitionId())
          projection
        }
      val root = VectorSchemaRoot.create(
        ArrowUtils.toArrowSchema(inputSchema, timeZoneId, false), ArrowUtils.rootAllocator)
      val writer = try {
        ArrowWriter.create(root)
      } catch {
        case t: Throwable => Utils.tryWithSafeFinally { throw t } { root.close() }
      }
      val results = ArrayBuffer.empty[ArrowColumnVector]
      var closed = false

      def closeResults(): Unit = {
        val previous = results.toArray
        results.clear()
        AutoCloseables.close(previous: _*)
      }

      def close(): Unit = {
        if (!closed) {
          closed = true
          Utils.tryWithSafeFinally { closeResults() } { writer.root.close() }
        }
      }

      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

      new Iterator[InternalRow] {
        private var batchIter: Iterator[InternalRow] = Iterator.empty

        override def hasNext: Boolean = {
          val available = !closed && (batchIter.hasNext || rows.hasNext)
          if (!available) close()
          available
        }

        override def next(): InternalRow = {
          if (!hasNext) throw new NoSuchElementException("End of in-process UDF input")
          try {
            if (!batchIter.hasNext) {
              closeResults()
              writer.reset()
              var count = 0
              while (rows.hasNext && (batchSize <= 0 || count < batchSize) &&
                  (count == 0 || maxBytes <= 0 || writer.sizeInBytes() < maxBytes)) {
                writer.write(projectInput(rows.next()))
                count += 1
              }
              writer.finish()

              udfs.zip(inputOrdinals).foreach { case (udf, ordinals) =>
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
                    udf.serializedFunc,
                    inArrays.map(_.memoryAddress()).toArray,
                    inSchemas.map(_.memoryAddress()).toArray,
                    outArray.memoryAddress(), outSchema.memoryAddress(),
                    count, udf.dataType.json, timeZoneId)
                  results += InProcessArrowBridge.cdiToColumn(outArray, outSchema)
                } {
                  AutoCloseables.close(structs.asJava)
                }
              }

              val columns: Array[ColumnVector] =
                childOutput.indices.map(i =>
                  new ArrowColumnVector(writer.root.getVector(i))).toArray[ColumnVector] ++
                  results.toArray[ColumnVector]
              batchIter = new ColumnarBatch(columns, count).rowIterator().asScala
            }
            resultProjection(batchIter.next())
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
