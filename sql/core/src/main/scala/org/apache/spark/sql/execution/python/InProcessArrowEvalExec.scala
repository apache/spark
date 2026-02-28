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

import java.util.{ArrayList => JArrayList}

import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * Physical plan node for in-process Python UDF evaluation via jep.
 *
 * Execution flow per batch:
 *  1. Project input rows down to UDF-required columns
 *  2. Write projected rows into Arrow vectors via [[ArrowWriter]]
 *     (one copy: row format to Arrow native memory)
 *  3. Extract native buffer addresses from each Arrow vector (zero-copy)
 *  4. Pass addresses to CPython via [[InProcessPythonRuntime.invoke]] (in-process, no socket)
 *  5. Python reconstructs PyArrow arrays via `pa.foreign_buffer` (zero-copy)
 *  6. Python executes UDF and returns native buffer addresses of the result array
 *  7. JVM wraps those addresses via `ForeignAllocation` (zero-copy: Python to JVM Arrow)
 *  8. Append result column(s) to the original row and emit
 *  9. After all rows in a batch are consumed, close result columns and release the
 *     Python export so CPython can GC the array
 *
 * Net copies: 1 per batch (row-to-Arrow on input only) vs. the out-of-process path
 * which additionally serializes over a socket.
 *
 * Concurrency: [[InProcessPythonChecks]] enforces spark.executor.cores == spark.task.cpus,
 * guaranteeing exactly one task runs per executor and eliminating GIL contention on
 * [[InProcessPythonRuntime]]'s shared interpreter.
 */
case class InProcessArrowEvalExec(
    udfs: Seq[InProcessPythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override protected def doExecute(): RDD[InternalRow] = {
    val inputSchema = child.schema
    val batchSize = conf.arrowMaxRecordsPerBatch
    val timeZoneId = conf.sessionLocalTimeZone

    child.execute().mapPartitions { rows =>
      // One ArrowWriter per partition - reset between batches.
      val writer = ArrowWriter.create(inputSchema, timeZoneId)

      // Track result columns and Python export IDs from the previous batch.
      // We close the columns (releasing JVM references to Python memory) and then
      // release the Python exports at the start of each new batch, and at task end.
      var pendingResultCols: Array[ArrowColumnVector] = Array.empty
      var pendingExportIds: Array[Int] = Array.empty

      // Release the last batch's resources when the task completes.
      val taskCtx = TaskContext.get()
      if (taskCtx != null) {
        taskCtx.addTaskCompletionListener[Unit] { _ =>
          pendingResultCols.foreach(_.close())
          pendingExportIds.foreach(InProcessPythonRuntime.releaseExport)
        }
      }

      new Iterator[InternalRow] {
        // Current batch's row iterator; empty until the first batch is filled.
        private var batchIter: Iterator[InternalRow] = Iterator.empty

        override def hasNext: Boolean = batchIter.hasNext || rows.hasNext

        override def next(): InternalRow = {
          if (!batchIter.hasNext) {
            // Close previous batch's result columns before releasing Python exports.
            // Closing first ensures the JVM no longer holds any reference to Python's
            // native buffers before Python is allowed to GC the array.
            pendingResultCols.foreach(_.close())
            pendingResultCols = Array.empty
            pendingExportIds.foreach(InProcessPythonRuntime.releaseExport)
            pendingExportIds = Array.empty

            // Fill the next Arrow batch using a while loop.
            // ArrowWriter.write() copies each row's fields into off-heap Arrow memory
            // immediately, so it is safe even when WholeStageCodegenExec reuses the same
            // UnsafeRow object across calls to rows.next(). No .copy() needed.
            writer.reset()
            var count = 0
            while (rows.hasNext && count < batchSize) {
              writer.write(rows.next())
              count += 1
            }
            writer.finish()

            val root = writer.root
            val numRows = root.getRowCount

            // For each UDF, collect the native buffer addresses of its input columns,
            // invoke Python in-process, and reconstruct the result as an Arrow column
            // wrapping Python's native memory (zero-copy).
            val exportIds = new scala.collection.mutable.ArrayBuffer[Int]()
            val resultColumns = udfs.map { udf =>
              val inputAddrList = new JArrayList[java.util.Map[String, AnyRef]]()

              udf.children.foreach { case attr: Attribute =>
                val idx = child.output.indexWhere(_.exprId == attr.exprId)
                require(idx >= 0,
                  s"InProcessArrowEvalExec: cannot find input column '${attr}' " +
                  s"in child output ${child.output.map(_.name).mkString("[", ", ", "]")}")
                val arrowCol = new ArrowColumnVector(root.getVector(idx))
                inputAddrList.add(InProcessArrowBridge.extractAddresses(arrowCol, numRows))
              }

              val resultMap = InProcessPythonRuntime.invoke(
                udf.serializedFunc, inputAddrList, numRows)
              val (col, exportId) = InProcessArrowBridge.foreignToColumn(resultMap, udf.dataType)
              exportIds += exportId
              col
            }

            pendingResultCols = resultColumns.toArray
            pendingExportIds = exportIds.toArray

            val allColumns: Array[ArrowColumnVector] =
              (0 until root.getFieldVectors.size()).map(i =>
                new ArrowColumnVector(root.getVector(i))).toArray ++ resultColumns

            val columnarBatch = new ColumnarBatch(
              allColumns.asInstanceOf[Array[org.apache.spark.sql.vectorized.ColumnVector]],
              numRows)
            batchIter = columnarBatch.rowIterator().asScala
          }
          batchIter.next()
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
