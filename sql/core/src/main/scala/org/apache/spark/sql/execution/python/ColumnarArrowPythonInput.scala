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

import java.io.DataOutputStream
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * A trait that can be mixed-in with [[BasePythonRunner]] to send Arrow-backed
 * [[ColumnarBatch]] data to Python via Arrow IPC, bypassing the row-based conversion.
 *
 * When the batch's columns are [[ArrowColumnVector]], the underlying Arrow
 * [[FieldVector]]s are extracted directly and serialized via [[VectorUnloader]]
 * and [[MessageSerializer]] -- no data copy of vector buffers occurs.
 *
 * When the batch's columns are NOT [[ArrowColumnVector]], falls back to iterating
 * rows and writing via [[ArrowWriter]] (same as the existing row-based path).
 */
private[python] trait ColumnarArrowPythonInput
    extends PythonArrowInput[ColumnarBatch] {
  self: BasePythonRunner[ColumnarBatch, _] =>

  /**
   * Column indices to select from each [[ColumnarBatch]] for UDF input.
   * Set by the evaluator factory based on UDF argument resolution.
   */
  protected def inputColumnIndices: Array[Int]

  // Lazy-initialized ArrowWriter for the fallback (non-Arrow) path.
  // Uses the pre-allocated `root` from PythonArrowInput.
  private lazy val arrowWriter: ArrowWriter = ArrowWriter.create(root)
  private lazy val unloader: VectorUnloader = new VectorUnloader(root, true, codec, true)

  protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[ColumnarBatch]): Boolean = {
    if (inputIterator.hasNext) {
      val batch = inputIterator.next()
      val startData = dataOut.size()

      if (isArrowBacked(batch)) {
        writeArrowDirect(batch, dataOut)
      } else {
        writeRowByRow(batch, dataOut)
      }

      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      true
    } else {
      super[PythonArrowInput].close()
      false
    }
  }

  /**
   * Check whether all selected columns in the batch are Arrow-backed.
   */
  private def isArrowBacked(batch: ColumnarBatch): Boolean = {
    inputColumnIndices.forall(i => batch.column(i).isInstanceOf[ArrowColumnVector])
  }

  /**
   * Arrow-direct path: extract FieldVectors from ArrowColumnVector, wrap in a
   * temporary VectorSchemaRoot, and serialize via VectorUnloader + MessageSerializer.
   * No copy of vector data occurs -- VectorSchemaRoot.of() is a lightweight wrapper.
   */
  private def writeArrowDirect(batch: ColumnarBatch, dataOut: DataOutputStream): Unit = {
    val selectedVectors = inputColumnIndices.map { i =>
      batch.column(i).asInstanceOf[ArrowColumnVector]
        .getValueVector.asInstanceOf[FieldVector]
    }
    val batchRoot = VectorSchemaRoot.of(selectedVectors: _*)
    batchRoot.setRowCount(batch.numRows())

    val batchUnloader = new VectorUnloader(batchRoot, true, codec, true)
    val recordBatch = batchUnloader.getRecordBatch()
    try {
      val writeChannel = new WriteChannel(Channels.newChannel(dataOut))
      MessageSerializer.serialize(writeChannel, recordBatch)
    } finally {
      recordBatch.close()
    }
    // Do NOT close batchRoot or selectedVectors -- they are owned by the upstream batch.
  }

  /**
   * Fallback path: iterate rows from the ColumnarBatch and write via ArrowWriter,
   * same as the existing row-based path. No performance regression compared to
   * ColumnarToRow + ArrowWriter.
   */
  private def writeRowByRow(batch: ColumnarBatch, dataOut: DataOutputStream): Unit = {
    val selectedBatch = selectColumns(batch)
    val rowIter = selectedBatch.rowIterator().asScala
    while (rowIter.hasNext) {
      arrowWriter.write(rowIter.next())
    }
    arrowWriter.finish()

    val recordBatch = unloader.getRecordBatch()
    try {
      val writeChannel = new WriteChannel(Channels.newChannel(dataOut))
      MessageSerializer.serialize(writeChannel, recordBatch)
    } finally {
      recordBatch.close()
    }
    arrowWriter.reset()
  }

  /**
   * Select only the UDF input columns from the batch, producing a new ColumnarBatch
   * with only those columns.
   */
  private def selectColumns(batch: ColumnarBatch): ColumnarBatch = {
    val selectedColumns = inputColumnIndices.map(i => batch.column(i))
    val selected = new ColumnarBatch(selectedColumns, batch.numRows())
    selected
  }
}
