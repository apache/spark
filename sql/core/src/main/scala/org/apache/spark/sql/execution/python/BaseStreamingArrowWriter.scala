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

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter

/**
 * Base class to handle writing data to Arrow stream to Python workers. When the rows
 * for a group exceed the maximum number of records per batch, we chunk the data into multiple
 * batches.
 */
class BaseStreamingArrowWriter(
    root: VectorSchemaRoot,
    writer: ArrowStreamWriter,
    arrowMaxRecordsPerBatch: Int,
    arrowWriterForTest: ArrowWriter = null) {
  protected val arrowWriterForData: ArrowWriter = if (arrowWriterForTest == null) {
    ArrowWriter.create(root)
  } else {
    arrowWriterForTest
  }

  // variables for tracking the status of current batch
  protected var totalNumRowsForBatch = 0

  // variables for tracking the status of current chunk
  protected var numRowsForCurrentChunk = 0

  /**
   * Indicates writer to write a row for current batch.
   *
   * @param dataRow The row to write for current batch.
   */
  def writeRow(dataRow: InternalRow): Unit = {
    // If it exceeds the condition of batch (number of records) and there is more data for the
    // same group, finalize and construct a new batch.

    if (totalNumRowsForBatch >= arrowMaxRecordsPerBatch) {
      finalizeCurrentChunk(isLastChunkForGroup = false)
      finalizeCurrentArrowBatch()
    }

    arrowWriterForData.write(dataRow)

    numRowsForCurrentChunk += 1
    totalNumRowsForBatch += 1
  }

  /**
   * Finalizes the current batch and writes it to the Arrow stream.
   */
  def finalizeCurrentArrowBatch(): Unit = {
    arrowWriterForData.finish()
    writer.writeBatch()
    arrowWriterForData.reset()
    totalNumRowsForBatch = 0
  }

  /**
   * Finalizes the current chunk. We only reset the number of rows for the current chunk here since
   * not all the writers need this step.
   */
  protected def finalizeCurrentChunk(isLastChunkForGroup: Boolean): Unit = {
    numRowsForCurrentChunk = 0
  }
}
