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
 * Class to handle writing data to Arrow stream in batches for TransformWithState. When the rows
 * for a group exceed the maximum number of records per batch, we chunk the data into multiple
 * batches.
 */
class TransformWithStateInPandasWriter(
    root: VectorSchemaRoot,
    writer: ArrowStreamWriter,
    arrowMaxRecordsPerBatch: Int,
    arrowWriterForTest: ArrowWriter = null) {
  private val arrowWriter: ArrowWriter = if (arrowWriterForTest == null) {
    ArrowWriter.create(root)
  } else {
    arrowWriterForTest
  }

  // variables for tracking the status of current batch
  private var numRowsForBatch = 0

  /**
   * Indicates writer to write a row for current batch.
   *
   * @param dataRow The row to write for current batch.
   */
  def writeRow(dataRow: InternalRow): Unit = {
    // If it exceeds the condition of batch (number of records) and there is more data for the
    // same group, finalize and construct a new batch.
    if (numRowsForBatch >= arrowMaxRecordsPerBatch) {
      finalizeCurrentArrowBatch()
    }
    arrowWriter.write(dataRow)
    numRowsForBatch += 1
  }

  /**
   * Finalizes the current batch and writes it to the Arrow stream.
   */
  def finalizeCurrentArrowBatch(): Unit = {
    arrowWriter.finish()
    writer.writeBatch()
    arrowWriter.reset()
    numRowsForBatch = 0
  }
}
