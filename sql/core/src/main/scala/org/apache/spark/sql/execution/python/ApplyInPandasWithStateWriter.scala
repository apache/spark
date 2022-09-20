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

import scala.collection.JavaConverters._

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.arrow.ArrowWriter.createFieldWriter
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.types.{BinaryType, BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This class abstracts the complexity on constructing Arrow RecordBatches for data and state with
 * bin-packing and chunking. The caller only need to call the proper public methods of this class
 * `startNewGroup`, `writeRow`, `finalizeGroup`, `finalizeData` and this class will write the data
 * and state into Arrow RecordBatches with performing bin-pack and chunk internally.
 *
 * This class requires that the parameter `root` has been initialized with the Arrow schema like
 * below:
 * - data fields
 * - state field
 *   - nested schema (Refer ApplyInPandasWithStateWriter.STATE_METADATA_SCHEMA)
 *
 * Please refer the code comment in the implementation to see how the writes of data and state
 * against Arrow RecordBatch work with consideration of bin-packing and chunking.
 */
class ApplyInPandasWithStateWriter(
    root: VectorSchemaRoot,
    writer: ArrowStreamWriter,
    arrowMaxRecordsPerBatch: Int) {

  import ApplyInPandasWithStateWriter._

  // Unlike applyInPandas (and other PySpark operators), applyInPandasWithState requires to produce
  // the additional data `state`, along with the input data.
  //
  // ArrowStreamWriter supports only single VectorSchemaRoot, which means all Arrow RecordBatches
  // being sent out from ArrowStreamWriter should have same schema. That said, we have to construct
  // "an" Arrow schema to contain both types of data, and also construct Arrow RecordBatches to
  // contain both data.
  //
  // To achieve this, we extend the schema for input data to have a column for state at the end.
  // But also, we logically group the columns by family (data vs state) and initialize writer
  // separately, since it's lot more easier and probably performant to write the row directly
  // rather than projecting the row to match up with the overall schema.
  //
  // Although Arrow RecordBatch enables to write the data as columnar, we figure out it gives
  // strange outputs if we don't ensure that all columns have the same number of values. Since
  // there are one or more data for a grouping key (applies to case of handling timed out state
  // as well) whereas there is only one state for a grouping key, we have to fill up the empty rows
  // in state side to ensure both have the same number of rows.
  private val arrowWriterForData = createArrowWriter(
    root.getFieldVectors.asScala.toSeq.dropRight(1))
  private val arrowWriterForState = createArrowWriter(
    root.getFieldVectors.asScala.toSeq.takeRight(1))

  // - Bin-packing
  //
  // We apply bin-packing the data from multiple groups into one Arrow RecordBatch to
  // gain the performance. In many cases, the amount of data per grouping key is quite
  // small, which does not seem to maximize the benefits of using Arrow.
  //
  // We have to split the record batch down to each group in Python worker to convert the
  // data for group to Pandas, but hopefully, Arrow RecordBatch provides the way to split
  // the range of data and give a view, say, "zero-copy". To help splitting the range for
  // data, we provide the "start offset" and the "number of data" in the state metadata.
  //
  // We don't bin-pack all groups into a single record batch - we have a limit on the number
  // of rows in the current Arrow RecordBatch to stop adding next group.
  //
  // - Chunking
  //
  // We also chunk the data from single group into multiple Arrow RecordBatch to ensure
  // scalability. Note that we don't know the volume (number of rows, overall size) of data for
  // specific group key before we read the entire data. The easiest approach to address both
  // bin-pack and chunk is to check the number of rows in the current Arrow RecordBatch for each
  // write of row.
  //
  // - Consideration
  //
  // Since the number of rows in Arrow RecordBatch does not represent the actual size (bytes),
  // the limit should be set very conservatively. Using a small number of limit does not introduce
  // correctness issues.

  private var numRowsForCurGroup = 0
  private var startOffsetForCurGroup = 0
  private var totalNumRowsForBatch = 0
  private var totalNumStatesForBatch = 0

  private var currentGroupKeyRow: UnsafeRow = _
  private var currentGroupState: GroupStateImpl[Row] = _

  /**
   * Indicates writer to start with new grouping key.
   *
   * @param keyRow The grouping key row for current group.
   * @param groupState The instance of GroupStateImpl for current group.
   */
  def startNewGroup(keyRow: UnsafeRow, groupState: GroupStateImpl[Row]): Unit = {
    currentGroupKeyRow = keyRow
    currentGroupState = groupState
  }

  /**
   * Indicates writer to write a row in the current group.
   *
   * @param dataRow The row to write in the current group.
   */
  def writeRow(dataRow: InternalRow): Unit = {
    // If it exceeds the condition of batch (number of records) and there is more data for the
    // same group, finalize and construct a new batch.

    if (totalNumRowsForBatch >= arrowMaxRecordsPerBatch) {
      // Provide state metadata row as intermediate
      val stateInfoRow = buildStateInfoRow(currentGroupKeyRow, currentGroupState,
        startOffsetForCurGroup, numRowsForCurGroup, isLastChunk = false)
      arrowWriterForState.write(stateInfoRow)
      totalNumStatesForBatch += 1

      finalizeCurrentArrowBatch()
    }

    arrowWriterForData.write(dataRow)
    numRowsForCurGroup += 1
    totalNumRowsForBatch += 1
  }

  /**
   * Indicates writer that current group has finalized and there will be no further row bound to
   * the current group.
   */
  def finalizeGroup(): Unit = {
    // Provide state metadata row
    val stateInfoRow = buildStateInfoRow(currentGroupKeyRow, currentGroupState,
      startOffsetForCurGroup, numRowsForCurGroup, isLastChunk = true)
    arrowWriterForState.write(stateInfoRow)
    totalNumStatesForBatch += 1

    // The start offset for next group would be same as the total number of rows for batch,
    // unless the next group starts with new batch.
    startOffsetForCurGroup = totalNumRowsForBatch
  }

  /**
   * Indicates writer that all groups have been processed.
   */
  def finalizeData(): Unit = {
    if (numRowsForCurGroup > 0) {
      // We still have some rows in the current record batch. Need to finalize them as well.
      finalizeCurrentArrowBatch()
    }
  }

  private def createArrowWriter(fieldVectors: Seq[FieldVector]): ArrowWriter = {
    val children = fieldVectors.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }

    new ArrowWriter(root, children.toArray)
  }

  private def buildStateInfoRow(
      keyRow: UnsafeRow,
      groupState: GroupStateImpl[Row],
      startOffset: Int,
      numRows: Int,
      isLastChunk: Boolean): InternalRow = {
    // NOTE: see ApplyInPandasWithStateWriter.STATE_METADATA_SCHEMA
    val stateUnderlyingRow = new GenericInternalRow(
      Array[Any](
        UTF8String.fromString(groupState.json()),
        keyRow.getBytes,
        groupState.getOption.map(PythonSQLUtils.toPyRow).orNull,
        startOffset,
        numRows,
        isLastChunk
      )
    )
    new GenericInternalRow(Array[Any](stateUnderlyingRow))
  }

  private def finalizeCurrentArrowBatch(): Unit = {
    val remainingEmptyStateRows = totalNumRowsForBatch - totalNumStatesForBatch
    (0 until remainingEmptyStateRows).foreach { _ =>
      arrowWriterForState.write(EMPTY_STATE_METADATA_ROW)
    }

    arrowWriterForState.finish()
    arrowWriterForData.finish()
    writer.writeBatch()
    arrowWriterForState.reset()
    arrowWriterForData.reset()

    startOffsetForCurGroup = 0
    numRowsForCurGroup = 0
    totalNumRowsForBatch = 0
    totalNumStatesForBatch = 0
  }
}

object ApplyInPandasWithStateWriter {
  val STATE_METADATA_SCHEMA: StructType = StructType(
    Array(
      StructField("properties", StringType),
      StructField("keyRowAsUnsafe", BinaryType),
      StructField("object", BinaryType),
      StructField("startOffset", IntegerType),
      StructField("numRows", IntegerType),
      StructField("isLastChunk", BooleanType)
    )
  )

  // To avoid initializing a new row for empty state metadata row.
  val EMPTY_STATE_METADATA_ROW = new GenericInternalRow(
    Array[Any](null, null, null, null, null, null))
}
