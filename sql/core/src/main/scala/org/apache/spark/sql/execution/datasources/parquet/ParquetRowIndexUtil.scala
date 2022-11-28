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
package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.ParquetRecordReader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, RowIndexUtil}
import org.apache.spark.sql.execution.datasources.RowIndexUtil.findRowIndexColumnIndexInSchema
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.StructType


object ParquetRowIndexUtil {
  /**
   * Generate row indexes for vectorized readers.
  */
  class RowIndexGenerator(rowIndexColumnIdx: Int) {
    var rowIndexIterator: Iterator[Long] = _

    /** For Parquet only: initialize the generator using provided PageReadStore. */
    def initFromPageReadStore(pages: PageReadStore): Unit = {
      if (!pages.getRowIndexOffset.isPresent) {
        throw new IOException("PageReadStore returned no row index offset.")
      }
      val startingRowIdx: Long = pages.getRowIndexOffset.get()
      if (pages.getRowIndexes.isPresent) {
        // The presence of `getRowIndexes` indicates that page skipping is effective and only
        // a subset of rows in the row group is going to be read. Note that there is a name
        // collision here: these row indexes (unlike ones this class is generating) are counted
        // starting from 0 in each of the row groups.
        rowIndexIterator = pages.getRowIndexes.get.asScala.map(idx => idx + startingRowIdx)
      } else {
        val numRowsInRowGroup = pages.getRowCount
        rowIndexIterator = (startingRowIdx until startingRowIdx + numRowsInRowGroup).iterator
      }
    }

    def populateRowIndex(columnVectors: Array[ParquetColumnVector], numRows: Int): Unit = {
      populateRowIndex(columnVectors(rowIndexColumnIdx).getValueVector, numRows)
    }

    def populateRowIndex(columnVector: WritableColumnVector, numRows: Int): Unit = {
      for (i <- 0 until numRows) {
        columnVector.putLong(i, rowIndexIterator.next())
      }
    }
  }

  def createGeneratorIfNeeded(sparkSchema: StructType): RowIndexGenerator = {
    val columnIdx = findRowIndexColumnIndexInSchema(sparkSchema)
    if (columnIdx >= 0) new RowIndexGenerator(columnIdx)
    else null
  }

  /**
   * A wrapper for `ParquetRecordReader` that sets row index column to the correct value in
   * the returned InternalRow. Used in combination with non-vectorized (parquet-mr) Parquet reader.
   */
  private class RecordReaderWithRowIndexes(
      parent: ParquetRecordReader[InternalRow],
      rowIndexColumnIdx: Int)
    extends RecordReader[Void, InternalRow] {

    override def initialize(
        inputSplit: InputSplit,
        taskAttemptContext: TaskAttemptContext): Unit = {
      parent.initialize(inputSplit, taskAttemptContext)
    }

    override def nextKeyValue(): Boolean = parent.nextKeyValue()

    override def getCurrentKey: Void = parent.getCurrentKey

    override def getCurrentValue: InternalRow = {
      val row = parent.getCurrentValue
      row.setLong(rowIndexColumnIdx, parent.getCurrentRowIndex)
      row
    }

    override def getProgress: Float = parent.getProgress

    override def close(): Unit = parent.close()
  }

  def addRowIndexToRecordReaderIfNeeded(
      reader: ParquetRecordReader[InternalRow],
      sparkSchema: StructType): RecordReader[Void, InternalRow] = {
    val rowIndexColumnIdx = RowIndexUtil.findRowIndexColumnIndexInSchema(sparkSchema)
    if (rowIndexColumnIdx >= 0) {
      new RecordReaderWithRowIndexes(reader, rowIndexColumnIdx)
    } else {
      reader
    }
  }

  def isRowIndexColumn(column: ParquetColumn): Boolean = {
    column.path.length == 1 && column.path.last == FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
  }
}
