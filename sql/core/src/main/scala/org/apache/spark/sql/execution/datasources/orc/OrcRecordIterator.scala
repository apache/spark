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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.orc._
import org.apache.orc.mapred.OrcInputFormat
import org.apache.orc.storage.ql.exec.vector._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A RecordIterator returns InternalRow from ORC data source.
 */
private[orc] class OrcRecordIterator extends Iterator[InternalRow] with Logging {

  /**
   * ORC File Reader.
   */
  private var reader: Reader = _

  /**
   * ORC Data Schema.
   */
  private var schema: TypeDescription = _

  /**
   * Use index to find corresponding fields.
   */
  private var useIndex: Boolean = _

  /**
   * Spark Schema.
   */
  private var sparkSchema: StructType = _

  /**
   * Required Schema.
   */
  private var requiredSchema: StructType = _

  /**
   * ORC Batch Record Reader.
   */
  private var rows: org.apache.orc.RecordReader = _

  /**
   * The number of total rows.
   */
  private var totalRowCount: Long = 0L

  /**
   * The number of rows that have been returned.
   */
  private var rowsReturned: Long = 0L

  /**
   * Vectorized Row Batch.
   */
  private var batch: VectorizedRowBatch = _

  /**
   * Current index in the batch.
   */
  private var batchIdx = -1

  /**
   * The number of rows in the current batch.
   */
  private var numBatched = 0

  /**
   * The current row.
   */
  private var mutableRow: InternalRow = _

  def initialize(
      inputSplit: InputSplit,
      taskAttemptContext: TaskAttemptContext,
      orcSchema: TypeDescription,
      requiredSchema: StructType,
      partitionColumns: StructType,
      partitionValues: InternalRow,
      useIndex: Boolean): Unit = {
    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    val conf = taskAttemptContext.getConfiguration

    reader = OrcFile.createReader(
      fileSplit.getPath,
      OrcFile.readerOptions(conf).maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)))
    schema = orcSchema
    sparkSchema = CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    totalRowCount = reader.getNumberOfRows

    // Create batch and load the first batch.
    val options = OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart, fileSplit.getLength)
    batch = schema.createRowBatch
    rows = reader.rows(options)
    rows.nextBatch(batch)
    batchIdx = 0
    numBatched = batch.size

    // Create a mutableRow for the full schema which is
    // requiredSchema.toAttributes ++ partitionSchema.toAttributes
    this.requiredSchema = requiredSchema
    val fullSchema = new StructType(this.requiredSchema.fields ++ partitionColumns)
    mutableRow = new SpecificInternalRow(fullSchema.map(_.dataType))

    this.useIndex = useIndex

    // Initialize the partition column values once.
    for (i <- requiredSchema.length until fullSchema.length) {
      mutableRow.update(i, partitionValues.get(i - requiredSchema.length, fullSchema(i).dataType))
    }
  }

  private def updateRow(): Unit = {
    // Fill the required fields into mutableRow.
    for (index <- 0 until requiredSchema.length) {
      val field = requiredSchema(index)
      val fieldType = field.dataType
      val vector = if (useIndex) {
        batch.cols(index)
      } else {
        batch.cols(sparkSchema.fieldIndex(field.name))
      }
      updateField(fieldType, vector, mutableRow, index)
    }
  }

  private def updateField(
      fieldType: DataType,
      vector: ColumnVector,
      mutableRow: InternalRow,
      index: Int) = {
    val dataIndex = if (vector.isRepeating) 0 else batchIdx
    if (vector.noNulls || !vector.isNull(dataIndex)) {
      fieldType match {
        case BooleanType =>
          val fieldValue = vector.asInstanceOf[LongColumnVector].vector(dataIndex) == 1
          mutableRow.setBoolean(index, fieldValue)
        case ByteType =>
          val fieldValue = vector.asInstanceOf[LongColumnVector].vector(dataIndex)
          mutableRow.setByte(index, fieldValue.asInstanceOf[Byte])
        case ShortType =>
          val fieldValue = vector.asInstanceOf[LongColumnVector].vector(dataIndex)
          mutableRow.setShort(index, fieldValue.asInstanceOf[Short])
        case IntegerType =>
          val fieldValue = vector.asInstanceOf[LongColumnVector].vector(dataIndex)
          mutableRow.setInt(index, fieldValue.asInstanceOf[Int])
        case LongType =>
          val fieldValue = vector.asInstanceOf[LongColumnVector].vector(dataIndex)
          mutableRow.setLong(index, fieldValue)

        case FloatType =>
          val fieldValue = vector.asInstanceOf[DoubleColumnVector].vector(dataIndex)
          mutableRow.setFloat(index, fieldValue.asInstanceOf[Float])
        case DoubleType =>
          val fieldValue = vector.asInstanceOf[DoubleColumnVector].vector(dataIndex)
          mutableRow.setDouble(index, fieldValue.asInstanceOf[Double])
        case _: DecimalType =>
          val fieldValue = vector.asInstanceOf[DecimalColumnVector].vector(dataIndex)
          mutableRow.update(index, OrcFileFormat.getCatalystValue(fieldValue, fieldType))

        case _: DateType =>
          val fieldValue = vector.asInstanceOf[LongColumnVector].vector(dataIndex)
          mutableRow.update(index, fieldValue.asInstanceOf[SQLDate])

        case _: TimestampType =>
          val fieldValue =
            vector.asInstanceOf[TimestampColumnVector].asScratchTimestamp(dataIndex)
          mutableRow.update(index, DateTimeUtils.fromJavaTimestamp(fieldValue))

        case StringType =>
          val v = vector.asInstanceOf[BytesColumnVector]
          val fieldValue =
            UTF8String.fromBytes(v.vector(dataIndex), v.start(dataIndex), v.length(dataIndex))
          mutableRow.update(index, fieldValue)

        case BinaryType =>
          val fieldVector = vector.asInstanceOf[BytesColumnVector]
          val fieldValue = java.util.Arrays.copyOfRange(
            fieldVector.vector(dataIndex),
            fieldVector.start(dataIndex),
            fieldVector.start(dataIndex) + fieldVector.length(dataIndex))
          mutableRow.update(index, fieldValue)

        case dt => throw new UnsupportedOperationException(s"Unknown Data Type: $dt")

      }
    } else {
      fieldType match {
        case dt: DecimalType => mutableRow.setDecimal(index, null, dt.precision)
        case _ => mutableRow.setNullAt(index)
      }
    }
  }

  def hasNext: Boolean = {
    0 <= batchIdx && batchIdx < numBatched && rowsReturned < totalRowCount
  }

  def next: InternalRow = {
    updateRow()

    if (rowsReturned == totalRowCount) {
      close()
    } else {
      batchIdx += 1
      rowsReturned += 1
      if (batchIdx == numBatched && rowsReturned < totalRowCount) {
        rows.nextBatch(batch)
        batchIdx = 0
        numBatched = batch.size
      }
    }

    mutableRow
  }

  def close(): Unit = {
    rows.close()
  }
}

