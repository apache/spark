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

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.orc._
import org.apache.orc.mapred.OrcInputFormat
import org.apache.orc.storage.ql.exec.vector._
import org.apache.orc.storage.serde2.io.HiveDecimalWritable

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._


/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 */
private[orc] class OrcColumnarBatchReader extends RecordReader[Void, ColumnarBatch] with Logging {
  import OrcColumnarBatchReader._

  /**
   * ORC File Reader.
   */
  private var reader: Reader = _

  /**
   * Vectorized Row Batch.
   */
  private var batch: VectorizedRowBatch = _

  /**
   * Requested Column IDs.
   */
  private var requestedColIds: Array[Int] = _

  /**
   * Record reader from row batch.
   */
  private var rows: org.apache.orc.RecordReader = _

  /**
   * Required Schema.
   */
  private var requiredSchema: StructType = _

  /**
   * ColumnarBatch for vectorized execution by whole-stage codegen.
   */
  private var columnarBatch: ColumnarBatch = _

  /**
   * Writable columnVectors of ColumnarBatch.
   */
  private var columnVectors: Seq[WritableColumnVector] = _

  /**
   * The number of rows read and considered to be returned.
   */
  private var rowsReturned: Long = 0L

  /**
   * Total number of rows.
   */
  private var totalRowCount: Long = 0L

  override def getCurrentKey: Void = null

  override def getCurrentValue: ColumnarBatch = columnarBatch

  override def getProgress: Float = rowsReturned.toFloat / totalRowCount

  override def nextKeyValue(): Boolean = nextBatch()

  override def close(): Unit = {
    if (columnarBatch != null) {
      columnarBatch.close()
      columnarBatch = null
    }
    if (rows != null) {
      rows.close()
      rows = null
    }
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `setRequiredSchema` is needed to be called after this.
   */
  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    val conf = taskAttemptContext.getConfiguration
    reader = OrcFile.createReader(
      fileSplit.getPath,
      OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
        .filesystem(fileSplit.getPath.getFileSystem(conf)))

    val options = OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart, fileSplit.getLength)
    rows = reader.rows(options)
  }

  /**
   * Set required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  def setRequiredSchema(
      orcSchema: TypeDescription,
      requestedColIds: Array[Int],
      resultSchema: StructType,
      requiredSchema: StructType,
      partitionValues: InternalRow): Unit = {
    batch = orcSchema.createRowBatch(DEFAULT_SIZE)
    assert(!batch.selectedInUse)
    totalRowCount = reader.getNumberOfRows
    logDebug(s"totalRowCount = $totalRowCount")

    this.requiredSchema = requiredSchema
    this.requestedColIds = requestedColIds

    val memMode = DEFAULT_MEMORY_MODE
    val capacity = ColumnarBatch.DEFAULT_BATCH_SIZE
    if (memMode == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, resultSchema)
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, resultSchema)
    }
    columnarBatch = new ColumnarBatch(resultSchema, columnVectors.toArray, capacity)

    if (partitionValues.numFields > 0) {
      val partitionIdx = requiredSchema.fields.length
      for (i <- 0 until partitionValues.numFields) {
        ColumnVectorUtils.populate(columnVectors(i + partitionIdx), partitionValues, i)
        columnVectors(i + partitionIdx).setIsConstant()
      }
    }
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  private def nextBatch(): Boolean = {
    if (rowsReturned >= totalRowCount) {
      return false
    }

    rows.nextBatch(batch)
    val batchSize = batch.size
    if (batchSize == 0) {
      return false
    }
    rowsReturned += batchSize
    columnarBatch.reset()
    columnarBatch.setNumRows(batchSize)

    var i = 0
    while (i < requiredSchema.length) {
      val field = requiredSchema(i)
      val toColumn = columnVectors(i)

      if (requestedColIds(i) < 0) {
        toColumn.appendNulls(batchSize)
      } else {
        val fromColumn = batch.cols(requestedColIds(i))

        if (fromColumn.isRepeating) {
          if (fromColumn.isNull(0)) {
            toColumn.appendNulls(batchSize)
          } else {
            field.dataType match {
              case BooleanType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0) == 1
                toColumn.appendBooleans(batchSize, data)

              case ByteType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toByte
                toColumn.appendBytes(batchSize, data)
              case ShortType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toShort
                toColumn.appendShorts(batchSize, data)
              case IntegerType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toInt
                toColumn.appendInts(batchSize, data)
              case LongType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0)
                toColumn.appendLongs(batchSize, data)

              case DateType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toInt
                toColumn.appendInts(batchSize, data)

              case TimestampType =>
                val data = fromColumn.asInstanceOf[TimestampColumnVector]
                toColumn.appendLongs(batchSize, fromTimestampColumnVector(data, 0))

              case FloatType =>
                val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0).toFloat
                toColumn.appendFloats(batchSize, data)
              case DoubleType =>
                val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0)
                toColumn.appendDoubles(batchSize, data)

              case StringType =>
                val data = fromColumn.asInstanceOf[BytesColumnVector]
                var index = 0
                while (index < batchSize) {
                  toColumn.appendByteArray(data.vector(0), data.start(0), data.length(0))
                  index += 1
                }
              case BinaryType =>
                val data = fromColumn.asInstanceOf[BytesColumnVector]
                var index = 0
                while (index < batchSize) {
                  toColumn.appendByteArray(data.vector(0), data.start(0), data.length(0))
                  index += 1
                }

              case DecimalType.Fixed(precision, scale) =>
                val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(0)
                appendDecimalWritable(toColumn, precision, scale, d)

              case dt =>
                throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
            }
          }
        } else if (!field.nullable || fromColumn.noNulls) {
          field.dataType match {
            case BooleanType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendBoolean(data(index) == 1)
                index += 1
              }

            case ByteType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendByte(data(index).toByte)
                index += 1
              }
            case ShortType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendShort(data(index).toShort)
                index += 1
              }
            case IntegerType | DateType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendInt(data(index).toInt)
                index += 1
              }
            case LongType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              toColumn.appendLongs(batchSize, data, 0)

            case TimestampType =>
              val data = fromColumn.asInstanceOf[TimestampColumnVector]
              var index = 0
              while (index < batchSize) {
                toColumn.appendLong(fromTimestampColumnVector(data, index))
                index += 1
              }

            case FloatType =>
              val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendFloat(data(index).toFloat)
                index += 1
              }
            case DoubleType =>
              val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
              toColumn.appendDoubles(batchSize, data, 0)

            case StringType =>
              val data = fromColumn.asInstanceOf[BytesColumnVector]
              var index = 0
              while (index < batchSize) {
                toColumn.appendByteArray(
                  data.vector(index), data.start(index), data.length(index))
                index += 1
              }
            case BinaryType =>
              val data = fromColumn.asInstanceOf[BytesColumnVector]
              var index = 0
              while (index < batchSize) {
                toColumn.appendByteArray(data.vector(index), data.start(index), data.length(index))
                index += 1
              }

            case DecimalType.Fixed(precision, scale) =>
              val data = fromColumn.asInstanceOf[DecimalColumnVector]
              var index = 0
              while (index < batchSize) {
                val d = data.vector(index)
                appendDecimalWritable(toColumn, precision, scale, d)
                index += 1
              }

            case dt =>
              throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
          }
        } else {
          for (index <- 0 until batchSize) {
            if (fromColumn.isNull(index)) {
              toColumn.appendNull()
            } else {
              field.dataType match {
                case BooleanType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index) == 1
                  toColumn.appendBoolean(data)

                case ByteType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toByte
                  toColumn.appendByte(data)
                case ShortType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toShort
                  toColumn.appendShort(data)
                case IntegerType | DateType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toInt
                  toColumn.appendInt(data)
                case LongType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index)
                  toColumn.appendLong(data)

                case TimestampType =>
                  val data = fromColumn.asInstanceOf[TimestampColumnVector]
                  toColumn.appendLong(fromTimestampColumnVector(data, index))

                case FloatType =>
                  val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(index).toFloat
                  toColumn.appendFloat(data)
                case DoubleType =>
                  val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(index)
                  toColumn.appendDouble(data)

                case StringType =>
                  val v = fromColumn.asInstanceOf[BytesColumnVector]
                  toColumn.appendByteArray(v.vector(index), v.start(index), v.length(index))

                case BinaryType =>
                  val v = fromColumn.asInstanceOf[BytesColumnVector]
                  toColumn.appendByteArray(v.vector(index), v.start(index), v.length(index))

                case DecimalType.Fixed(precision, scale) =>
                  val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(index)
                  appendDecimalWritable(toColumn, precision, scale, d)

                case dt =>
                  throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
              }
            }
          }
        }
      }
      i += 1
    }
    true
  }
}

/**
 * Constants for OrcColumnarBatchReader.
 */
object OrcColumnarBatchReader {
  /**
   * Default memory mode for ColumnarBatch.
   */
  val DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP

  /**
   * The default size of batch. We use this value for both ORC and Spark consistently
   * because they have different default values like the following.
   *
   * - ORC's VectorizedRowBatch.DEFAULT_SIZE = 1024
   * - Spark's ColumnarBatch.DEFAULT_BATCH_SIZE = 4 * 1024
   */
  val DEFAULT_SIZE: Int = 4 * 1024

  /**
   * Returns the number of micros since epoch from an element of TimestampColumnVector.
   */
  private def fromTimestampColumnVector(vector: TimestampColumnVector, index: Int): Long =
    vector.time(index) * 1000L + vector.nanos(index) / 1000L

  /**
   * Append a decimalWritable to a writableColumnVector.
   */
  private def appendDecimalWritable(
      toColumn: WritableColumnVector,
      precision: Int,
      scale: Int,
      decimalWritable: HiveDecimalWritable): Unit = {
    val decimal = decimalWritable.getHiveDecimal()
    val value = Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
    value.changePrecision(precision, scale)

    if (precision <= Decimal.MAX_INT_DIGITS) {
      toColumn.appendInt(value.toUnscaledLong.toInt)
    } else if (precision <= Decimal.MAX_LONG_DIGITS) {
      toColumn.appendLong(value.toUnscaledLong)
    } else {
      val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
      toColumn.appendByteArray(bytes, 0, bytes.length)
    }
  }
}

