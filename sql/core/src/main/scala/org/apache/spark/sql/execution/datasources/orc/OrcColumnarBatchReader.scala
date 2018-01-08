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

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, OffHeapColumnVector,
  OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 * After creating, `initialize` and `initBatch` should be called sequentially.
 */
private[orc] class OrcColumnarBatchReader extends RecordReader[Void, ColumnarBatch] {
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
  private var recordReader: org.apache.orc.RecordReader = _

  /**
   * Required Schema.
   */
  private var requiredFields: Array[StructField] = _

  /**
   * ColumnarBatch for vectorized execution by whole-stage codegen.
   */
  private var columnarBatch: ColumnarBatch = _

  /**
   * Writable column vectors of ColumnarBatch.
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
    if (recordReader != null) {
      recordReader.close()
      recordReader = null
    }
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
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
    recordReader = reader.rows(options)
    totalRowCount = reader.getNumberOfRows
  }

  /**
   * Set required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  def initBatch(
      useOffHeap: Boolean,
      orcSchema: TypeDescription,
      requestedColIds: Array[Int],
      requiredFields: Array[StructField],
      partitionSchema: StructType,
      partitionValues: InternalRow): Unit = {
    batch = orcSchema.createRowBatch(DEFAULT_SIZE)
    assert(!batch.selectedInUse, "`selectedInUse` should be initialized with `false`.")

    this.requiredFields = requiredFields
    this.requestedColIds = requestedColIds
    assert(requiredFields.length == requestedColIds.length)

    val resultSchema = StructType(requiredFields ++ partitionSchema.fields)

    val capacity = DEFAULT_SIZE
    val memoryMode = if (useOffHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP
    if (memoryMode == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, resultSchema)
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, resultSchema)
    }
    columnarBatch = new ColumnarBatch(resultSchema, columnVectors.toArray, capacity)

    if (partitionValues.numFields > 0) {
      val partitionIdx = requiredFields.length
      for (i <- 0 until partitionValues.numFields) {
        ColumnVectorUtils.populate(columnVectors(i + partitionIdx), partitionValues, i)
        columnVectors(i + partitionIdx).setIsConstant()
      }
    }

    // Initialize the missing columns once.
    for (i <- 0 until requiredFields.length) {
      if (requestedColIds(i) < 0) {
        columnVectors(i).putNulls(0, columnarBatch.capacity)
        columnVectors(i).setIsConstant()
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

    recordReader.nextBatch(batch)
    assert(!batch.selectedInUse, "`selectdInUse` is not used and all rows are qualified.")
    val batchSize = batch.size
    if (batchSize == 0) {
      return false
    }
    rowsReturned += batchSize
    columnVectors.foreach(_.reset)
    columnarBatch.setNumRows(batchSize)

    var i = 0
    while (i < requiredFields.length) {
      val field = requiredFields(i)
      val toColumn = columnVectors(i)

      if (requestedColIds(i) >= 0) {
        val fromColumn = batch.cols(requestedColIds(i))

        if (fromColumn.isRepeating) {
          putRepeatingValues(batchSize, field, fromColumn, toColumn)
        } else if (fromColumn.noNulls) {
          putNonNullValues(batchSize, field, fromColumn, toColumn)
        } else {
          putValues(batchSize, field, fromColumn, toColumn)
        }
      }
      i += 1
    }
    true
  }

  private def putRepeatingValues(
      batchSize: Int,
      field: StructField,
      fromColumn: ColumnVector,
      toColumn: WritableColumnVector) : Unit = {
    if (fromColumn.isNull(0)) {
      toColumn.putNulls(0, batchSize)
    } else {
      field.dataType match {
        case BooleanType =>
          val data = fromColumn.asInstanceOf[LongColumnVector].vector(0) == 1
          toColumn.putBooleans(0, batchSize, data)

        case ByteType =>
          val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toByte
          toColumn.putBytes(0, batchSize, data)
        case ShortType =>
          val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toShort
          toColumn.putShorts(0, batchSize, data)
        case IntegerType | DateType =>
          val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toInt
          toColumn.putInts(0, batchSize, data)
        case LongType =>
          val data = fromColumn.asInstanceOf[LongColumnVector].vector(0)
          toColumn.putLongs(0, batchSize, data)

        case TimestampType =>
          val data = fromColumn.asInstanceOf[TimestampColumnVector]
          toColumn.putLongs(0, batchSize, fromTimestampColumnVector(data, 0))

        case FloatType =>
          val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0).toFloat
          toColumn.putFloats(0, batchSize, data)
        case DoubleType =>
          val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0)
          toColumn.putDoubles(0, batchSize, data)

        case StringType | BinaryType =>
          val data = fromColumn.asInstanceOf[BytesColumnVector]
          toColumn.putByteArray(0, data.vector(0))
          var index = 0
          while (index < batchSize) {
            toColumn.putArray(index, data.start(index), data.length(index))
            index += 1
          }

        case DecimalType.Fixed(precision, scale) =>
          val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(0)
          putDecimalWritables(toColumn, batchSize, precision, scale, d)

        case dt =>
          throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
      }
    }
  }

  private def putNonNullValues(
      batchSize: Int,
      field: StructField,
      fromColumn: ColumnVector,
      toColumn: WritableColumnVector) : Unit = {
    field.dataType match {
      case BooleanType =>
        val data = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          toColumn.putBoolean(index, data(index) == 1)
          index += 1
        }

      case ByteType =>
        val data = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          toColumn.putByte(index, data(index).toByte)
          index += 1
        }
      case ShortType =>
        val data = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          toColumn.putShort(index, data(index).toShort)
          index += 1
        }
      case IntegerType | DateType =>
        val data = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          toColumn.putInt(index, data(index).toInt)
          index += 1
        }
      case LongType =>
        val data = fromColumn.asInstanceOf[LongColumnVector].vector
        toColumn.putLongs(0, batchSize, data, 0)

      case TimestampType =>
        val data = fromColumn.asInstanceOf[TimestampColumnVector]
        var index = 0
        while (index < batchSize) {
          toColumn.putLong(index, fromTimestampColumnVector(data, index))
          index += 1
        }

      case FloatType =>
        val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
        var index = 0
        while (index < batchSize) {
          toColumn.putFloat(index, data(index).toFloat)
          index += 1
        }
      case DoubleType =>
        val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
        toColumn.putDoubles(0, batchSize, data, 0)

      case StringType | BinaryType =>
        val data = fromColumn.asInstanceOf[BytesColumnVector]
        var index = 0
        while (index < batchSize) {
          toColumn.putByteArray(index, data.vector(index), data.start(index), data.length(index))
          index += 1
        }

      case DecimalType.Fixed(precision, scale) =>
        val data = fromColumn.asInstanceOf[DecimalColumnVector]
        var index = 0
        while (index < batchSize) {
          putDecimalWritable(toColumn, index, precision, scale, data.vector(index))
          index += 1
        }

      case dt =>
        throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
    }
  }

  private def putValues(
      batchSize: Int,
      field: StructField,
      fromColumn: ColumnVector,
      toColumn: WritableColumnVector) : Unit = {
    field.dataType match {
      case BooleanType =>
        val vector = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putBoolean(index, vector(index) == 1)
          }
          index += 1
        }

      case ByteType =>
        val vector = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putByte(index, vector(index).toByte)
          }
          index += 1
        }

      case ShortType =>
        val vector = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putShort(index, vector(index).toShort)
          }
          index += 1
        }

      case IntegerType | DateType =>
        val vector = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putInt(index, vector(index).toInt)
          }
          index += 1
        }

      case LongType =>
        val vector = fromColumn.asInstanceOf[LongColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putLong(index, vector(index))
          }
          index += 1
        }

      case TimestampType =>
        val vector = fromColumn.asInstanceOf[TimestampColumnVector]
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putLong(index, fromTimestampColumnVector(vector, index))
          }
          index += 1
        }

      case FloatType =>
        val vector = fromColumn.asInstanceOf[DoubleColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putFloat(index, vector(index).toFloat)
          }
          index += 1
        }

      case DoubleType =>
        val vector = fromColumn.asInstanceOf[DoubleColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putDouble(index, vector(index))
          }
          index += 1
        }

      case StringType | BinaryType =>
        val vector = fromColumn.asInstanceOf[BytesColumnVector]
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            toColumn.putByteArray(
              index, vector.vector(index), vector.start(index), vector.length(index))
          }
          index += 1
        }

      case DecimalType.Fixed(precision, scale) =>
        val vector = fromColumn.asInstanceOf[DecimalColumnVector].vector
        var index = 0
        while (index < batchSize) {
          if (fromColumn.isNull(index)) {
            toColumn.putNull(index)
          } else {
            putDecimalWritable(toColumn, index, precision, scale, vector(index))
          }
          index += 1
        }

      case dt =>
        throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
    }
  }
}

/**
 * Constants for OrcColumnarBatchReader.
 */
object OrcColumnarBatchReader {
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
   * Put a `HiveDecimalWritable` to a `WritableColumnVector`.
   */
  private def putDecimalWritable(
      toColumn: WritableColumnVector,
      index: Int,
      precision: Int,
      scale: Int,
      decimalWritable: HiveDecimalWritable): Unit = {
    val decimal = decimalWritable.getHiveDecimal()
    val value = Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
    value.changePrecision(precision, scale)

    if (precision <= Decimal.MAX_INT_DIGITS) {
      toColumn.putInt(index, value.toUnscaledLong.toInt)
    } else if (precision <= Decimal.MAX_LONG_DIGITS) {
      toColumn.putLong(index, value.toUnscaledLong)
    } else {
      val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
      toColumn.putByteArray(index, bytes, 0, bytes.length)
    }
  }

  /**
   * Put `HiveDecimalWritable`s to a `WritableColumnVector`.
   */
  private def putDecimalWritables(
      toColumn: WritableColumnVector,
      size: Int,
      precision: Int,
      scale: Int,
      decimalWritable: HiveDecimalWritable): Unit = {
    val decimal = decimalWritable.getHiveDecimal()
    val value = Decimal(decimal.bigDecimalValue, decimal.precision(), decimal.scale())
    value.changePrecision(precision, scale)

    if (precision <= Decimal.MAX_INT_DIGITS) {
      toColumn.putInts(0, size, value.toUnscaledLong.toInt)
    } else if (precision <= Decimal.MAX_LONG_DIGITS) {
      toColumn.putLongs(0, size, value.toUnscaledLong)
    } else {
      val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
      var index = 0
      while (index < size) {
        toColumn.putByteArray(index, bytes, 0, bytes.length)
        index += 1
      }
    }
  }
}

