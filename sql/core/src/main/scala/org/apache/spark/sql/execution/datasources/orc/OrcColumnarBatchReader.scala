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

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, ColumnVectorUtils}
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
   * ORC Data Schema.
   */
  private var schema: TypeDescription = _

  /**
   * Vectorized Row Batch.
   */
  private var batch: VectorizedRowBatch = _

  /**
   * Record reader from row batch.
   */
  private var rows: org.apache.orc.RecordReader = _

  /**
   * Spark Schema.
   */
  private var sparkSchema: StructType = _

  /**
   * Required Schema.
   */
  private var requiredSchema: StructType = _

  /**
   * Partition Column.
   */
  private var partitionColumns: StructType = _

  private var useIndex: Boolean = false

  /**
   * Full Schema: requiredSchema + partition schema.
   */
  private var fullSchema: StructType = _

  /**
   * ColumnarBatch for vectorized execution by whole-stage codegen.
   */
  private var columnarBatch: ColumnarBatch = _

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
    schema = reader.getSchema
    sparkSchema = CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]

    batch = schema.createRowBatch(DEFAULT_SIZE)
    totalRowCount = reader.getNumberOfRows
    logDebug(s"totalRowCount = $totalRowCount")

    val options = OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart, fileSplit.getLength)
    rows = reader.rows(options)
  }

  /**
   * Set required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  def setRequiredSchema(
      requiredSchema: StructType,
      partitionColumns: StructType,
      partitionValues: InternalRow,
      useIndex: Boolean): Unit = {
    this.requiredSchema = requiredSchema
    this.partitionColumns = partitionColumns
    this.useIndex = useIndex
    fullSchema = new StructType(requiredSchema.fields ++ partitionColumns.fields)

    columnarBatch = ColumnarBatch.allocate(fullSchema, DEFAULT_MEMORY_MODE, DEFAULT_SIZE)
    if (partitionColumns != null) {
      val partitionIdx = requiredSchema.fields.length
      for (i <- partitionColumns.fields.indices) {
        ColumnVectorUtils.populate(columnarBatch.column(i + partitionIdx), partitionValues, i)
        columnarBatch.column(i + partitionIdx).setIsConstant()
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

    for (i <- 0 until requiredSchema.length) {
      val field = requiredSchema(i)
      val schemaIndex = if (useIndex) i else schema.getFieldNames.indexOf(field.name)
      assert(schemaIndex >= 0)

      val fromColumn = batch.cols(schemaIndex)
      val toColumn = columnarBatch.column(i)

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
              val data = fromColumn.asInstanceOf[TimestampColumnVector].getTimestampAsLong(0)
              toColumn.appendLongs(batchSize, data)

            case FloatType =>
              val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0).toFloat
              toColumn.appendFloats(batchSize, data)
            case DoubleType =>
              val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0)
              toColumn.appendDoubles(batchSize, data)

            case StringType =>
              val data = fromColumn.asInstanceOf[BytesColumnVector]
              for (index <- 0 until batchSize) {
                toColumn.appendByteArray(data.vector(0), data.start(0), data.length(0))
              }
            case BinaryType =>
              val data = fromColumn.asInstanceOf[BytesColumnVector]
              for (index <- 0 until batchSize) {
                toColumn.appendByteArray(data.vector(0), data.start(0), data.length(0))
              }

            case DecimalType.Fixed(precision, _) =>
              val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(0)
              val value = Decimal(d.getHiveDecimal.bigDecimalValue, d.precision(), d.scale)
              if (precision <= Decimal.MAX_INT_DIGITS) {
                toColumn.appendInts(batchSize, value.toUnscaledLong.toInt)
              } else if (precision <= Decimal.MAX_LONG_DIGITS) {
                toColumn.appendLongs(batchSize, value.toUnscaledLong)
              } else {
                val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
                for (index <- 0 until batchSize) {
                  toColumn.appendByteArray(bytes, 0, bytes.length)
                }
              }

            case dt =>
              throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
          }
        }
      } else if (!field.nullable || fromColumn.noNulls) {
        field.dataType match {
          case BooleanType =>
            val data = fromColumn.asInstanceOf[LongColumnVector].vector
            data.foreach { x => toColumn.appendBoolean(x == 1) }

          case ByteType =>
            val data = fromColumn.asInstanceOf[LongColumnVector].vector
            toColumn.appendBytes(batchSize, data.map(_.toByte), 0)
          case ShortType =>
            val data = fromColumn.asInstanceOf[LongColumnVector].vector
            toColumn.appendShorts(batchSize, data.map(_.toShort), 0)
          case IntegerType =>
            val data = fromColumn.asInstanceOf[LongColumnVector].vector
            toColumn.appendInts(batchSize, data.map(_.toInt), 0)
          case LongType =>
            val data = fromColumn.asInstanceOf[LongColumnVector].vector
            toColumn.appendLongs(batchSize, data, 0)

          case DateType =>
            val data = fromColumn.asInstanceOf[LongColumnVector].vector
            toColumn.appendInts(batchSize, data.map(_.toInt), 0)

          case TimestampType =>
            val data = fromColumn.asInstanceOf[TimestampColumnVector]
            for (index <- 0 until batchSize) {
              toColumn.appendLong(data.getTimestampAsLong(index))
            }

          case FloatType =>
            val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
            toColumn.appendFloats(batchSize, data.map(_.toFloat), 0)
          case DoubleType =>
            val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
            toColumn.appendDoubles(batchSize, data, 0)

          case StringType =>
            val data = fromColumn.asInstanceOf[BytesColumnVector]
            for (index <- 0 until batchSize) {
              toColumn.appendByteArray(data.vector(index), data.start(index), data.length(index))
            }
          case BinaryType =>
            val data = fromColumn.asInstanceOf[BytesColumnVector]
            for (index <- 0 until batchSize) {
              toColumn.appendByteArray(data.vector(index), data.start(index), data.length(index))
            }

          case DecimalType.Fixed(precision, _) =>
            val data = fromColumn.asInstanceOf[DecimalColumnVector]
            for (index <- 0 until batchSize) {
              val d = data.vector(index)
              val value = Decimal(d.getHiveDecimal.bigDecimalValue, d.precision(), d.scale)
              if (precision <= Decimal.MAX_INT_DIGITS) {
                toColumn.appendInt(value.toUnscaledLong.toInt)
              } else if (precision <= Decimal.MAX_LONG_DIGITS) {
                toColumn.appendLong(value.toUnscaledLong)
              } else {
                val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
                toColumn.appendByteArray(bytes, 0, bytes.length)
              }
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
              case IntegerType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toInt
                toColumn.appendInt(data)
              case LongType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(index)
                toColumn.appendLong(data)

              case DateType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toInt
                toColumn.appendInt(data)

              case TimestampType =>
                val data = fromColumn.asInstanceOf[TimestampColumnVector]
                  .getTimestampAsLong(index)
                toColumn.appendLong(data)

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

              case DecimalType.Fixed(precision, _) =>
                val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(index)
                val value = Decimal(d.getHiveDecimal.bigDecimalValue, d.precision(), d.scale)
                if (precision <= Decimal.MAX_INT_DIGITS) {
                  toColumn.appendInt(value.toUnscaledLong.toInt)
                } else if (precision <= Decimal.MAX_LONG_DIGITS) {
                  toColumn.appendLong(value.toUnscaledLong)
                } else {
                  val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
                  toColumn.appendByteArray(bytes, 0, bytes.length)
                }

              case dt =>
                throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
            }
          }
        }
      }
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
}

