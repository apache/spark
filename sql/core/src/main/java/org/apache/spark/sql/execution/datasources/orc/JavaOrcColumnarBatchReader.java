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

package org.apache.spark.sql.execution.datasources.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.*;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;


/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 * After creating, `initialize` and `setRequiredSchema` should be called sequentially.
 */
public class JavaOrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  /**
   * ORC File Reader.
   */
  private Reader reader;

  /**
   * Vectorized Row Batch.
   */
  private VectorizedRowBatch batch;

  /**
   * Requested Column IDs.
   */
  private int[] requestedColIds;

  /**
   * Record reader from row batch.
   */
  private org.apache.orc.RecordReader recordReader;

  /**
   * Required Schema.
   */
  private StructType requiredSchema;

  /**
   * ColumnarBatch for vectorized execution by whole-stage codegen.
   */
  private ColumnarBatch columnarBatch;

  /**
   * Writable column vectors of ColumnarBatch.
   */
  private WritableColumnVector[] columnVectors;

  /**
   * The number of rows read and considered to be returned.
   */
  private long rowsReturned = 0L;

  /**
   * Total number of rows.
   */
  private long totalRowCount = 0L;

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public ColumnarBatch getCurrentValue() throws IOException, InterruptedException {
    return columnarBatch;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) rowsReturned / totalRowCount;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return nextBatch();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `setRequiredSchema` is needed to be called after this.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit)inputSplit;
    Configuration conf = taskAttemptContext.getConfiguration();
    reader = OrcFile.createReader(
      fileSplit.getPath(),
      OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
        .filesystem(fileSplit.getPath().getFileSystem(conf)));

    Reader.Options options =
            OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart(), fileSplit.getLength());
    recordReader = reader.rows(options);
    totalRowCount = reader.getNumberOfRows();
  }

  /**
   * Set required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  public void setRequiredSchema(
    TypeDescription orcSchema,
    int[] requestedColIds,
    StructType requiredSchema,
    StructType partitionSchema,
    InternalRow partitionValues) {
    batch = orcSchema.createRowBatch(DEFAULT_SIZE);
    assert(!batch.selectedInUse); // `selectedInUse` should be initialized with `false`.

    StructType resultSchema = new StructType(requiredSchema.fields());
    for (StructField f : partitionSchema.fields())
      resultSchema = resultSchema.add(f);
    this.requiredSchema = requiredSchema;
    this.requestedColIds = requestedColIds;

    int capacity = DEFAULT_SIZE;
    if (DEFAULT_MEMORY_MODE == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, resultSchema);
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, resultSchema);
    }
    columnarBatch = new ColumnarBatch(resultSchema, columnVectors, capacity);

    if (partitionValues.numFields() > 0) {
      int partitionIdx = requiredSchema.fields().length;
      for (int i = 0; i < partitionValues.numFields(); i++) {
        ColumnVectorUtils.populate(columnVectors[i + partitionIdx], partitionValues, i);
        columnVectors[i + partitionIdx].setIsConstant();
      }
    }

    // Initialize the missing columns once.
    for (int i = 0; i < requiredSchema.length(); i++) {
      if (requestedColIds[i] < 0) {
        columnVectors[i].putNulls(0, columnarBatch.capacity());
        columnVectors[i].setIsConstant();
      }
    }
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  private boolean nextBatch() throws IOException {
    if (rowsReturned >= totalRowCount) {
      return false;
    }

    recordReader.nextBatch(batch);
    int batchSize = batch.size;
    if (batchSize == 0) {
      return false;
    }
    rowsReturned += batchSize;
    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(batchSize);
    int i = 0;
    while (i < requiredSchema.length()) {
      StructField field = requiredSchema.fields()[i];
      WritableColumnVector toColumn = columnVectors[i];

      if (requestedColIds[i] < 0) {
        toColumn.appendNulls(batchSize);
      } else {
        ColumnVector fromColumn = batch.cols[requestedColIds[i]];

        if (fromColumn.isRepeating) {
          if (fromColumn.isNull[0]) {
            toColumn.appendNulls(batchSize);
          } else {
            DataType type = field.dataType();
            if (type instanceof BooleanType) {
              toColumn.appendBooleans(batchSize, ((LongColumnVector)fromColumn).vector[0] == 1);
            } else if (type instanceof ByteType) {
              toColumn.appendBytes(batchSize, (byte)((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof ShortType) {
              toColumn.appendShorts(batchSize, (short)((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof IntegerType || type instanceof DateType) {
              toColumn.appendInts(batchSize, (int)((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof LongType) {
              toColumn.appendLongs(batchSize, ((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof TimestampType) {
              toColumn.appendLongs(batchSize, fromTimestampColumnVector((TimestampColumnVector)fromColumn, 0));
            } else if (type instanceof FloatType) {
              toColumn.appendFloats(batchSize, (float)((DoubleColumnVector)fromColumn).vector[0]);
            } else if (type instanceof DoubleType) {
              toColumn.appendDoubles(batchSize, ((DoubleColumnVector)fromColumn).vector[0]);
            } else if (type instanceof StringType || type instanceof BinaryType) {
              BytesColumnVector data = (BytesColumnVector)fromColumn;
              int index = 0;
              while (index < batchSize) {
                toColumn.appendByteArray(data.vector[0], data.start[0], data.length[0]);
                index += 1;
              }
            } else if (type instanceof DecimalType) {
              DecimalType decimalType = (DecimalType)type;
              appendDecimalWritable(
                toColumn,
                decimalType.precision(),
                decimalType.scale(),
                ((DecimalColumnVector)fromColumn).vector[0]);
            } else {
              throw new UnsupportedOperationException("Unsupported Data Type: " + type);
            }
          }
        } else if (fromColumn.noNulls) {
          DataType type = field.dataType();
          if (type instanceof BooleanType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              toColumn.appendBoolean(data[index] == 1);
              index += 1;
            }
          } else if (type instanceof ByteType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              toColumn.appendByte((byte)data[index]);
              index += 1;
            }
          } else if (type instanceof ShortType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              toColumn.appendShort((short)data[index]);
              index += 1;
            }
          } else if (type instanceof IntegerType || type instanceof DateType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              toColumn.appendInt((int)data[index]);
              index += 1;
            }
          } else if (type instanceof LongType) {
            toColumn.appendLongs(batchSize, ((LongColumnVector)fromColumn).vector, 0);
          } else if (type instanceof TimestampType) {
            TimestampColumnVector data = ((TimestampColumnVector)fromColumn);
            int index = 0;
            while (index < batchSize) {
              toColumn.appendLong(fromTimestampColumnVector(data, index));
              index += 1;
            }
          } else if (type instanceof FloatType) {
            double[] data = ((DoubleColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              toColumn.appendFloat((float)data[index]);
              index += 1;
            }
          } else if (type instanceof DoubleType) {
            toColumn.appendDoubles(batchSize, ((DoubleColumnVector)fromColumn).vector, 0);
          } else if (type instanceof StringType || type instanceof BinaryType) {
            BytesColumnVector data = ((BytesColumnVector)fromColumn);
            int index = 0;
            while (index < batchSize) {
              toColumn.appendByteArray(data.vector[index], data.start[index], data.length[index]);
              index += 1;
            }
          } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType)type;
            DecimalColumnVector data = ((DecimalColumnVector)fromColumn);
            int index = 0;
            while (index < batchSize) {
              appendDecimalWritable(
                toColumn,
                decimalType.precision(),
                decimalType.scale(),
                data.vector[index]);
              index += 1;
            }
          } else {
            throw new UnsupportedOperationException("Unsupported Data Type: " + type);
          }
        } else {
          DataType type = field.dataType();
          if (type instanceof BooleanType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendBoolean(vector[index] == 1);
              }
              index += 1;
            }
          } else if (type instanceof ByteType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendByte((byte)vector[index]);
              }
              index += 1;
            }
          } else if (type instanceof ShortType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendShort((short)vector[index]);
              }
              index += 1;
            }
          } else if (type instanceof IntegerType || type instanceof DateType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendInt((int)vector[index]);
              }
              index += 1;
            }
          } else if (type instanceof LongType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendLong(vector[index]);
              }
              index += 1;
            }
          } else if (type instanceof TimestampType) {
            TimestampColumnVector vector = ((TimestampColumnVector)fromColumn);
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendLong(fromTimestampColumnVector(vector, index));
              }
              index += 1;
            }
          } else if (type instanceof FloatType) {
            double[] vector = ((DoubleColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendFloat((float)vector[index]);
              }
              index += 1;
            }
          } else if (type instanceof DoubleType) {
            double[] vector = ((DoubleColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendDouble(vector[index]);
              }
              index += 1;
            }
          } else if (type instanceof StringType || type instanceof BinaryType) {
            BytesColumnVector vector = (BytesColumnVector)fromColumn;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                toColumn.appendByteArray(
                  vector.vector[index], vector.start[index], vector.length[index]);
              }
              index += 1;
            }
          } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType)type;
            HiveDecimalWritable[] vector = ((DecimalColumnVector)fromColumn).vector;
            int index = 0;
            while (index < batchSize) {
              if (fromColumn.isNull[index]) {
                toColumn.appendNull();
              } else {
                appendDecimalWritable(
                  toColumn,
                  decimalType.precision(),
                  decimalType.scale(),
                  vector[index]);
              }
              index += 1;
            }
          } else {
            throw new UnsupportedOperationException("Unsupported Data Type: " + type);
          }
        }
      }
      i += 1;
    }
    return true;
  }

  /**
   * Default memory mode for ColumnarBatch.
   */
  public static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP;

  /**
   * The default size of batch. We use this value for both ORC and Spark consistently
   * because they have different default values like the following.
   *
   * - ORC's VectorizedRowBatch.DEFAULT_SIZE = 1024
   * - Spark's ColumnarBatch.DEFAULT_BATCH_SIZE = 4 * 1024
   */
  public static final int DEFAULT_SIZE = 4 * 1024;

  /**
   * Returns the number of micros since epoch from an element of TimestampColumnVector.
   */
  private static long fromTimestampColumnVector(TimestampColumnVector vector, int index) {
    return vector.time[index] * 1000L + vector.nanos[index] / 1000L;
  }

  /**
   * Append a `HiveDecimalWritable` to a `WritableColumnVector`.
   */
  private static void appendDecimalWritable(
      WritableColumnVector toColumn,
      int precision,
      int scale,
      HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
      Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.appendInt((int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.appendLong(value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.appendByteArray(bytes, 0, bytes.length);
    }
  }
}
