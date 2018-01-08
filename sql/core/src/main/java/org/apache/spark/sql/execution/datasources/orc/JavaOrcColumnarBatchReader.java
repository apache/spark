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
 * After creating, `initialize` and `initBatch` should be called sequentially.
 */
public class JavaOrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // ORC File Reader
  private Reader reader;

  // Vectorized ORC Row Batch
  private VectorizedRowBatch batch;

  /**
   * The column IDs of the physical ORC file schema which are required by this reader.
   * -1 means this required column doesn't exist in the ORC file.
   */
  private int[] requestedColIds;

  // Record reader from ORC row batch.
  private org.apache.orc.RecordReader recordReader;

  private StructType requiredSchema;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  private ColumnarBatch columnarBatch;

  // Writable column vectors of the result columnar batch.
  private WritableColumnVector[] columnVectors;

  // The number of rows read and considered to be returned.
  private long rowsReturned = 0L;

  private long totalRowCount = 0L;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public JavaOrcColumnarBatchReader(boolean useOffHeap) {
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
  }


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
   * Please note that `initBatch` is needed to be called after this.
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
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  public void initBatch(
      TypeDescription orcSchema,
      int[] requestedColIds,
      StructType requiredSchema,
      StructType partitionSchema,
      InternalRow partitionValues) {
    batch = orcSchema.createRowBatch(DEFAULT_SIZE);
    assert(!batch.selectedInUse); // `selectedInUse` should be initialized with `false`.

    StructType resultSchema = new StructType(requiredSchema.fields());
    for (StructField f : partitionSchema.fields()) {
      resultSchema = resultSchema.add(f);
    }
    this.requiredSchema = requiredSchema;
    this.requestedColIds = requestedColIds;
    assert(requiredSchema.length() == requestedColIds.length);

    int capacity = DEFAULT_SIZE;
    if (MEMORY_MODE == MemoryMode.OFF_HEAP) {
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
      if (requestedColIds[i] == -1) {
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
    for (int i = 0; i < requiredSchema.length(); i++) {
      StructField field = requiredSchema.fields()[i];
      WritableColumnVector toColumn = columnVectors[i];

      if (requestedColIds[i] >= 0) {
        ColumnVector fromColumn = batch.cols[requestedColIds[i]];
        toColumn.reserve(batchSize);

        if (fromColumn.isRepeating) {
          if (fromColumn.isNull[0]) {
            toColumn.putNulls(0, batchSize);
          } else {
            DataType type = field.dataType();
            if (type instanceof BooleanType) {
              toColumn.putBooleans(0, batchSize, ((LongColumnVector)fromColumn).vector[0] == 1);
            } else if (type instanceof ByteType) {
              toColumn.putBytes(0, batchSize, (byte)((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof ShortType) {
              toColumn.putShorts(0, batchSize, (short)((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof IntegerType || type instanceof DateType) {
              toColumn.putInts(0, batchSize, (int)((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof LongType) {
              toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector[0]);
            } else if (type instanceof TimestampType) {
              toColumn.putLongs(0, batchSize, fromTimestampColumnVector((TimestampColumnVector)fromColumn, 0));
            } else if (type instanceof FloatType) {
              toColumn.putFloats(0, batchSize, (float)((DoubleColumnVector)fromColumn).vector[0]);
            } else if (type instanceof DoubleType) {
              toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector[0]);
            } else if (type instanceof StringType || type instanceof BinaryType) {
              BytesColumnVector data = (BytesColumnVector)fromColumn;
              for (int index = 0; index < batchSize; index++) {
                toColumn.putByteArray(index, data.vector[0], data.start[0], data.length[0]);
              }
            } else if (type instanceof DecimalType) {
              DecimalType decimalType = (DecimalType)type;
              putDecimalWritables(
                toColumn,
                batchSize,
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
            for (int index = 0; index < batchSize; index++) {
              toColumn.putBoolean(index, data[index] == 1);
            }
          } else if (type instanceof ByteType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              toColumn.putByte(index, (byte)data[index]);
            }
          } else if (type instanceof ShortType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              toColumn.putShort(index, (short)data[index]);
            }
          } else if (type instanceof IntegerType || type instanceof DateType) {
            long[] data = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              toColumn.putInt(index, (int)data[index]);
            }
          } else if (type instanceof LongType) {
            toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector, 0);
          } else if (type instanceof TimestampType) {
            TimestampColumnVector data = ((TimestampColumnVector)fromColumn);
            for (int index = 0; index < batchSize; index++) {
              toColumn.putLong(index, fromTimestampColumnVector(data, index));
            }
          } else if (type instanceof FloatType) {
            double[] data = ((DoubleColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              toColumn.putFloat(index, (float)data[index]);
            }
          } else if (type instanceof DoubleType) {
            toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector, 0);
          } else if (type instanceof StringType || type instanceof BinaryType) {
            BytesColumnVector data = ((BytesColumnVector)fromColumn);
            for (int index = 0; index < batchSize; index++) {
              toColumn.putByteArray(index, data.vector[index], data.start[index], data.length[index]);
            }
          } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType)type;
            DecimalColumnVector data = ((DecimalColumnVector)fromColumn);
            for (int index = 0; index < batchSize; index++) {
              putDecimalWritable(
                toColumn,
                index,
                decimalType.precision(),
                decimalType.scale(),
                data.vector[index]);
            }
          } else {
            throw new UnsupportedOperationException("Unsupported Data Type: " + type);
          }
        } else {
          DataType type = field.dataType();
          if (type instanceof BooleanType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putBoolean(index, vector[index] == 1);
              }
            }
          } else if (type instanceof ByteType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putByte(index, (byte)vector[index]);
              }
            }
          } else if (type instanceof ShortType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putShort(index, (short)vector[index]);
              }
            }
          } else if (type instanceof IntegerType || type instanceof DateType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putInt(index, (int)vector[index]);
              }
            }
          } else if (type instanceof LongType) {
            long[] vector = ((LongColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putLong(index, vector[index]);
              }
            }
          } else if (type instanceof TimestampType) {
            TimestampColumnVector vector = ((TimestampColumnVector)fromColumn);
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putLong(index, fromTimestampColumnVector(vector, index));
              }
            }
          } else if (type instanceof FloatType) {
            double[] vector = ((DoubleColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putFloat(index, (float)vector[index]);
              }
            }
          } else if (type instanceof DoubleType) {
            double[] vector = ((DoubleColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putDouble(index, vector[index]);
              }
            }
          } else if (type instanceof StringType || type instanceof BinaryType) {
            BytesColumnVector vector = (BytesColumnVector)fromColumn;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                toColumn.putByteArray(
                  index, vector.vector[index], vector.start[index], vector.length[index]);
              }
            }
          } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType)type;
            HiveDecimalWritable[] vector = ((DecimalColumnVector)fromColumn).vector;
            for (int index = 0; index < batchSize; index++) {
              if (fromColumn.isNull[index]) {
                toColumn.putNull(index);
              } else {
                putDecimalWritable(
                  toColumn,
                  index,
                  decimalType.precision(),
                  decimalType.scale(),
                  vector[index]);
              }
            }
          } else {
            throw new UnsupportedOperationException("Unsupported Data Type: " + type);
          }
        }
      }
    }
    return true;
  }

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
   * Put a `HiveDecimalWritable` to a `WritableColumnVector`.
   */
  private static void putDecimalWritable(
      WritableColumnVector toColumn,
      int index,
      int precision,
      int scale,
      HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
      Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInt(index, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLong(index, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.putByteArray(index, bytes, 0, bytes.length);
    }
  }

  /**
   * Put `HiveDecimalWritable`s to a `WritableColumnVector`.
   */
  private static void putDecimalWritables(
      WritableColumnVector toColumn,
      int size,
      int precision,
      int scale,
      HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
            Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInts(0, size, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLongs(0, size, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      for (int index = 0; index < size; index++) {
        toColumn.putByteArray(index, bytes, 0, bytes.length);
      }
    }
  }
}
