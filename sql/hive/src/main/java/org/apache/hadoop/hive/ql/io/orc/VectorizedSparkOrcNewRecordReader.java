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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A RecordReader that returns InternalRow for Spark SQL execution.
 * This reader uses an internal reader that returns Hive's VectorizedRowBatch. An adapter
 * class is used to return internal row by directly accessing data in column vectors.
 */
public class VectorizedSparkOrcNewRecordReader
    extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, InternalRow> {
  private final org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> reader;
  private final int numColumns;
  private VectorizedRowBatch internalValue;
  private float progress = 0.0f;
  private List<Integer> columnIDs;

  private long numRowsOfBatch = 0;
  private int indexOfRow = 0;

  private final Row row;

  public VectorizedSparkOrcNewRecordReader(
      Reader file,
      Configuration conf,
      FileSplit fileSplit,
      List<Integer> columnIDs) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    this.reader = new SparkVectorizedOrcRecordReader(file, conf,
      new org.apache.hadoop.mapred.FileSplit(fileSplit));

    this.columnIDs = new ArrayList<>(columnIDs);
    this.internalValue = this.reader.createValue();
    this.progress = reader.getProgress();
    this.row = new Row(this.internalValue.cols, this.columnIDs);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public InternalRow getCurrentValue() throws IOException, InterruptedException {
    if (indexOfRow >= numRowsOfBatch) {
      return null;
    }
    row.rowId = indexOfRow;
    indexOfRow++;

    return row;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return progress;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (indexOfRow == numRowsOfBatch) {
      if (reader.next(NullWritable.get(), internalValue)) {
        if (internalValue.endOfFile) {
          progress = 1.0f;
          numRowsOfBatch = 0;
          indexOfRow = 0;
          return false;
        } else {
          assert internalValue.numCols == numColumns : "Incorrect number of columns in OrcBatch";
          numRowsOfBatch = internalValue.count();
          indexOfRow = 0;
          progress = reader.getProgress();
        }
        return true;
      } else {
        return false;
      }
    } else {
      if (indexOfRow < numRowsOfBatch) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Adapter class to return an internal row.
   */
  public static final class Row extends InternalRow {
    protected int rowId;
    private List<Integer> columnIDs;
    private final ColumnVector[] columns;

    private Row(ColumnVector[] columns, List<Integer> columnIDs) {
      this.columns = columns;
      this.columnIDs = columnIDs;
    }

    @Override
    public int numFields() { return columnIDs.size(); }

    @Override
    public boolean anyNull() {
      for (int i = 0; i < columns.length; i++) {
        if (columnIDs.contains(i)) {
          boolean isRepeating = columns[i].isRepeating;
          if (isRepeating && columns[i].isNull[0]) {
            return true;
          } else if (!isRepeating && columns[i].isNull[rowId]) {
            return true;
          }
        }
      }
      return false;
    }

    private int getColIndex(ColumnVector col) {
      return col.isRepeating ? 0 : rowId;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      ColumnVector col = columns[columnIDs.get(ordinal)];
      return col.isNull[getColIndex(col)];
    }

    @Override
    public boolean getBoolean(int ordinal) {
      LongColumnVector col = (LongColumnVector) columns[columnIDs.get(ordinal)];
      return col.vector[getColIndex(col)] > 0;
    }

    @Override
    public byte getByte(int ordinal) {
      LongColumnVector col = (LongColumnVector) columns[columnIDs.get(ordinal)];
      return (byte)col.vector[getColIndex(col)];
    }

    @Override
    public short getShort(int ordinal) {
      LongColumnVector col = (LongColumnVector) columns[columnIDs.get(ordinal)];
      return (short)col.vector[getColIndex(col)];
    }

    @Override
    public int getInt(int ordinal) {
      LongColumnVector col = (LongColumnVector) columns[columnIDs.get(ordinal)];
      return (int)col.vector[getColIndex(col)];
    }

    @Override
    public long getLong(int ordinal) {
      LongColumnVector col = (LongColumnVector) columns[columnIDs.get(ordinal)];
      return (long)col.vector[getColIndex(col)];
    }

    @Override
    public float getFloat(int ordinal) {
      DoubleColumnVector col = (DoubleColumnVector) columns[columnIDs.get(ordinal)];
      return (float)col.vector[getColIndex(col)];
    }

    @Override
    public double getDouble(int ordinal) {
      DoubleColumnVector col = (DoubleColumnVector) columns[columnIDs.get(ordinal)];
      return (double)col.vector[getColIndex(col)];
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      DecimalColumnVector col = (DecimalColumnVector) columns[columnIDs.get(ordinal)];
      int index = getColIndex(col);
      return Decimal.apply(col.vector[index].getHiveDecimal().bigDecimalValue(), precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      BytesColumnVector col = ((BytesColumnVector) columns[columnIDs.get(ordinal)]);
      int index = getColIndex(col);
      return UTF8String.fromBytes(col.vector[index], col.start[index], col.length[index]);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      BytesColumnVector col = (BytesColumnVector) columns[columnIDs.get(ordinal)];
      int index = getColIndex(col);
      byte[] binary = new byte[col.length[index]];
      System.arraycopy(col.vector[index], col.start[index], binary, 0, binary.length);
      return binary;
    }

    /**
     * The data type CalendarInterval is not suppported due to the Hive version used by Spark
     * internally. When we upgrade to newer Hive versions in the future, this is possibly to
     * support.
     */
    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new NotImplementedException();
    }

    /**
     * The data type CalendarInterval is not suppported due to the Hive version used by Spark
     * internally. When we upgrade to newer Hive versions in the future, this is possibly to
     * be supported.
     */
    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      throw new NotImplementedException();
    }

    /**
     * The data type Array is not suppported due to the Hive version used by Spark internally.
     * When we upgrade to newer Hive versions in the future, this is possibly to be supported.
     */
    @Override
    public ArrayData getArray(int ordinal) {
      throw new NotImplementedException();
    }

    /**
     * The data type Map is not suppported due to the Hive version used by Spark internally.
     * When we upgrade to newer Hive versions in the future, this is possibly to be supported.
     */
    @Override
    public MapData getMap(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      throw new NotImplementedException();
    }

    @Override
    public InternalRow copy() {
      throw new NotImplementedException();
    }
  }
}
