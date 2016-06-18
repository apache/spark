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
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
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
 * This is based on hive-exec-1.2.1
 * {@link org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat.OrcRecordReader}.
 * This class exposes getObjectInspector which can be used for reducing
 * NameNode calls in OrcRelation.
 */
public class VectorizedSparkOrcNewRecordReader
    extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, InternalRow>
    implements SparkOrcNewRecordReaderBase {
  private final org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> reader;
  private final int numColumns;
  private VectorizedRowBatch internalValue;
  private float progress = 0.0f;
  private ObjectInspector objectInspector;
  private List<Integer> columnIDs;

  private long numRowsOfBatch = 0;
  private int indexOfRow = 0;

  private final Row row;

  public VectorizedSparkOrcNewRecordReader(
      Reader file,
      JobConf conf,
      FileSplit fileSplit,
      List<Integer> columnIDs) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    this.reader = new SparkVectorizedOrcRecordReader(file, conf,
      new org.apache.hadoop.mapred.FileSplit(fileSplit));

    this.objectInspector = file.getObjectInspector();
    this.columnIDs = columnIDs;
    this.internalValue = this.reader.createValue();
    this.progress = reader.getProgress();
    this.row = new Row(this.internalValue.cols, columnIDs);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException,
      InterruptedException {
    return NullWritable.get();
  }

  @Override
  public InternalRow getCurrentValue() throws IOException,
      InterruptedException {
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
    if (indexOfRow == numRowsOfBatch && progress < 1.0f) {
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

  @Override
  public ObjectInspector getObjectInspector() {
    return objectInspector;
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
    public int numFields() { return columns.length; }

    @Override
    public boolean anyNull() {
      for (int i = 0; i < columns.length; i++) {
        if (columnIDs.contains(i)) {
          if (columns[i].isRepeating && columns[i].isNull[0]) {
            return true;
          } else if (!columns[i].isRepeating && columns[i].isNull[rowId]) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      ColumnVector col = columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return col.isNull[0];
      } else {
        return col.isNull[rowId];
      }
    }

    @Override
    public boolean getBoolean(int ordinal) {
      LongColumnVector col = (LongColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return col.vector[0] > 0;
      } else {
        return col.vector[rowId] > 0;
      }
    }

    @Override
    public byte getByte(int ordinal) {
      LongColumnVector col = (LongColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (byte)col.vector[0];
      } else {
        return (byte)col.vector[rowId];
      }
    }

    @Override
    public short getShort(int ordinal) {
      LongColumnVector col = (LongColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (short)col.vector[0];
      } else {
        return (short)col.vector[rowId];
      }
    }

    @Override
    public int getInt(int ordinal) {
      LongColumnVector col = (LongColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (int)col.vector[0];
      } else {
        return (int)col.vector[rowId];
      }
    }

    @Override
    public long getLong(int ordinal) {
      LongColumnVector col = (LongColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (long)col.vector[0];
      } else {
        return (long)col.vector[rowId];
      }
    }

    @Override
    public float getFloat(int ordinal) {
      DoubleColumnVector col = (DoubleColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (float)col.vector[0];
      } else {
        return (float)col.vector[rowId];
      }
    }

    @Override
    public double getDouble(int ordinal) {
      DoubleColumnVector col = (DoubleColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (double)col.vector[0];
      } else {
        return (double)col.vector[rowId];
      }
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      DecimalColumnVector col = (DecimalColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return Decimal.apply(col.vector[0].getHiveDecimal().bigDecimalValue(), precision, scale);
      } else {
        return Decimal.apply(col.vector[rowId].getHiveDecimal().bigDecimalValue(),
          precision, scale);
      }
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      BytesColumnVector bv = ((BytesColumnVector)columns[columnIDs.get(ordinal)]);
      String str = null;
      if (bv.isRepeating) {
        str = new String(bv.vector[0], bv.start[0], bv.length[0], StandardCharsets.UTF_8);
      } else {
        str = new String(bv.vector[rowId], bv.start[rowId], bv.length[rowId],
          StandardCharsets.UTF_8);
      }
      return UTF8String.fromString(str);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      BytesColumnVector col = (BytesColumnVector)columns[columnIDs.get(ordinal)];
      if (col.isRepeating) {
        return (byte[])col.vector[0];
      } else {
        return (byte[])col.vector[rowId];
      }
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      throw new NotImplementedException();
    }

    @Override
    public ArrayData getArray(int ordinal) {
      throw new NotImplementedException();
    }

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
