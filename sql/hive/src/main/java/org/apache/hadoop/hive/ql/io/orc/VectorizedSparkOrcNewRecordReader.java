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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A RecordReader that returns ColumnarBatch for Spark SQL execution.
 * This reader uses an internal reader that returns Hive's VectorizedRowBatch.
 */
public class VectorizedSparkOrcNewRecordReader
    extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, Object> {
  private final org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> reader;
  private final int numColumns;
  private VectorizedRowBatch hiveBatch;
  private float progress = 0.0f;
  private List<Integer> columnIDs;

  private ColumnVector[] orcColumns;
  private ColumnarBatch columnarBatch;;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  private long numRowsOfBatch = 0;
  private int indexOfRow = 0;

  public VectorizedSparkOrcNewRecordReader(
      Reader file,
      Configuration conf,
      FileSplit fileSplit,
      List<Integer> columnIDs,
      StructType requiredSchema,
      StructType partitionColumns,
      InternalRow partitionValues) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    this.reader = new SparkVectorizedOrcRecordReader(file, conf,
      new org.apache.hadoop.mapred.FileSplit(fileSplit), columnIDs);

    this.hiveBatch = this.reader.createValue();

    this.columnIDs = new ArrayList<>(columnIDs);
    this.orcColumns = new ColumnVector[columnIDs.size() + partitionValues.numFields()];

    // Allocate Spark ColumnVectors for data columns.
    for (int i = 0; i < columnIDs.size(); i++) {
      org.apache.hadoop.hive.ql.exec.vector.ColumnVector col =
        this.hiveBatch.cols[columnIDs.get(i)];
      this.orcColumns[i] = new OrcColumnVector(col, requiredSchema.fields()[i].dataType());
    }

    // Allocate Spark ColumnVectors for partition columns.
    if (partitionValues.numFields() > 0) {
      int i = 0;
      int base = columnIDs.size();
      for (StructField f : partitionColumns.fields()) {
        // Use onheap for partition column vectors.
        ColumnVector col = ColumnVector.allocate(
          VectorizedRowBatch.DEFAULT_SIZE,
          f.dataType(),
          MemoryMode.ON_HEAP);
        ColumnVectorUtils.populate(col, partitionValues, i);
        col.setIsConstant();
        this.orcColumns[base + i] = col;
        i++;
      }
    }

    // Allocate Spark ColumnBatch
    this.columnarBatch = new ColumnarBatch(this.orcColumns, VectorizedRowBatch.DEFAULT_SIZE);

    this.progress = reader.getProgress();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  /*
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) return this.columnarBatch;
    return columnarBatch.getRow(indexOfRow - 1);
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
    if (returnColumnarBatch) return nextBatch();

    if (indexOfRow >= numRowsOfBatch) {
      return nextBatch();
    } else {
      indexOfRow++;
      return true;
    }
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException, InterruptedException {
    if (reader.next(NullWritable.get(), hiveBatch)) {
      if (hiveBatch.endOfFile) {
        progress = 1.0f;
        numRowsOfBatch = 0;
        columnarBatch.setNumRows((int) numRowsOfBatch);
        indexOfRow = 0;
        return false;
      } else {
        assert hiveBatch.numCols == numColumns : "Incorrect number of columns in the current batch";
        numRowsOfBatch = hiveBatch.count();
        columnarBatch.setNumRows((int) numRowsOfBatch);
        indexOfRow = 0;
        progress = reader.getProgress();
        return true;
      }
    } else {
      return false;
    }
  }
}
