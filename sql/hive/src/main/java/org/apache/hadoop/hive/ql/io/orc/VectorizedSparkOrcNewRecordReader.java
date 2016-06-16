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

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
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

import java.io.IOException;
import java.util.List;

/**
 * This is based on hive-exec-1.2.1
 * {@link org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat.OrcRecordReader}.
 * This class exposes getObjectInspector which can be used for reducing
 * NameNode calls in OrcRelation.
 */
public class VectorizedSparkOrcNewRecordReader
    extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct>
    implements SparkOrcNewRecordReaderBase {
  private final org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> reader;
  private final int numColumns;
  private VectorizedRowBatch internalValue;
  OrcStruct value;
  private float progress = 0.0f;
  private ObjectInspector objectInspector;
  private List<Integer> columnIDs;

  private final VectorExpressionWriter [] valueWriters;
  private long numRowsOfBatch = 0;
  private int indexOfRow = 0;

  public VectorizedSparkOrcNewRecordReader(
      Reader file,
      JobConf conf,
      FileSplit fileSplit,
      List<Integer> columnIDs) throws IOException {
    List<OrcProto.Type> types = file.getTypes();
    numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
    value = new OrcStruct(numColumns);
    this.reader = new SparkVectorizedOrcRecordReader(file, conf,
      new org.apache.hadoop.mapred.FileSplit(fileSplit));
    this.objectInspector = file.getObjectInspector();
    this.columnIDs = columnIDs;
    this.internalValue = this.reader.createValue();

    try {
      valueWriters = VectorExpressionWriterFactory
          .getExpressionWriters((StructObjectInspector) this.objectInspector);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    this.progress = reader.getProgress();
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
  public OrcStruct getCurrentValue() throws IOException,
      InterruptedException {
    if (indexOfRow >= numRowsOfBatch) {
      return null;
    }
    try {
      for (int p = 0; p < internalValue.numCols; p++) {
        // Only when this column is a required column, we populate the data.
        if (columnIDs.contains(p)) {
          if (internalValue.cols[p].isRepeating) {
            valueWriters[p].setValue(value, internalValue.cols[p], 0);
          } else {
            valueWriters[p].setValue(value, internalValue.cols[p], indexOfRow);
          }
        }
      }
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    indexOfRow++;

    return value;
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
}
