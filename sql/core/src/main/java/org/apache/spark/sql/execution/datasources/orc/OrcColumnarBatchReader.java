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

import com.google.common.annotations.VisibleForTesting;
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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.orc.OrcShimUtils.VectorizedRowBatchWrap;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;


/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 * After creating, `initialize` and `initBatch` should be called sequentially.
 */
public class OrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.
  private int capacity;

  // Vectorized ORC Row Batch wrap.
  private VectorizedRowBatchWrap wrap;

  /**
   * The column IDs of the physical ORC file schema which are required by this reader.
   * -1 means this required column is partition column, or it doesn't exist in the ORC file.
   * Ideally partition column should never appear in the physical file, and should only appear
   * in the directory name. However, Spark allows partition columns inside physical file,
   * but Spark will discard the values from the file, and use the partition value got from
   * directory name. The column order will be reserved though.
   */
  @VisibleForTesting
  public int[] requestedDataColIds;

  // Record reader from ORC row batch.
  private org.apache.orc.RecordReader recordReader;

  private StructField[] requiredFields;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  @VisibleForTesting
  public ColumnarBatch columnarBatch;

  // The wrapped ORC column vectors.
  private org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  public OrcColumnarBatchReader(int capacity) {
    this.capacity = capacity;
  }


  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public ColumnarBatch getCurrentValue() {
    return columnarBatch;
  }

  @Override
  public float getProgress() throws IOException {
    return recordReader.getProgress();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
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
  public void initialize(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    FileSplit fileSplit = (FileSplit)inputSplit;
    Configuration conf = taskAttemptContext.getConfiguration();
    Reader reader = OrcFile.createReader(
      fileSplit.getPath(),
      OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
        .filesystem(fileSplit.getPath().getFileSystem(conf)));
    Reader.Options options =
      OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart(), fileSplit.getLength());
    recordReader = reader.rows(options);
  }

  /**
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   *
   * @param orcSchema Schema from ORC file reader.
   * @param requiredFields All the fields that are required to return, including partition fields.
   * @param requestedDataColIds Requested column ids from orcSchema. -1 if not existed.
   * @param requestedPartitionColIds Requested column ids from partition schema. -1 if not existed.
   * @param partitionValues Values of partition columns.
   */
  public void initBatch(
      TypeDescription orcSchema,
      StructField[] requiredFields,
      int[] requestedDataColIds,
      int[] requestedPartitionColIds,
      InternalRow partitionValues) {
    wrap = new VectorizedRowBatchWrap(orcSchema.createRowBatch(capacity));
    assert(!wrap.batch().selectedInUse); // `selectedInUse` should be initialized with `false`.
    assert(requiredFields.length == requestedDataColIds.length);
    assert(requiredFields.length == requestedPartitionColIds.length);
    // If a required column is also partition column, use partition value and don't read from file.
    for (int i = 0; i < requiredFields.length; i++) {
      if (requestedPartitionColIds[i] != -1) {
        requestedDataColIds[i] = -1;
      }
    }
    this.requiredFields = requiredFields;
    this.requestedDataColIds = requestedDataColIds;

    StructType resultSchema = new StructType(requiredFields);

    // Just wrap the ORC column vector instead of copying it to Spark column vector.
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

    StructType requiredSchema = new StructType(requiredFields);
    for (int i = 0; i < requiredFields.length; i++) {
      DataType dt = requiredFields[i].dataType();
      if (requestedPartitionColIds[i] != -1) {
        ConstantColumnVector partitionCol = new ConstantColumnVector(capacity, dt);
        ColumnVectorUtils.populate(partitionCol, partitionValues, requestedPartitionColIds[i]);
        orcVectorWrappers[i] = partitionCol;
      } else {
        int colId = requestedDataColIds[i];
        // Initialize the missing columns once.
        if (colId == -1) {
          OnHeapColumnVector missingCol = new OnHeapColumnVector(capacity, dt);
          // Check if the missing column has an associated default value in the schema metadata.
          // If so, fill the corresponding column vector with the value.
          Object defaultValue = requiredSchema.existenceDefaultValues()[i];
          if (defaultValue == null) {
            missingCol.putNulls(0, capacity);
          } else if (!missingCol.appendObjects(capacity, defaultValue).isPresent()) {
            throw new IllegalArgumentException("Cannot assign default column value to result " +
              "column batch in vectorized Orc reader because the data type is not supported: " +
              defaultValue);
          }
          missingCol.setIsConstant();
          orcVectorWrappers[i] = missingCol;
        } else {
          orcVectorWrappers[i] = OrcColumnVectorUtils.toOrcColumnVector(
            dt, wrap.batch().cols[colId]);
        }
      }
    }

    columnarBatch = new ColumnarBatch(orcVectorWrappers);
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  private boolean nextBatch() throws IOException {
    recordReader.nextBatch(wrap.batch());
    int batchSize = wrap.batch().size;
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < requiredFields.length; i++) {
      if (requestedDataColIds[i] != -1) {
        ((OrcColumnVector) orcVectorWrappers[i]).setBatchSize(batchSize);
      }
    }
    return true;
  }
}
