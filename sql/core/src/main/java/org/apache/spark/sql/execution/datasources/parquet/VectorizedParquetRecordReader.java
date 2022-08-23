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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import scala.collection.JavaConverters;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * Parquet column APIs. This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 *
 * This class can either return InternalRows or ColumnarBatches. With whole stage codegen
 * enabled, this class returns ColumnarBatches which offers significant performance gains.
 * TODO: make this always return ColumnarBatches.
 */
public class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object> {

  // The capacity of vectorized batch.
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * Encapsulate writable column vectors with other Parquet related info such as
   * repetition / definition levels.
   */
  private ParquetColumnVector[] columnVectors;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * For each leaf column, if it is in the set, it means the column is missing in the file and
   * we'll instead return NULLs.
   */
  private Set<ParquetColumn> missingColumns;

  /**
   * The timezone that timestamp INT96 values should be converted to. Null if no conversion. Here to
   * workaround incompatibilities between different engines when writing timestamp values.
   */
  private final ZoneId convertTz;

  /**
   * The mode of rebasing date/timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String datetimeRebaseMode;
  // The time zone Id in which rebasing of date/timestamp is performed
  private final String datetimeRebaseTz;

  /**
   * The mode of rebasing INT96 timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String int96RebaseMode;
  // The time zone Id in which rebasing of INT96 is performed
  private final String int96RebaseTz;

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private ColumnarBatch columnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * Populates the row index column if needed.
   */
  private ParquetRowIndexUtil.RowIndexGenerator rowIndexGenerator = null;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public VectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz,
      boolean useOffHeap,
      int capacity) {
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.datetimeRebaseTz = datetimeRebaseTz;
    this.int96RebaseMode = int96RebaseMode;
    this.int96RebaseTz = int96RebaseTz;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.capacity = capacity;
  }

  // For test only.
  public VectorizedParquetRecordReader(boolean useOffHeap, int capacity) {
    this(
      null,
      "CORRECTED",
      "UTC",
      "LEGACY",
      ZoneId.systemDefault().getId(),
      useOffHeap,
      capacity);
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
      UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @VisibleForTesting
  @Override
  public void initialize(
      MessageType fileSchema,
      MessageType requestedSchema,
      ParquetRowGroupReader rowGroupReader,
      int totalRowCount) throws IOException {
    super.initialize(fileSchema, requestedSchema, rowGroupReader, totalRowCount);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  private void initBatch(
      MemoryMode memMode,
      StructType partitionColumns,
      InternalRow partitionValues) {
    StructType batchSchema = new StructType();
    for (StructField f: sparkSchema.fields()) {
      batchSchema = batchSchema.add(f);
    }
    int constantColumnLength = 0;
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
      constantColumnLength = partitionColumns.fields().length;
    }

    ColumnVector[] vectors = allocateColumns(
      capacity, batchSchema, memMode == MemoryMode.OFF_HEAP, constantColumnLength);

    columnarBatch = new ColumnarBatch(vectors);

    columnVectors = new ParquetColumnVector[sparkSchema.fields().length];
    for (int i = 0; i < columnVectors.length; i++) {
      Object defaultValue = null;
      if (sparkRequestedSchema != null) {
        defaultValue = sparkRequestedSchema.existenceDefaultValues()[i];
      }
      columnVectors[i] = new ParquetColumnVector(parquetColumn.children().apply(i),
        (WritableColumnVector) vectors[i], capacity, memMode, missingColumns, true, defaultValue);
    }

    if (partitionColumns != null) {
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(
          (ConstantColumnVector) vectors[i + partitionIdx], partitionValues, i);
      }
    }

    rowIndexGenerator = ParquetRowIndexUtil.createGeneratorIfNeeded(sparkSchema);
  }

  private void initBatch() {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    for (ParquetColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(0);
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();

    int num = (int) Math.min(capacity, totalCountLoadedSoFar - rowsReturned);
    for (ParquetColumnVector cv : columnVectors) {
      for (ParquetColumnVector leafCv : cv.getLeaves()) {
        VectorizedColumnReader columnReader = leafCv.getColumnReader();
        if (columnReader != null) {
          columnReader.readBatch(num, leafCv.getValueVector(),
            leafCv.getRepetitionLevelVector(), leafCv.getDefinitionLevelVector());
        }
      }
      cv.assemble();
    }
    // If needed, compute row indexes within a file.
    if (rowIndexGenerator != null) {
      rowIndexGenerator.populateRowIndex(columnVectors, num);
    }

    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    missingColumns = new HashSet<>();
    for (ParquetColumn column : JavaConverters.seqAsJavaList(parquetColumn.children())) {
      checkColumn(column);
    }
  }

  /**
   * Check whether a column from requested schema is missing from the file schema, or whether it
   * conforms to the type of the file schema.
   */
  private void checkColumn(ParquetColumn column) throws IOException {
    String[] path = JavaConverters.seqAsJavaList(column.path()).toArray(new String[0]);
    if (containsPath(fileSchema, path)) {
      if (column.isPrimitive()) {
        ColumnDescriptor desc = column.descriptor().get();
        ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
        if (!fd.equals(desc)) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
      } else {
        for (ParquetColumn childColumn : JavaConverters.seqAsJavaList(column.children())) {
          checkColumn(childColumn);
        }
      }
    } else { // A missing column which is either primitive or complex
      if (column.required()) {
        // Column is missing in data but the required data is non-nullable. This file is invalid.
        throw new IOException("Required column is missing in data file. Col: " +
          Arrays.toString(path));
      }
      missingColumns.add(column);
    }
  }

  /**
   * Checks whether the given 'path' exists in 'parquetType'. The difference between this and
   * {@link MessageType#containsPath(String[])} is that the latter only support paths to leaf
   * nodes, while this support paths both to leaf and non-leaf nodes.
   */
  private boolean containsPath(Type parquetType, String[] path) {
    return containsPath(parquetType, path, 0);
  }

  private boolean containsPath(Type parquetType, String[] path, int depth) {
    if (path.length == depth) return true;
    if (parquetType instanceof GroupType) {
      String fieldName = path[depth];
      GroupType parquetGroupType = (GroupType) parquetType;
      if (parquetGroupType.containsField(fieldName)) {
        return containsPath(parquetGroupType.getType(fieldName), path, depth + 1);
      }
    }
    return false;
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
          + rowsReturned + " out of " + totalRowCount);
    }
    if (rowIndexGenerator != null) {
      rowIndexGenerator.initFromPageReadStore(pages);
    }
    for (ParquetColumnVector cv : columnVectors) {
      initColumnReader(pages, cv);
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }

  private void initColumnReader(PageReadStore pages, ParquetColumnVector cv) throws IOException {
    if (!missingColumns.contains(cv.getColumn())) {
      if (cv.getColumn().isPrimitive()) {
        ParquetColumn column = cv.getColumn();
        VectorizedColumnReader reader = new VectorizedColumnReader(
          column.descriptor().get(), column.required(), pages, convertTz, datetimeRebaseMode,
          datetimeRebaseTz, int96RebaseMode, int96RebaseTz, writerVersion);
        cv.setColumnReader(reader);
      } else {
        // Not in missing columns and is a complex type: this must be a struct
        for (ParquetColumnVector childCv : cv.getChildren()) {
          initColumnReader(pages, childCv);
        }
      }
    }
  }

  /**
   * <b>This method assumes that all constant column are at the end of schema
   * and `constantColumnLength` represents the number of constant column.<b/>
   *
   * This method allocates columns to store elements of each field of the schema,
   * the data columns use `OffHeapColumnVector` when `useOffHeap` is true and
   * use `OnHeapColumnVector` when `useOffHeap` is false, the constant columns
   * always use `ConstantColumnVector`.
   *
   * Capacity is the initial capacity of the vector, and it will grow as necessary.
   * Capacity is in number of elements, not number of bytes.
   */
  private ColumnVector[] allocateColumns(
      int capacity, StructType schema, boolean useOffHeap, int constantColumnLength) {
    StructField[] fields = schema.fields();
    int fieldsLength = fields.length;
    ColumnVector[] vectors = new ColumnVector[fieldsLength];
    if (useOffHeap) {
      for (int i = 0; i < fieldsLength - constantColumnLength; i++) {
        vectors[i] = new OffHeapColumnVector(capacity, fields[i].dataType());
      }
    } else {
      for (int i = 0; i < fieldsLength - constantColumnLength; i++) {
        vectors[i] = new OnHeapColumnVector(capacity, fields[i].dataType());
      }
    }
    for (int i = fieldsLength - constantColumnLength; i < fieldsLength; i++) {
      vectors[i] = new ConstantColumnVector(capacity, fields[i].dataType());
    }
    return vectors;
  }
}
