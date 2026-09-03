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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.internal.SQLConf$;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

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

  /**
   * Optional storage filter for late materialization: read key-column pages first, evaluate the
   * filter per row to build {@link RowRanges}, then read data-column pages restricted to surviving
   * row ranges. Null means the normal (eager) read path is used.
   */
  private ParquetStorageFilter storageFilter;

  /**
   * Set to true once all row groups have been processed (used with late materialization).
   * */
  private boolean hitEndOfData = false;

  /**
   * Late-materialization state, populated by {@link #initializeLateMaterialization()}.
   *
   * <p>One {@link ParquetFileReader} ({@link #lateMatReader}, the base class's reader exposed via
   * {@link ParquetRowGroupReader#getUnderlyingReader()}) drives all three phases. Its requested
   * schema is mutated per phase via {@link ParquetFileReader#setRequestedSchema}: full schema for
   * phase 0 ({@code getRowRanges}), key-only for phase 1, non-key only for phase 2 (or skipped
   * entirely when the projected schema is all keys, indicated by {@link #nonKeyRequestedSchema}
   * being null).
   */
  private ParquetFileReader lateMatReader;
  private MessageType keyOnlyRequestedSchema;
  private MessageType nonKeyRequestedSchema;
  private int nextBlockIndex;
  private int totalBlockCount;
  private ColumnDescriptor[] keyDescriptors;
  private boolean[] keyRequired;
  private WritableColumnVector[] keyScratchVectors;
  private ColumnarBatch keyScratchBatch;
  /**
   * Mirrors parquet's {@code ParquetReadOptions.useColumnIndexFilter()}. Phase 0 consults it
   * because {@link ParquetFileReader#getRowRanges(int)} does not: it checks only whether a filter
   * is pushed, so it would keep narrowing by column index after a user disabled that filtering.
   */
  private boolean useColumnIndexFilter = true;

  /**
   * Splicing state. {@link #isKeyTopLevel} marks top-level requested-schema slots that are sourced
   * from the per-key-column queues instead of phase-2 reads.
   * {@link #keyVectorQueues} holds one queue per (present) key column of capacity-sized survivor
   * vectors filled during phase 1; {@link #currentKeyAccumulators} are the currently-filling
   * vectors not yet pushed to the queue.
   * {@link #pendingCloseKeyVectors} stashes the previous emit's dequeued vectors so they can be
   * closed at the start of the next emit, releasing survivor memory incrementally as we emit.
   * {@link #persistentBatchColumns} captures the original {@link #initBatch} vector array so it can
   * be closed separately from the per-emit splicing batch (which shares those vectors and would
   * otherwise double-close them).
   *
   * <p>Phase 2 is skipped entirely when the projected schema is all key columns
   * ({@link #nonKeyRequestedSchema} is null); emit reconstructs each batch purely from the key
   * queues.
   *
   * <p>Memory: phase 1 evaluates the whole row group before the first batch of that row group is
   * emitted, so the queues hold every surviving key value for one row group at once -- up to one
   * extra copy of the key columns per row group, versus one capacity-sized vector on the plain read
   * path. {@link #pendingCloseKeyVectors} then releases them batch by batch as emit progresses.
   */
  private boolean[] isKeyTopLevel;
  private java.util.ArrayDeque<WritableColumnVector>[] keyVectorQueues;
  private WritableColumnVector[] currentKeyAccumulators;
  /** Row count of {@link #currentKeyAccumulators}; all key columns advance in lockstep. */
  private int currentKeyAccumulatorRowCount;
  /** Per-key-column copier picked once at init time; called per surviving row in the hot loop. */
  private ValueCopier[] keyCopiers;
  private WritableColumnVector[] pendingCloseKeyVectors;
  private ColumnVector[] persistentBatchColumns;

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

  @Override
  public void initialize(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext,
      Option<HadoopInputFile> inputFile,
      Option<SeekableInputStream> inputStream,
      Option<ParquetMetadata> fileFooter)
      throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext, inputFile, inputStream, fileFooter);
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
    // Each release below is independent, and super.close() owns the file handle and input
    // stream, so they are chained through finally blocks: one failing vector close must not leak
    // the rest.
    try {
      if (isKeyTopLevel != null) {
        // Splicing: the per-emit columnarBatch is a transient view whose slots alias
        // persistentBatchColumns (non-key + partition) and pendingCloseKeyVectors (dequeued key
        // slots). We never call columnarBatch.close() because that would re-close those shared
        // vectors after the direct closes below, freeing the same buffer twice.
        try {
          if (persistentBatchColumns != null) {
            for (ColumnVector v : persistentBatchColumns) {
              if (v != null) v.close();
            }
            persistentBatchColumns = null;
          }
          columnarBatch = null;
        } finally {
          closeSplicingState();
        }
      } else if (columnarBatch != null) {
        columnarBatch.close();
        columnarBatch = null;
        persistentBatchColumns = null;
      }
    } finally {
      try {
        if (keyScratchBatch != null) {
          keyScratchBatch.close();
          keyScratchBatch = null;
          keyScratchVectors = null;
        }
      } finally {
        // lateMatReader aliases the base-class reader; super.close() owns it.
        lateMatReader = null;
        super.close();
      }
    }
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
    // Under a storage filter, rowsReturned counts survivors while totalRowCount is the pre-filter
    // count, so the ratio would stall below 1. hitEndOfData is the real terminator there.
    if (hitEndOfData) return 1.0f;
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
    boolean returnNullStructIfAllFieldsMissing = configuration.getBoolean(
      SQLConf$.MODULE$.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING().key(),
      (boolean) SQLConf$.MODULE$.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING()
        .defaultValue().get());
    StructType batchSchema = returnNullStructIfAllFieldsMissing
      ? new StructType(sparkSchema.fields())
      // Truncate to match requested schema to make sure extra struct field that we read for
      // nullability is not included in columnarBatch and exposed outside.
      : (StructType) truncateType(sparkSchema, sparkRequestedSchema);

    int constantColumnLength = 0;
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
      constantColumnLength = partitionColumns.fields().length;
    }

    Set<Integer> keySlotsToSkip = null;
    if (isKeyTopLevel != null) {
      int[] keyIndices = storageFilter.keyColumnIndices();
      keySlotsToSkip = new HashSet<>(keyIndices.length);
      for (int idx : keyIndices) keySlotsToSkip.add(idx);
    }
    ColumnVector[] vectors = allocateColumns(
      capacity, batchSchema, memMode == MemoryMode.OFF_HEAP, constantColumnLength, keySlotsToSkip);

    columnarBatch = new ColumnarBatch(vectors);
    persistentBatchColumns = vectors;

    columnVectors = new ParquetColumnVector[sparkSchema.fields().length];
    for (int i = 0; i < columnVectors.length; i++) {
      if (vectors[i] == null) continue;  // splicing key slot; ParquetColumnVector unused
      Object defaultValue = null;
      if (sparkRequestedSchema != null) {
        defaultValue = ResolveDefaultColumns.existenceDefaultValues(sparkRequestedSchema)[i];
      }
      columnVectors[i] = new ParquetColumnVector(parquetColumn.children().apply(i),
        (WritableColumnVector) vectors[i], capacity, missingColumns, /* isTopLevel= */ true,
        defaultValue);
    }

    if (partitionColumns != null) {
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(
          (ConstantColumnVector) vectors[i + partitionIdx], partitionValues, i, MEMORY_MODE);
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
   * Keeps the hierarchy and fields of readType, recursively truncating struct fields from the end
   * of the fields list to match the same number of fields in requestedType. This is used to get rid
   * of the extra fields that are added to the structs when the fields we wanted to read initially
   * were missing in the file schema. So this returns a type that we would be reading if everything
   * was present in the file, matching Spark's expected schema.
   *
   * <p> Example: <pre>{@code
   * readType:      array<struct<a:int,b:long,c:int>>
   * requestedType: array<struct<a:int,b:long>>
   * returns:       array<struct<a:int,b:long>>
   * }</pre>
   * We cannot return requestedType here because there might be slight differences, like nullability
   * of fields or the type precision (smallint/int)
   */
  @VisibleForTesting
  static DataType truncateType(DataType readType, DataType requestedType) {
    if (requestedType instanceof UserDefinedType<?> requestedUDT) {
      requestedType = requestedUDT.sqlType();
    }

    if (readType instanceof StructType readStruct &&
        requestedType instanceof StructType requestedStruct) {
      StructType result = new StructType();
      for (int i = 0; i < requestedStruct.fields().length; i++) {
        StructField readField = readStruct.fields()[i];
        StructField requestedField = requestedStruct.fields()[i];
        DataType truncatedType = truncateType(readField.dataType(), requestedField.dataType());
        result = result.add(readField.copy(
          readField.name(), truncatedType, readField.nullable(), readField.metadata()));
      }
      return result;
    }

    if (readType instanceof ArrayType readArray &&
        requestedType instanceof ArrayType requestedArray) {
      DataType truncatedElementType = truncateType(
        readArray.elementType(), requestedArray.elementType());
      return readArray.copy(truncatedElementType, readArray.containsNull());
    }

    if (readType instanceof MapType readMap && requestedType instanceof MapType requestedMap) {
      DataType truncatedKeyType = truncateType(readMap.keyType(), requestedMap.keyType());
      DataType truncatedValueType = truncateType(readMap.valueType(), requestedMap.valueType());
      return readMap.copy(truncatedKeyType, truncatedValueType, readMap.valueContainsNull());
    }

    assert !ParquetSchemaConverter.isComplexType(readType);
    assert !ParquetSchemaConverter.isComplexType(requestedType);
    return readType;
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
    if (isKeyTopLevel != null) return nextBatchSplicing();
    for (ParquetColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(0);
    if (hitEndOfData) return false;
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();
    if (hitEndOfData) return false;

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

  /**
   * Splicing emit path. Closes the previous emit's dequeued key vectors (releasing survivor memory
   * incrementally), advances to the next row group if needed via {@link #checkEndOfRowGroup()},
   * dequeues one survivor key vector per key column, drives non-key column readers for {@code num}
   * rows, and assembles a fresh {@link ColumnarBatch} interleaving key (dequeued) and non-key
   * (persistent value-vector) slots in the original projection order.
   * The per-emit batch is a transient view over vectors owned elsewhere; see {@link #close()} and
   * {@link #closeSplicingState()}.
   */
  private boolean nextBatchSplicing() throws IOException {
    if (pendingCloseKeyVectors != null) {
      for (WritableColumnVector v : pendingCloseKeyVectors) {
        if (v != null) v.close();
      }
      pendingCloseKeyVectors = null;
    }
    for (int i = 0; i < columnVectors.length; i++) {
      if (isKeyTopLevel[i]) continue;
      columnVectors[i].reset();
    }
    // Match the eager path and zero the outgoing batch before the terminal checks below. Without
    // this, a terminal call leaves `columnarBatch` pointing at the previous emit whose key slots
    // were just closed above -- with off-heap vectors those buffers are already freed, so a
    // consumer that read the batch after nextBatch() returned false would see freed memory.
    if (columnarBatch != null) columnarBatch.setNumRows(0);
    if (hitEndOfData) return false;
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();
    if (hitEndOfData) return false;

    int num = (int) Math.min(capacity, totalCountLoadedSoFar - rowsReturned);

    WritableColumnVector[] dequeued = new WritableColumnVector[keyVectorQueues.length];
    for (int i = 0; i < keyVectorQueues.length; i++) {
      dequeued[i] = keyVectorQueues[i].removeFirst();
    }

    for (int i = 0; i < columnVectors.length; i++) {
      if (isKeyTopLevel[i]) continue;
      ParquetColumnVector cv = columnVectors[i];
      for (ParquetColumnVector leafCv : cv.getLeaves()) {
        VectorizedColumnReader columnReader = leafCv.getColumnReader();
        if (columnReader != null) {
          columnReader.readBatch(num, leafCv.getValueVector(),
            leafCv.getRepetitionLevelVector(), leafCv.getDefinitionLevelVector());
        }
      }
      cv.assemble();
    }
    if (rowIndexGenerator != null) {
      // Row-index column is identified by name (ROW_INDEX_TEMPORARY_COLUMN_NAME), which is a
      // synthetic metadata column never referenced by a storage filter, so its slot is a non-key
      // slot with a persistent ParquetColumnVector.
      rowIndexGenerator.populateRowIndex(columnVectors, num);
    }

    ColumnVector[] cols = new ColumnVector[persistentBatchColumns.length];
    // This walks batch slots in ascending order while `keyIdx` walks the survivor queues in
    // key-row-position order, so it pairs the k-th smallest key slot with key-row position k.
    // That is only the identity because `ParquetStorageFilter.create` sorts `keyColumnIndices`
    // ascending -- see the comment there. `isKeyTopLevel` marks which slots are keys but not their
    // position in that list, so this loop cannot reconstruct the pairing on its own: if the list
    // ever stops being sorted, key columns silently swap places in the output batch.
    int keyIdx = 0;
    for (int i = 0; i < persistentBatchColumns.length; i++) {
      if (i < isKeyTopLevel.length && isKeyTopLevel[i]) {
        cols[i] = dequeued[keyIdx++];
      } else {
        cols[i] = persistentBatchColumns[i];
      }
    }
    columnarBatch = new ColumnarBatch(cols);
    columnarBatch.setNumRows(num);

    rowsReturned += num;
    numBatched = num;
    batchIdx = 0;
    pendingCloseKeyVectors = dequeued;
    return true;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    missingColumns = new HashSet<>();
    for (ParquetColumn column : CollectionConverters.asJava(parquetColumn.children())) {
      checkColumn(column);
    }
    if (storageFilter != null) {
      initializeLateMaterialization();
    }
  }

  /**
   * Sets the storage filter for late materialization. Must be called before {@link #initialize};
   * {@link #initializeLateMaterialization()} (run from {@link #initialize}) inspects the per-file
   * schema and decides whether splicing actually engages.
   *
   * <p>Splicing does NOT engage in one real case: all key columns are missing from this physical
   * file under schema evolution. The predicate is rewritten with each missing key replaced by the
   * constant the reader materializes for it (its existence DEFAULT, else null -- see
   * {@code ParquetStorageFilter.rewriteForMissingKeys}) and evaluated as a constant: a true result
   * keeps the file with no filtering, a false/null result skips it entirely. Either way the rows
   * the scan returns are exactly the rows that satisfy the filter, so this is safe.
   *
   * <p>Every OTHER precondition is guaranteed by planning-time checks in
   * {@code FileSourceStrategy.extractStorageFilters} and {@code ParquetStorageFilter.create}, and
   * violations throw rather than fall back: once extraction has moved a bloom filter onto the scan
   * it is gone from the post-scan Filter, so quietly not applying it would produce wrong rows.
   */
  public void setStorageFilter(ParquetStorageFilter storageFilter) {
    this.storageFilter = storageFilter;
  }

  private void initializeLateMaterialization() throws IOException {
    lateMatReader = reader.getUnderlyingReader();
    if (lateMatReader == null) {
      // Unreachable in production: the only ParquetRowGroupReader ParquetFileFormat builds is
      // ParquetRowGroupReaderImpl, which returns its reader. Dropping the filter here would return
      // rows it rejects, since extraction already removed it from the post-scan Filter.
      throw new IllegalStateException(
          "Storage-filter pushdown requires a reader backed by a ParquetFileReader, but "
              + reader.getClass().getName() + " does not expose one");
    }
    if (configuration != null) {
      useColumnIndexFilter = configuration.getBoolean(
          ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true);
    }

    // Resolve each key column's top-level ParquetColumn. Partition into present and missing (the
    // latter can happen under schema evolution: a column is in the requested schema but not in this
    // physical parquet file). For any non-primitive key we still bail; phase-1 reads only primitive
    // leaves.
    int[] keyIndices = storageFilter.keyColumnIndices();
    List<ParquetColumn> presentKeyColumns = new ArrayList<>(keyIndices.length);
    List<Integer> missingKeyLocalPositions = new ArrayList<>();
    for (int i = 0; i < keyIndices.length; i++) {
      int idx = keyIndices[i];
      if (idx < 0 || idx >= parquetColumn.children().size()) {
        // Unreachable: ParquetStorageFilter.create rejects out-of-range ordinals. Fail loudly
        // rather than dropping the filter -- extractStorageFilters already removed it from the
        // post-scan Filter, so silently ignoring it here would return wrong rows.
        throw new IllegalStateException(String.format(
            "Storage-filter key ordinal %d is out of range for a %d-column requested schema",
            idx, parquetColumn.children().size()));
      }
      ParquetColumn column = parquetColumn.children().apply(idx);
      if (!column.isPrimitive()) {
        // Unreachable: ParquetStorageFilter.isSupportedKeyType admits only types with a primitive
        // Parquet leaf, and it gates both planning and ParquetStorageFilter.create. Fail loudly for
        // the same reason as above.
        throw new IllegalStateException(
            "Storage-filter key column is not a primitive Parquet column: " + column.path());
      }
      if (missingColumns.contains(column)) {
        missingKeyLocalPositions.add(i);
      } else {
        presentKeyColumns.add(column);
      }
    }

    // If any key column is missing from this file, rewrite the predicate to substitute the constant
    // the reader will actually materialize for that column. That is the column's existence DEFAULT
    // when it has one (ParquetColumnVector writes it into the output vector and marks the vector
    // constant), otherwise null. Substituting null for a column that reads back as its default
    // would filter on a value the scan never returns. The predicate must be evaluated against the
    // substituted constant rather than skipped: null does not always mean false in a filter.
    if (!missingKeyLocalPositions.isEmpty()) {
      int[] missing = new int[missingKeyLocalPositions.size()];
      Object[] missingValues = new Object[missingKeyLocalPositions.size()];
      Object[] existenceDefaults =
          ResolveDefaultColumns.existenceDefaultValues(sparkRequestedSchema);
      for (int i = 0; i < missing.length; i++) {
        missing[i] = missingKeyLocalPositions.get(i);
        missingValues[i] = existenceDefaults[keyIndices[missing[i]]];
      }
      storageFilter = storageFilter.rewriteForMissingKeys(missing, missingValues);

      if (presentKeyColumns.isEmpty()) {
        // All key columns missing: the rewritten predicate is fully constant. Evaluate once and
        // apply uniformly to the whole file.
        boolean keepAll = storageFilter.evalAllMissing();
        if (keepAll) {
          // Predicate is constant-true for this file: no filtering to do.
          storageFilter = null;
        } else {
          // Predicate is constant-false/null: no row from this file can pass the filter.
          storageFilter = null;
          hitEndOfData = true;
        }
        return;
      }
    }

    keyDescriptors = new ColumnDescriptor[presentKeyColumns.size()];
    keyRequired = new boolean[presentKeyColumns.size()];
    Types.MessageTypeBuilder keySchemaBuilder = Types.buildMessage();
    Set<String> keyTopLevelNames = new HashSet<>();
    for (int i = 0; i < presentKeyColumns.size(); i++) {
      ParquetColumn column = presentKeyColumns.get(i);
      keyDescriptors[i] = column.descriptor().get();
      keyRequired[i] = column.required();
      // Preserve the field name/type as it appears at the top of requestedSchema.
      String topLevelName = keyDescriptors[i].getPath()[0];
      keySchemaBuilder.addField(requestedSchema.getType(topLevelName));
      keyTopLevelNames.add(topLevelName);
    }
    keyOnlyRequestedSchema = keySchemaBuilder.named(requestedSchema.getName());

    // Build the non-key (complement) schema. When the projection has at least one non-key column,
    // phase 2 will switch lateMatReader's schema to this and read those columns under finalRanges.
    // When the projection is *all* key columns (e.g. a scan whose only column is the bloom probe),
    // splicing still pays off: phase 2 has nothing useful to read, so we skip it entirely (no read,
    // no IO) and emit batches purely from the key queues. The `nonKeyRequestedSchema != null` check
    // downstream gates phase-2 IO.
    Types.MessageTypeBuilder nonKeyBuilder = Types.buildMessage();
    int nonKeyFieldCount = 0;
    for (Type field : requestedSchema.getFields()) {
      if (!keyTopLevelNames.contains(field.getName())) {
        nonKeyBuilder.addField(field);
        nonKeyFieldCount++;
      }
    }
    if (nonKeyFieldCount > 0) {
      nonKeyRequestedSchema = nonKeyBuilder.named(requestedSchema.getName());
      requireOffsetIndexesForPhase2();
    }
    initializeSplicingState(presentKeyColumns);

    totalBlockCount = lateMatReader.getRowGroups().size();
    nextBlockIndex = 0;
  }

  /**
   * Fails now if any projected column of any row group lacks a Parquet offset index.
   *
   * <p>Phase 2 reads a strict subset of a row group's rows, which parquet can only do via the
   * offset index; files written before parquet-mr 1.11, or by writers that omit it, have none. We
   * cannot
   * widen phase 2 to the whole block instead, because the key vectors already hold only the
   * survivors and the batch would misalign -- and we cannot skip the filter either, since
   * {@code extractStorageFilters} has already removed it from the post-scan Filter.
   *
   * <p>Every projected column is checked, not just the non-key ones phase 2 reads, because parquet
   * builds one column index store per row group and reuses it. Phase 0 asks for the row ranges
   * under the full requested schema, so {@code ColumnIndexStoreImpl.create} is called with the key
   * columns in its path set, and it returns its {@code EMPTY} singleton as soon as any one of those
   * paths has no offset index. {@code ParquetFileReader.getColumnIndexStore} memoizes that store
   * per block, and {@code EMPTY.getOffsetIndex} throws for *every* column. So a key column with no
   * offset index kills phase 2 too, with a raw {@code MissingOffsetIndexException} naming some
   * non-key column and none of the guidance below.
   *
   * <p>Checking up front rather than at the first partially-kept row group is deliberate: whether
   * phase 2 needs the offset index otherwise depends on how selective the filter turns out to be on
   * this particular file, so the same query would fail or not depending on the data. This is
   * conservative -- a filter that happens to keep every row of every block would not have needed
   * the offset index -- but such a filter also saves nothing, so failing loudly loses nothing.
   *
   * <p>The check itself is free: {@code getOffsetIndexReference()} is a footer field that
   * {@link #initialize} has already read.
   */
  private void requireOffsetIndexesForPhase2() {
    Set<ColumnPath> projectedPaths = new HashSet<>();
    for (ColumnDescriptor column : requestedSchema.getColumns()) {
      projectedPaths.add(ColumnPath.get(column.getPath()));
    }
    List<BlockMetaData> blocks = lateMatReader.getRowGroups();
    for (int blockIdx = 0; blockIdx < blocks.size(); blockIdx++) {
      for (ColumnChunkMetaData chunk : blocks.get(blockIdx).getColumns()) {
        if (projectedPaths.contains(chunk.getPath()) && chunk.getOffsetIndexReference() == null) {
          throw new IllegalStateException(String.format(
              "Storage-filter pushdown requires a Parquet offset index to read a subset of a row "
                  + "group, but column %s of row group %d in %s has none. Set %s=false to read "
                  + "this file.",
              chunk.getPath(), blockIdx, lateMatReader.getFile(),
              SQLConf$.MODULE$.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED().key()));
        }
      }
    }
  }

  /**
   * Populates splicing bookkeeping: {@link #isKeyTopLevel}, {@link #keyVectorQueues},
   * {@link #currentKeyAccumulators}. {@code keyColumnIndices} already index the top-level slots of
   * {@link #sparkSchema} (same indexing as {@link #columnVectors}), so they map directly onto
   * {@link #isKeyTopLevel}. After this method returns, the splicing path is fully initialized;
   * {@link #nextBatch()} and {@link #close()} use {@link #isKeyTopLevel} as the active-state
   * indicator.
   */
  @SuppressWarnings("unchecked")
  private void initializeSplicingState(List<ParquetColumn> presentKeyColumns) {
    int numTop = sparkSchema.fields().length;
    isKeyTopLevel = new boolean[numTop];
    int[] keyIndices = storageFilter.keyColumnIndices();
    for (int slot : keyIndices) {
      isKeyTopLevel[slot] = true;
    }
    int numKeys = presentKeyColumns.size();
    keyVectorQueues = new java.util.ArrayDeque[numKeys];
    for (int i = 0; i < numKeys; i++) {
      keyVectorQueues[i] = new java.util.ArrayDeque<>();
    }
    currentKeyAccumulators = new WritableColumnVector[numKeys];
    currentKeyAccumulatorRowCount = 0;
    keyCopiers = new ValueCopier[numKeys];
    StructField[] fields = sparkRequestedSchema.fields();
    for (int i = 0; i < numKeys; i++) {
      keyCopiers[i] = copierFor(fields[keyIndices[i]].dataType());
    }
  }

  /**
   * Check whether a column from requested schema is missing from the file schema, or whether it
   * conforms to the type of the file schema.
   */
  private void checkColumn(ParquetColumn column) throws IOException {
    String[] path = CollectionConverters.asJava(column.path()).toArray(new String[0]);
    if (containsPath(fileSchema, path)) {
      if (column.isPrimitive()) {
        ColumnDescriptor desc = column.descriptor().get();
        ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
        if (!fd.equals(desc)) {
          throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3185");
        }
      } else {
        for (ParquetColumn childColumn : CollectionConverters.asJava(column.children())) {
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
    if (parquetType instanceof GroupType parquetGroupType) {
      String fieldName = path[depth];
      if (parquetGroupType.containsField(fieldName)) {
        return containsPath(parquetGroupType.getType(fieldName), path, depth + 1);
      }
    }
    return false;
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    if (storageFilter != null) {
      loadNextRowGroupWithLateMaterialization();
      return;
    }
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

  /**
   * Loads the next row group using the three-phase late-materialization pattern, all driven by the
   * single {@link #lateMatReader} with its requested schema mutated per phase:
   *   - Phase 0 (full schema): compute {@code pushedFilterRanges} from the pushed data filter via
   *     column index (metadata-only) using {@link ParquetFileReader#getRowRanges}.
   *   - Phase 1 (key-only schema): read key-column pages restricted to {@code pushedFilterRanges},
   *     evaluate the storage filter per row, build {@code finalRanges}.
   *   - Phase 2 (non-key schema): read non-key columns restricted to {@code finalRanges}.
   *     Skipped entirely when {@link #nonKeyRequestedSchema} is null (all-keys projection).
   *
   * Row groups for which {@code finalRanges} is empty are skipped entirely (no phase-2 IO).
   * Sets {@link #hitEndOfData} when all row groups have been processed.
   */
  private void loadNextRowGroupWithLateMaterialization() throws IOException {
    while (nextBlockIndex < totalBlockCount) {
      int blockIdx = nextBlockIndex++;
      long blockRowCount = lateMatReader.getRowGroups().get(blockIdx).getRowCount();
      if (blockRowCount == 0) {
        // parquet-mr never writes these, but RowRanges.createSingle(0) would build Range(0, -1) and
        // trip parquet's own `from <= to` assertion. The plain read path skips them too.
        continue;
      }

      // Phase 0: rows allowed by the pushed data filter (column-index granularity). Restore the
      // full requestedSchema first: phases 1 and 2 below narrow the reader's schema, and
      // ParquetFileReader.getRowRanges computes ranges against whatever schema is set (it passes
      // the reader's current `paths` to ColumnIndexFilter). This is defensive -- with column-index
      // filtering on, getFilteredRecordCount() in initialize() has already memoized every block's
      // ranges under the full schema, and with it off we do not call getRowRanges at all.
      lateMatReader.setRequestedSchema(requestedSchema);
      // ParquetFileReader.getRowRanges only checks whether a filter is pushed, NOT
      // options.useColumnIndexFilter(), so calling it unconditionally would keep applying
      // column-index filtering after a user turned it off. That conf is the documented escape hatch
      // for files whose column index is wrong, and trusting a wrong column index here would drop
      // rows for good: finalRanges is a subset of pushedFilterRanges, and the post-scan Filter no
      // longer holds this predicate.
      RowRanges pushedFilterRanges = useColumnIndexFilter
          ? lateMatReader.getRowRanges(blockIdx)
          : RowRanges.createSingle(blockRowCount);
      if (pushedFilterRanges.rowCount() == 0) {
        // Pushed data filter rejects this block entirely via column index. Not a storage-filter
        // skip, so we don't increment storage-filter metrics.
        continue;
      }

      // Both byte metrics are always wired in production (FileSourceScanLike creates all five
      // whenever storageFilters is non-empty), so this only skips the work on the test-only path
      // that drives the reader directly. compressedBytesForRowRanges never does IO of its own, so
      // there is nothing here to avoid on the production path.
      StorageFilterMetrics m = storageFilter.metrics();
      SQLMetric bytesAvoidedRg = m.bytesAvoidedByRowGroup();
      SQLMetric bytesAvoidedPf = m.bytesAvoidedByPageFiltering();
      boolean needBytes = bytesAvoidedRg != null || bytesAvoidedPf != null;
      long baselineRows = pushedFilterRanges.rowCount();
      long baselineBytes = needBytes
          ? compressedBytesForRowRanges(lateMatReader, blockIdx, requestedSchema,
              pushedFilterRanges)
          : 0L;

      // Phase 1: switch to key-only schema, read key columns under pushedFilterRanges, evaluate the
      // storage filter per row.
      lateMatReader.setRequestedSchema(keyOnlyRequestedSchema);
      long phase1Bytes = needBytes
          ? compressedBytesForRowRanges(lateMatReader, blockIdx, keyOnlyRequestedSchema,
              pushedFilterRanges)
          : 0L;
      PageReadStore keyPages = lateMatReader.readFilteredRowGroup(blockIdx, pushedFilterRanges);
      if (keyPages == null) {
        // Unreachable: readFilteredRowGroup returns null only for an empty block, and we already
        // know pushedFilterRanges selects at least one row. Skipping the block here would drop its
        // surviving rows from the output, so assert rather than `continue`.
        throw new IllegalStateException(
            "No key pages for row group " + blockIdx + " despite "
                + pushedFilterRanges.rowCount() + " rows selected by the pushed filter");
      }
      RowRanges finalRanges = evaluateStorageFilter(keyPages, pushedFilterRanges);

      if (finalRanges.rowCount() == 0) {
        // Every surviving row was rejected by the storage filter; skip block entirely. We still
        // paid phase-1 to read the key column, so the bytes avoided vs a no-storage-filter read
        // are baseline - phase1 (the non-key bytes the no-filter path would have read).
        SQLMetric rgSkipped = m.rowGroupsSkipped();
        if (rgSkipped != null) rgSkipped.add(1L);
        SQLMetric rowsExcludedRg = m.rowsExcludedByRowGroup();
        if (rowsExcludedRg != null) rowsExcludedRg.add(baselineRows);
        if (bytesAvoidedRg != null) bytesAvoidedRg.add(baselineBytes - phase1Bytes);
        continue;
      }

      // Phase 2: switch to non-key schema, read non-key columns under finalRanges. Skipped entirely
      // when the projection is all keys (nonKeyRequestedSchema is null); emit reconstructs each
      // batch from the key queues alone.
      long keptRows;
      long phase2Bytes;
      PageReadStore dataPages = null;
      if (nonKeyRequestedSchema == null) {
        keptRows = finalRanges.rowCount();
        phase2Bytes = 0L;
      } else {
        lateMatReader.setRequestedSchema(nonKeyRequestedSchema);
        // requireOffsetIndexesForPhase2() already established that every projected column of every
        // row group has an offset index, so this page-filtering read cannot fail for want of one.
        dataPages = lateMatReader.readFilteredRowGroup(blockIdx, finalRanges);
        if (dataPages == null) {
          // Unreachable: readFilteredRowGroup returns null only for an empty block or empty ranges,
          // both excluded above. Match phase 1 and fail with a message rather than an NPE.
          throw new IllegalStateException(
              "No data pages for row group " + blockIdx + " despite " + finalRanges.rowCount()
                  + " surviving rows");
        }
        keptRows = dataPages.getRowCount();
        phase2Bytes = bytesAvoidedPf != null
            ? compressedBytesForRowRanges(lateMatReader, blockIdx, nonKeyRequestedSchema,
                finalRanges)
            : 0L;
      }
      long filteredRows = baselineRows - keptRows;
      SQLMetric rowsExcludedWithinRg = m.rowsExcludedWithinRowGroup();
      if (rowsExcludedWithinRg != null && filteredRows > 0) rowsExcludedWithinRg.add(filteredRows);
      if (bytesAvoidedPf != null) {
        bytesAvoidedPf.add(baselineBytes - phase1Bytes - phase2Bytes);
      }

      if (dataPages != null) {
        if (rowIndexGenerator != null) {
          rowIndexGenerator.initFromPageReadStore(dataPages);
        }
        for (int i = 0; i < columnVectors.length; i++) {
          if (isKeyTopLevel[i]) {
            // Key columns are sourced from the queues during emit; skip phase-2 reader init.
            continue;
          }
          initColumnReader(dataPages, columnVectors[i]);
        }
      }
      totalCountLoadedSoFar += keptRows;
      return;
    }
    hitEndOfData = true;
  }

  /**
   * Compressed bytes the reader transfers for the leaf columns of {@code schema} when it reads
   * exactly {@code rowRanges} of the given block. Page headers and the dictionary page are
   * included, because both are read whenever any page of a chunk is read.
   *
   * <p>Two sources, chosen so this never causes IO of its own:
   * <ul>
   *   <li>{@code rowRanges} covers the whole block: the answer is the sum of the chunks'
   *       {@code getTotalSize()}, which is already in the footer. This is the case that matters --
   *       whenever nothing else has built the block's {@link ColumnIndexStore}, {@code rowRanges}
   *       is necessarily the whole block, because a narrower range can only come from column-index
   *       filtering, which builds the store as a side effect.
   *   <li>{@code rowRanges} is a strict subset: walk the offset index, as parquet's own read path
   *       does, and add the dictionary page the way {@code calculateOffsetRanges} does. The store
   *       is guaranteed to exist here, so the walk is pure metadata arithmetic.
   * </ul>
   *
   * <p>Columns absent from this physical file (schema evolution) contribute nothing, which is
   * correct: the reader transfers nothing for them. Every caller for a given block walks the same
   * metadata, so a skipped column drops out of the baseline and the per-phase totals alike.
   */
  private static long compressedBytesForRowRanges(
      ParquetFileReader reader,
      int blockIndex,
      MessageType schema,
      RowRanges rowRanges) {
    if (schema == null || rowRanges.rowCount() == 0 || schema.getColumns().isEmpty()) {
      return 0L;
    }
    BlockMetaData block = reader.getRowGroups().get(blockIndex);
    long blockRowCount = block.getRowCount();
    Map<ColumnPath, ColumnChunkMetaData> chunks = new HashMap<>();
    for (ColumnChunkMetaData chunk : block.getColumns()) {
      chunks.put(chunk.getPath(), chunk);
    }
    boolean wholeBlock = rowRanges.rowCount() == blockRowCount;
    ColumnIndexStore ciStore = wholeBlock ? null : reader.getColumnIndexStore(blockIndex);
    long total = 0L;
    for (ColumnDescriptor column : schema.getColumns()) {
      ColumnPath path = ColumnPath.get(column.getPath());
      ColumnChunkMetaData chunk = chunks.get(path);
      if (chunk == null) {
        // Column is in the (clipped) requested schema but not in this file.
        continue;
      }
      if (wholeBlock) {
        total += chunk.getTotalSize();
        continue;
      }
      OffsetIndex offsetIndex;
      try {
        offsetIndex = ciStore.getOffsetIndex(path);
      } catch (MissingOffsetIndexException e) {
        continue;
      }
      if (offsetIndex == null) {
        continue;
      }
      // The dictionary page is read whenever any data page of the chunk is, so count it here the
      // same way parquet's ColumnIndexFilterUtils.calculateOffsetRanges does.
      total += dictionaryPageSize(chunk);
      int pageCount = offsetIndex.getPageCount();
      for (int i = 0; i < pageCount; i++) {
        long from = offsetIndex.getFirstRowIndex(i);
        long to = offsetIndex.getLastRowIndex(i, blockRowCount);
        if (rowRanges.isOverlapping(from, to)) {
          total += offsetIndex.getCompressedPageSize(i);
        }
      }
    }
    return total;
  }

  /**
   * Compressed size of a chunk's dictionary page, or 0 if it has none.
   * {@link ColumnChunkMetaData#getStartingPos()} already resolves to the dictionary page offset
   * when there is a valid one, so the gap up to the first data page is exactly the dictionary page.
   */
  private static long dictionaryPageSize(ColumnChunkMetaData chunk) {
    long startingPos = chunk.getStartingPos();
    long firstDataPageOffset = chunk.getFirstDataPageOffset();
    return startingPos < firstDataPageOffset ? firstDataPageOffset - startingPos : 0L;
  }

  /**
   * Reads all rows of the given key-only {@link PageReadStore} (which contains only rows in
   * {@code pushedFilterRanges}) in capacity-sized chunks, evaluates the storage filter on each row,
   * builds a {@link RowRanges} of surviving rows in original block-row coordinates, and appends
   * survivor key values into the per-key-column accumulators ({@link #currentKeyAccumulators}).
   * When an accumulator hits {@link #capacity}, it's pushed into {@link #keyVectorQueues} and a
   * fresh one is allocated. After all rows have been examined, any partial trailing accumulator is
   * pushed too.
   *
   * <p>The result is a subset of {@code pushedFilterRanges}: rows not in {@code
   * pushedFilterRanges} were never read and are implicitly excluded.
   */
  private RowRanges evaluateStorageFilter(
      PageReadStore keyPages,
      RowRanges pushedFilterRanges) throws IOException {
    ensureKeyScratchAllocated();
    VectorizedColumnReader[] readers = new VectorizedColumnReader[keyDescriptors.length];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = new VectorizedColumnReader(
          keyDescriptors[i], keyRequired[i], keyPages, convertTz, datetimeRebaseMode,
          datetimeRebaseTz, int96RebaseMode, int96RebaseTz, writerVersion);
    }
    ensureCurrentKeyAccumulatorsAllocated();

    long keyRowsTotal = pushedFilterRanges.rowCount();
    PrimitiveIterator.OfLong rowIndexIter = pushedFilterRanges.iterator();
    RowRanges.Builder finalRangesBuilder = RowRanges.builder();
    long remaining = keyRowsTotal;
    while (remaining > 0) {
      int num = (int) Math.min((long) capacity, remaining);
      for (int i = 0; i < keyScratchVectors.length; i++) {
        keyScratchVectors[i].reset();
        readers[i].readBatch(num, keyScratchVectors[i], null, null);
      }
      keyScratchBatch.setNumRows(num);
      for (int r = 0; r < num; r++) {
        long blockRow = rowIndexIter.nextLong();
        if (storageFilter.test(keyScratchBatch.getRow(r))) {
          finalRangesBuilder.addSelectedRow(blockRow);
          appendSurvivorRowToAccumulators(r);
        }
      }
      remaining -= num;
    }

    finalizePartialAccumulators();

    return finalRangesBuilder.build();
  }

  private void ensureKeyScratchAllocated() {
    if (keyScratchVectors != null) return;
    keyScratchVectors = new WritableColumnVector[keyDescriptors.length];
    boolean useOffHeap = MEMORY_MODE == MemoryMode.OFF_HEAP;
    int[] keyIndices = storageFilter.keyColumnIndices();
    for (int i = 0; i < keyDescriptors.length; i++) {
      DataType dt = sparkRequestedSchema.fields()[keyIndices[i]].dataType();
      keyScratchVectors[i] = useOffHeap
          ? new OffHeapColumnVector(capacity, dt)
          : new OnHeapColumnVector(capacity, dt);
    }
    keyScratchBatch = new ColumnarBatch(keyScratchVectors);
  }

  /**
   * Allocates the per-key-column accumulator vectors if any slot is null (i.e. the previous
   * accumulator was just pushed to the queue or this is the first row group). Each accumulator has
   * {@link #capacity} rows.
   */
  private void ensureCurrentKeyAccumulatorsAllocated() {
    boolean useOffHeap = MEMORY_MODE == MemoryMode.OFF_HEAP;
    int[] keyIndices = storageFilter.keyColumnIndices();
    for (int i = 0; i < currentKeyAccumulators.length; i++) {
      if (currentKeyAccumulators[i] == null) {
        DataType dt = sparkRequestedSchema.fields()[keyIndices[i]].dataType();
        currentKeyAccumulators[i] = useOffHeap
            ? new OffHeapColumnVector(capacity, dt)
            : new OnHeapColumnVector(capacity, dt);
      }
    }
    currentKeyAccumulatorRowCount = 0;
  }

  /**
   * Appends row {@code srcRow} of each {@link #keyScratchVectors} into the corresponding
   * {@link #currentKeyAccumulators}. When the accumulators fill, they're pushed onto their queues
   * and fresh ones allocated. All key columns are appended in lockstep so accumulators stay
   * aligned.
   */
  private void appendSurvivorRowToAccumulators(int srcRow) {
    final int dstRow = currentKeyAccumulatorRowCount;
    final WritableColumnVector[] accs = currentKeyAccumulators;
    final WritableColumnVector[] srcs = keyScratchVectors;
    final ValueCopier[] copiers = keyCopiers;
    for (int i = 0, n = accs.length; i < n; i++) {
      WritableColumnVector src = srcs[i];
      WritableColumnVector dst = accs[i];
      if (src.isNullAt(srcRow)) {
        dst.putNull(dstRow);
      } else {
        copiers[i].copy(dst, dstRow, src, srcRow);
      }
    }
    currentKeyAccumulatorRowCount = dstRow + 1;
    if (currentKeyAccumulatorRowCount == capacity) {
      for (int i = 0; i < currentKeyAccumulators.length; i++) {
        keyVectorQueues[i].addLast(currentKeyAccumulators[i]);
        currentKeyAccumulators[i] = null;
      }
      ensureCurrentKeyAccumulatorsAllocated();
    }
  }

  /**
   * Pushes any partially-filled accumulator into its queue at row-group end so the emit path can
   * dequeue it as the row group's final batch.
   */
  private void finalizePartialAccumulators() {
    if (currentKeyAccumulatorRowCount == 0) return;
    for (int i = 0; i < currentKeyAccumulators.length; i++) {
      keyVectorQueues[i].addLast(currentKeyAccumulators[i]);
      currentKeyAccumulators[i] = null;
    }
    currentKeyAccumulatorRowCount = 0;
  }

  /**
   * Closes anything held by the splicing path: pending dequeued key vectors not yet rolled over,
   * any vectors still queued (e.g. on early termination), and partially-filled accumulators.
   * Called from {@link #close()}.
   */
  private void closeSplicingState() {
    if (pendingCloseKeyVectors != null) {
      for (WritableColumnVector v : pendingCloseKeyVectors) {
        if (v != null) v.close();
      }
      pendingCloseKeyVectors = null;
    }
    if (keyVectorQueues != null) {
      for (java.util.ArrayDeque<WritableColumnVector> q : keyVectorQueues) {
        if (q != null) {
          for (WritableColumnVector v : q) v.close();
          q.clear();
        }
      }
    }
    if (currentKeyAccumulators != null) {
      for (WritableColumnVector v : currentKeyAccumulators) {
        if (v != null) v.close();
      }
      currentKeyAccumulators = null;
    }
  }

  /**
   * Per-key-column value copier: appends one value from {@code src[srcRow]} to
   * {@code dst[dstRow]}. Picked once at init via {@link #copierFor(DataType)}; called per surviving
   * row in {@link #appendSurvivorRowToAccumulators}. Caller handles null sources.
   */
  @FunctionalInterface
  private interface ValueCopier {
    void copy(WritableColumnVector dst, int dstRow, WritableColumnVector src, int srcRow);
  }

  /**
   * Returns a {@link ValueCopier} for the given key {@link DataType}. The set of types handled here
   * is the definition behind {@code ParquetStorageFilter.isSupportedKeyType}, which gates both
   * planning-time extraction and {@code ParquetStorageFilter.create} -- so the throw at the end is
   * unreachable. Teach both sides at once when adding a type; a type admitted there but missing
   * here becomes a task failure instead of a planning-time rejection.
   */
  private static ValueCopier copierFor(DataType dt) {
    if (dt instanceof BooleanType) {
      return (dst, dRow, src, sRow) -> dst.putBoolean(dRow, src.getBoolean(sRow));
    }
    if (dt instanceof ByteType) {
      return (dst, dRow, src, sRow) -> dst.putByte(dRow, src.getByte(sRow));
    }
    if (dt instanceof ShortType) {
      return (dst, dRow, src, sRow) -> dst.putShort(dRow, src.getShort(sRow));
    }
    if (dt instanceof IntegerType
        || dt instanceof DateType
        || dt instanceof YearMonthIntervalType) {
      return (dst, dRow, src, sRow) -> dst.putInt(dRow, src.getInt(sRow));
    }
    if (dt instanceof LongType
        || dt instanceof TimestampType
        || dt instanceof TimestampNTZType
        || dt instanceof TimeType
        || dt instanceof DayTimeIntervalType) {
      return (dst, dRow, src, sRow) -> dst.putLong(dRow, src.getLong(sRow));
    }
    if (dt instanceof FloatType) {
      return (dst, dRow, src, sRow) -> dst.putFloat(dRow, src.getFloat(sRow));
    }
    if (dt instanceof DoubleType) {
      return (dst, dRow, src, sRow) -> dst.putDouble(dRow, src.getDouble(sRow));
    }
    if (dt instanceof DecimalType decimalType) {
      int precision = decimalType.precision();
      if (precision <= Decimal.MAX_INT_DIGITS()) {
        return (dst, dRow, src, sRow) -> dst.putInt(dRow, src.getInt(sRow));
      }
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        return (dst, dRow, src, sRow) -> dst.putLong(dRow, src.getLong(sRow));
      }
      return (dst, dRow, src, sRow) -> dst.putByteArray(dRow, src.getBinary(sRow));
    }
    if (dt instanceof StringType
        || dt instanceof VarcharType
        || dt instanceof CharType
        || dt instanceof BinaryType) {
      return (dst, dRow, src, sRow) -> dst.putByteArray(dRow, src.getBinary(sRow));
    }
    throw new UnsupportedOperationException(
        "Splicing storage-filter pushdown does not support key type: " + dt);
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
   * <p>Data slots whose indices appear in {@code skipDataSlots} are left null. The splicing
   * late-materialization path uses this to skip allocation for storage-filter key columns: those
   * slots are sourced from per-key-column queues populated in phase 1 (see {@code
   * nextBatchSplicing}).
   *
   * Capacity is the initial capacity of the vector, and it will grow as necessary.
   * Capacity is in number of elements, not number of bytes.
   */
  private ColumnVector[] allocateColumns(
      int capacity, StructType schema, boolean useOffHeap, int constantColumnLength,
      Set<Integer> skipDataSlots) {
    StructField[] fields = schema.fields();
    int fieldsLength = fields.length;
    ColumnVector[] vectors = new ColumnVector[fieldsLength];
    if (useOffHeap) {
      for (int i = 0; i < fieldsLength - constantColumnLength; i++) {
        if (skipDataSlots != null && skipDataSlots.contains(i)) continue;
        vectors[i] = new OffHeapColumnVector(capacity, fields[i].dataType());
      }
    } else {
      for (int i = 0; i < fieldsLength - constantColumnLength; i++) {
        if (skipDataSlots != null && skipDataSlots.contains(i)) continue;
        vectors[i] = new OnHeapColumnVector(capacity, fields[i].dataType());
      }
    }
    for (int i = fieldsLength - constantColumnLength; i < fieldsLength; i++) {
      vectors[i] = new ConstantColumnVector(capacity, fields[i].dataType());
    }
    return vectors;
  }
}
