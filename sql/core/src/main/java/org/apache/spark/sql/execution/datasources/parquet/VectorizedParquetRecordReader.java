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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns;
import org.apache.spark.sql.catalyst.InternalRow;
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

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(VectorizedParquetRecordReader.class);

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
   * <p>One {@link ParquetFileReader} ({@link #lateMatReader}, which is the base class's
   * {@link #fileReader}) drives all three phases. Its requested
   * schema is mutated per phase via {@link ParquetFileReader#setRequestedSchema}: all projected
   * columns for phase 0 ({@code getRowRanges}), the key columns for phase 1, and for phase 2 the
   * non-key columns while splicing or the whole projection once a row group gives it up.
   *
   * <p>The three sets are held as leaf-column lists rather than as {@link MessageType}s because
   * that is what both the reader and the byte metrics consume, and because
   * {@link MessageType#getColumns()} rebuilds the list on every call.
   *
   * <p>Each list's leaf {@link ColumnPath}s are resolved alongside it, because that is all the
   * footer walks below need and {@link ColumnPath#get} is not free: it allocates and interns
   * through parquet's canonicalizer, which those walks would otherwise repeat per column on every
   * one of their several calls per row group. As sets, because a walk goes over the block's column
   * chunks and asks which of them it is about, and a requested schema's leaf paths are unique
   * anyway.
   */
  private ParquetFileReader lateMatReader;
  private List<ColumnDescriptor> requestedColumns;
  private List<ColumnDescriptor> keyOnlyColumns;
  private List<ColumnDescriptor> nonKeyColumns;
  private Set<ColumnPath> keyOnlyPaths;
  private Set<ColumnPath> nonKeyPaths;
  /**
   * Whether a row group can be read in part, and what reading part of one transfers. Both are read
   * off the footer {@link #lateMatReader} already holds, so the byte metrics cost no IO.
   */
  private ParquetBlockAccounting blockAccounting;
  /**
   * What an accumulator holds per row for each key column, not counting the value bytes of a
   * variable-length type. Together with those value bytes it is what the per-row-group buffer is
   * measured against. See
   * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes}.
   */
  private int keyFixedBytesPerRow;
  /**
   * What one buffered variable-length value really costs the vector that holds it, as a multiple of
   * the value's own bytes: those bytes in the byte child, one null byte per element of that child,
   * and up to as much again because {@link WritableColumnVector#reserve} doubles the child when it
   * grows. Charged as a factor so the budget over-counts rather than under-counts, since nothing
   * above this buffer can spill it. A fixed-width key needs no factor: its accumulator is allocated
   * at capacity and never grows.
   */
  private static final long VARIABLE_LENGTH_BYTES_FACTOR = 4L;
  /**
   * The pages phase 2 read for the row group being emitted. Held because the column readers draw
   * from it for the whole row group, and closed when the next one is loaded: `readFilteredRowGroup`
   * hands out a store the file reader does not track, unlike `readNextRowGroup`.
   */
  private PageReadStore dataPages;

  /** Whether the filter has already been reported as failing to evaluate on this file. */
  private boolean loggedFilterEvaluationError;

  /**
   * Whether this read must not narrow a row group to part of its rows, which is what the Parquet
   * page index is for. Two things say so, and both leave the filter with the row groups it empties,
   * which need no index at all:
   *
   * <ul>
   *   <li>the file has no offset index for some column phase 2 needs, learned once from the read
   *       that fails;</li>
   *   <li>{@code parquet.filter.columnindex.enabled} is false, which says the page index this file
   *       does have is not to be trusted.</li>
   * </ul>
   *
   * <p>It also stops the reader from buffering key values phase 2 will have to read again.
   */
  private boolean pageIndexUnusable;

  /**
   * Whether the row group currently loaded is spliced. It starts true unless phase 2 will have to
   * read every projected column anyway, and turns false in phase 1 once the survivors buffered pass
   * the cap. False means phase 2 read every projected column, key columns included, so the emit
   * path takes them straight from the persistent batch.
   */
  private boolean spliceCurrentRowGroup;
  private int nextBlockIndex;
  private int totalBlockCount;
  /**
   * The key columns this file has, in the filter's key order, with everything init resolves about
   * each. Phase 1 reads them, and the survivor queues and accumulators are indexed the same way.
   */
  private KeyColumn[] keyColumns;
  /**
   * Where each key column this file does not have sits in the row the predicate evaluates. The
   * value the scan returns for it comes from the batch slot `keyColumnIndices` names for that
   * position.
   */
  private int[] missingKeyRowPositions;
  /**
   * Whether every key column is missing from this file, which makes the predicate constant for it.
   * Decided in {@link #initBatch}, since the constants only exist once its vectors are built.
   */
  private boolean allKeysMissing;
  private WritableColumnVector[] keyScratchVectors;
  private ColumnarBatch keyScratchBatch;

  /**
   * Splicing state. Phase 1 keeps the surviving key values it has already decoded, and the emit
   * path splices them back into the output batch, so phase 2 never reads the key columns.
   *
   * <p>The alternative is for phase 2 to read the key columns again under the surviving row ranges,
   * which needs none of this state but pays a second read of them for every row group. Nothing
   * absorbs that read on object storage, where it is a new GET rather than a page-cache hit.
   *
   * <p>{@link #isKeyTopLevel} marks the top-level slots that emit takes from the queues rather
   * than from a phase-2 read. {@link #keyVectorQueues} holds one queue per present key column of
   * capacity-sized survivor vectors, and {@link #currentKeyAccumulators} the vectors still filling.
   * A queue keeps owning its head while the batch is built on it, until the next emit closes it, so
   * survivor memory drains as the row group is emitted. {@link #persistentBatchColumns} is
   * {@link #initBatch}'s vector array and {@link #spliceBatchColumns} the array the emitted batch
   * is built over, which is why the two are closed separately.
   *
   * <p>Phase 1 evaluates a whole row group before its first batch is emitted, so the queues hold
   * every surviving key value of one row group at once. That is bounded per row group by
   * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes}, past which the row
   * group is read the plain way and nothing is buffered.
   */
  private boolean[] isKeyTopLevel;
  private ArrayDeque<WritableColumnVector>[] keyVectorQueues;
  private WritableColumnVector[] currentKeyAccumulators;
  /** Row count of {@link #currentKeyAccumulators}; all key columns advance in lockstep. */
  private int currentKeyAccumulatorRowCount;
  /** Whether each queue's head is the vector the current batch is built on. */
  private boolean keyVectorsPublished;
  private ColumnVector[] persistentBatchColumns;
  private ColumnVector[] spliceBatchColumns;

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
      // The batch's vectors are closed through `persistentBatchColumns` rather than through
      // `columnarBatch.close()`, which lets both paths share one body. While splicing the emitted
      // batch is a view whose slots alias these vectors (non-key and partition) and the head of
      // each survivor queue (key slots), so closing it would re-close a shared vector and free the
      // same buffer twice. Without splicing its array is this one.
      try {
        if (persistentBatchColumns != null) {
          closeAll(persistentBatchColumns);
          persistentBatchColumns = null;
        }
        spliceBatchColumns = null;
        columnarBatch = null;
      } finally {
        closeSplicingState();
      }
    } finally {
      try {
        // Through the array, not through `keyScratchBatch`: the batch is assigned only after the
        // allocation loop finishes, so a partial failure leaves vectors only the array can reach.
        closeAll(keyScratchVectors);
        keyScratchVectors = null;
        keyScratchBatch = null;
      } finally {
        try {
          closeDataPages();
        } finally {
          // lateMatReader aliases the base-class reader; super.close() owns it.
          lateMatReader = null;
          super.close();
        }
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

    // Every slot gets a vector, key columns included. While splicing a key slot's vector is unused,
    // since the emitted batch takes that slot from the survivor queues. A row group read the plain
    // way past the buffer cap does read into it, and one capacity-sized vector per key column is
    // cheap next to the buffer the cap is there to bound.
    ColumnVector[] vectors = allocateColumns(
      capacity, batchSchema, memMode == MemoryMode.OFF_HEAP, constantColumnLength);

    persistentBatchColumns = vectors;
    if (keyColumns != null) {
      // Splicing hands out one batch for the whole read, over its own array, whose key slots the
      // emit path rewrites in place. `ColumnarBatch` holds the array by reference, its staging row
      // included, so rewriting a slot is what publishes it.
      spliceBatchColumns = vectors.clone();
      columnarBatch = new ColumnarBatch(spliceBatchColumns);
    } else {
      columnarBatch = new ColumnarBatch(vectors);
    }

    columnVectors = new ParquetColumnVector[sparkSchema.fields().length];
    for (int i = 0; i < columnVectors.length; i++) {
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

    if (allKeysMissing) {
      decideAllKeysMissingFile();
    }
  }

  /**
   * Answers the storage filter for a file that has none of its key columns, which makes the
   * predicate constant over the file. It is evaluated against the vectors just built for those
   * columns, so the value it sees is the one the scan returns for them.
   *
   * <p>False skips the file. True, or an error, reads it the way a plain scan would: the same
   * fail-open rule as a row's value, since the constant can be one the predicate throws on.
   */
  private void decideAllKeysMissingFile() {
    allKeysMissing = false;
    int[] keyIndices = storageFilter.keyColumnIndices();
    ColumnVector[] keyVectors = new ColumnVector[keyIndices.length];
    for (int p = 0; p < keyIndices.length; p++) {
      keyVectors[p] = columnVectors[keyIndices[p]].getValueVector();
    }
    // Not closed, and must not be: the vectors in it belong to the batch this reader emits.
    ColumnarBatch keyRow = new ColumnarBatch(keyVectors, 1);
    boolean keep;
    try {
      keep = storageFilter.test(keyRow.getRow(0));
    } catch (RuntimeException e) {
      logFilterGivenUpOnError(e);
      keep = true;
    }
    if (!keep) {
      recordFileSkipped();
      hitEndOfData = true;
    }
    storageFilter = null;
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
   * Calling this enables the vectorized reader. This should be called before any calls to
   * nextKeyValue/nextBatch.
   *
   * <p>The object is reused, a storage filter included: the reader then rewrites the batch's key
   * slots in place with the survivor vectors it spliced for that batch.
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
    releasePublishedKeyVectors();
    for (ParquetColumnVector vector : columnVectors) {
      vector.reset();
    }
    // Zeroed before the terminal checks below, so a terminal call cannot leave a spliced batch
    // pointing at the key vectors just released, which off heap is freed memory.
    columnarBatch.setNumRows(0);
    if (hitEndOfData) return false;
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();
    if (hitEndOfData) return false;

    int num = (int) Math.min(capacity, totalCountLoadedSoFar - rowsReturned);
    // Without a storage filter the emitted batch is `initBatch`'s array and nothing below applies.
    if (spliceBatchColumns != null) {
      pointKeySlotsAtThisBatch(num);
    }
    readPersistentColumns(num);
    // If needed, compute row indexes within a file. The row-index column is identified by name
    // (ROW_INDEX_TEMPORARY_COLUMN_NAME), a synthetic metadata column no storage filter references,
    // so its slot is always a non-key one and its ParquetColumnVector is always the persistent one.
    if (rowIndexGenerator != null) {
      rowIndexGenerator.populateRowIndex(columnVectors, num);
    }
    finishBatch(num);
    return true;
  }

  /**
   * Reads {@code num} rows into the persistent batch slots and assembles them. Every case goes
   * through here: the plain read, a spliced row group, whose key slots have no phase-2 reader
   * because the emitted batch takes them from the survivor queues, and a row group read the plain
   * way past the buffer cap, which has a reader for every slot.
   */
  private void readPersistentColumns(int num) throws IOException {
    for (int i = 0; i < columnVectors.length; i++) {
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
  }

  /** Publishes {@code num} rows as the current batch. */
  private void finishBatch(int num) {
    columnarBatch.setNumRows(num);
    rowsReturned += num;
    numBatched = num;
    batchIdx = 0;
  }

  /**
   * Points the emitted batch's key slots at the vectors this batch takes them from. They are the
   * only slots that ever change: a non-key slot holds the vector {@link #initBatch} built for it,
   * for the reader's life. A spliced row group takes the head of each survivor queue, which is what
   * publishes the buffered keys, and any other row group takes the persistent vector, which is what
   * a row group following a spliced one needs.
   *
   * <p>The same {@link ColumnarBatch} is handed out every time, over an array it holds by
   * reference. The queues keep owning the vectors they publish until the next batch releases them,
   * so a phase-2 read that throws afterwards leaves them reachable for {@link #close()}.
   */
  private void pointKeySlotsAtThisBatch(int num) {
    if (spliceCurrentRowGroup) {
      for (int k = 0; k < keyVectorQueues.length; k++) {
        if (keyVectorQueues[k].isEmpty()) {
          // Unreachable: the queues hold exactly the survivors phase 1 accumulated, and the emit
          // loop is driven by that same count. Named rather than left to NoSuchElementException.
          throw new IllegalStateException(String.format(
              "Storage-filter survivor queue %d of row group %d in %s ran out with %d rows still "
                  + "to emit", k, nextBlockIndex - 1, lateMatReader.getFile(), num));
        }
      }
    }
    keyVectorsPublished = spliceCurrentRowGroup;
    // Queue k holds the survivors of key column k, and its `batchSlot` is where that key column
    // sits in the batch, so the pairing is read off rather than re-derived from slot order.
    for (int k = 0; k < keyColumns.length; k++) {
      int slot = keyColumns[k].batchSlot();
      spliceBatchColumns[slot] =
          spliceCurrentRowGroup ? keyVectorQueues[k].peekFirst() : persistentBatchColumns[slot];
    }
  }

  /**
   * Closes the key vectors the previous batch was built on. The queues own them until here, so
   * survivor memory drains as a row group is emitted rather than all at its end.
   */
  private void releasePublishedKeyVectors() {
    if (!keyVectorsPublished) return;
    keyVectorsPublished = false;
    for (ArrayDeque<WritableColumnVector> queue : keyVectorQueues) {
      queue.removeFirst().close();
    }
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
   * {@link #initializeLateMaterialization()} then inspects the per-file schema and decides whether
   * splicing engages.
   *
   * <p>It does not engage in one real case: every key column is missing from this physical file
   * under schema evolution. The predicate is then rewritten with each missing key replaced by the
   * constant the reader materializes for it (its existence DEFAULT, else null) and evaluated once.
   * True keeps the file unfiltered, false or null skips it.
   *
   * <p>Everything else the filter needs is guaranteed by
   * {@code FileSourceStrategy.storageFiltersFor} and {@code ParquetStorageFilter.create}, so a
   * violation of it here is a planner bug and is asserted rather than handled. A file the reader
   * simply cannot prune is a different matter: it reads it the way a plain scan would.
   */
  public void setStorageFilter(ParquetStorageFilter storageFilter) {
    this.storageFilter = storageFilter;
  }

  private void initializeLateMaterialization() throws IOException {
    lateMatReader = fileReader;
    if (lateMatReader == null) {
      // The three phases drive a ParquetFileReader directly, and this reader was handed a row-group
      // reader with none behind it, so the filter is simply not applied and the plain read path
      // runs. The conjunct is in the post-scan filter as well, so the rows it would have dropped
      // are dropped above the scan.
      storageFilter = null;
      return;
    }
    blockAccounting = new ParquetBlockAccounting(lateMatReader);
    // That conf is the escape hatch for a file whose page index is wrong, so the filter must not
    // narrow a row group here either. Phase 2 reads part of a row group through the offset index,
    // which parquet consults whatever the conf says, and a wrong one there pairs a row's key with
    // another row's values. The post-scan Filter cannot catch that: the key it sees is the right
    // one. What the filter keeps is the row groups it empties, the same degrade a file with no page
    // index gets.
    pageIndexUnusable =
        !configuration.getBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true);
    // Resolve each key column's top-level ParquetColumn. Partition into present and missing (the
    // latter can happen under schema evolution: a column is in the requested schema but not in this
    // physical parquet file). For any non-primitive key we still bail; phase-1 reads only primitive
    // leaves.
    int[] keyIndices = storageFilter.keyColumnIndices();
    List<ParquetColumn> presentKeyColumns = new ArrayList<>(keyIndices.length);
    List<Integer> presentKeyPositions = new ArrayList<>(keyIndices.length);
    List<Integer> missingKeyPositions = new ArrayList<>();
    for (int i = 0; i < keyIndices.length; i++) {
      int idx = keyIndices[i];
      if (idx < 0 || idx >= parquetColumn.children().size()) {
        // Unreachable: ParquetStorageFilter.create rejects out-of-range ordinals.
        throw new IllegalStateException(String.format(
            "Storage-filter key ordinal %d is out of range for a %d-column requested schema",
            idx, parquetColumn.children().size()));
      }
      ParquetColumn column = parquetColumn.children().apply(idx);
      if (!column.isPrimitive()) {
        // Unreachable: ParquetStorageFilter.isSupportedKeyType admits only types with a primitive
        // Parquet leaf, and it gates both planning and ParquetStorageFilter.create.
        throw new IllegalStateException(
            "Storage-filter key column is not a primitive Parquet column: " + column.path());
      }
      if (missingColumns.contains(column)) {
        missingKeyPositions.add(i);
      } else {
        presentKeyPositions.add(i);
        presentKeyColumns.add(column);
      }
    }

    // A key column this file does not have is not substituted into the predicate. Its slot in the
    // row phase 1 evaluates points at the vector `initBatch` builds for it, which holds exactly
    // what the scan will return for that column: its existence DEFAULT, or null where it has none.
    // So there is one rule for what a missing column reads as, ParquetColumnVector's, and the
    // filter cannot decide on a value the rows never show.
    requestedColumns = requestedSchema.getColumns();
    if (presentKeyColumns.isEmpty()) {
      // Nothing for phase 1 to read, so the predicate is constant for this file. Deciding needs the
      // vectors `initBatch` has not built yet, so it happens there, in `decideAllKeysMissingFile`.
      allKeysMissing = true;
      return;
    }

    int numKeys = presentKeyColumns.size();
    ColumnDescriptor[] keyDescriptors = new ColumnDescriptor[numKeys];
    Types.MessageTypeBuilder keySchemaBuilder = Types.buildMessage();
    Set<String> keyTopLevelNames = new HashSet<>();
    for (int i = 0; i < numKeys; i++) {
      keyDescriptors[i] = presentKeyColumns.get(i).descriptor().get();
      // Preserve the field name/type as it appears at the top of requestedSchema.
      String topLevelName = keyDescriptors[i].getPath()[0];
      keySchemaBuilder.addField(requestedSchema.getType(topLevelName));
      keyTopLevelNames.add(topLevelName);
    }
    keyOnlyColumns = keySchemaBuilder.named(requestedSchema.getName()).getColumns();
    keyOnlyPaths = ParquetBlockAccounting.pathsOf(keyOnlyColumns);

    // Build the non-key (complement) schema, which phase 2 reads under finalRanges. With nothing in
    // it there is nothing to prune: such a scan reads the same columns for the same rows as a plain
    // one, because phase 1 has to read a key column to evaluate the filter on it. So the reader
    // declines rather than carrying a second emit path for it. The planner does not push this shape
    // either, which makes the two consistent without depending on each other.
    Types.MessageTypeBuilder nonKeyBuilder = Types.buildMessage();
    int nonKeyFieldCount = 0;
    for (Type field : requestedSchema.getFields()) {
      if (!keyTopLevelNames.contains(field.getName())) {
        nonKeyBuilder.addField(field);
        nonKeyFieldCount++;
      }
    }
    if (nonKeyFieldCount == 0) {
      storageFilter = null;
      return;
    }
    nonKeyColumns = nonKeyBuilder.named(requestedSchema.getName()).getColumns();
    nonKeyPaths = ParquetBlockAccounting.pathsOf(nonKeyColumns);
    missingKeyRowPositions = new int[missingKeyPositions.size()];
    for (int i = 0; i < missingKeyRowPositions.length; i++) {
      missingKeyRowPositions[i] = missingKeyPositions.get(i);
    }

    // Resolved here, past the two returns above, because `ValueCopier.forType` rejects a type the
    // reader cannot copy, and a file the reader has already declined must not be failed over one.
    keyColumns = new KeyColumn[numKeys];
    keyFixedBytesPerRow = 0;
    StructField[] fields = sparkRequestedSchema.fields();
    for (int i = 0; i < numKeys; i++) {
      int rowPosition = presentKeyPositions.get(i);
      int batchSlot = keyIndices[rowPosition];
      DataType type = fields[batchSlot].dataType();
      boolean variableLength = ValueCopier.isVariableLength(type);
      keyColumns[i] = new KeyColumn(batchSlot, rowPosition, keyDescriptors[i],
          presentKeyColumns.get(i).required(), type, ValueCopier.forType(type), variableLength);
      // One null byte per row either way. A fixed-width value adds its own width, a variable-length
      // one the int offset and int length that point at the byte child.
      keyFixedBytesPerRow += variableLength ? 1 + 8 : 1 + type.defaultSize();
    }
    initializeSplicingState();

    totalBlockCount = lateMatReader.getRowGroups().size();
    nextBlockIndex = 0;
  }

  /**
   * Populates the splicing bookkeeping. {@code keyColumnIndices} already index the top-level
   * slots of {@link #sparkSchema}, the same indexing as {@link #columnVectors}, so they map
   * directly onto {@link #isKeyTopLevel}, which says which batch slots the emit path may take from
   * the survivor queues.
   */
  @SuppressWarnings("unchecked")
  private void initializeSplicingState() {
    isKeyTopLevel = new boolean[sparkSchema.fields().length];
    // Only the key columns this file has: a missing one keeps the constant vector `initBatch` built
    // for its slot, since there is nothing to buffer and nothing to splice back.
    for (KeyColumn key : keyColumns) {
      isKeyTopLevel[key.batchSlot()] = true;
    }
    keyVectorQueues = new ArrayDeque[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyVectorQueues[i] = new ArrayDeque<>();
    }
    currentKeyAccumulators = new WritableColumnVector[keyColumns.length];
    currentKeyAccumulatorRowCount = 0;
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
      // This path never picks the rows itself: whether parquet narrowed the row group by a pushed
      // filter's column index is known only to the store it handed back.
      initColumnReader(pages, null, cv);
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
   *   - Phase 2: read the non-key columns restricted to {@code finalRanges}. A row group that gave
   *     splicing up reads the whole projection instead, still under {@code finalRanges}, and one
   *     that gave the filter up reads it under {@code pushedFilterRanges}, which is what a plain
   *     scan reads.
   *
   * Row groups for which {@code finalRanges} is empty are skipped entirely (no phase-2 IO).
   * Sets {@link #hitEndOfData} when all row groups have been processed.
   */
  private void loadNextRowGroupWithLateMaterialization() throws IOException {
    // The previous row group is fully emitted by the time this is called, so its pages are done
    // with. Released here rather than at the next assignment, so an all-keys row group, which reads
    // no data pages at all, does not keep the one before it alive.
    closeDataPages();
    while (nextBlockIndex < totalBlockCount) {
      int blockIdx = nextBlockIndex++;
      long blockRowCount = lateMatReader.getRowGroups().get(blockIdx).getRowCount();
      if (blockRowCount == 0) {
        // parquet-mr never writes these, but an empty block makes parquet's own getRowRanges build
        // Range(0, -1) and trip its `from <= to` assertion. The plain read path skips them too.
        continue;
      }
      // Splicing buffers one key value per surviving row of the whole row group before it can emit
      // the first batch, and that buffer is outside any MemoryConsumer, so phase 1 counts what it
      // holds against `maxSplicedRowGroupBytes` together with the row ranges phase 2 will hold.
      // Past that it gives splicing up, and past it again the filter itself.

      // Phase 0: rows allowed by the pushed data filter, at column-index granularity. The full
      // requestedSchema goes back on first, because phases 1 and 2 narrow it and
      // ParquetFileReader.getRowRanges computes ranges against the reader's current paths.
      lateMatReader.setRequestedSchema(requestedColumns);
      RowRanges pushedFilterRanges = pushedFilterRangesFor(blockIdx, blockRowCount);
      // RowRanges.rowCount() walks every range, so resolve each range set's count once.
      long baselineRows = pushedFilterRanges.rowCount();
      if (baselineRows == 0) {
        // Pushed data filter rejects this block entirely via column index. Not a storage-filter
        // skip, so we don't increment storage-filter metrics.
        continue;
      }

      // What this feature can avoid reading is the non-key columns of the rows the storage filter
      // rejects, so that is the baseline the byte metrics are measured against: the non-key bytes
      // a plain read of this projection would transfer for every row the pushed filter kept.
      // compressedBytesForRowRanges never does IO of its own.
      StorageFilterMetrics m = storageFilter.metrics();
      long nonKeyBaselineBytes = blockAccounting.compressedBytesForRowRanges(blockIdx, nonKeyPaths,
          pushedFilterRanges, baselineRows);

      // Whether phase 2 can read part of this row group at all, which is what everything below
      // buffers for. It needs an offset index for every column it reads, and the footer already
      // says which columns have one, for free. Without that check the cost of learning it is a row
      // group's survivors copied and then thrown away, since the phase-2 read is where parquet
      // resolves the indexes and throws. The check cannot replace that catch: it covers the columns
      // a spliced row group reads, not the ones it would read after giving splicing up, and an
      // index can be present but unreadable.
      // False says the filter is left with emptying this row group whole, and then nothing is
      // buffered either: phase 2 will read the key columns again along with everything else.
      boolean canNarrowRowGroup =
          !pageIndexUnusable && blockAccounting.hasOffsetIndexes(blockIdx, nonKeyPaths);
      spliceCurrentRowGroup = canNarrowRowGroup;

      // Phase 1: switch to key-only schema, read key columns under pushedFilterRanges, evaluate the
      // storage filter per row. The defaults below are what a row group whose filter is given up
      // emits, which is every row of `pushedFilterRanges`, exactly what a plain read would.
      RowRanges finalRanges = pushedFilterRanges;
      long finalRowCount = baselineRows;
      lateMatReader.setRequestedSchema(keyOnlyColumns);
      // Closed at the end of the phase that reads it. `readFilteredRowGroup` hands out a store
      // the file reader does not track, unlike `readNextRowGroup`, so nothing else would.
      RowRanges survivors;
      try (PageReadStore keyPages =
               lateMatReader.readFilteredRowGroup(blockIdx, pushedFilterRanges)) {
        if (keyPages == null) {
          // Unreachable: readFilteredRowGroup returns null only for an empty block, and we know
          // pushedFilterRanges selects at least one row. Skipping the block here would drop its
          // surviving rows from the output, so assert rather than `continue`.
          throw new IllegalStateException(
              "No key pages for row group " + blockIdx + " despite " + baselineRows
                  + " rows selected by the pushed filter");
        }
        survivors = evaluateStorageFilter(
            keyPages, pushedFilterRanges, baselineRows, blockRowCount, canNarrowRowGroup);
      }
      // Null says phase 1 gave the filter up for this row group, and then the ranges it built are
      // incomplete: what gets read is every row the pushed filter kept, exactly what a plain read
      // would read. Phase 2 can give it up too, in the catch below.
      boolean filterGivenUp = survivors == null;
      if (survivors != null) {
        finalRanges = survivors;
        finalRowCount = survivors.rowCount();
        if (finalRowCount == 0) {
          // Every surviving row was rejected by the storage filter; skip the block entirely,
          // which avoids the whole non-key baseline. Phase 1 still paid to read the key columns,
          // and that cost is not part of the baseline, so nothing is subtracted from it here.
          recordRowGroupSkipped(m, baselineRows, nonKeyBaselineBytes);
          continue;
        }
      }

      // Phase 2 reads the non-key columns under the surviving rows, or the whole projection under
      // `pushedFilterRanges` for a row group whose filter was given up.
      lateMatReader.setRequestedSchema(spliceCurrentRowGroup ? nonKeyColumns : requestedColumns);
      // Reading a strict subset of a block's rows needs a Parquet offset index, and parquet
      // enforces that itself: it resolves every requested column's offset index before reading
      // anything, and a column without one makes its column index store throw
      // MissingOffsetIndexException. Files written before parquet-mr 1.11, or by a writer that
      // omits the page index (pyarrow's `write_table` defaults to `write_page_index=False`), have
      // none. The filter is then given up for this row group and the read retried over
      // `pushedFilterRanges`, which is what a plain scan reads. That retry cannot hit the same
      // wall: a store missing one column's offset index reports no column index either, so
      // `getRowRanges` could not have narrowed anything and the ranges cover the whole block.
      //
      // Asked this way rather than up front, from the footer. Parquet resolves the index over the
      // paths current at its first lookup for the block, which without a pushed data filter is
      // the non-key columns alone, so a footer walk over the projection is both stricter than the
      // read and blind to an index that is claimed but unreadable. It is also where the throw
      // costs least: it lands before any data page is read.
      try {
        dataPages = lateMatReader.readFilteredRowGroup(blockIdx, finalRanges);
      } catch (MissingOffsetIndexException e) {
        // The exception's own message rather than the exception: this is a file shape the feature
        // expects and degrades on, so a stack trace per file of a table written without a page
        // index is noise. Parquet logs the same condition on its own path without one either.
        LOG.warn("Reading {} without page-level storage filtering: reading part of a row group "
            + "needs a Parquet offset index, and this file has none for at least one column the "
            + "read needs ({}). Row groups the filter empties are still skipped whole",
            MDC.of(LogKeys.PATH, lateMatReader.getFile()),
            MDC.of(LogKeys.REASON, e.getMessage()));
        pageIndexUnusable = true;
        filterGivenUp = true;
        abandonSplicing();
        finalRanges = pushedFilterRanges;
        finalRowCount = baselineRows;
        // The retry only avoids the same wall because a store missing one column's offset index
        // reports no column index either, so these ranges cover the whole block and parquet reads
        // it without consulting an index. That is three parquet internals deep, so it is checked:
        // a release that changes any of them should fail here rather than throw from the read.
        if (baselineRows != blockRowCount) {
          throw new IllegalStateException(String.format(
              "Cannot read row group %d of %s without an offset index: the pushed filter selects "
                  + "%d of %d rows, so a plain read of the block is not what it asks for",
              blockIdx, lateMatReader.getFile(), baselineRows, blockRowCount));
        }
        restateTotalRowCount(blockIdx);
        lateMatReader.setRequestedSchema(requestedColumns);
        dataPages = lateMatReader.readFilteredRowGroup(blockIdx, finalRanges);
      }
      if (dataPages == null) {
        // Unreachable: readFilteredRowGroup returns null only for an empty block or empty ranges,
        // both excluded above. Match phase 1 and fail with a message rather than an NPE.
        throw new IllegalStateException(
            "No data pages for row group " + blockIdx + " despite " + finalRowCount
                + " rows to read");
      }
      long keptRows = dataPages.getRowCount();
      // Nothing is reported for a row group whose filter was given up: it read what a plain scan
      // reads, so the saving is a certain zero.
      if (!filterGivenUp) {
        long phase2Bytes = blockAccounting.compressedBytesForRowRanges(blockIdx,
            nonKeyPaths, finalRanges, finalRowCount);
        if (!spliceCurrentRowGroup) {
          // This row group gave splicing up, so phase 2 read the key columns a second time. The
          // baseline counts them once, in phase 1, so the extra read is a cost against it.
          phase2Bytes += blockAccounting.compressedBytesForRowRanges(blockIdx,
              keyOnlyPaths, finalRanges, finalRowCount);
        }
        // `SQLMetric.add` ignores a negative value, so a row group that read more than the
        // baseline after giving splicing up contributes nothing rather than subtracting.
        m.bytesAvoidedByPageFiltering().add(nonKeyBaselineBytes - phase2Bytes);
      }
      long filteredRows = baselineRows - keptRows;
      if (filteredRows > 0) m.rowsExcludedWithinRowGroup().add(filteredRows);

      if (rowIndexGenerator != null) {
        rowIndexGenerator.initFromPageReadStore(dataPages);
      }
      // Phase 2 read exactly these rows, so its readers are handed the ranges rather than left to
      // rebuild them from the store's one row index per row.
      RowRanges readerRanges = readerRangesFor(finalRanges, finalRowCount, blockRowCount);
      for (int i = 0; i < columnVectors.length; i++) {
        if (spliceCurrentRowGroup && isKeyTopLevel[i]) {
          // A spliced key slot takes its values from the survivor queues at emit, so phase 2 has no
          // reader for it. Cleared rather than left alone: a reader a previous row group set would
          // otherwise be driven over this row group's pages. Key columns are primitive, which is
          // what `setColumnReader` requires.
          columnVectors[i].setColumnReader(null);
          continue;
        }
        initColumnReader(dataPages, readerRanges, columnVectors[i]);
      }
      totalCountLoadedSoFar += keptRows;
      return;
    }
    hitEndOfData = true;
  }


  /** Counts a row group whose data columns the filter kept the reader from touching at all. */
  private static void recordRowGroupSkipped(
      StorageFilterMetrics m, long excludedRows, long avoidedBytes) {
    m.rowGroupsSkipped().add(1L);
    m.rowsExcludedByRowGroup().add(excludedRows);
    m.bytesAvoidedByRowGroup().add(avoidedBytes);
  }

  /**
   * Counts a file the filter rejects whole, which happens when every key column is missing from it
   * and the predicate is constant-false for the value the reader would have materialized. Every row
   * group counts as skipped and every projected byte as avoided, which is what the counters mean
   * for a row group the filter empties.
   */
  private void recordFileSkipped() {
    StorageFilterMetrics m = storageFilter.metrics();
    Set<ColumnPath> requestedPaths = ParquetBlockAccounting.pathsOf(requestedColumns);
    List<BlockMetaData> blocks = lateMatReader.getRowGroups();
    for (int blockIdx = 0; blockIdx < blocks.size(); blockIdx++) {
      // Measured against the rows the pushed data filter kept, which is the baseline every other
      // skip path uses: the rows its column index already excluded were never this filter's to
      // save. Resolving them again is a cache hit whenever the two can differ, because
      // `getFilteredRecordCount()` at initialize resolved every block's ranges then.
      long blockRowCount = blocks.get(blockIdx).getRowCount();
      if (blockRowCount == 0) continue;
      RowRanges blockRanges = pushedFilterRangesFor(blockIdx, blockRowCount);
      long survivingRows = blockRanges.rowCount();
      if (survivingRows == 0) continue;
      // The key columns are missing from this file, so they contribute nothing to the walk, and the
      // whole projection is what a plain read would have transferred.
      long avoidedBytes = blockAccounting.compressedBytesForRowRanges(
          blockIdx, requestedPaths, blockRanges, survivingRows);
      recordRowGroupSkipped(m, survivingRows, avoidedBytes);
    }
  }

  /**
   * The rows of a block the pushed data filter allows, at column-index granularity, or every row of
   * the block when {@link #pageIndexUnusable} says the page index must not be used.
   *
   * <p>{@code getRowRanges} asks only whether a filter is pushed, not whether column-index
   * filtering is still enabled, so this is where that conf is honoured. Every phase reads within
   * these ranges, so narrowing them by an index the read may not trust would cost rows.
   */
  private RowRanges pushedFilterRangesFor(int blockIdx, long blockRowCount) {
    return pageIndexUnusable
        ? RowRanges.createSingle(blockRowCount)
        : lateMatReader.getRowRanges(blockIdx);
  }

  /**
   * Restates how many rows this reader will return, from {@code firstWholeBlock} on, as every row
   * of every block left. Called when {@link #pageIndexUnusable} latches mid-file, which is the one
   * thing that can make this reader return more rows than it said it would.
   *
   * <p>{@link #totalRowCount} is {@code getFilteredRecordCount()}, which counted every block
   * through the page index at initialize, and {@link #nextBatch} stops the read once it has
   * returned that many. From here on the index is not to be trusted, so every remaining block is
   * read whole, and the old count would stop the read inside one of them, silently dropping the
   * rest of the file. It cannot be too low afterwards: a block the filter empties is skipped
   * without contributing, so the restated count can only overshoot, and then
   * {@code hitEndOfData} ends the read instead.
   */
  private void restateTotalRowCount(int firstWholeBlock) {
    totalRowCount = totalCountLoadedSoFar;
    List<BlockMetaData> blocks = lateMatReader.getRowGroups();
    for (int i = firstWholeBlock; i < totalBlockCount; i++) {
      totalRowCount += blocks.get(i).getRowCount();
    }
  }

  /**
   * The ranges to hand the column readers of a read that asked for {@code rowRanges}, which select
   * {@code rowCount} of the block's {@code blockRowCount} rows. They then find those rows in the
   * pages with no per-row work.
   *
   * <p>None for a read of every row: parquet reads the block whole in that case, and a reader that
   * filters by nothing is cheaper than one that filters by everything.
   */
  private static RowRanges readerRangesFor(
      RowRanges rowRanges, long rowCount, long blockRowCount) {
    return rowCount == blockRowCount ? null : rowRanges;
  }

  /**
   * What the surviving rows of the current row group cost to hold as row ranges, which is the half
   * of the budget that a row group buffering nothing still pays. A filter whose survivors are
   * scattered makes one range per surviving row, and phase 2 needs the whole set to select its
   * pages. One set is held, not one per column reader: the readers of a row group walk this same
   * list, each with a cursor of its own.
   */
  private static long rowRangeStateBytes(long rangeCount) {
    // Parquet's own `RowRanges.Range`: two longs, their object header, and the list slot for it.
    return rangeCount * 40L;
  }

  /**
   * Evaluates the storage filter over every row of a key-only {@link PageReadStore}, in
   * capacity-sized chunks, and returns the surviving rows as {@link RowRanges} in block-row
   * coordinates. The result is a subset of {@code pushedFilterRanges}: rows outside it were never
   * read.
   *
   * <p>Each survivor's key values are appended to {@link #currentKeyAccumulators} for the emit path
   * to splice, until the buffer passes its cap. From there the row group is evaluated without
   * buffering and {@link #spliceCurrentRowGroup} is false, so its phase 2 reads the key columns
   * again along with everything else.
   *
   * <p>Returns null once the budget makes the reader give the filter up for this row group: the
   * ranges built so far are then incomplete, and the caller reads the row group the plain way.
   */
  private RowRanges evaluateStorageFilter(
      PageReadStore keyPages,
      RowRanges pushedFilterRanges,
      long pushedFilterRowCount,
      long blockRowCount,
      boolean canNarrowRowGroup) throws IOException {
    ensureKeyScratchAllocated();
    long remaining = pushedFilterRowCount;
    // Phase 1 read exactly the pushed filter's rows, so its readers are handed those ranges.
    RowRanges keyReaderRanges =
        readerRangesFor(pushedFilterRanges, pushedFilterRowCount, blockRowCount);
    VectorizedColumnReader[] readers = new VectorizedColumnReader[keyColumns.length];
    for (int i = 0; i < readers.length; i++) {
      KeyColumn key = keyColumns[i];
      readers[i] =
          newColumnReader(key.descriptor(), key.required(), keyPages, keyReaderRanges);
    }
    if (spliceCurrentRowGroup) ensureCurrentKeyAccumulatorsAllocated();

    PrimitiveIterator.OfLong rowIndexIter = pushedFilterRanges.iterator();
    RowRanges.Builder finalRangesBuilder = RowRanges.builder();
    // What this row group retains, weighed against the budget below: the bytes buffered for
    // splicing, and the ranges the surviving rows fall into.
    long splicedBytes = 0L;
    long survivorRangeCount = 0L;
    long previousSurvivor = -2L;
    long cap = storageFilter.maxSplicedRowGroupBytes();
    while (remaining > 0) {
      int num = (int) Math.min((long) capacity, remaining);
      for (int i = 0; i < keyScratchVectors.length; i++) {
        keyScratchVectors[i].reset();
        readers[i].readBatch(num, keyScratchVectors[i], null, null);
      }
      keyScratchBatch.setNumRows(num);
      for (int r = 0; r < num; r++) {
        long blockRow = rowIndexIter.nextLong();
        boolean survives;
        try {
          survives = storageFilter.test(keyScratchBatch.getRow(r));
        } catch (RuntimeException e) {
          // Fail open, whatever the exception is. The predicate ran on a row that, in the plan, an
          // earlier conjunct would have rejected before it, so a plain scan never evaluates it
          // there. Giving the filter up for this row group puts every row the pushed filter kept
          // back in the output, and the post-scan Filter then evaluates the conjuncts in their own
          // order. Either an earlier one drops the row before this expression runs, or it does not
          // and the query fails the way it would have without this feature.
          //
          // Not narrowed to Spark's own errors, because a built-in expression can raise a plain JDK
          // one: `timestamp_seconds` of a decimal throws ArithmeticException from longValueExact.
          // Rethrowing has two bad ends, and neither is the error the plan would have raised: the
          // query fails where a plain read succeeds, and under `ignoreCorruptFiles` this reader's
          // exception is read as a corrupt file, which drops the rest of it silently.
          logFilterGivenUpOnError(e);
          abandonSplicing();
          return null;
        }
        if (survives) {
          if (!canNarrowRowGroup) {
            // Nothing narrower than the whole row group can be read, so whether anything survives
            // at all was the only thing left to learn, and it does. Stop here: the ranges are not
            // needed, and the rest of the key column does not have to be decoded or evaluated. The
            // row groups the filter empties are still skipped, which is the case that returns a
            // non-null empty range set below.
            abandonSplicing();
            return null;
          }
          finalRangesBuilder.addSelectedRow(blockRow);
          if (blockRow != previousSurvivor + 1) survivorRangeCount++;
          previousSurvivor = blockRow;
          if (spliceCurrentRowGroup) splicedBytes += appendSurvivorRowToAccumulators(r);
          // Both halves of what this row group retains grow per survivor, and either can cross
          // the budget on its own, so they are weighed together here and nowhere else. The cheaper
          // concession comes first: release the buffer, and give the filter up as well if the
          // ranges alone still do not fit.
          long rangeBytes = rowRangeStateBytes(survivorRangeCount);
          if (splicedBytes + rangeBytes > cap) {
            if (spliceCurrentRowGroup) {
              abandonSplicing();
              splicedBytes = 0L;
            }
            if (rangeBytes > cap) {
              // The ranges being built are about to be thrown away, so stop evaluating the rest.
              abandonSplicing();
              return null;
            }
          }
        }
      }
      remaining -= num;
    }

    if (spliceCurrentRowGroup) {
      finalizePartialAccumulators();
    }

    return finalRangesBuilder.build();
  }

  private void ensureKeyScratchAllocated() {
    if (keyScratchVectors != null) return;
    // Assigned before the loop on purpose: an allocation failure part way through then leaves the
    // vectors allocated so far reachable for `close()`, which walks this array element-wise.
    keyScratchVectors = new WritableColumnVector[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyScratchVectors[i] = newKeyVector(i);
    }
    // The row the predicate evaluates: the key columns phase 1 reads, and for a key column this
    // file does not have, the constant vector `initBatch` built for it. `reset()` is a no-op on
    // those, and this reader never closes them, since the emitted batch owns them.
    int[] keyIndices = storageFilter.keyColumnIndices();
    ColumnVector[] rowVectors = new ColumnVector[keyIndices.length];
    for (int i = 0; i < keyScratchVectors.length; i++) {
      rowVectors[keyColumns[i].rowPosition()] = keyScratchVectors[i];
    }
    for (int position : missingKeyRowPositions) {
      rowVectors[position] = columnVectors[keyIndices[position]].getValueVector();
    }
    keyScratchBatch = new ColumnarBatch(rowVectors);
  }

  /** A capacity-sized vector for the i-th key column this file has, in the reader's memory mode. */
  private WritableColumnVector newKeyVector(int keyIdx) {
    DataType dt = keyColumns[keyIdx].type();
    return MEMORY_MODE == MemoryMode.OFF_HEAP
        ? new OffHeapColumnVector(capacity, dt)
        : new OnHeapColumnVector(capacity, dt);
  }

  /**
   * Allocates any accumulator slot left null by the last push to the queues, {@link #capacity} rows
   * each.
   */
  private void ensureCurrentKeyAccumulatorsAllocated() {
    for (int i = 0; i < currentKeyAccumulators.length; i++) {
      if (currentKeyAccumulators[i] == null) {
        currentKeyAccumulators[i] = newKeyVector(i);
      }
    }
  }

  /**
   * Appends row {@code srcRow} of every key column to the accumulators, pushing them onto their
   * queues once full. All key columns advance in lockstep, which is what keeps the queues aligned.
   * Returns the bytes the row added, which the caller weighs against its budget.
   */
  private long appendSurvivorRowToAccumulators(int srcRow) {
    final int dstRow = currentKeyAccumulatorRowCount;
    final WritableColumnVector[] accs = currentKeyAccumulators;
    final WritableColumnVector[] srcs = keyScratchVectors;
    final KeyColumn[] keys = keyColumns;
    long valueBytes = 0L;
    for (int i = 0, n = accs.length; i < n; i++) {
      WritableColumnVector src = srcs[i];
      WritableColumnVector dst = accs[i];
      KeyColumn key = keys[i];
      if (src.isNullAt(srcRow)) {
        dst.putNull(dstRow);
      } else {
        key.copier().copy(dst, dstRow, src, srcRow);
        // Measured on the destination: a dictionary-encoded source has no length of its own, since
        // its values are read through the dictionary.
        if (key.variableLength()) {
          valueBytes += VARIABLE_LENGTH_BYTES_FACTOR * dst.getArrayLength(dstRow);
        }
      }
    }
    currentKeyAccumulatorRowCount = dstRow + 1;
    if (currentKeyAccumulatorRowCount == capacity) {
      pushAccumulatorsToQueues();
      ensureCurrentKeyAccumulatorsAllocated();
    }
    return keyFixedBytesPerRow + valueBytes;
  }

  /**
   * Hands every accumulator to its queue, which is what the emit path dequeues from. The row count
   * goes with them: it describes what the accumulators hold, and they now hold nothing.
   */
  private void pushAccumulatorsToQueues() {
    for (int i = 0; i < currentKeyAccumulators.length; i++) {
      keyVectorQueues[i].addLast(currentKeyAccumulators[i]);
      currentKeyAccumulators[i] = null;
    }
    currentKeyAccumulatorRowCount = 0;
  }

  /** Releases the pages phase 2 read for the row group just emitted, if any. */
  private void closeDataPages() {
    if (dataPages != null) {
      dataPages.close();
      dataPages = null;
    }
  }

  /**
   * Reports the first row group of this file whose filter could not be evaluated. Once per file,
   * because a file whose values do that tends to do it again, and the row groups that follow are
   * still filtered normally.
   */
  private void logFilterGivenUpOnError(RuntimeException e) {
    if (loggedFilterEvaluationError) return;
    loggedFilterEvaluationError = true;
    LOG.warn("Reading a row group of {} without the storage filter: evaluating it on a row raised "
        + "an error. The filter is still applied above the scan, so the answer is unchanged, and "
        + "the remaining row groups are filtered as usual", e,
        MDC.of(LogKeys.PATH, lateMatReader.getFile()));
  }

  /**
   * Gives up splicing for the row group being evaluated and releases every survivor vector it has
   * buffered. Phase 2 then reads the full projected schema and the emit path takes the persistent
   * batch, so the rows are unaffected.
   *
   * <p>Giving the filter up for that row group is this plus reading it over the rows the pushed
   * data filter allowed, exactly what a plain read does. Phase 1 says so by returning no ranges,
   * and phase 2 by the `filterGivenUp` local of the row-group loop.
   */
  private void abandonSplicing() {
    for (ArrayDeque<WritableColumnVector> q : keyVectorQueues) {
      // A queue can be null if an allocation failed part way through `initializeSplicingState`.
      if (q == null) continue;
      for (WritableColumnVector v : q) v.close();
      q.clear();
    }
    closeAll(currentKeyAccumulators);
    Arrays.fill(currentKeyAccumulators, null);
    currentKeyAccumulatorRowCount = 0;
    spliceCurrentRowGroup = false;
  }

  /**
   * Pushes any partially-filled accumulator into its queue at row-group end so the emit path can
   * dequeue it as the row group's final batch.
   */
  private void finalizePartialAccumulators() {
    if (currentKeyAccumulatorRowCount == 0) return;
    pushAccumulatorsToQueues();
  }

  /**
   * Closes anything held by the splicing path: every vector still queued, the published head
   * included, and partially-filled accumulators. Called from {@link #close()}.
   */
  private void closeSplicingState() {
    keyVectorsPublished = false;
    // Releasing everything the splicing path holds is what `abandonSplicing` does, and both arrays
    // are set together by `initializeSplicingState`, so one null check covers the state.
    if (keyVectorQueues != null) {
      abandonSplicing();
      currentKeyAccumulators = null;
      keyVectorQueues = null;
    }
  }

  /** Closes every non-null vector of {@code vectors}; tolerates a null array. */
  private static void closeAll(ColumnVector[] vectors) {
    if (vectors == null) return;
    for (ColumnVector v : vectors) {
      if (v != null) v.close();
    }
  }

  /**
   * One key column this file has, as init resolves it: the batch slot it is projected into, where
   * it sits in the row the predicate reads, what phase 1 reads it with, and how its values are
   * copied and measured. One array of these rather than an array per fact, so nothing can pair the
   * wrong two.
   */
  private record KeyColumn(
      int batchSlot,
      int rowPosition,
      ColumnDescriptor descriptor,
      boolean required,
      DataType type,
      ValueCopier copier,
      boolean variableLength) {}

  /**
   * A column reader for one leaf column over one row group's pages, reading the rows
   * {@code rowRanges} names, or every row of the chunk when it is null. Every column reader this
   * class builds needs the same rebase and timezone settings, which are the file's.
   */
  private VectorizedColumnReader newColumnReader(
      ColumnDescriptor descriptor,
      boolean isRequired,
      PageReadStore pages,
      RowRanges rowRanges) throws IOException {
    return new VectorizedColumnReader(descriptor, isRequired, pages, rowRanges, convertTz,
        datetimeRebaseMode, datetimeRebaseTz, int96RebaseMode, int96RebaseTz, writerVersion);
  }

  private void initColumnReader(PageReadStore pages, RowRanges rowRanges, ParquetColumnVector cv)
      throws IOException {
    if (!missingColumns.contains(cv.getColumn())) {
      if (cv.getColumn().isPrimitive()) {
        ParquetColumn column = cv.getColumn();
        cv.setColumnReader(
            newColumnReader(column.descriptor().get(), column.required(), pages, rowRanges));
      } else {
        // Not in missing columns and is a complex type: this must be a struct
        for (ParquetColumnVector childCv : cv.getChildren()) {
          initColumnReader(pages, rowRanges, childCv);
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
