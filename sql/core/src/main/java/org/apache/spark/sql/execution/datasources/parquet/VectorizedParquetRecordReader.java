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
import org.apache.spark.sql.execution.datasources.UnsupportedFileReadException;
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
   * schema is mutated per phase via {@link ParquetFileReader#setRequestedSchema}: all projected
   * columns for phase 0 ({@code getRowRanges}), the key columns for phase 1, the non-key columns
   * for phase 2 (or skipped entirely when the projection is all keys, which is what
   * {@link #nonKeyColumns} being null means).
   *
   * <p>The three sets are held as leaf-column lists rather than as {@link MessageType}s because
   * that is what both the reader and the byte metrics consume, and because
   * {@link MessageType#getColumns()} rebuilds the list on every call.
   */
  private ParquetFileReader lateMatReader;
  private List<ColumnDescriptor> requestedColumns;
  private List<ColumnDescriptor> keyOnlyColumns;
  private List<ColumnDescriptor> nonKeyColumns;
  /**
   * What an accumulator holds per row for each key column, not counting the value bytes of a
   * variable-length type. Together with those value bytes it is what the per-row-group buffer is
   * measured against. See
   * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes}.
   */
  private int keyFixedBytesPerRow;
  /** Which key columns hold their values out of line, so a length has to be measured per row. */
  private boolean[] keyVariableLength;
  /** Key-value bytes buffered for the row group currently loading. */
  private long splicedBytes;
  /**
   * Whether the row group currently loaded is spliced. Starts true for every row group and can turn
   * false in phase 1, once the survivors buffered pass the cap; false means phase 2 read every
   * projected column, key columns included, so the emit path takes them straight from the
   * persistent batch.
   */
  private boolean spliceCurrentRowGroup;
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
   * Splicing state. Phase 1 keeps the surviving key values it has already decoded, and the emit
   * path splices them back into the output batch, so phase 2 never reads the key columns.
   *
   * <p>The alternative is for phase 2 to read the key columns again under the surviving row
   * ranges, which needs none of this state but pays a second read of them. That second read is
   * the whole cost of the scan when the projection is key columns alone, the shape a pushed join
   * filter most often produces: splicing skips phase 2 there, while a re-read has nothing left to
   * skip.
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
  private java.util.ArrayDeque<WritableColumnVector>[] keyVectorQueues;
  private WritableColumnVector[] currentKeyAccumulators;
  /** Row count of {@link #currentKeyAccumulators}; all key columns advance in lockstep. */
  private int currentKeyAccumulatorRowCount;
  /** Per-key-column copier picked once at init time; called per surviving row in the hot loop. */
  private ValueCopier[] keyCopiers;
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
      if (isKeyTopLevel != null) {
        // Splicing: the emitted batch is a view whose slots alias persistentBatchColumns (non-key
        // and partition) and the head of each survivor queue (key slots). We never call
        // columnarBatch.close() because that would re-close those shared vectors after the direct
        // closes below, freeing the same buffer twice.
        try {
          if (persistentBatchColumns != null) {
            for (ColumnVector v : persistentBatchColumns) {
              if (v != null) v.close();
            }
            persistentBatchColumns = null;
          }
          spliceBatchColumns = null;
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
        // Through the array, not through `keyScratchBatch`: the batch is assigned only after the
        // allocation loop finishes, so a partial failure leaves vectors only the array can reach.
        closeAll(keyScratchVectors);
        keyScratchVectors = null;
        keyScratchBatch = null;
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

    // Every slot gets a vector, key columns included. While splicing, a key slot's vector is unused
    // -- the emitted batch takes that slot from the survivor queues -- but a row group read the
    // plain way past the buffer cap reads into it, and one capacity-sized vector per key column is
    // cheap next to the buffer the cap is there to bound.
    ColumnVector[] vectors = allocateColumns(
      capacity, batchSchema, memMode == MemoryMode.OFF_HEAP, constantColumnLength);

    columnarBatch = new ColumnarBatch(vectors);
    persistentBatchColumns = vectors;
    if (isKeyTopLevel != null) {
      // Splicing hands out one batch for the whole read, over its own array, whose key slots the
      // emit path rewrites in place. `ColumnarBatch` holds the array by reference, its staging row
      // included, so rewriting a slot is what publishes it.
      spliceBatchColumns = vectors.clone();
      columnarBatch = new ColumnarBatch(spliceBatchColumns);
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
    readPersistentColumns(num, /* skipKeySlots= */ false);
    // If needed, compute row indexes within a file.
    if (rowIndexGenerator != null) {
      rowIndexGenerator.populateRowIndex(columnVectors, num);
    }
    finishBatch(num);
    return true;
  }

  /**
   * Reads {@code num} rows into the persistent batch slots and assembles them. All three emit paths
   * share this: the plain read, a spliced row group (which skips the key slots, since the emitted
   * batch takes those from the survivor queues), and a row group read the plain way past the buffer
   * cap (which reads every slot).
   *
   * <p>{@code skipKeySlots} is not the same as "the slot has no vector": every slot has one, and a
   * key slot's phase-2 column reader is deliberately left unset while splicing, so driving it would
   * use whatever a previous row group left behind.
   */
  private void readPersistentColumns(int num, boolean skipKeySlots) throws IOException {
    for (int i = 0; i < columnVectors.length; i++) {
      if (skipKeySlots && isKeyTopLevel[i]) continue;
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
   * Splicing emit path. Publishes one survivor key vector per key column into the batch, reads the
   * non-key slots for {@code num} rows, and hands out the same {@link ColumnarBatch} every time,
   * its key slots rewritten in place. The batch is a view over vectors owned elsewhere; see
   * {@link #close()} and {@link #closeSplicingState()}.
   */
  private boolean nextBatchSplicing() throws IOException {
    releasePublishedKeyVectors();
    for (ParquetColumnVector cv : columnVectors) {
      cv.reset();
    }
    // Zero the outgoing batch before the terminal checks below, as the plain path does. Otherwise a
    // terminal call leaves the batch pointing at key vectors that were just closed -- off-heap,
    // that is freed memory.
    if (columnarBatch != null) columnarBatch.setNumRows(0);
    if (hitEndOfData) return false;
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();
    if (hitEndOfData) return false;

    int num = (int) Math.min(capacity, totalCountLoadedSoFar - rowsReturned);

    if (!spliceCurrentRowGroup) {
      // This row group was read the plain way: phase 2 took every projected column, so every slot
      // of the batch comes from the persistent one.
      readPersistentColumns(num, /* skipKeySlots= */ false);
      if (rowIndexGenerator != null) {
        rowIndexGenerator.populateRowIndex(columnVectors, num);
      }
      System.arraycopy(persistentBatchColumns, 0, spliceBatchColumns, 0, spliceBatchColumns.length);
      finishBatch(num);
      return true;
    }

    for (int i = 0; i < keyVectorQueues.length; i++) {
      if (keyVectorQueues[i].isEmpty()) {
        // Unreachable: the queues hold exactly the survivors phase 1 accumulated, and the emit loop
        // is driven by that same count. Named rather than left to NoSuchElementException because
        // this one must not be swallowed: under ignoreCorruptFiles that would silently drop the
        // rest of a healthy file's rows.
        throw new UnsupportedFileReadException(String.format(
            "Storage-filter survivor queue %d of row group %d in %s ran out with %d rows still to "
                + "emit", i, nextBlockIndex - 1, lateMatReader.getFile(), num));
      }
    }
    // The queues keep owning these until the next emit releases them, so a phase-2 read below that
    // throws leaves them reachable for `close()`.
    keyVectorsPublished = true;
    // Key slots are filled in ascending slot order while `keyIdx` walks the queues in key-list
    // order, so the pairing is the identity only because `ParquetStorageFilter.create` sorts
    // `keyColumnIndices` ascending. `isKeyTopLevel` says which slots are keys, not where each
    // sits in that list, so this loop cannot re-derive the pairing: an unsorted list would swap
    // key columns in the output batch.
    int keyIdx = 0;
    for (int i = 0; i < spliceBatchColumns.length; i++) {
      if (i < isKeyTopLevel.length && isKeyTopLevel[i]) {
        spliceBatchColumns[i] = keyVectorQueues[keyIdx++].peekFirst();
      } else {
        spliceBatchColumns[i] = persistentBatchColumns[i];
      }
    }

    readPersistentColumns(num, /* skipKeySlots= */ true);
    if (rowIndexGenerator != null) {
      // Row-index column is identified by name (ROW_INDEX_TEMPORARY_COLUMN_NAME), which is a
      // synthetic metadata column never referenced by a storage filter, so its slot is a non-key
      // slot with a persistent ParquetColumnVector.
      rowIndexGenerator.populateRowIndex(columnVectors, num);
    }
    finishBatch(num);
    return true;
  }

  /**
   * Closes the key vectors the previous batch was built on. The queues own them until here, so
   * survivor memory drains as a row group is emitted rather than all at its end.
   */
  private void releasePublishedKeyVectors() {
    if (!keyVectorsPublished) return;
    keyVectorsPublished = false;
    for (java.util.ArrayDeque<WritableColumnVector> queue : keyVectorQueues) {
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
   * constant the reader materializes for it (its existence DEFAULT, else null) and evaluated once
   * -- true keeps the file unfiltered, false or null skips it.
   *
   * <p>Every other precondition is guaranteed by
   * {@code FileSourceStrategy.extractStorageFilters} and {@code ParquetStorageFilter.create}, and a
   * violation throws rather than falling back: extraction has already removed the filter from the
   * post-scan Filter, so not applying it would return rows the query rejected.
   */
  public void setStorageFilter(ParquetStorageFilter storageFilter) {
    this.storageFilter = storageFilter;
  }

  private void initializeLateMaterialization() throws IOException {
    lateMatReader = reader.getUnderlyingReader();
    if (lateMatReader == null) {
      // Unreachable: the only ParquetRowGroupReader ParquetFileFormat builds is
      // ParquetRowGroupReaderImpl, which exposes its reader.
      throw new UnsupportedFileReadException(
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
        // Unreachable: ParquetStorageFilter.create rejects out-of-range ordinals.
        throw new UnsupportedFileReadException(String.format(
            "Storage-filter key ordinal %d is out of range for a %d-column requested schema",
            idx, parquetColumn.children().size()));
      }
      ParquetColumn column = parquetColumn.children().apply(idx);
      if (!column.isPrimitive()) {
        // Unreachable: ParquetStorageFilter.isSupportedKeyType admits only types with a primitive
        // Parquet leaf, and it gates both planning and ParquetStorageFilter.create.
        throw new UnsupportedFileReadException(
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
        // Every key column is missing, so the rewritten predicate is constant for this file.
        boolean keepAll = storageFilter.evalAllMissing();
        storageFilter = null;
        hitEndOfData = !keepAll;
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
    requestedColumns = requestedSchema.getColumns();
    keyOnlyColumns = keySchemaBuilder.named(requestedSchema.getName()).getColumns();

    // Build the non-key (complement) schema, which phase 2 reads under finalRanges. When the
    // projection is all key columns, phase 2 has nothing to read and is skipped entirely, which a
    // null `nonKeyColumns` is what says downstream.
    Types.MessageTypeBuilder nonKeyBuilder = Types.buildMessage();
    int nonKeyFieldCount = 0;
    for (Type field : requestedSchema.getFields()) {
      if (!keyTopLevelNames.contains(field.getName())) {
        nonKeyBuilder.addField(field);
        nonKeyFieldCount++;
      }
    }
    if (nonKeyFieldCount > 0) {
      nonKeyColumns = nonKeyBuilder.named(requestedSchema.getName()).getColumns();
    }
    initializeSplicingState(presentKeyColumns);

    totalBlockCount = lateMatReader.getRowGroups().size();
    nextBlockIndex = 0;
  }

  /**
   * Populates the splicing bookkeeping. {@code keyColumnIndices} already index the top-level
   * slots of {@link #sparkSchema}, the same indexing as {@link #columnVectors}, so they map
   * directly onto {@link #isKeyTopLevel}, which {@link #nextBatch()} and {@link #close()} then
   * use as the splicing-is-active indicator.
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
    keyVariableLength = new boolean[numKeys];
    keyFixedBytesPerRow = 0;
    StructField[] fields = sparkRequestedSchema.fields();
    for (int i = 0; i < numKeys; i++) {
      DataType dt = fields[keyIndices[i]].dataType();
      keyCopiers[i] = copierFor(dt);
      keyVariableLength[i] = isVariableLength(dt);
      // One null byte per row either way. A fixed-width value adds its own width, a variable-length
      // one the int offset and int length that point at the byte child.
      keyFixedBytesPerRow += keyVariableLength[i] ? 1 + 8 : 1 + dt.defaultSize();
    }
  }

  /** Whether a key value lives in the vector's byte child rather than in its fixed-width array. */
  private static boolean isVariableLength(DataType dt) {
    if (dt instanceof DecimalType decimalType) {
      return decimalType.precision() > Decimal.MAX_LONG_DIGITS();
    }
    return dt instanceof StringType || dt instanceof BinaryType;
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
   *     Skipped entirely when {@link #nonKeyColumns} is null (all-keys projection).
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
      // Splicing buffers one key value per surviving row of the whole row group before it can emit
      // the first batch, and that buffer is outside any MemoryConsumer, so phase 1 counts what it
      // holds and gives up past the cap. This row group is then read the plain way: phase 2 takes
      // every projected column under finalRanges, key columns included, so nothing is buffered and
      // the cost is one extra read of the key columns.
      spliceCurrentRowGroup = true;
      splicedBytes = 0L;

      // Phase 0: rows allowed by the pushed data filter, at column-index granularity. The full
      // requestedSchema goes back on first, because phases 1 and 2 narrow it and
      // ParquetFileReader.getRowRanges computes ranges against the reader's current paths.
      lateMatReader.setRequestedSchema(requestedColumns);
      // getRowRanges checks only whether a filter is pushed, not options.useColumnIndexFilter(),
      // so calling it unconditionally would keep applying column-index filtering after a user
      // turned it off -- the documented escape hatch for files whose column index is wrong.
      // Trusting a wrong column index here drops rows for good, since finalRanges is a subset of
      // these ranges and the post-scan Filter no longer holds the predicate. Phase 2 is
      // unaffected: it selects pages through the offset index, which this conf says nothing
      // about.
      RowRanges pushedFilterRanges = useColumnIndexFilter
          ? lateMatReader.getRowRanges(blockIdx)
          : RowRanges.createSingle(blockRowCount);
      // RowRanges.rowCount() walks every range, so resolve each range set's count once.
      long baselineRows = pushedFilterRanges.rowCount();
      if (baselineRows == 0) {
        // Pushed data filter rejects this block entirely via column index. Not a storage-filter
        // skip, so we don't increment storage-filter metrics.
        continue;
      }

      // What this feature can avoid reading is the non-key columns of the rows the storage filter
      // rejects, so that is the baseline both byte metrics are measured against: the non-key bytes
      // a plain read of this projection would transfer for every row the pushed filter kept. The
      // null checks only skip work for a caller that drives this reader without a scan's metrics;
      // FileSourceScanLike creates all five whenever storageFilters is non-empty.
      // compressedBytesForRowRanges never does IO of its own.
      StorageFilterMetrics m = storageFilter.metrics();
      SQLMetric bytesAvoidedRg = m.bytesAvoidedByRowGroup();
      SQLMetric bytesAvoidedPf = m.bytesAvoidedByPageFiltering();
      boolean needBytes = bytesAvoidedRg != null || bytesAvoidedPf != null;
      Map<ColumnPath, ColumnChunkMetaData> blockChunks =
          needBytes ? chunksByPath(lateMatReader, blockIdx) : null;
      long nonKeyBaselineBytes = needBytes
          ? compressedBytesForRowRanges(lateMatReader, blockIdx, blockChunks, nonKeyColumns,
              pushedFilterRanges, baselineRows)
          : 0L;

      // Phase 1: switch to key-only schema, read key columns under pushedFilterRanges, evaluate the
      // storage filter per row.
      lateMatReader.setRequestedSchema(keyOnlyColumns);
      PageReadStore keyPages = lateMatReader.readFilteredRowGroup(blockIdx, pushedFilterRanges);
      if (keyPages == null) {
        // Unreachable: readFilteredRowGroup returns null only for an empty block, and we already
        // know pushedFilterRanges selects at least one row. Skipping the block here would drop its
        // surviving rows from the output, so assert rather than `continue`.
        throw new UnsupportedFileReadException(
            "No key pages for row group " + blockIdx + " despite " + baselineRows
                + " rows selected by the pushed filter");
      }
      RowRanges finalRanges = evaluateStorageFilter(keyPages, pushedFilterRanges);
      long finalRowCount = finalRanges.rowCount();

      if (finalRowCount == 0) {
        // Every surviving row was rejected by the storage filter; skip block entirely, which avoids
        // the whole non-key baseline. Phase 1 still paid to read the key columns, and that cost is
        // not part of the baseline, so nothing has to be subtracted from it here.
        SQLMetric rgSkipped = m.rowGroupsSkipped();
        if (rgSkipped != null) rgSkipped.add(1L);
        SQLMetric rowsExcludedRg = m.rowsExcludedByRowGroup();
        if (rowsExcludedRg != null) rowsExcludedRg.add(baselineRows);
        if (bytesAvoidedRg != null) bytesAvoidedRg.add(nonKeyBaselineBytes);
        continue;
      }

      // Phase 2: switch to non-key schema, read non-key columns under finalRanges. Skipped entirely
      // when the projection is all keys (nonKeyColumns is null); emit reconstructs each batch from
      // the key queues alone.
      long keptRows;
      long phase2Bytes;
      PageReadStore dataPages = null;
      // An all-keys projection has nothing for phase 2 to read -- but only if the key values were
      // buffered. A row group past the cap has to read them here like any other column.
      if (nonKeyColumns == null && spliceCurrentRowGroup) {
        keptRows = finalRowCount;
        phase2Bytes = 0L;
      } else {
        lateMatReader.setRequestedSchema(
            spliceCurrentRowGroup ? nonKeyColumns : requestedColumns);
        // Reading a strict subset of a block's rows needs a Parquet offset index, and parquet
        // enforces that itself: it resolves every requested column's offset index before reading
        // anything, and a column without one makes its column index store throw
        // MissingOffsetIndexException. Files written before parquet-mr 1.11, or by a writer that
        // omits the page index (pyarrow's `write_table` defaults to `write_page_index=False`),
        // have none. Degrading to a whole-block read is not an option, because the key vectors
        // hold only the survivors and the batch would misalign, and neither is dropping the
        // predicate, which extraction has removed from the post-scan Filter. So the read fails,
        // and all this adds is what the user can do about it.
        //
        // Nothing is checked up front: a row group the filter keeps whole never needs the index
        // (`readFilteredRowGroup` falls back to a plain read when the ranges cover the block), and
        // one it rejects whole is never read at all, so a file with no page index still scans as
        // long as the filter never has to prune inside a row group.
        try {
          dataPages = lateMatReader.readFilteredRowGroup(blockIdx, finalRanges);
        } catch (MissingOffsetIndexException e) {
          throw new UnsupportedFileReadException(String.format(
              "Storage-filter pushdown needs a Parquet offset index to read the %d of %d rows its "
                  + "filter kept in row group %d of %s, but the file was written without a page "
                  + "index. Set %s=false to read this file.",
              finalRowCount, blockRowCount, blockIdx, lateMatReader.getFile(),
              SQLConf$.MODULE$.PARQUET_STORAGE_FILTER_PUSHDOWN_ENABLED().key()), e);
        }
        if (dataPages == null) {
          // Unreachable: readFilteredRowGroup returns null only for an empty block or empty ranges,
          // both excluded above. Match phase 1 and fail with a message rather than an NPE.
          throw new UnsupportedFileReadException(
              "No data pages for row group " + blockIdx + " despite " + finalRowCount
                  + " surviving rows");
        }
        keptRows = dataPages.getRowCount();
        // `needBytes`, not just `bytesAvoidedPf != null`: it is what built `blockChunks`.
        if (needBytes && bytesAvoidedPf != null) {
          phase2Bytes = compressedBytesForRowRanges(lateMatReader, blockIdx, blockChunks,
              nonKeyColumns, finalRanges, finalRowCount);
          if (!spliceCurrentRowGroup) {
            // This row group gave splicing up, so phase 2 read the key columns a second time. The
            // baseline counts them once, in phase 1, so the extra read is a cost against it --
            // which can make the row group's contribution negative, and that is the truth about it.
            phase2Bytes += compressedBytesForRowRanges(lateMatReader, blockIdx, blockChunks,
                keyOnlyColumns, finalRanges, finalRowCount);
          }
        } else {
          phase2Bytes = 0L;
        }
      }
      long filteredRows = baselineRows - keptRows;
      SQLMetric rowsExcludedWithinRg = m.rowsExcludedWithinRowGroup();
      if (rowsExcludedWithinRg != null && filteredRows > 0) rowsExcludedWithinRg.add(filteredRows);
      if (bytesAvoidedPf != null) {
        bytesAvoidedPf.add(nonKeyBaselineBytes - phase2Bytes);
      }

      if (dataPages != null) {
        if (rowIndexGenerator != null) {
          rowIndexGenerator.initFromPageReadStore(dataPages);
        }
        for (int i = 0; i < columnVectors.length; i++) {
          if (spliceCurrentRowGroup && isKeyTopLevel[i]) {
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
   * The block's column chunks by path, built once per row group and shared by the byte-metric calls
   * that consume it, since {@link BlockMetaData} offers no lookup of its own.
   */
  private static Map<ColumnPath, ColumnChunkMetaData> chunksByPath(
      ParquetFileReader reader, int blockIndex) {
    Map<ColumnPath, ColumnChunkMetaData> chunks = new HashMap<>();
    for (ColumnChunkMetaData chunk : reader.getRowGroups().get(blockIndex).getColumns()) {
      chunks.put(chunk.getPath(), chunk);
    }
    return chunks;
  }

  /**
   * Compressed bytes the reader transfers for the given leaf {@code columns} when it reads exactly
   * {@code rowRanges} of the given block. Page headers and the dictionary page are included, since
   * both are read whenever any page of a chunk is read. {@code rowRangeCount} is
   * {@code rowRanges.rowCount()}, passed in because that walks every range and the caller has it.
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
   * correct: the reader transfers nothing for them.
   */
  private static long compressedBytesForRowRanges(
      ParquetFileReader reader,
      int blockIndex,
      Map<ColumnPath, ColumnChunkMetaData> chunks,
      List<ColumnDescriptor> columns,
      RowRanges rowRanges,
      long rowRangeCount) {
    if (columns == null || columns.isEmpty() || rowRangeCount == 0) {
      return 0L;
    }
    long blockRowCount = reader.getRowGroups().get(blockIndex).getRowCount();
    boolean wholeBlock = rowRangeCount == blockRowCount;
    ColumnIndexStore ciStore = wholeBlock ? null : reader.getColumnIndexStore(blockIndex);
    long total = 0L;
    for (ColumnDescriptor column : columns) {
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
   * Evaluates the storage filter over every row of a key-only {@link PageReadStore}, in
   * capacity-sized chunks, and returns the surviving rows as {@link RowRanges} in block-row
   * coordinates. The result is a subset of {@code pushedFilterRanges}: rows outside it were never
   * read.
   *
   * <p>Each survivor's key values are appended to {@link #currentKeyAccumulators} for the emit path
   * to splice, until the buffer passes its cap. From there the row group is evaluated without
   * buffering and {@link #spliceCurrentRowGroup} is false, so its phase 2 reads the key columns
   * again along with everything else.
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

    PrimitiveIterator.OfLong rowIndexIter = pushedFilterRanges.iterator();
    RowRanges.Builder finalRangesBuilder = RowRanges.builder();
    // Recomputed rather than taken from the caller: a count that disagreed with this iterator would
    // silently drop surviving rows, and no post-scan Filter is left to catch that.
    long remaining = pushedFilterRanges.rowCount();
    boolean accumulate = true;
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
          if (accumulate) {
            accumulate = appendSurvivorRowToAccumulators(r);
          }
        }
      }
      remaining -= num;
    }

    if (accumulate) {
      finalizePartialAccumulators();
    }

    return finalRangesBuilder.build();
  }

  private void ensureKeyScratchAllocated() {
    if (keyScratchVectors != null) return;
    // Assigned before the loop on purpose: an allocation failure part way through then leaves the
    // vectors allocated so far reachable for `close()`, which walks this array element-wise.
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
   * Allocates any accumulator slot left null by the last push to the queues, {@link #capacity} rows
   * each.
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
   * Appends row {@code srcRow} of every key column to the accumulators, pushing them onto their
   * queues once full. All key columns advance in lockstep, which is what keeps the queues aligned.
   *
   * <p>Returns false when the buffered survivors have passed
   * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes} and this row group has
   * given splicing up, in which case it has already released what it held and the caller must stop
   * calling this.
   */
  private boolean appendSurvivorRowToAccumulators(int srcRow) {
    final int dstRow = currentKeyAccumulatorRowCount;
    final WritableColumnVector[] accs = currentKeyAccumulators;
    final WritableColumnVector[] srcs = keyScratchVectors;
    final ValueCopier[] copiers = keyCopiers;
    long valueBytes = 0L;
    for (int i = 0, n = accs.length; i < n; i++) {
      WritableColumnVector src = srcs[i];
      WritableColumnVector dst = accs[i];
      if (src.isNullAt(srcRow)) {
        dst.putNull(dstRow);
      } else {
        copiers[i].copy(dst, dstRow, src, srcRow);
        // Measured on the destination: a dictionary-encoded source has no length of its own, since
        // its values are read through the dictionary.
        if (keyVariableLength[i]) valueBytes += dst.getArrayLength(dstRow);
      }
    }
    splicedBytes += keyFixedBytesPerRow + valueBytes;
    currentKeyAccumulatorRowCount = dstRow + 1;
    if (currentKeyAccumulatorRowCount == capacity) {
      for (int i = 0; i < currentKeyAccumulators.length; i++) {
        keyVectorQueues[i].addLast(currentKeyAccumulators[i]);
        currentKeyAccumulators[i] = null;
      }
      // Only here, so the cost is one comparison per capacity-sized vector rather than per row. A
      // row group whose survivors fit in a single accumulator is never checked at all: it then
      // holds one capacity-sized vector per key column, which is what a plain read holds anyway.
      if (splicedBytes > storageFilter.maxSplicedRowGroupBytes()) {
        abandonSplicing();
        return false;
      }
      ensureCurrentKeyAccumulatorsAllocated();
    }
    return true;
  }

  /**
   * Gives up splicing for the row group being evaluated and releases every survivor vector it has
   * buffered. Phase 2 then reads the full projected schema and the emit path takes the persistent
   * batch, so the rows are unaffected.
   */
  private void abandonSplicing() {
    for (java.util.ArrayDeque<WritableColumnVector> q : keyVectorQueues) {
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
    for (int i = 0; i < currentKeyAccumulators.length; i++) {
      keyVectorQueues[i].addLast(currentKeyAccumulators[i]);
      currentKeyAccumulators[i] = null;
    }
    currentKeyAccumulatorRowCount = 0;
  }

  /**
   * Closes anything held by the splicing path: every vector still queued, the published head
   * included, and partially-filled accumulators. Called from {@link #close()}.
   */
  private void closeSplicingState() {
    keyVectorsPublished = false;
    if (keyVectorQueues != null) {
      for (java.util.ArrayDeque<WritableColumnVector> q : keyVectorQueues) {
        if (q != null) {
          for (WritableColumnVector v : q) v.close();
          q.clear();
        }
      }
    }
    closeAll(currentKeyAccumulators);
    currentKeyAccumulators = null;
  }

  /** Closes every non-null vector of {@code vectors}; tolerates a null array. */
  private static void closeAll(WritableColumnVector[] vectors) {
    if (vectors == null) return;
    for (WritableColumnVector v : vectors) {
      if (v != null) v.close();
    }
  }

  /**
   * Copies one key value between column vectors. Picked per key column at init time by
   * {@link #copierFor(DataType)}; the caller handles null sources.
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
    // StringType covers CHAR and VARCHAR: both extend it.
    if (dt instanceof StringType || dt instanceof BinaryType) {
      return (dst, dRow, src, sRow) -> dst.putByteArray(dRow, src.getBinary(sRow));
    }
    throw new UnsupportedFileReadException(
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
