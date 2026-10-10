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
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.expressions.BasePredicate;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A {@link VectorizedParquetRecordReader} that reads a row group in phases under a
 * {@code ParquetStorageFilter}, so the projected columns of the rows the filter rejects are never
 * read. The post-scan {@code Filter} still holds the filter, so any phase may stop short and read
 * the rows the way a plain scan would.
 *
 * <p>Per row group:
 * <ul>
 *   <li>phase 0 asks the column index which rows the pushed data filter allows;</li>
 *   <li>phase 1 reads the key columns over those rows, evaluates the filter, and buffers the
 *       surviving key values;</li>
 *   <li>phase 2 reads the other projected columns over the surviving row ranges.</li>
 * </ul>
 * The emit path splices the buffered key values into the batch, so the key columns are read once.
 * A row group whose buffer passes {@code maxSplicedRowGroupBytes} gives splicing up, and phase 2
 * reads its key columns again. If the row ranges alone pass it, the filter is given up too.
 *
 * <p>Some files and row groups get less:
 * <ul>
 *   <li>a file with none of the key columns has a constant predicate, evaluated once. False skips
 *       the file, and true reads it whole;</li>
 *   <li>a file with none of the projected non-key columns is read plainly, since phase 2 would have
 *       nothing to read;</li>
 *   <li>a row group without a page index the read may use can only be skipped whole.</li>
 * </ul>
 *
 * <p>A spliced key slot holds a different vector in each batch, and {@link #nextBatch()} frees the
 * previous one. A caller must fetch {@code batch.column(i)} again after every {@code nextBatch()}.
 *
 * <p>When the scan has the column {@code FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME}, this
 * reader fills it. A batch never spans two row groups, so one value covers each batch. It is true
 * where the filter was applied, whose rows are then exactly the survivors, and false where it was
 * given up or declined. A primitive file column of that name is never decoded, see
 * {@link #suppliesOwnVector}.
 *
 * <p>What {@code FileSourceStrategy.storageFiltersFor} and {@code ParquetStorageFilter.create}
 * guarantee is asserted here, since a violation is a planner bug.
 */
public class LateMaterializationParquetRecordReader extends VectorizedParquetRecordReader {

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(LateMaterializationParquetRecordReader.class);

  /**
   * What one buffered variable-length value costs, as a multiple of its own bytes. The byte child
   * holds a null byte per byte, and {@link WritableColumnVector#reserve} doubles it when it grows.
   * It over-counts on purpose, since nothing can spill this buffer.
   */
  private static final long VARIABLE_LENGTH_BYTES_FACTOR = 4L;

  /**
   * What one surviving row range costs to hold, parquet's {@code RowRanges.Range} with its object
   * header and list slot. Scattered survivors make one range each.
   */
  private static final long ROW_RANGE_BYTES = 40L;

  /**
   * The byte child a variable-length vector is allocated with, per row, which is
   * `WritableColumnVector.DEFAULT_ARRAY_LENGTH`.
   */
  private static final int DEFAULT_CHILD_BYTES_PER_ROW = 4;

  /**
   * The filter this reader applies. {@link #keyColumns} is null for a file it is not applied to row
   * by row.
   */
  private final ParquetStorageFilter storageFilter;

  /**
   * Late-materialization state, set by {@link #initializeLateMaterialization()}.
   * {@link #fileReader} drives all three phases, with its requested schema switched per phase.
   */
  private List<ColumnDescriptor> requestedColumns;
  private List<ColumnDescriptor> keyOnlyColumns;
  private List<ColumnDescriptor> nonKeyColumns;
  private Set<ColumnPath> requestedPaths;
  private Set<ColumnPath> nonKeyPaths;
  /** Answers from the footer whether a row group can be read in part, and what that transfers. */
  private ParquetBlockAccounting blockAccounting;
  /**
   * What the accumulators hold per row for the key columns, besides the value bytes of a
   * variable-length key.
   */
  private int keyFixedBytesPerRow;
  /**
   * The pages phase 2 read for the row group being emitted. Closed when the next one is loaded,
   * since parquet does not close the stores it hands out.
   */
  private PageReadStore dataPages;

  /** The messages this reader has reported, each once per split. */
  private final Set<String> reportedMessages = new HashSet<>();

  /**
   * Whether {@code parquet.filter.columnindex.enabled} is false, so this file's page index must not
   * be used. That turns off phase 0 and every partial read, which parquet does not check itself.
   * Read off {@link #readOptions}, so it agrees with {@link #fileReader}.
   */
  private boolean pageIndexDisabled;

  /**
   * Whether the row group currently loaded is spliced. It starts as whether the row group can be
   * read in part, and {@link #abandonSplicing} clears it. False means phase 2 read every projected
   * column, so the emit path takes the key columns from the persistent batch.
   */
  private boolean spliceCurrentRowGroup;
  private int nextBlockIndex;
  /**
   * The key columns this file has, in the filter's key order, with what init resolves about each.
   * Null when the filter is not applied to this file row by row.
   */
  private KeyColumn[] keyColumns;
  /** Whether this file has none of the key columns, decided in {@link #initBatch}. */
  private boolean allKeysMissing;
  private WritableColumnVector[] keyScratchVectors;
  private ColumnarBatch keyScratchBatch;

  /** The batch slot of the checked column, or -1 when the scan has none. */
  private int checkedSlot = -1;
  /** What {@link #checkedVector} says for every row of the next batch, see the class javadoc. */
  private boolean rowsChecked;
  /** The vector in the checked column's slot, or null when the scan has no such column. */
  private WritableColumnVector checkedVector;

  /**
   * Splicing state. {@link #survivorBatches} holds one array of survivor vectors per batch to emit,
   * indexed like {@link #keyColumns}, and {@link #currentKeyAccumulators} the array still filling.
   * Together they hold every survivor of one row group, outside any MemoryConsumer.
   */
  private final ArrayDeque<WritableColumnVector[]> survivorBatches = new ArrayDeque<>();
  /** The survivor vectors still filling, or null when there are none. */
  private WritableColumnVector[] currentKeyAccumulators;
  /** Row count of {@link #currentKeyAccumulators}. */
  private int currentKeyAccumulatorRowCount;
  /**
   * The survivor vectors the current batch is built on. The next batch frees them, or
   * {@link #close()} when no next batch comes.
   */
  private WritableColumnVector[] publishedKeyVectors;
  /** {@link #initBatch}'s vectors, which this reader closes. */
  private ColumnVector[] persistentBatchColumns;
  /** The array the emitted batch is built over, whose key slots each batch rewrites. */
  private ColumnVector[] spliceBatchColumns;

  public LateMaterializationParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz,
      boolean useOffHeap,
      int capacity,
      ParquetStorageFilter storageFilter) {
    super(convertTz, datetimeRebaseMode, datetimeRebaseTz, int96RebaseMode, int96RebaseTz,
      useOffHeap, capacity);
    this.storageFilter = Objects.requireNonNull(storageFilter, "storageFilter");
  }

  // For test only.
  public LateMaterializationParquetRecordReader(
      boolean useOffHeap, int capacity, ParquetStorageFilter storageFilter) {
    super(useOffHeap, capacity);
    this.storageFilter = Objects.requireNonNull(storageFilter, "storageFilter");
  }

  /**
   * Releases what this reader allocated, then what the base owns, through finally blocks so one
   * failing close does not leak the rest.
   */
  @Override
  public void close() throws IOException {
    try {
      try {
        if (persistentBatchColumns != null) {
          // Through the persistent vectors, since the emitted batch's key and checked slots hold
          // vectors closed below. Super then has no batch to close.
          columnarBatch = null;
          closeAll(persistentBatchColumns);
          persistentBatchColumns = null;
          spliceBatchColumns = null;
        }
      } finally {
        try {
          closeSplicingState();
        } finally {
          if (checkedVector != null) {
            checkedVector.close();
            checkedVector = null;
          }
        }
      }
    } finally {
      try {
        // Through the array, since `keyScratchBatch` is assigned only once every allocation
        // succeeded.
        closeAll(keyScratchVectors);
        keyScratchVectors = null;
        keyScratchBatch = null;
      } finally {
        try {
          closeDataPages();
        } finally {
          super.close();
        }
      }
    }
  }

  /**
   * Builds the batch as the plain reader does and takes it over. A file with none of the key
   * columns is decided here, once the constant vectors its predicate reads exist.
   */
  @Override
  protected void initBatch(
      MemoryMode memMode,
      StructType partitionColumns,
      InternalRow partitionValues) {
    super.initBatch(memMode, partitionColumns, partitionValues);
    takeOverBatch();
    if (allKeysMissing) {
      decideAllKeysMissingFile();
    }
  }

  /**
   * Takes over the vectors of the batch super built. For a file the filter is applied to row by
   * row, or a scan with the checked column, it hands out a batch of its own over a copy of that
   * array, whose key and checked slots it rewrites. {@link ColumnarBatch} holds the array by
   * reference, so rewriting a slot publishes it.
   */
  private void takeOverBatch() {
    persistentBatchColumns = new ColumnVector[columnarBatch.numCols()];
    for (int i = 0; i < persistentBatchColumns.length; i++) {
      persistentBatchColumns[i] = columnarBatch.column(i);
    }
    if (keyColumns == null && checkedSlot < 0) return;
    spliceBatchColumns = persistentBatchColumns.clone();
    if (checkedSlot >= 0) {
      checkedVector = newVector(DataTypes.BooleanType);
      spliceBatchColumns[checkedSlot] = checkedVector;
    }
    columnarBatch = new ColumnarBatch(spliceBatchColumns);
    if (keyColumns != null) allocateKeyScratch();
  }

  /**
   * Evaluates the predicate once for a file with none of its key columns. False skips the file.
   * True, or an error, reads it the way a plain scan would, and only true marks its rows checked.
   */
  private void decideAllKeysMissingFile() {
    // Not closed, and must not be, since the vectors in it belong to the batch this reader emits.
    ColumnarBatch keyRow = new ColumnarBatch(keyVectorsFromBatch(), 1);
    // Resolved outside the `try`, so a broken invariant behind it fails rather than falls back.
    BasePredicate predicate = storageFilter.preparedPredicate();
    boolean keep;
    try {
      keep = predicate.eval(keyRow.getRow(0));
      rowsChecked = keep;
    } catch (Throwable t) {
      giveUpOnError(t, FILE_GIVEN_UP_ON_EVALUATION, false);
      keep = true;
    }
    if (!keep) {
      recordFileSkipped();
      totalRowCount = 0;
    }
  }

  /**
   * Frees the survivor vectors of the previous batch, reads the next batch as the plain reader
   * does, and points the key slots at this batch's survivors, or at the persistent vectors for a
   * row group that is not spliced. Then it marks the batch's rows in the checked column. The
   * row-index column is never a key column, since {@code ParquetStorageFilter.create} rejects its
   * name.
   */
  @Override
  public boolean nextBatch() throws IOException {
    closeAll(publishedKeyVectors);
    publishedKeyVectors = null;
    if (!super.nextBatch()) return false;
    if (keyColumns != null) {
      if (spliceCurrentRowGroup) {
        publishedKeyVectors = survivorBatches.pollFirst();
        if (publishedKeyVectors == null) {
          // Unreachable, since the queue holds exactly the survivors the emit loop counts. Without
          // this check the key slots would show stale values.
          throw ParquetStorageFilter.internalError(String.format(
              "Storage-filter survivor queue of row group %d in %s ran out with %d rows still "
                  + "to emit", nextBlockIndex - 1, fileReader.getFile(), columnarBatch.numRows()));
        }
      }
      for (int k = 0; k < keyColumns.length; k++) {
        int slot = keyColumns[k].batchSlot();
        spliceBatchColumns[slot] =
            publishedKeyVectors != null ? publishedKeyVectors[k] : persistentBatchColumns[slot];
      }
    }
    if (checkedVector != null) {
      checkedVector.putBooleans(0, columnarBatch.numRows(), rowsChecked);
    }
    return true;
  }

  @Override
  protected void initializeInternal() throws IOException, UnsupportedOperationException {
    super.initializeInternal();
    checkedSlot = ParquetStorageFilter.checkedColumnIndex(sparkRequestedSchema);
    initializeLateMaterialization();
  }

  private void initializeLateMaterialization() throws IOException {
    if (fileReader == null) {
      // A row-group reader was handed in with no file reader behind it, so the file is read the
      // plain way.
      return;
    }
    blockAccounting = new ParquetBlockAccounting(fileReader);
    pageIndexDisabled = !readOptions.useColumnIndexFilter();
    // The key columns this file has, since one can be missing under schema evolution.
    int[] keyIndices = storageFilter.keyColumnIndices();
    List<Integer> presentKeyPositions = new ArrayList<>(keyIndices.length);
    Set<String> keyTopLevelNames = new HashSet<>();
    for (int i = 0; i < keyIndices.length; i++) {
      int idx = keyIndices[i];
      if (idx < 0 || idx >= parquetColumn.children().size()) {
        // Unreachable: ParquetStorageFilter.create rejects out-of-range ordinals.
        throw ParquetStorageFilter.internalError(String.format(
            "Storage-filter key ordinal %d is out of range for a %d-column requested schema",
            idx, parquetColumn.children().size()));
      }
      ParquetColumn column = parquetColumn.children().apply(idx);
      if (!column.isPrimitive()) {
        // Unreachable: ParquetStorageFilter.isSupportedKeyType admits only types with a primitive
        // Parquet leaf, and it gates both planning and ParquetStorageFilter.create.
        throw ParquetStorageFilter.internalError(
            "Storage-filter key column is not a primitive Parquet column: " + column.path());
      }
      if (!missingColumns.contains(column)) {
        presentKeyPositions.add(i);
        keyTopLevelNames.add(column.descriptor().get().getPath()[0]);
      }
    }
    requestedColumns = requestedSchema.getColumns();
    requestedPaths = ParquetBlockAccounting.pathsOf(requestedColumns);
    if (presentKeyPositions.isEmpty()) {
      // Nothing for phase 1 to read, so the predicate is constant for this file. `initBatch`
      // decides it, once the vectors it evaluates against exist.
      allKeysMissing = true;
      return;
    }

    // The non-key columns phase 2 reads. A file with none of them gains nothing from the filter,
    // so the reader declines it. A struct none of whose requested fields the file has counts as
    // missing only under the legacy `returnNullStructIfAllFieldsMissing`, since otherwise the
    // clipped schema reads one of its other fields. The checked column never counts.
    String checkedName = checkedSlot >= 0 ? FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME() : null;
    List<ColumnDescriptor> nonKey = requestedColumns.stream()
        .filter(column -> !keyTopLevelNames.contains(column.getPath()[0]))
        .filter(column -> !column.getPath()[0].equalsIgnoreCase(checkedName))
        .toList();
    if (nonKey.stream().noneMatch(column -> fileSchema.containsPath(column.getPath()))) return;
    nonKeyColumns = nonKey;
    nonKeyPaths = ParquetBlockAccounting.pathsOf(nonKeyColumns);

    // Resolved here, past the two returns above, because `ValueCopier.forType` rejects a type the
    // reader cannot copy, and a file the reader has already declined must not be failed over one.
    KeyColumn[] keys = new KeyColumn[presentKeyPositions.size()];
    StructField[] fields = sparkRequestedSchema.fields();
    int fixedBytesPerRow = 0;
    for (int i = 0; i < keys.length; i++) {
      int rowPosition = presentKeyPositions.get(i);
      int batchSlot = keyIndices[rowPosition];
      ParquetColumn column = parquetColumn.children().apply(batchSlot);
      DataType type = fields[batchSlot].dataType();
      boolean variableLength = ValueCopier.isVariableLength(type);
      keys[i] = new KeyColumn(batchSlot, rowPosition, column.descriptor().get(),
          column.required(), type, ValueCopier.forType(type), variableLength);
      // A null byte, plus the value's width, or for a variable-length value its int offset and
      // length and the byte child's default size per row, with a null byte each. A decimal of up
      // to 9 digits is held as an int.
      int valueWidth = type instanceof DecimalType && DecimalType.is32BitDecimalType(type)
          ? 4 : type.defaultSize();
      fixedBytesPerRow +=
          variableLength ? 1 + 4 + 4 + 2 * DEFAULT_CHILD_BYTES_PER_ROW : 1 + valueWidth;
    }
    keyFixedBytesPerRow = fixedBytesPerRow;
    keyOnlyColumns = Arrays.stream(keys).map(KeyColumn::descriptor).toList();
    // Last, since a non-null `keyColumns` is what says the filter is applied to this file.
    keyColumns = keys;
  }

  /**
   * Loads the next row group with rows left to emit, through the phases of the class javadoc.
   * Returns false when the row groups run out.
   */
  @Override
  protected boolean loadNextRowGroup() throws IOException {
    if (keyColumns == null) return super.loadNextRowGroup();
    // The previous row group is fully emitted, so its pages and column readers go now rather than
    // at the next assignment, which a row group the filter empties would delay.
    closeDataPages();
    releaseRowGroupReaders();
    StorageFilterMetrics m = storageFilter.metrics();
    List<BlockMetaData> blocks = fileReader.getRowGroups();
    while (nextBlockIndex < blocks.size()) {
      // A plain read checks for a task kill after every batch, and this loop can pass any number
      // of row groups without emitting one.
      ParquetStorageFilter.throwIfKilled();
      int blockIdx = nextBlockIndex++;
      long blockRowCount = blocks.get(blockIdx).getRowCount();

      // Phase 0. The full projection goes back on first, because `getRowRanges` builds an unseen
      // block's store over the current paths.
      fileReader.setRequestedSchema(requestedColumns);
      RowRanges pushedFilterRanges = pushedFilterRangesFor(blockIdx, blockRowCount);
      long baselineRows = pushedFilterRanges.rowCount();
      if (baselineRows == 0) {
        // Not this filter's skip, so no storage-filter metric counts it.
        continue;
      }

      // Phase 2 can read part of the row group only with an offset index for every column of the
      // block's store, since parquet empties the store when one column lacks it. Files written by
      // parquet-mr before 1.11, or by pyarrow's `write_table` by default, have none. Otherwise the
      // filter can only skip the row group whole.
      boolean canNarrowRowGroup =
          !pageIndexDisabled && blockAccounting.hasOffsetIndexes(blockIdx, requestedPaths);
      spliceCurrentRowGroup = canNarrowRowGroup;

      // Phase 1 reads the key columns over `pushedFilterRanges` and evaluates the filter per row.
      fileReader.setRequestedSchema(keyOnlyColumns);
      RowRanges survivors;
      // Closed here, since parquet closes no store it hands out.
      try (PageReadStore keyPages =
               fileReader.readFilteredRowGroup(blockIdx, pushedFilterRanges)) {
        requireRowCount(keyPages, baselineRows, blockIdx);
        survivors = evaluateStorageFilter(
            keyPages, pushedFilterRanges, baselineRows, blockRowCount, canNarrowRowGroup);
      }
      // Null means phase 1 gave the filter up, and phase 2 then reads every row the pushed filter
      // kept.
      boolean filterGivenUp = survivors == null;
      if (filterGivenUp) abandonSplicing();
      RowRanges finalRanges = filterGivenUp ? pushedFilterRanges : survivors;
      long finalRowCount = filterGivenUp ? baselineRows : survivors.rowCount();
      // The byte metrics' baseline, the non-key bytes a plain read asks for over the rows the
      // pushed filter kept.
      long nonKeyBaselineBytes = filterGivenUp ? 0L : blockAccounting.compressedBytesForRowRanges(
          blockIdx, nonKeyPaths, pushedFilterRanges, baselineRows);
      if (finalRowCount == 0) {
        // The filter rejected every row, so the row group is skipped whole.
        m.recordRowGroupSkipped(baselineRows, nonKeyBaselineBytes);
        continue;
      }

      fileReader.setRequestedSchema(spliceCurrentRowGroup ? nonKeyColumns : requestedColumns);
      // An offset index the footer lists but parquet fails to read empties that block's store.
      // The row group is then read again over `pushedFilterRanges`, which the empty store has
      // widened to the whole block, so the retry cannot throw. `MissingOffsetIndexException` is
      // parquet's internal API, and the damaged-index test fails if parquet stops throwing it.
      try {
        dataPages = fileReader.readFilteredRowGroup(blockIdx, finalRanges);
      } catch (MissingOffsetIndexException e) {
        // Parquet turns any IOException reading an offset index into this, a kill's interrupt
        // included, which must not be taken for a missing index.
        ParquetStorageFilter.throwIfKilled();
        if (reportedMessages.add(OFFSET_INDEX_UNREADABLE)) {
          // The message only, since parquet has logged the stack trace.
          LOG.warn(OFFSET_INDEX_UNREADABLE, MDC.of(LogKeys.PATH, fileReader.getFile()),
              MDC.of(LogKeys.REASON, e.getMessage()));
        }
        filterGivenUp = true;
        abandonSplicing();
        finalRanges = pushedFilterRanges;
        finalRowCount = baselineRows;
        fileReader.setRequestedSchema(requestedColumns);
        dataPages = fileReader.readFilteredRowGroup(blockIdx, finalRanges);
      }
      requireRowCount(dataPages, finalRowCount, blockIdx);
      // A row group whose filter was given up reports nothing (see `StorageFilterMetrics`).
      if (!filterGivenUp) {
        // Over the columns phase 2 read, the key columns included once splicing is given up. A
        // spliced row group that pruned nothing read exactly the baseline.
        long phase2Bytes = spliceCurrentRowGroup && finalRowCount == baselineRows
            ? nonKeyBaselineBytes
            : blockAccounting.compressedBytesForRowRanges(blockIdx,
                spliceCurrentRowGroup ? nonKeyPaths : requestedPaths, finalRanges, finalRowCount);
        // `SQLMetric.add` ignores a negative value, so reading more than the baseline counts
        // nothing.
        m.bytesAvoidedByPageFiltering().add(nonKeyBaselineBytes - phase2Bytes);
        m.rowsExcludedWithinRowGroup().add(baselineRows - finalRowCount);
      }

      // The rows phase 2 read are exactly the survivors unless the filter was given up.
      rowsChecked = !filterGivenUp;
      // Phase 2 read exactly these rows, so its readers get the ranges.
      installRowGroup(dataPages, readerRangesFor(finalRanges, finalRowCount, blockRowCount));
      return true;
    }
    return false;
  }

  /**
   * The checked slot always holds this reader's own vector, so a primitive file column of that
   * name, which a user-given schema can match, is never decoded. A spliced row group's key slots
   * take their values from the survivor queue. Both are primitive, as the base class requires.
   */
  @Override
  protected boolean suppliesOwnVector(int slot) {
    if (slot == checkedSlot) return true;
    if (!spliceCurrentRowGroup) return false;
    for (KeyColumn key : keyColumns) {
      if (key.batchSlot() == slot) return true;
    }
    return false;
  }

  /**
   * Counts a file whose constant predicate is false. Every row group with rows the pushed data
   * filter kept counts as skipped, with all their projected bytes avoided, since the file has no
   * key column.
   */
  private void recordFileSkipped() {
    StorageFilterMetrics m = storageFilter.metrics();
    List<BlockMetaData> blocks = fileReader.getRowGroups();
    for (int blockIdx = 0; blockIdx < blocks.size(); blockIdx++) {
      RowRanges blockRanges = pushedFilterRangesFor(blockIdx, blocks.get(blockIdx).getRowCount());
      long survivingRows = blockRanges.rowCount();
      if (survivingRows == 0) continue;
      long avoidedBytes = blockAccounting.compressedBytesForRowRanges(
          blockIdx, requestedPaths, blockRanges, survivingRows);
      m.recordRowGroupSkipped(survivingRows, avoidedBytes);
    }
  }

  /**
   * The rows of a block the pushed data filter allows, at column-index granularity, or every row
   * when {@link #pageIndexDisabled}. {@code getRowRanges} does not check that conf itself.
   */
  private RowRanges pushedFilterRangesFor(int blockIdx, long blockRowCount) {
    if (blockRowCount == 0) {
      // parquet-mr never writes an empty block, but `RowRanges.createSingle(0)` would fail on
      // one.
      return RowRanges.EMPTY;
    }
    return pageIndexDisabled
        ? RowRanges.createSingle(blockRowCount)
        : fileReader.getRowRanges(blockIdx);
  }

  /**
   * The ranges to hand the column readers of a read over {@code rowRanges}, or none for a read of
   * every row. Parquet hands back no row indexes for such a read, and a column reader refuses
   * ranges it cannot find by them (see {@code ParquetReadState.forRead}).
   */
  private static RowRanges readerRangesFor(
      RowRanges rowRanges, long rowCount, long blockRowCount) {
    return rowCount == blockRowCount ? null : rowRanges;
  }

  /**
   * Fails unless a page store holds the rows asked for. A different count would leave a batch's
   * tail stale, or splice survivors into the wrong rows, with no error.
   */
  private static void requireRowCount(PageReadStore pages, long expected, int blockIdx) {
    long actual = pages == null ? 0L : pages.getRowCount();
    if (actual != expected) {
      throw ParquetStorageFilter.internalError(String.format(
          "Row group %d was read as %d rows where %d were asked for", blockIdx, actual, expected));
    }
  }

  /**
   * Evaluates the filter over every row of the key pages and returns the surviving rows in block
   * coordinates. The result is exactly the rows the filter kept, which the checked column relies
   * on. While splicing, each survivor's key values are buffered for the emit path.
   *
   * <p>Returns null when the filter is given up for this row group:
   * <ul>
   *   <li>at the first survivor of a row group that cannot be read in part;</li>
   *   <li>when the surviving ranges pass the budget;</li>
   *   <li>on an error decoding the key pages or applying the filter.</li>
   * </ul>
   */
  private RowRanges evaluateStorageFilter(
      PageReadStore keyPages,
      RowRanges pushedFilterRanges,
      long pushedFilterRowCount,
      long blockRowCount,
      boolean canNarrowRowGroup) throws IOException {
    long remaining = pushedFilterRowCount;
    // Phase 1 read exactly the pushed filter's rows, so its readers get those ranges.
    RowRanges keyReaderRanges =
        readerRangesFor(pushedFilterRanges, pushedFilterRowCount, blockRowCount);
    VectorizedColumnReader[] readers = new VectorizedColumnReader[keyColumns.length];
    for (int i = 0; i < readers.length; i++) {
      KeyColumn key = keyColumns[i];
      readers[i] =
          newColumnReader(key.descriptor(), key.required(), keyPages, keyReaderRanges);
    }

    PrimitiveIterator.OfLong rowIndexIter = pushedFilterRanges.iterator();
    RowRanges.Builder finalRangesBuilder = RowRanges.builder();
    // The budget weighs both the bytes buffered for splicing and the survivors' ranges.
    long splicedBytes = 0L;
    long rangeBytes = 0L;
    long previousSurvivor = -2L;
    long cap = storageFilter.maxSplicedRowGroupBytes();
    // Out of the loop, since resolving it is a volatile read.
    BasePredicate predicate = storageFilter.preparedPredicate();
    while (remaining > 0) {
      // Per chunk, since this decodes a whole row group before its first batch.
      ParquetStorageFilter.throwIfKilled();
      int num = (int) Math.min((long) capacity, remaining);
      try {
        for (int i = 0; i < keyScratchVectors.length; i++) {
          keyScratchVectors[i].reset();
          readers[i].readBatch(num, keyScratchVectors[i], null, null);
        }
      } catch (Throwable t) {
        // A corrupt page shows here, since parquet decodes a page only when it is read, a codec's
        // `InternalError` included. Giving the row group up has phase 2 read the same pages in the
        // same batches as a plain scan, so it fails at the same row, and `ignoreCorruptFiles` keeps
        // what it keeps of a plain read.
        giveUpOnError(t, ROW_GROUP_GIVEN_UP_ON_DECODING, true);
        // The failed decode may have grown the scratch vectors, so capacity-sized ones replace
        // them.
        closeAll(keyScratchVectors);
        allocateKeyScratch();
        return null;
      }
      keyScratchBatch.setNumRows(num);
      for (int r = 0; r < num; r++) {
        long blockRow = rowIndexIter.nextLong();
        boolean survives;
        try {
          survives = predicate.eval(keyScratchBatch.getRow(r));
        } catch (Throwable t) {
          // Fall back on anything but a task kill or a fatal error. The predicate runs here without
          // the conjuncts before it in the plan, so it can throw on a row a plain scan never
          // evaluates it on. The post-scan Filter then decides the rows in its own order. Not
          // narrowed by type, since expressions and UDFs throw any kind.
          giveUpOnError(t, ROW_GROUP_GIVEN_UP_ON_EVALUATION, false);
          return null;
        }
        if (!survives) continue;
        if (!canNarrowRowGroup) {
          // One survivor settles a row group that cannot be read in part. One with no survivor
          // still gets empty ranges below and is skipped.
          return null;
        }
        finalRangesBuilder.addSelectedRow(blockRow);
        if (blockRow != previousSurvivor + 1) rangeBytes += ROW_RANGE_BYTES;
        previousSurvivor = blockRow;
        if (spliceCurrentRowGroup) {
          try {
            if (currentKeyAccumulators == null) allocateKeyAccumulators();
            splicedBytes += appendSurvivorRowToAccumulators(r);
          } catch (RuntimeException e) {
            // The buffer is optional, so failing to fill it gives splicing up, not the filter. That
            // covers a vector that cannot grow, and a key value the predicate did not read on this
            // row, such as `b` in `coalesce(a, b)`, since the copy decodes every key.
            reportOnce(e, SPLICING_GIVEN_UP_ON_ERROR);
            abandonSplicing();
            splicedBytes = 0L;
          }
        }
        if (splicedBytes + rangeBytes > cap) {
          // The buffer goes first, which is a no-op once splicing is given up.
          abandonSplicing();
          splicedBytes = 0L;
          // The ranges would be thrown away, so stop.
          if (rangeBytes > cap) return null;
        }
      }
      remaining -= num;
    }

    // A partly filled set of accumulators goes to the queue as this row group's last batch.
    if (currentKeyAccumulators != null) pushAccumulators();
    return finalRangesBuilder.build();
  }

  /** Allocates phase 1's key vectors and the row the predicate evaluates over them. */
  private void allocateKeyScratch() {
    // Assigned before the loop, so a partial allocation stays reachable for `close()`.
    keyScratchVectors = new WritableColumnVector[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyScratchVectors[i] = newVector(keyColumns[i].type());
    }
    // Missing key columns keep their batch vectors, which the emitted batch owns.
    ColumnVector[] rowVectors = keyVectorsFromBatch();
    for (int i = 0; i < keyScratchVectors.length; i++) {
      rowVectors[keyColumns[i].rowPosition()] = keyScratchVectors[i];
    }
    keyScratchBatch = new ColumnarBatch(rowVectors);
  }

  /**
   * The key columns' vectors from the emitted batch, in the predicate's order. A key column the
   * file lacks keeps its batch vector, so the filter sees the values the rows show. Call only after
   * {@link #takeOverBatch}.
   */
  private ColumnVector[] keyVectorsFromBatch() {
    int[] keyIndices = storageFilter.keyColumnIndices();
    ColumnVector[] vectors = new ColumnVector[keyIndices.length];
    for (int p = 0; p < keyIndices.length; p++) {
      vectors[p] = persistentBatchColumns[keyIndices[p]];
    }
    return vectors;
  }

  /** A capacity-sized vector in the reader's memory mode, like the rest of the batch. */
  private WritableColumnVector newVector(DataType dt) {
    return MEMORY_MODE == MemoryMode.OFF_HEAP
        ? new OffHeapColumnVector(capacity, dt)
        : new OnHeapColumnVector(capacity, dt);
  }

  /**
   * Allocates a set of accumulators, {@link #capacity} rows each, charged per row as survivors
   * arrive. Each set grows from the default size rather than being sized from the previous one,
   * since a size charged up front would make the charge depend on the cap.
   */
  private void allocateKeyAccumulators() {
    // Assigned before the loop, so a partial allocation stays reachable for `abandonSplicing`.
    currentKeyAccumulators = new WritableColumnVector[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      currentKeyAccumulators[i] = newVector(keyColumns[i].type());
    }
  }

  /**
   * Appends row {@code srcRow} of every key column to the accumulators, which the caller has
   * allocated, and hands them to the queue once they are full. Returns the bytes the row added,
   * which the caller weighs against its budget.
   */
  private long appendSurvivorRowToAccumulators(int srcRow) {
    long charged = 0L;
    final int dstRow = currentKeyAccumulatorRowCount;
    final WritableColumnVector[] accs = currentKeyAccumulators;
    final WritableColumnVector[] srcs = keyScratchVectors;
    final KeyColumn[] keys = keyColumns;
    for (int i = 0, n = accs.length; i < n; i++) {
      WritableColumnVector src = srcs[i];
      WritableColumnVector dst = accs[i];
      KeyColumn key = keys[i];
      if (src.isNullAt(srcRow)) {
        dst.putNull(dstRow);
      } else {
        key.copier().copy(dst, dstRow, src, srcRow);
        // On the destination, since a dictionary-encoded source has no length of its own.
        if (key.variableLength()) {
          charged += VARIABLE_LENGTH_BYTES_FACTOR * dst.getArrayLength(dstRow);
        }
      }
    }
    currentKeyAccumulatorRowCount = dstRow + 1;
    if (currentKeyAccumulatorRowCount == capacity) pushAccumulators();
    return keyFixedBytesPerRow + charged;
  }

  /** Hands the accumulators to the queue the emit path takes its batches from. */
  private void pushAccumulators() {
    survivorBatches.addLast(currentKeyAccumulators);
    currentKeyAccumulators = null;
    currentKeyAccumulatorRowCount = 0;
  }

  /** Releases the pages phase 2 read for the row group just emitted, if any. */
  private void closeDataPages() {
    if (dataPages != null) {
      dataPages.close();
      dataPages = null;
    }
  }

  /** What this reader reports, each once per split. */
  private static final String ROW_GROUP_GIVEN_UP_ON_EVALUATION =
      "Reading a row group of {} without the storage filter, because applying it to a row raised "
          + "an error. The filter is still applied above the scan, so the answer is unchanged, and "
          + "the remaining row groups are filtered as usual";

  private static final String ROW_GROUP_GIVEN_UP_ON_DECODING =
      "Reading a row group of {} without the storage filter, because decoding its key columns "
          + "raised an error. The row group is read the way a plain scan reads it, which raises "
          + "the same error if the data is corrupt, and the remaining row groups are filtered as "
          + "usual";

  private static final String FILE_GIVEN_UP_ON_EVALUATION =
      "Reading {} without the storage filter, because applying it to the values its missing key "
          + "columns read as raised an error. The filter is still applied above the scan, so the "
          + "answer is unchanged";

  private static final String OFFSET_INDEX_UNREADABLE =
      "Reading a row group of {} without page-level storage filtering, because parquet could not "
          + "read the offset index of a column the read needs ({}). The other row groups are "
          + "filtered as usual. Reported once per split";

  private static final String SPLICING_GIVEN_UP_ON_ERROR =
      "Reading the key columns of a row group of {} a second time rather than buffering them, "
          + "because buffering a surviving key value raised an error. The storage filter is still "
          + "applied";

  /**
   * Gives the filter up on an error, after rethrowing a task kill or a fatal error, see
   * {@code ParquetStorageFilter.rethrowIfMustPropagate}. Every catch of {@code Throwable} that
   * gives the filter up goes through here. {@code fromRead} says the error came from decoding the
   * key pages.
   */
  private void giveUpOnError(Throwable t, String message, boolean fromRead) {
    ParquetStorageFilter.rethrowIfMustPropagate(t, fromRead);
    reportOnce(t, message);
  }

  /**
   * Logs the first error of each kind per reader, which is once per split, so one kind does not
   * hide another.
   */
  private void reportOnce(Throwable t, String message) {
    if (!reportedMessages.add(message)) return;
    LOG.warn(message, t, MDC.of(LogKeys.PATH, fileReader.getFile()));
  }

  /**
   * Gives up splicing for the row group being evaluated and frees its buffered survivors. Phase 2
   * then reads every projected column, so the rows are unaffected.
   */
  private void abandonSplicing() {
    for (WritableColumnVector[] batch : survivorBatches) {
      closeAll(batch);
    }
    survivorBatches.clear();
    closeAll(currentKeyAccumulators);
    currentKeyAccumulators = null;
    currentKeyAccumulatorRowCount = 0;
    spliceCurrentRowGroup = false;
  }

  /** Closes every survivor vector this reader still holds, the published ones included. */
  private void closeSplicingState() {
    try {
      closeAll(publishedKeyVectors);
      publishedKeyVectors = null;
    } finally {
      abandonSplicing();
    }
  }

  /** Closes every non-null vector of {@code vectors}, which may itself be null. */
  private static void closeAll(ColumnVector[] vectors) {
    if (vectors == null) return;
    for (ColumnVector v : vectors) {
      if (v != null) v.close();
    }
  }

  /** One key column this file has, as init resolves it. */
  private record KeyColumn(
      int batchSlot,
      int rowPosition,
      ColumnDescriptor descriptor,
      boolean required,
      DataType type,
      ValueCopier copier,
      boolean variableLength) {}
}
