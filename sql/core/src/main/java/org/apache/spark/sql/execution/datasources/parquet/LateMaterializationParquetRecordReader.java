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
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A {@link VectorizedParquetRecordReader} that reads a row group in phases under a
 * {@code ParquetStorageFilter}, so the projected columns of the rows the filter rejects are never
 * read. The filter is a deterministic conjunct of the post-scan {@code Filter}, which still holds
 * it, so any phase below may stop short and read the file the way a plain scan would.
 *
 * <p>Per row group, all three phases driven by the one {@link #fileReader}:
 * <ul>
 *   <li>phase 0 asks the column index which rows the pushed data filter allows, metadata only;</li>
 *   <li>phase 1 reads the key columns the filter references over those rows, evaluates the filter
 *       per row, and buffers the surviving key values;</li>
 *   <li>phase 2 reads the remaining projected columns over the surviving row ranges.</li>
 * </ul>
 *
 * <p>The emit path then splices the buffered key vectors into the batch, so phase 2 never reads a
 * key column again. A row group whose buffer has passed
 * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes} gives splicing up, and
 * phase 2 reads its key columns again with the rest. If the survivors' row ranges alone pass it,
 * the filter is given up for that row group too.
 *
 * <p>A whole file is read without splicing in these cases:
 * <ul>
 *   <li>every key column is missing from it under schema evolution. The predicate is then constant
 *       over the file and is evaluated once, against the constants the reader materializes for
 *       those columns. True keeps the file unfiltered, false skips it;</li>
 *   <li>the file has none of the projected non-key columns, or none is projected. Phase 2 would
 *       have nothing to read, so the reader declines the filter and reads the file plainly;</li>
 *   <li>{@code parquet.filter.columnindex.enabled} is false, so its page index is not used. The
 *       filter then only skips row groups it rejects whole.</li>
 * </ul>
 *
 * <p>A row group whose columns do not all carry an offset index gets the same as the last case,
 * and so does one whose offset index parquet fails to read.
 *
 * <p>{@link #resultBatch()} is one batch object for the whole read, as for the plain reader, but a
 * spliced key slot holds a different vector in each batch, and {@link #nextBatch()} frees the
 * previous one. A caller must fetch {@code batch.column(i)} again after every {@code nextBatch()}.
 * The slots are swapped rather than copied into the persistent vectors on purpose. A copy would
 * pay a second value copy per surviving row, a byte copy for a variable-length key, where the
 * swap is one array write per key column per batch.
 *
 * <p>What {@code FileSourceStrategy.storageFiltersFor} and {@code ParquetStorageFilter.create}
 * guarantee is asserted here, since a violation is a planner bug.
 */
public class LateMaterializationParquetRecordReader extends VectorizedParquetRecordReader {

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(LateMaterializationParquetRecordReader.class);

  /**
   * What one buffered variable-length value costs the vector that holds it, as a multiple of the
   * value's own bytes. Those bytes go in the byte child, with one null byte per element of that
   * child, and up to as much again because {@link WritableColumnVector#reserve} doubles the child
   * when it grows, by default. A factor rather than an exact figure, so with that default this
   * half of the charge over-counts rather than under-counts, since nothing above this buffer can
   * spill it. A {@code hugeVectorReserveRatio} above 2 makes it under-count for a child past
   * {@code hugeVectorThreshold}. That threshold is off by default. A byte child sized up front is
   * charged by the same factor when it is sized, see {@link #allocateKeyAccumulators}. A
   * fixed-width key needs no factor, since its accumulator is allocated at capacity and never
   * grows.
   */
  private static final long VARIABLE_LENGTH_BYTES_FACTOR = 4L;

  /**
   * What one surviving row range costs to hold, which is the half of the budget a row group
   * buffering nothing still pays. Scattered survivors make one range each, and phase 2 needs the
   * whole set to select its pages. One set is held, which every column reader of the row group
   * walks with a cursor of its own. The figure is parquet's {@code RowRanges.Range}, two longs,
   * plus its object header and a list slot. That is what is held. At the end of phase 1 the
   * builder's list can have up to half as many slots again, and {@code build()} copies it, so the
   * peak is somewhat higher.
   */
  private static final long ROW_RANGE_BYTES = 40L;

  /**
   * The byte child a variable-length vector is allocated with, per row, which is
   * `WritableColumnVector.DEFAULT_ARRAY_LENGTH`. `keyFixedBytesPerRow` already charges it.
   */
  private static final int DEFAULT_CHILD_BYTES_PER_ROW = 4;

  /**
   * The filter this reader applies. {@link #keyColumns} is null for a file it is not applied to row
   * by row, and such a file is read the way the plain reader reads it.
   */
  private final ParquetStorageFilter storageFilter;

  /**
   * Late-materialization state, set by {@link #initializeLateMaterialization()}.
   *
   * <p>{@link #fileReader} drives all three phases, and its requested schema is switched per phase.
   * Phase 0 reads the whole projection, phase 1 the key columns, and phase 2 the non-key columns
   * while splicing or the whole projection otherwise.
   *
   * <p>Held as leaf-column lists, since that is what the reader and the byte metrics take and
   * {@code MessageType.getColumns()} rebuilds its list per call. The leaf paths of the whole
   * projection and of the non-key columns are resolved once, for the footer walks that run per row
   * group. The key columns need none, since phase 1 always reads them and no walk measures them
   * apart.
   */
  private List<ColumnDescriptor> requestedColumns;
  private List<ColumnDescriptor> keyOnlyColumns;
  private List<ColumnDescriptor> nonKeyColumns;
  private Set<ColumnPath> requestedPaths;
  private Set<ColumnPath> nonKeyPaths;
  /** Answers from the footer whether a row group can be read in part, and what that transfers. */
  private ParquetBlockAccounting blockAccounting;
  /**
   * What an accumulator holds per row for each key column, not counting the value bytes of a
   * variable-length type. Together with those value bytes it is what the per-row-group buffer is
   * measured against. See
   * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes}.
   */
  private int keyFixedBytesPerRow;
  /**
   * The pages phase 2 read for the row group being emitted. Held because the column readers draw
   * from it for the whole row group, and closed when the next one is loaded, since parquet does not
   * close the stores {@code readFilteredRowGroup} hands out.
   */
  private PageReadStore dataPages;

  /** The messages this reader has reported, each once per split. */
  private final Set<String> reportedMessages = new HashSet<>();

  /**
   * Whether {@code parquet.filter.columnindex.enabled} is false, so this file's page index must not
   * be used. That turns off phase 0 and every partial read. A wrong index would pair a row's key
   * with another row's values, which the post-scan Filter cannot catch. Parquet's partial read does
   * not check the conf itself. Read off {@link #readOptions}, so it agrees with the row count
   * {@link #fileReader} reported.
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

  /**
   * Splicing state. Phase 1 keeps the surviving key values it decoded, and the emit path splices
   * them into the output batch. That saves phase 2 a second read of the key columns, which parquet
   * does not cache, so it goes back to the filesystem.
   *
   * <p>{@link #survivorBatches} holds one array of survivor vectors per batch to emit, indexed like
   * {@link #keyColumns}, and {@link #currentKeyAccumulators} the array still filling. Together they
   * hold every survivor of one row group at once, outside any MemoryConsumer.
   */
  private final ArrayDeque<WritableColumnVector[]> survivorBatches = new ArrayDeque<>();
  /** The survivor vectors still filling, or null when there are none. */
  private WritableColumnVector[] currentKeyAccumulators;
  /** Row count of {@link #currentKeyAccumulators}. */
  private int currentKeyAccumulatorRowCount;
  /**
   * Per key column, the part of what {@link #currentKeyAccumulators}' byte child was charged when
   * it was sized that its values have not used up yet. Always zero for a fixed-width key.
   */
  private long[] prepaidValueBytes;
  /**
   * The survivor vectors the current batch is built on. This reader owns them until the next batch
   * releases them, so a consumer that stops between batches, such as a limit or the end of the
   * task, leaves them for {@link #close()} to free.
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
   * Releases what this reader allocated, then what the base owns. Chained through finally blocks,
   * so one failing close does not leak the rest.
   */
  @Override
  public void close() throws IOException {
    try {
      try {
        if (persistentBatchColumns != null) {
          // Through the persistent vectors rather than the emitted batch, whose key slots may hold
          // survivor vectors the splicing state closes instead. Super then has no batch to close.
          columnarBatch = null;
          closeAll(persistentBatchColumns);
          persistentBatchColumns = null;
          spliceBatchColumns = null;
        }
      } finally {
        closeSplicingState();
      }
    } finally {
      try {
        // Through the array, not through `keyScratchBatch`, which is assigned only after the
        // allocation loop finishes, so a partial failure leaves vectors only the array can reach.
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
   * Builds the batch as the plain reader does and takes it over for splicing. A file with none of
   * the key columns is decided here, because the constant vectors the predicate reads for those
   * columns exist only once super has built them.
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
   * Takes the vectors of the batch super built. For a file the filter is applied to row by row, it
   * then hands out a batch of its own over a copy of that array, whose key slots the emit path
   * rewrites in place, and allocates phase 1's key vectors. {@link ColumnarBatch} holds the array
   * by reference, its staging row included, so rewriting a slot is what publishes it.
   *
   * <p>Every slot got a vector, key columns included. A spliced row group leaves a key slot's
   * vector unused, and a row group that gave splicing up reads into it.
   */
  private void takeOverBatch() {
    persistentBatchColumns = new ColumnVector[columnarBatch.numCols()];
    for (int i = 0; i < persistentBatchColumns.length; i++) {
      persistentBatchColumns[i] = columnarBatch.column(i);
    }
    if (keyColumns == null) return;
    spliceBatchColumns = persistentBatchColumns.clone();
    columnarBatch = new ColumnarBatch(spliceBatchColumns);
    allocateKeyScratch();
  }

  /**
   * Answers the filter for a file with none of its key columns, which makes the predicate constant
   * over the file.
   *
   * <p>False skips the file, which then yields no rows. True, or an error, reads it the way a plain
   * scan would, under the same fail-open rule as a row's value, since the constant can be one the
   * predicate throws on.
   */
  private void decideAllKeysMissingFile() {
    // Not closed, and must not be, since the vectors in it belong to the batch this reader emits.
    ColumnarBatch keyRow = new ColumnarBatch(keyVectorsFromBatch(), 1);
    // Resolved outside the `try`, so a broken invariant behind it fails rather than falls back.
    BasePredicate predicate = storageFilter.preparedPredicate();
    boolean keep;
    try {
      keep = predicate.eval(keyRow.getRow(0));
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
   * Releases the survivor vectors the previous batch was built on, reads the next batch the way the
   * plain reader does, and then points the emitted batch's key slots at this batch's vectors. A
   * spliced row group takes them from the head of the survivor queue, and any other row group from
   * the persistent vectors, which is what a row group following a spliced one needs. Non-key slots
   * keep {@link #initBatch}'s vectors for the reader's life.
   *
   * <p>The row-index column the base fills is never a key column, since
   * {@code ParquetStorageFilter.create} rejects its name. So it always lands in a persistent
   * vector.
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
          // Unreachable, since the queue holds exactly the survivors phase 1 accumulated and the
          // emit loop is driven by that same count. Without this, the key slots would point at the
          // persistent vectors, which a spliced row group resets but never reads into, so the batch
          // would pair stale key values with this row group's rows as if they were real ones.
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
    return true;
  }

  @Override
  protected void initializeInternal() throws IOException, UnsupportedOperationException {
    super.initializeInternal();
    initializeLateMaterialization();
  }

  private void initializeLateMaterialization() throws IOException {
    if (fileReader == null) {
      // A row-group reader was handed in with no file reader behind it, so the phases cannot run.
      // The file is read the plain way.
      return;
    }
    blockAccounting = new ParquetBlockAccounting(fileReader);
    pageIndexDisabled = !readOptions.useColumnIndexFilter();
    // Resolve each key column's top-level ParquetColumn and keep the ones this file has. A key
    // column can be missing under schema evolution.
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

    // The non-key columns phase 2 reads. If this file has none of them, the filter saves nothing,
    // since phase 1 already reads every column the file has for every row. The reader then
    // declines it. The planner already declines a projection of key columns alone, so this catches
    // a file missing every projected non-key leaf, which also covers the row-index column. A struct
    // the file has, but none of whose requested fields it has, counts only under the legacy
    // `returnNullStructIfAllFieldsMissing`, since otherwise the clipped schema reads one of its
    // other fields to tell a null struct from one whose requested fields are all null.
    List<ColumnDescriptor> nonKey = requestedColumns.stream()
        .filter(column -> !keyTopLevelNames.contains(column.getPath()[0]))
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
      // One null byte per row either way. A fixed-width value adds its own width. A variable-length
      // one adds the int offset and int length that point at the byte child, plus the
      // `DEFAULT_CHILD_BYTES_PER_ROW` the child is allocated per row, and a null byte for each.
      // A decimal of up to 9 digits is held as an int, though its default size is a long's.
      int valueWidth = type instanceof DecimalType && DecimalType.is32BitDecimalType(type)
          ? 4 : type.defaultSize();
      fixedBytesPerRow +=
          variableLength ? 1 + 4 + 4 + 2 * DEFAULT_CHILD_BYTES_PER_ROW : 1 + valueWidth;
    }
    keyFixedBytesPerRow = fixedBytesPerRow;
    keyOnlyColumns = Arrays.stream(keys).map(KeyColumn::descriptor).toList();
    prepaidValueBytes = new long[keys.length];
    // Last, since a non-null `keyColumns` is what says the filter is applied to this file.
    keyColumns = keys;
  }

  /**
   * Loads the next row group with rows left to emit, through the three phases of the class javadoc.
   * Phase 2 reads the non-key columns over the survivors while splicing, the whole projection over
   * the survivors once splicing is given up, and the whole projection over
   * {@code pushedFilterRanges} once the filter is. Returns false when the row groups run out, which
   * ends a read the filter narrowed.
   */
  @Override
  protected boolean loadNextRowGroup() throws IOException {
    if (keyColumns == null) return super.loadNextRowGroup();
    // The previous row group is fully emitted by the time this is called, so its pages are done
    // with. Released here rather than at the next assignment, so a row group the filter empties,
    // or the end of the file, does not keep the previous one's pages alive. Its column readers go
    // too. They hold the row ranges it was read over, which the budget no longer counts, and the
    // dictionary and decoders of their last page.
    closeDataPages();
    releaseRowGroupReaders();
    StorageFilterMetrics m = storageFilter.metrics();
    List<BlockMetaData> blocks = fileReader.getRowGroups();
    while (nextBlockIndex < blocks.size()) {
      // A plain read goes back to `FileScanRDD`, which checks for a task kill, after every batch.
      // This loop can pass any number of row groups the filter empties without emitting one.
      ParquetStorageFilter.throwIfKilled();
      int blockIdx = nextBlockIndex++;
      long blockRowCount = blocks.get(blockIdx).getRowCount();

      // Phase 0: rows the pushed data filter allows, by column index. The full projection goes back
      // on first, because `getRowRanges` builds an unseen block's store over the current paths.
      fileReader.setRequestedSchema(requestedColumns);
      RowRanges pushedFilterRanges = pushedFilterRangesFor(blockIdx, blockRowCount);
      // RowRanges.rowCount() walks every range, so resolve each range set's count once.
      long baselineRows = pushedFilterRanges.rowCount();
      if (baselineRows == 0) {
        // An empty block, or one the pushed data filter rejects whole. Not this filter's skip, so
        // no storage-filter metric counts it.
        continue;
      }

      // Whether phase 2 can read part of this row group. That needs an offset index for every
      // column of the block's column index store, and parquet empties the store if one column
      // lacks it. With a data filter pushed, the store is built at initialize over the whole
      // projection, so every projected column is checked. Files written before parquet-mr 1.11,
      // or by pyarrow's `write_table` by default, have no offset index at all. With no data filter
      // pushed the check is stricter than needed, since the store would cover only the non-key
      // columns. False means nothing is buffered and the filter can only skip the row group whole.
      // Phase 2 then reads the whole projection over `pushedFilterRanges`. Parquet narrows those
      // only by an index it could read, so that read cannot throw.
      boolean canNarrowRowGroup =
          !pageIndexDisabled && blockAccounting.hasOffsetIndexes(blockIdx, requestedPaths);
      spliceCurrentRowGroup = canNarrowRowGroup;

      // Phase 1 reads the key columns over `pushedFilterRanges` and evaluates the filter per row.
      fileReader.setRequestedSchema(keyOnlyColumns);
      RowRanges survivors;
      // Closed at the end of the phase that reads it, since parquet closes no store it hands out.
      try (PageReadStore keyPages =
               fileReader.readFilteredRowGroup(blockIdx, pushedFilterRanges)) {
        requireRowCount(keyPages, baselineRows, blockIdx);
        survivors = evaluateStorageFilter(
            keyPages, pushedFilterRanges, baselineRows, blockRowCount, canNarrowRowGroup);
      }
      // Null means phase 1 gave the filter up for this row group, and phase 2 then reads every row
      // the pushed filter kept, as a plain read would. Phase 2 can also give it up, in the catch
      // below.
      boolean filterGivenUp = survivors == null;
      if (filterGivenUp) abandonSplicing();
      RowRanges finalRanges = filterGivenUp ? pushedFilterRanges : survivors;
      long finalRowCount = filterGivenUp ? baselineRows : survivors.rowCount();
      // The byte metrics' baseline, the non-key bytes a plain read asks for over the rows the
      // pushed filter kept. Not walked for a row group whose filter was given up.
      long nonKeyBaselineBytes = filterGivenUp ? 0L : blockAccounting.compressedBytesForRowRanges(
          blockIdx, nonKeyPaths, pushedFilterRanges, baselineRows);
      if (finalRowCount == 0) {
        // The filter rejected every row, so the row group is skipped whole.
        m.recordRowGroupSkipped(baselineRows, nonKeyBaselineBytes);
        continue;
      }

      fileReader.setRequestedSchema(spliceCurrentRowGroup ? nonKeyColumns : requestedColumns);
      // The footer check above catches every file written without offset indexes. This catch is
      // the backstop for an offset index the footer lists but parquet fails to read, which empties
      // that block's store, and only that block's, since parquet keeps one per block. The row group
      // is then read again over `pushedFilterRanges`, which that empty store has already widened to
      // the whole block, so the retry cannot throw. The next row group is narrowed as usual.
      //
      // `MissingOffsetIndexException` is parquet's internal API, as are the `ColumnIndexStore` and
      // `OffsetIndex` that `ParquetBlockAccounting` reads. Revisit both on a parquet upgrade. If
      // parquet stops throwing this one as it is, the damaged-index test in
      // `ParquetStorageFilterSuite` fails.
      try {
        dataPages = fileReader.readFilteredRowGroup(blockIdx, finalRanges);
      } catch (MissingOffsetIndexException e) {
        // Parquet turns any IOException reading an offset index into this, a kill's interrupt
        // included, which must not be taken for a missing index.
        ParquetStorageFilter.throwIfKilled();
        if (reportedMessages.add(OFFSET_INDEX_UNREADABLE)) {
          // The message only, since parquet has already logged the failed index read with its
          // stack trace.
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
        // Over the columns phase 2 read, which include the key columns again once splicing is
        // given up. A spliced row group that pruned nothing read exactly the baseline.
        long phase2Bytes = spliceCurrentRowGroup && finalRowCount == baselineRows
            ? nonKeyBaselineBytes
            : blockAccounting.compressedBytesForRowRanges(blockIdx,
                spliceCurrentRowGroup ? nonKeyPaths : requestedPaths, finalRanges, finalRowCount);
        // `SQLMetric.add` ignores a negative value, so a row group that read more than the
        // baseline after giving splicing up contributes nothing rather than subtracting.
        m.bytesAvoidedByPageFiltering().add(nonKeyBaselineBytes - phase2Bytes);
        m.rowsExcludedWithinRowGroup().add(baselineRows - finalRowCount);
      }

      // Phase 2 read exactly these rows, so its readers are handed the ranges rather than left to
      // rebuild them from the store's one row index per row.
      installRowGroup(dataPages, readerRangesFor(finalRanges, finalRowCount, blockRowCount));
      return true;
    }
    return false;
  }

  /**
   * A spliced row group's key slots take their values from the survivor queue at emit, so phase 2
   * reads no pages for them. Key columns are primitive, which is what the base class asks of a slot
   * answered true here.
   */
  @Override
  protected boolean suppliesOwnVector(int slot) {
    if (!spliceCurrentRowGroup) return false;
    for (KeyColumn key : keyColumns) {
      if (key.batchSlot() == slot) return true;
    }
    return false;
  }

  /**
   * Counts a file the filter rejects whole, which happens when every key column is missing from it
   * and the predicate is constant-false for the value the reader would have materialized. Every row
   * group the pushed data filter left rows in counts as skipped, with every projected byte of those
   * rows as avoided. Those are all non-key bytes, since the file has no key column.
   */
  private void recordFileSkipped() {
    StorageFilterMetrics m = storageFilter.metrics();
    List<BlockMetaData> blocks = fileReader.getRowGroups();
    for (int blockIdx = 0; blockIdx < blocks.size(); blockIdx++) {
      // Measured against the rows the pushed data filter kept, which is the baseline every other
      // skip path uses. Resolving them again is a cache hit whenever the two can differ, because
      // `getFilteredRecordCount()` at initialize resolved every block's ranges then.
      RowRanges blockRanges = pushedFilterRangesFor(blockIdx, blocks.get(blockIdx).getRowCount());
      long survivingRows = blockRanges.rowCount();
      if (survivingRows == 0) continue;
      // The key columns are missing from this file, so they contribute nothing to the walk, and the
      // whole projection is what a plain read would have transferred.
      long avoidedBytes = blockAccounting.compressedBytesForRowRanges(
          blockIdx, requestedPaths, blockRanges, survivingRows);
      m.recordRowGroupSkipped(survivingRows, avoidedBytes);
    }
  }

  /**
   * The rows of a block the pushed data filter allows, at column-index granularity, or every row of
   * the block when {@link #pageIndexDisabled} says the page index must not be used.
   *
   * <p>{@code getRowRanges} asks only whether a filter is pushed, not whether column-index
   * filtering is still enabled, so this is where that conf is honoured. Every phase reads within
   * these ranges, so narrowing them by an index the read may not trust would cost rows.
   */
  private RowRanges pushedFilterRangesFor(int blockIdx, long blockRowCount) {
    if (blockRowCount == 0) {
      // parquet-mr never writes an empty block, and the plain read path skips one too. For one,
      // `RowRanges.createSingle(0)` below would build Range(0, -1) and trip its `from <= to`
      // assertion.
      return RowRanges.EMPTY;
    }
    return pageIndexDisabled
        ? RowRanges.createSingle(blockRowCount)
        : fileReader.getRowRanges(blockIdx);
  }

  /**
   * The ranges to hand the column readers of a read that asked for {@code rowRanges}, which select
   * {@code rowCount} of the block's {@code blockRowCount} rows. They then find those rows in the
   * pages with no per-row work.
   *
   * <p>None for a read of every row, which is required rather than cheaper. Parquet reads the block
   * whole then and hands back a store with no row indexes, and a column reader refuses ranges it
   * cannot find in the pages by them (see {@code ParquetReadState.forRead}).
   */
  private static RowRanges readerRangesFor(
      RowRanges rowRanges, long rowCount, long blockRowCount) {
    return rowCount == blockRowCount ? null : rowRanges;
  }

  /**
   * Fails unless a page store holds as many rows as were asked for, which a null store never does,
   * since every caller asks for some. Parquet returns null only for an empty block or empty
   * ranges, and otherwise exactly the rows asked for. The column readers take their rows from the
   * ranges and the emit loop takes its count from the store, so a store that held a different
   * number would leave a batch's tail stale, or splice survivors into the next row group's rows,
   * with no error.
   */
  private static void requireRowCount(PageReadStore pages, long expected, int blockIdx) {
    long actual = pages == null ? 0L : pages.getRowCount();
    if (actual != expected) {
      throw ParquetStorageFilter.internalError(String.format(
          "Row group %d was read as %d rows where %d were asked for", blockIdx, actual, expected));
    }
  }

  /**
   * Evaluates the storage filter over every row of a key-only {@link PageReadStore}, in
   * capacity-sized chunks, and returns the surviving rows as {@link RowRanges} in block-row
   * coordinates. The result is a subset of {@code pushedFilterRanges}.
   *
   * <p>While splicing, each survivor's key values are appended to the accumulators for the emit
   * path to splice, until the buffer passes its cap.
   *
   * <p>Returns null once the reader gives the filter up for this row group, which happens on:
   * <ul>
   *   <li>the first survivor of a row group that cannot be narrowed, since whether anything
   *       survives was all that was left to learn;</li>
   *   <li>the surviving ranges passing the budget;</li>
   *   <li>an error decoding the key pages or applying the filter.</li>
   * </ul>
   */
  private RowRanges evaluateStorageFilter(
      PageReadStore keyPages,
      RowRanges pushedFilterRanges,
      long pushedFilterRowCount,
      long blockRowCount,
      boolean canNarrowRowGroup) throws IOException {
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

    PrimitiveIterator.OfLong rowIndexIter = pushedFilterRanges.iterator();
    RowRanges.Builder finalRangesBuilder = RowRanges.builder();
    // What this row group keeps, which the budget below weighs, is the bytes buffered for splicing
    // and the ranges the surviving rows fall into.
    long splicedBytes = 0L;
    long rangeBytes = 0L;
    long previousSurvivor = -2L;
    long cap = storageFilter.maxSplicedRowGroupBytes();
    // Out of the loop, because reaching it through the filter resolves a `lazy val`, which is a
    // volatile read the loop would pay per row.
    BasePredicate predicate = storageFilter.preparedPredicate();
    while (remaining > 0) {
      // Per chunk, since this decodes and evaluates a whole row group before its first batch.
      ParquetStorageFilter.throwIfKilled();
      int num = (int) Math.min((long) capacity, remaining);
      try {
        for (int i = 0; i < keyScratchVectors.length; i++) {
          keyScratchVectors[i].reset();
          readers[i].readBatch(num, keyScratchVectors[i], null, null);
        }
      } catch (Throwable t) {
        // A corrupt page shows here, since parquet reads a chunk's bytes up front but decodes a
        // page only when it is read. That includes a codec that reports corruption as an
        // `InternalError`, as hadoop-lzo does. Giving the row group up hands it to phase 2 whole,
        // which reads the same pages in the same batches as a plain scan. So the page fails at the
        // same batch, after the same rows, and `ignoreCorruptFiles` keeps what it keeps of a plain
        // read.
        giveUpOnError(t, ROW_GROUP_GIVEN_UP_ON_DECODING, true);
        // The failed decode may have grown the scratch vectors, and phase 2 reads the same pages
        // into vectors of its own. So capacity-sized ones replace them, and beside a plain read's
        // own vectors the reader then holds one capacity-sized batch of key vectors.
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
          // evaluates it on (see `ParquetStorageFilter.isSupportedStorageFilter`). Giving the
          // filter up returns every row the pushed filter kept, and the post-scan Filter decides
          // them in its own order, raising the same error for a row it does evaluate.
          //
          // This is not narrowed by type. A built-in expression can throw a plain JDK exception
          // (`timestamp_seconds` of a decimal) or an `AssertionError`, and a Hive UDF throws a
          // checked SparkException. Rethrowing would fail a query a plain read answers, or drop
          // the rest of the file silently under `ignoreCorruptFiles`.
          //
          // A fatal error goes out even on a row an earlier conjunct of the plan would have
          // rejected, so the reader never runs on after one the executor would take as fatal.
          giveUpOnError(t, ROW_GROUP_GIVEN_UP_ON_EVALUATION, false);
          return null;
        }
        if (!survives) continue;
        if (!canNarrowRowGroup) {
          // Nothing narrower than the whole row group can be read, so one survivor settles it.
          // Stop decoding. A row group with no survivor still gets empty ranges below and is
          // skipped.
          return null;
        }
        finalRangesBuilder.addSelectedRow(blockRow);
        if (blockRow != previousSurvivor + 1) rangeBytes += ROW_RANGE_BYTES;
        previousSurvivor = blockRow;
        if (spliceCurrentRowGroup) {
          try {
            if (currentKeyAccumulators == null) {
              splicedBytes +=
                  allocateKeyAccumulators(cap - splicedBytes - rangeBytes, remaining - r);
            }
            splicedBytes += appendSurvivorRowToAccumulators(r);
          } catch (RuntimeException e) {
            // The buffer is this reader's own and optional, so failing to fill it gives splicing up
            // rather than the filter, and the memory goes back at once. What fails here:
            //  - a survivor vector that cannot grow;
            //  - a key value the predicate did not read on this row, such as `b` in
            //    `coalesce(a, b)`, since the copy decodes every key;
            //  - a bug of ours, which only the WARN then shows.
            // Phase 2 reads the key columns the way a plain read does, so a value that cannot be
            // decoded fails the read only where a plain read fails.
            reportOnce(e, SPLICING_GIVEN_UP_ON_ERROR);
            abandonSplicing();
            splicedBytes = 0L;
          }
        }
        // Both parts of what this row group keeps grow per survivor, so they are weighed together.
        if (splicedBytes + rangeBytes > cap) {
          // The cheaper step first. Releasing the buffer is a no-op once splicing is given up.
          abandonSplicing();
          splicedBytes = 0L;
          // The ranges being built would be thrown away, so stop evaluating the rest.
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
    // Assigned before the loop on purpose, so that an allocation failure part way through leaves
    // the vectors allocated so far reachable for `close()`, which walks this array element-wise.
    keyScratchVectors = new WritableColumnVector[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyScratchVectors[i] = newKeyVector(i);
    }
    // Missing key columns keep their batch vectors (see `keyVectorsFromBatch`), which the emitted
    // batch owns.
    ColumnVector[] rowVectors = keyVectorsFromBatch();
    for (int i = 0; i < keyScratchVectors.length; i++) {
      rowVectors[keyColumns[i].rowPosition()] = keyScratchVectors[i];
    }
    keyScratchBatch = new ColumnarBatch(rowVectors);
  }

  /**
   * The key columns' vectors from the emitted batch, in the order the predicate's row reads them.
   * A key column this file does not have keeps its batch vector, which holds what the scan returns
   * for it (its existence DEFAULT or null). So the filter sees exactly the values the rows show.
   * Phase 1 replaces the others with its own vectors. Call only after {@link #takeOverBatch}.
   */
  private ColumnVector[] keyVectorsFromBatch() {
    int[] keyIndices = storageFilter.keyColumnIndices();
    ColumnVector[] vectors = new ColumnVector[keyIndices.length];
    for (int p = 0; p < keyIndices.length; p++) {
      vectors[p] = persistentBatchColumns[keyIndices[p]];
    }
    return vectors;
  }

  /** A capacity-sized vector for the i-th key column this file has, in the reader's memory mode. */
  private WritableColumnVector newKeyVector(int keyIdx) {
    DataType dt = keyColumns[keyIdx].type();
    return MEMORY_MODE == MemoryMode.OFF_HEAP
        ? new OffHeapColumnVector(capacity, dt)
        : new OnHeapColumnVector(capacity, dt);
  }

  /**
   * Allocates a set of accumulators, {@link #capacity} rows each, and returns the charge for the
   * byte children it sized up front. The rest of the set is charged per row as survivors arrive.
   * A variable-length key's byte child is reserved for what the set queued before it held, scaled
   * to the {@code rowsLeft} rows that can still reach this set, where that is more than its default
   * allocation. So it does not grow one doubling at a time for every capacity's worth of
   * survivors. With its default doubling, {@code reserve} takes twice what it is asked for, so
   * with its null bytes the child takes the factor's four bytes per value byte. That happens only
   * when the charge and the fixed part of those rows fit within {@code headroom}, what the cap
   * leaves. The memory is taken before any value arrives, so it is charged now, as if those values
   * had arrived, and the values written into it use that charge up before they add to it.
   */
  private long allocateKeyAccumulators(long headroom, long rowsLeft) {
    WritableColumnVector[] previous = survivorBatches.peekLast();
    int rows = (int) Math.min(capacity, rowsLeft);
    int[] sizeTo = new int[keyColumns.length];
    long charge = 0L;
    for (int i = 0; i < keyColumns.length; i++) {
      // Only a child the default allocation would not already hold gains from being sized.
      long valueBytes = previous != null && keyColumns[i].variableLength()
          ? (long) previous[i].arrayData().getElementsAppended() * rows / capacity : 0L;
      if (valueBytes > (long) capacity * DEFAULT_CHILD_BYTES_PER_ROW) {
        sizeTo[i] = (int) valueBytes;
        charge += VARIABLE_LENGTH_BYTES_FACTOR * valueBytes;
      }
    }
    // A set that is not sized up front grows as the first one does, charged value by value.
    boolean sizeUpFront = charge + (long) keyFixedBytesPerRow * rows <= headroom;
    // Assigned before the loop, so an allocation failure part way through leaves the vectors
    // allocated so far reachable for `abandonSplicing`.
    currentKeyAccumulators = new WritableColumnVector[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      currentKeyAccumulators[i] = newKeyVector(i);
      prepaidValueBytes[i] = 0L;
      if (sizeUpFront && sizeTo[i] > 0) {
        currentKeyAccumulators[i].arrayData().reserve(sizeTo[i]);
        prepaidValueBytes[i] = VARIABLE_LENGTH_BYTES_FACTOR * sizeTo[i];
      }
    }
    return sizeUpFront ? charge : 0L;
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
        // Measured on the destination, because a dictionary-encoded source has no length of its
        // own, its values being read through the dictionary.
        if (key.variableLength()) {
          long valueCharge = VARIABLE_LENGTH_BYTES_FACTOR * dst.getArrayLength(dstRow);
          long prepaid = Math.min(prepaidValueBytes[i], valueCharge);
          prepaidValueBytes[i] -= prepaid;
          charged += valueCharge - prepaid;
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

  /**
   * What this reader reports once per split. The give-up messages go through {@link #reportOnce},
   * and the offset-index one is logged at its catch, without a stack trace.
   */
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
   * Gives the filter up on an error, after rethrowing one the reader must not absorb, a task kill
   * or a fatal error. Every catch that takes any {@code Throwable} and gives the filter up goes
   * through here, so none can absorb those by leaving the rethrow out. {@code fromRead} says the
   * error came from decoding the key pages rather than from evaluating the filter, see
   * {@code ParquetStorageFilter.rethrowIfMustPropagate}. The {@code MissingOffsetIndexException}
   * catch takes that one type only, and checks for a kill itself.
   */
  private void giveUpOnError(Throwable t, String message, boolean fromRead) {
    ParquetStorageFilter.rethrowIfMustPropagate(t, fromRead);
    reportOnce(t, message);
  }

  /**
   * Reports the first time this reader gives something up on an error, in one of the messages
   * above. Once per reader and message, which is once per split, because a file whose values do
   * that tends to do it again. Per message, so one kind of error does not hide another.
   */
  private void reportOnce(Throwable t, String message) {
    if (!reportedMessages.add(message)) return;
    LOG.warn(message, t, MDC.of(LogKeys.PATH, fileReader.getFile()));
  }

  /**
   * Gives up splicing for the row group being evaluated and releases every survivor vector it has
   * buffered. Phase 2 then reads the full projected schema and the emit path takes the persistent
   * batch, so the rows are unaffected.
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

  /** Closes every non-null vector of {@code vectors}; tolerates a null array. */
  private static void closeAll(ColumnVector[] vectors) {
    if (vectors == null) return;
    for (ColumnVector v : vectors) {
      if (v != null) v.close();
    }
  }

  /**
   * One key column this file has, as init resolves it. One array of these, so no two facts about a
   * key can be paired wrongly.
   */
  private record KeyColumn(
      int batchSlot,
      int rowPosition,
      ColumnDescriptor descriptor,
      boolean required,
      DataType type,
      ValueCopier copier,
      boolean variableLength) {}
}
