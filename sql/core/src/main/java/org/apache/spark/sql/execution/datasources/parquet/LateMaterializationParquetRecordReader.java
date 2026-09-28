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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

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
 * key column again. A row group whose buffer would pass
 * {@code spark.sql.parquet.storageFilterPushdown.maxSplicedRowGroupBytes} gives splicing up, and
 * past that the filter itself, and is read the plain way.
 *
 * <p>Splicing does not engage at all in one real case: every key column is missing from this
 * physical file under schema evolution. The predicate is then constant over the file and is
 * evaluated once, against the constants the reader materializes for those columns. True keeps the
 * file unfiltered, false skips it.
 *
 * <p>Everything else the filter needs is guaranteed by {@code FileSourceStrategy.storageFiltersFor}
 * and {@code ParquetStorageFilter.create}, so a violation of it here is a planner bug and is
 * asserted rather than handled. A file the reader simply cannot prune is a different matter: it
 * reads it the way a plain scan would.
 */
public class LateMaterializationParquetRecordReader extends VectorizedParquetRecordReader {

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(LateMaterializationParquetRecordReader.class);

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
   * The storage filter this reader applies: read key-column pages first, evaluate the filter per
   * row to build {@link RowRanges}, then read data-column pages restricted to surviving row ranges.
   * Nulled once the reader gives it up for the whole file, after which every path here behaves as
   * the plain reader does.
   */
  private ParquetStorageFilter storageFilter;

  /**
   * Set to true once all row groups have been processed.
   * */
  private boolean hitEndOfData = false;

  /**
   * Late-materialization state, populated by {@link #initializeLateMaterialization()}.
   *
   * <p>The base class's {@link #fileReader} drives all three phases. Its requested
   * schema is mutated per phase via {@link ParquetFileReader#setRequestedSchema}: all projected
   * columns for phase 0 ({@code getRowRanges}), the key columns for phase 1, and for phase 2 the
   * non-key columns while splicing or the whole projection once a row group gives it up.
   *
   * <p>The three sets are held as leaf-column lists rather than as {@code MessageType}s because
   * that is what both the reader and the byte metrics consume, and because
   * {@code MessageType.getColumns()} rebuilds the list on every call.
   *
   * <p>Each list's leaf {@link ColumnPath}s are resolved alongside it, because that is all the
   * footer walks below need and {@link ColumnPath#get} is not free: it allocates and interns
   * through parquet's canonicalizer, which those walks would otherwise repeat per column on every
   * one of their several calls per row group. As sets, because a walk goes over the block's column
   * chunks and asks which of them it is about, and a requested schema's leaf paths are unique
   * anyway.
   */
  private List<ColumnDescriptor> requestedColumns;
  private List<ColumnDescriptor> keyOnlyColumns;
  private List<ColumnDescriptor> nonKeyColumns;
  private Set<ColumnPath> keyOnlyPaths;
  private Set<ColumnPath> nonKeyPaths;
  /**
   * Whether a row group can be read in part, and what reading part of one transfers. Both are read
   * off the footer {@link #fileReader} already holds, so the byte metrics cost no IO.
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
  /** This file's row groups, as the footer lists them after parquet's own row-group filtering. */
  private List<BlockMetaData> blocks;
  /**
   * The key columns this file has, in the filter's key order, with everything init resolves about
   * each. Phase 1 reads them, and the survivor queues and accumulators are indexed the same way.
   */
  private KeyColumn[] keyColumns;
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

  /**
   * The filter is a constructor argument rather than a setter, because
   * {@link #initializeLateMaterialization()} inspects the per-file schema against it and runs from
   * {@code initialize}: there is no point in the reader's life at which it is useful without one.
   */
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

  @Override
  protected void closeAllocated() {
    // Each release below is independent, and the base class's close() owns the file handle and
    // input stream, so they are chained through finally blocks: one failing vector close must not
    // leak the rest.
    try {
      // The batch's vectors are closed through `persistentBatchColumns` rather than through
      // `columnarBatch.close()`, which lets both paths share one body. While splicing the emitted
      // batch is a view whose slots alias these vectors (non-key and partition) and the head of
      // each survivor queue (key slots), so closing it would re-close a shared vector and free the
      // same buffer twice. Without splicing its array is this one.
      try {
        if (persistentBatchColumns != null) {
          closeAll(persistentBatchColumns);
        } else if (columnarBatch != null) {
          // `takeOverBatch` has not run, or threw part way: the batch super built is then the only
          // thing holding its vectors.
          columnarBatch.close();
        }
        persistentBatchColumns = null;
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
        closeDataPages();
      }
    }
  }

  @Override
  public float getProgress() {
    // Under a storage filter, rowsReturned counts survivors while totalRowCount is the count before
    // the filter narrowed anything, so the ratio neither reaches 1 on its own nor stays below it: a
    // row group read whole after the page index is given up can return more rows than that count.
    // `hitEndOfData` is the real terminator here, and the ratio is clamped to what it means.
    if (hitEndOfData) return 1.0f;
    return Math.min(1.0f, super.getProgress());
  }

  /**
   * Builds the batch the way the plain reader does, takes it over for splicing, and then answers
   * the filter for a file that has none of its key columns.
   *
   * <p>Both after super's body: the batch it hands out is what the splicing one is built over, and
   * the all-keys-missing decision evaluates the predicate against the constant vectors this reader
   * returns for those columns, which exist only once super has wrapped the allocated vectors in
   * {@link ParquetColumnVector}s.
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
   * Takes the vectors of the batch super built, and while splicing hands out a batch of this
   * reader's own over a copy of that array, whose key slots the emit path rewrites in place.
   * {@link ColumnarBatch} holds the array by reference, its staging row included, so rewriting a
   * slot is what publishes it, and the object being reused is what makes a consumer see it.
   *
   * <p>Every slot got a vector, key columns included. While splicing a key slot's vector is
   * unused, since the emitted batch takes that slot from the survivor queues. A row group read the
   * plain way past the buffer cap does read into it, and one capacity-sized vector per key column
   * is cheap next to the buffer the cap is there to bound.
   */
  private void takeOverBatch() {
    persistentBatchColumns = new ColumnVector[columnarBatch.numCols()];
    for (int i = 0; i < persistentBatchColumns.length; i++) {
      persistentBatchColumns[i] = columnarBatch.column(i);
    }
    if (keyColumns == null) return;
    spliceBatchColumns = persistentBatchColumns.clone();
    columnarBatch = new ColumnarBatch(spliceBatchColumns);
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

  /**
   * Whether this read has returned every row it is going to.
   *
   * <p>{@code totalRowCount} is how many rows the file yields before this filter narrows anything,
   * so it is a terminator only while there is no filter. Under one it is an upper bound the read
   * never reaches, and a row group read whole after the page index was given up can pass it, so
   * {@link #hitEndOfData} from the row-group loop is what ends the read.
   */
  @Override
  protected boolean noMoreRows() {
    return hitEndOfData || (storageFilter == null && super.noMoreRows());
  }

  /**
   * Releases the key vectors the previous batch was built on, before the batch is emptied, so a
   * terminal call cannot leave a spliced batch pointing at vectors just released, which off heap is
   * freed memory.
   */
  @Override
  protected void resetBatch() {
    releasePublishedKeyVectors();
    super.resetBatch();
  }

  /**
   * Points the emitted batch's key slots at what this batch takes them from, then reads the rest
   * the way the plain path does. Three cases reach the base's loop: a spliced row group, whose key
   * slots have no phase-2 reader because the emitted batch takes them from the survivor queues, a
   * row group read the plain way past the buffer cap, which has a reader for every slot, and a file
   * the filter was given up for, which is the plain path exactly.
   *
   * <p>The row-index column the base fills there is identified by name
   * (ROW_INDEX_TEMPORARY_COLUMN_NAME), a synthetic metadata column no storage filter references, so
   * its slot is always a non-key one and its ParquetColumnVector is always the persistent one.
   */
  @Override
  protected void readBatchColumns(int num) throws IOException {
    // Without a storage filter the emitted batch is `initBatch`'s array and this does not apply.
    if (spliceBatchColumns != null) {
      pointKeySlotsAtThisBatch(num);
    }
    super.readBatchColumns(num);
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
          // loop is driven by that same count. Named here rather than left to a null batch slot and
          // an NPE somewhere in the read that follows.
          throw new IllegalStateException(String.format(
              "Storage-filter survivor queue %d of row group %d in %s ran out with %d rows still "
                  + "to emit", k, nextBlockIndex - 1, fileReader.getFile(), num));
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

  @Override
  protected void initializeInternal() throws IOException, UnsupportedOperationException {
    super.initializeInternal();
    initializeLateMaterialization();
  }

  private void initializeLateMaterialization() throws IOException {
    if (fileReader == null) {
      // The three phases drive a ParquetFileReader directly, and this reader was handed a row-group
      // reader with none behind it, so the filter is simply not applied and the plain read path
      // runs. The conjunct is in the post-scan filter as well, so the rows it would have dropped
      // are dropped above the scan.
      storageFilter = null;
      return;
    }
    blockAccounting = new ParquetBlockAccounting(fileReader);
    blocks = fileReader.getRowGroups();
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
    // Both arrays before either is filled, so `closeSplicingState` finding one of them finds both,
    // whatever failed part way through here.
    keyVectorQueues = new ArrayDeque[keyColumns.length];
    currentKeyAccumulators = new WritableColumnVector[keyColumns.length];
    for (int i = 0; i < keyColumns.length; i++) {
      keyVectorQueues[i] = new ArrayDeque<>();
    }
    currentKeyAccumulatorRowCount = 0;
  }

  @Override
  protected void checkEndOfRowGroup() throws IOException {
    // Nulled once the reader gave the filter up for the whole file, and then the plain path is
    // exactly what this read wants.
    if (storageFilter == null) {
      super.checkEndOfRowGroup();
      return;
    }
    if (rowsReturned != totalCountLoadedSoFar) return;
    loadNextRowGroupWithLateMaterialization();
  }

  /**
   * Loads the next row group using the three-phase late-materialization pattern, all driven by the
   * one {@link #fileReader} with its requested schema mutated per phase:
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
    while (nextBlockIndex < blocks.size()) {
      int blockIdx = nextBlockIndex++;
      long blockRowCount = blocks.get(blockIdx).getRowCount();
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
      fileReader.setRequestedSchema(requestedColumns);
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
      // buffers for. Reading part of one goes through the block's column index store, and parquet
      // builds that store over every column of the requested schema, emptying it altogether if one
      // of them has no offset index. So this asks the footer about the whole projection rather than
      // about the columns phase 2 reads: a key column without an index would otherwise pass here
      // and throw in phase 2, and the cost of learning it there is a row group's survivors copied
      // and then thrown away. The footer answers for free.
      // False says the filter is left with emptying this row group whole, and then nothing is
      // buffered either: phase 2 reads the key columns again along with everything else, over
      // ranges an empty store has already widened to the whole block, which is why it cannot throw.
      // Parquet's own exception stays as the backstop for what the footer cannot see: an offset
      // index it claims but cannot produce.
      boolean canNarrowRowGroup = !pageIndexUnusable
          && blockAccounting.hasOffsetIndexes(blockIdx, keyOnlyPaths)
          && blockAccounting.hasOffsetIndexes(blockIdx, nonKeyPaths);
      spliceCurrentRowGroup = canNarrowRowGroup;

      // Phase 1: switch to key-only schema, read key columns under pushedFilterRanges, evaluate the
      // storage filter per row. The defaults below are what a row group whose filter is given up
      // emits, which is every row of `pushedFilterRanges`, exactly what a plain read would.
      RowRanges finalRanges = pushedFilterRanges;
      long finalRowCount = baselineRows;
      fileReader.setRequestedSchema(keyOnlyColumns);
      // Closed at the end of the phase that reads it. `readFilteredRowGroup` hands out a store
      // the file reader does not track, unlike `readNextRowGroup`, so nothing else would.
      RowRanges survivors;
      try (PageReadStore keyPages =
               fileReader.readFilteredRowGroup(blockIdx, pushedFilterRanges)) {
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
          m.recordRowGroupSkipped(baselineRows, nonKeyBaselineBytes);
          continue;
        }
      }

      // Phase 2 reads the non-key columns under the surviving rows, or the whole projection under
      // `pushedFilterRanges` for a row group whose filter was given up.
      fileReader.setRequestedSchema(spliceCurrentRowGroup ? nonKeyColumns : requestedColumns);
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
        dataPages = fileReader.readFilteredRowGroup(blockIdx, finalRanges);
      } catch (MissingOffsetIndexException e) {
        // The exception's own message rather than the exception: this is a file shape the feature
        // expects and degrades on, so a stack trace per file of a table written without a page
        // index is noise. Parquet logs the same condition on its own path without one either.
        LOG.warn("Reading {} without page-level storage filtering: reading part of a row group "
            + "needs a Parquet offset index, and this file has none for at least one column the "
            + "read needs ({}). Row groups the filter empties are still skipped whole",
            MDC.of(LogKeys.PATH, fileReader.getFile()),
            MDC.of(LogKeys.REASON, e.getMessage()));
        pageIndexUnusable = true;
        filterGivenUp = true;
        abandonSplicing();
        finalRanges = pushedFilterRanges;
        finalRowCount = baselineRows;
        if (baselineRows != blockRowCount) {
          // The retry avoids the same wall because a store missing one column's offset index
          // reports no column index either, so these ranges cover the whole block and parquet reads
          // it without consulting an index. That is three parquet internals deep, so it is not
          // assumed: if they are narrower, the block is read whole instead, which is a superset of
          // them. The extra rows cost the post-scan Filter work, where failing here would fail a
          // query a plain read answers, and under `ignoreCorruptFiles` would be read as a corrupt
          // file and drop the rest of this one.
          finalRanges = RowRanges.createSingle(blockRowCount);
          finalRowCount = blockRowCount;
        }
        fileReader.setRequestedSchema(requestedColumns);
        dataPages = fileReader.readFilteredRowGroup(blockIdx, finalRanges);
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
        // The same ranges as the baseline when nothing was pruned, and then the walk would arrive
        // at the same number a second time.
        long phase2Bytes = finalRowCount == baselineRows
            ? nonKeyBaselineBytes
            : blockAccounting.compressedBytesForRowRanges(
                blockIdx, nonKeyPaths, finalRanges, finalRowCount);
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

  /**
   * Counts a file the filter rejects whole, which happens when every key column is missing from it
   * and the predicate is constant-false for the value the reader would have materialized. Every row
   * group counts as skipped and every projected byte as avoided, which is what the counters mean
   * for a row group the filter empties.
   */
  private void recordFileSkipped() {
    StorageFilterMetrics m = storageFilter.metrics();
    Set<ColumnPath> requestedPaths = ParquetBlockAccounting.pathsOf(requestedColumns);
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
      m.recordRowGroupSkipped(survivingRows, avoidedBytes);
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
        : fileReader.getRowRanges(blockIdx);
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
    // Out of the loop: reaching it through the filter resolves a `lazy val`, which is a volatile
    // read the loop would pay per row.
    BasePredicate predicate = storageFilter.preparedPredicate();
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
          survives = predicate.eval(keyScratchBatch.getRow(r));
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
            // The cheaper concession first: release the buffer, which is a no-op for a row group
            // that has already given splicing up.
            abandonSplicing();
            splicedBytes = 0L;
            if (rangeBytes > cap) {
              // The ranges being built are about to be thrown away, so stop evaluating the rest.
              return null;
            }
          }
        }
      }
      remaining -= num;
    }

    // Any partially-filled accumulator goes to its queue, so the emit path can dequeue it as this
    // row group's final batch.
    if (spliceCurrentRowGroup && currentKeyAccumulatorRowCount > 0) {
      pushAccumulatorsToQueues();
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
    // Every position from the batch first, which is what a missing key column reads as, then the
    // ones this file has over the top. Reading a present key's batch vector and discarding it is a
    // getter over a final field, and it saves holding the missing positions as state.
    for (int p = 0; p < keyIndices.length; p++) {
      rowVectors[p] = columnVectors[keyIndices[p]].getValueVector();
    }
    for (int i = 0; i < keyScratchVectors.length; i++) {
      rowVectors[keyColumns[i].rowPosition()] = keyScratchVectors[i];
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
        MDC.of(LogKeys.PATH, fileReader.getFile()));
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
   * Closes anything held by the splicing path: every vector still queued, the published head
   * included, and partially-filled accumulators. Called from {@link #closeAllocated()}.
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
}
