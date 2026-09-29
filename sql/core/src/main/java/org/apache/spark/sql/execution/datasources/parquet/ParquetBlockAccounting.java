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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.columnindex.RowRanges;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * What a partial read of a Parquet file transfers, and whether one is possible at all, answered
 * from the footer the {@link ParquetFileReader} already holds. Every method here is metadata
 * arithmetic and causes no IO of its own, which is what lets a reader report these numbers per row
 * group.
 *
 * <p>One instance per file, because the warning below is reported once per file.
 */
final class ParquetBlockAccounting {
  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(ParquetBlockAccounting.class);

  private final ParquetFileReader reader;

  /** Whether the byte count has already been reported as undercounting on this file. */
  private boolean loggedMissingStoreEntry;

  ParquetBlockAccounting(ParquetFileReader reader) {
    this.reader = reader;
  }

  /** Resolves a column list's leaf paths, so the per-row-group footer walks do not have to. */
  static Set<ColumnPath> pathsOf(List<ColumnDescriptor> columns) {
    Set<ColumnPath> paths = new HashSet<>();
    for (ColumnDescriptor column : columns) {
      paths.add(ColumnPath.get(column.getPath()));
    }
    return paths;
  }

  /**
   * Whether every one of {@code paths} that this file has can be read in part, which needs an
   * offset index for it in this block. A column the file does not have is no obstacle, since a read
   * never reads it and it is not in the walk to begin with.
   */
  boolean hasOffsetIndexes(int blockIndex, Set<ColumnPath> paths) {
    for (ColumnChunkMetaData chunk : reader.getRowGroups().get(blockIndex).getColumns()) {
      if (paths.contains(chunk.getPath()) && chunk.getOffsetIndexReference() == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compressed bytes a reader transfers for the leaf columns named by {@code paths} when it reads
   * exactly {@code rowRanges} of the given block. Page headers and the dictionary page are
   * included, since both are read whenever any page of a chunk is read. {@code rowRangeCount} is
   * {@code rowRanges.rowCount()}, passed in because that walks every range and the caller has it.
   *
   * <p>Two sources, chosen so this never causes IO of its own:
   * <ul>
   *   <li>{@code rowRanges} covers the whole block, and the answer is the sum of the chunks'
   *       {@code getTotalSize()}, which is already in the footer. This is the case that matters.
   *       Whenever nothing else has built the block's {@link ColumnIndexStore}, {@code rowRanges}
   *       is necessarily the whole block, because a narrower range can only come from column-index
   *       filtering, which builds the store as a side effect.
   *   <li>{@code rowRanges} is a strict subset, and the walk goes over the offset index, as
   *       parquet's own read path does, adding the dictionary page the way
   *       {@code calculateOffsetRanges} does. The store is guaranteed to exist here, so the walk is
   *       pure metadata arithmetic. For ranges a reader narrowed itself, which column-index
   *       filtering had no hand in, that guarantee is an ordering one, since the reader's own read
   *       of those ranges built the store first.
   * </ul>
   *
   * <p>Columns absent from this physical file (schema evolution) contribute nothing, which is
   * correct, since the reader transfers nothing for them. They fall out of the walk by themselves,
   * which goes over the block's chunks rather than over {@code paths}.
   */
  long compressedBytesForRowRanges(
      int blockIndex,
      Set<ColumnPath> paths,
      RowRanges rowRanges,
      long rowRangeCount) {
    if (paths.isEmpty() || rowRangeCount == 0) {
      return 0L;
    }
    BlockMetaData block = reader.getRowGroups().get(blockIndex);
    long blockRowCount = block.getRowCount();
    boolean wholeBlock = rowRangeCount == blockRowCount;
    ColumnIndexStore ciStore = wholeBlock ? null : reader.getColumnIndexStore(blockIndex);
    long total = 0L;
    for (ColumnChunkMetaData chunk : block.getColumns()) {
      ColumnPath path = chunk.getPath();
      if (!paths.contains(path)) {
        // A leaf of the file this walk is not about, either not projected at all, or projected but
        // read in another phase than the one being measured.
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
        // The store answers null, rather than throwing, for a path it was not built with. That can
        // only mean the block's store was built while a narrower schema was requested than this
        // walk asks about, an ordering bug rather than a property of the file, and the footer tells
        // the two apart. It is reported rather than thrown, because this walk only produces a
        // counter, and `ignoreCorruptFiles` turns any exception from a reader into a silently
        // truncated file, so a byte metric must not be able to change the answer.
        if (chunk.getOffsetIndexReference() != null && !loggedMissingStoreEntry) {
          loggedMissingStoreEntry = true;
          LOG.warn("Undercounting the storage filter's avoided bytes for {}: column {} of row "
              + "group {} has an offset index the block's column index store was not built with",
              MDC.of(LogKeys.PATH, reader.getFile()),
              MDC.of(LogKeys.COLUMN_NAME, path.toDotString()),
              MDC.of(LogKeys.INDEX, blockIndex));
        }
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
}
