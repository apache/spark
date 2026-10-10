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
 * from the metadata the {@link ParquetFileReader} already holds, with no IO of its own. One
 * instance per reader, since the warning below is reported once per instance.
 */
final class ParquetBlockAccounting {
  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(ParquetBlockAccounting.class);

  private final ParquetFileReader reader;

  /** Whether the byte count has already been reported as undercounting by this instance. */
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
   * Whether every one of {@code paths} that this file has carries an offset index in this block,
   * which a partial read needs.
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
   * Compressed bytes a reader transfers for the columns in {@code paths} when it reads exactly
   * {@code rowRanges} of the block, page headers and the dictionary page included.
   * {@code rowRangeCount} is {@code rowRanges.rowCount()}, which the caller has.
   *
   * <p>A whole block is the chunks' sizes from the footer. A strict subset is walked over the
   * offset index, as parquet's read does. That reads the block's {@link ColumnIndexStore}, which
   * costs no IO only once a read of those ranges has built it, so a new caller has to keep that
   * order. A column the file does not have contributes nothing.
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
        // The store answers null for a path it was not built with, which means it was built over
        // a narrower schema than this walk asks about, an ordering bug. Reported rather than
        // thrown, since a counter must not fail a read.
        if (chunk.getOffsetIndexReference() != null && !loggedMissingStoreEntry) {
          loggedMissingStoreEntry = true;
          LOG.warn("Miscounting the storage filter's avoided bytes for {}: column {} of row "
              + "group {} has an offset index the block's column index store was not built with",
              MDC.of(LogKeys.PATH, reader.getFile()),
              MDC.of(LogKeys.COLUMN_NAME, path.toDotString()),
              MDC.of(LogKeys.INDEX, blockIndex));
        }
        continue;
      }
      int pageCount = offsetIndex.getPageCount();
      if (pageCount == 0) {
        continue;
      }
      total += dictionaryPageSize(chunk, offsetIndex);
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
   * Compressed size of a chunk's dictionary page, or 0 if it has none, taken from the offset index
   * as parquet's read takes it. The footer's data page offset can point at the dictionary page
   * itself, as parquet-mr 1.11 wrote it (PARQUET-1977).
   */
  private static long dictionaryPageSize(ColumnChunkMetaData chunk, OffsetIndex offsetIndex) {
    long startingPos = chunk.getStartingPos();
    long firstDataPageOffset = offsetIndex.getOffset(0);
    return startingPos < firstDataPageOffset ? firstDataPageOffset - startingPos : 0L;
  }
}
