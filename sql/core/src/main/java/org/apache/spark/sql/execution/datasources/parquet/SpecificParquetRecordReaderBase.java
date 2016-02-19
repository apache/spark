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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.schema.MessageType;

import org.apache.spark.deploy.SparkHadoopUtil;

/**
 * Base class for custom RecordReaaders for Parquet that directly materialize to `T`.
 * This class handles computing row groups, filtering on them, setting up the column readers,
 * etc.
 * This is heavily based on parquet-mr's RecordReader.
 * TODO: move this to the parquet-mr project. There are performance benefits of doing it
 * this way, albeit at a higher cost to implement. This base class is reusable.
 */
public abstract class SpecificParquetRecordReaderBase<T> extends RecordReader<Void, T> {
  protected Path file;
  protected MessageType fileSchema;
  protected MessageType requestedSchema;
  protected ReadSupport<T> readSupport;

  /**
   * The total number of rows this RecordReader will eventually read. The sum of the
   * rows of all the row groups.
   */
  protected long totalRowCount;

  protected ParquetFileReader reader;

  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration configuration =
      SparkHadoopUtil.get().getConfigurationFromJobContext(taskAttemptContext);
    ParquetInputSplit split = (ParquetInputSplit)inputSplit;
    this.file = split.getPath();
    long[] rowGroupOffsets = split.getRowGroupOffsets();

    ParquetMetadata footer;
    List<BlockMetaData> blocks;

    // if task.side.metadata is set, rowGroupOffsets is null
    if (rowGroupOffsets == null) {
      // then we need to apply the predicate push down filter
      footer = readFooter(configuration, file, range(split.getStart(), split.getEnd()));
      MessageType fileSchema = footer.getFileMetaData().getSchema();
      FilterCompat.Filter filter = getFilter(configuration);
      blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
      footer = readFooter(configuration, file, NO_FILTER);
      Set<Long> offsets = new HashSet<>();
      for (long offset : rowGroupOffsets) {
        offsets.add(offset);
      }
      blocks = new ArrayList<>();
      for (BlockMetaData block : footer.getBlocks()) {
        if (offsets.contains(block.getStartingPos())) {
          blocks.add(block);
        }
      }
      // verify we found them all
      if (blocks.size() != rowGroupOffsets.length) {
        long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
        for (int i = 0; i < foundRowGroupOffsets.length; i++) {
          foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
        }
        // this should never happen.
        // provide a good error message in case there's a bug
        throw new IllegalStateException(
            "All the offsets listed in the split should be found in the file."
                + " expected: " + Arrays.toString(rowGroupOffsets)
                + " found: " + blocks
                + " out of: " + Arrays.toString(foundRowGroupOffsets)
                + " in range " + split.getStart() + ", " + split.getEnd());
      }
    }
    MessageType fileSchema = footer.getFileMetaData().getSchema();
    Map<String, String> fileMetadata = footer.getFileMetaData().getKeyValueMetaData();
    this.readSupport = getReadSupportInstance(
        (Class<? extends ReadSupport<T>>) getReadSupportClass(configuration));
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        configuration, toSetMultiMap(fileMetadata), fileSchema));
    this.requestedSchema = readContext.getRequestedSchema();
    this.fileSchema = fileSchema;
    this.reader = new ParquetFileReader(configuration, file, blocks, requestedSchema.getColumns());
    for (BlockMetaData block : blocks) {
      this.totalRowCount += block.getRowCount();
    }
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  /**
   * Utility classes to abstract over different way to read ints with different encodings.
   * TODO: remove this layer of abstraction?
   */
  abstract static class IntIterator {
    abstract int nextInt() throws IOException;
  }

  protected static final class ValuesReaderIntIterator extends IntIterator {
    ValuesReader delegate;

    public ValuesReaderIntIterator(ValuesReader delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() throws IOException {
      return delegate.readInteger();
    }
  }

  protected static final class RLEIntIterator extends IntIterator {
    RunLengthBitPackingHybridDecoder delegate;

    public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() throws IOException {
      return delegate.readInt();
    }
  }

  protected static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() throws IOException { return 0; }
  }

  /**
   * Creates a reader for definition and repetition levels, returning an optimized one if
   * the levels are not needed.
   */
  static protected IntIterator createRLEIterator(int maxLevel, BytesInput bytes,
                                              ColumnDescriptor descriptor) throws IOException {
    try {
      if (maxLevel == 0) return new NullIntIterator();
      return new RLEIntIterator(
          new RunLengthBitPackingHybridDecoder(
              BytesUtils.getWidthFromMaxInt(maxLevel),
              new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new IOException("could not read levels in page for col " + descriptor, e);
    }
  }

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

  private static Class<?> getReadSupportClass(Configuration configuration) {
    return ConfigurationUtil.getClassFromConfig(configuration,
        ParquetInputFormat.READ_SUPPORT_CLASS, ReadSupport.class);
  }

  /**
   * @param readSupportClass to instantiate
   * @return the configured read support
   */
  private static <T> ReadSupport<T> getReadSupportInstance(
      Class<? extends ReadSupport<T>> readSupportClass){
    try {
      return readSupportClass.newInstance();
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    }
  }
}
