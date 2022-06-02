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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.column.page.PageReadStore;
import scala.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.util.AccumulatorV2;

/**
 * Base class for custom RecordReaders for Parquet that directly materialize to `T`.
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
  protected StructType sparkSchema;
  // Keep track of the version of the parquet writer. An older version wrote
  // corrupt delta byte arrays, and the version check is needed to detect that.
  protected ParsedVersion writerVersion;
  protected ParquetColumn parquetColumn;

  /**
   * The total number of rows this RecordReader will eventually read. The sum of the
   * rows of all the row groups.
   */
  protected long totalRowCount;

  protected ParquetRowGroupReader reader;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    FileSplit split = (FileSplit) inputSplit;
    this.file = split.getPath();

    ParquetReadOptions options = HadoopReadOptions
      .builder(configuration, file)
      .withRange(split.getStart(), split.getStart() + split.getLength())
      .build();
    ParquetFileReader fileReader = new ParquetFileReader(
        HadoopInputFile.fromPath(file, configuration), options);
    this.reader = new ParquetRowGroupReaderImpl(fileReader);
    this.fileSchema = fileReader.getFileMetaData().getSchema();
    try {
      this.writerVersion = VersionParser.parse(fileReader.getFileMetaData().getCreatedBy());
    } catch (Exception e) {
      // Swallow any exception, if we cannot parse the version we will revert to a sequential read
      // if the column is a delta byte array encoding (due to PARQUET-246).
    }
    Map<String, String> fileMetadata = fileReader.getFileMetaData().getKeyValueMetaData();
    ReadSupport<T> readSupport = getReadSupportInstance(getReadSupportClass(configuration));
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        taskAttemptContext.getConfiguration(), toSetMultiMap(fileMetadata), fileSchema));
    this.requestedSchema = readContext.getRequestedSchema();
    fileReader.setRequestedSchema(requestedSchema);
    String sparkRequestedSchemaString =
        configuration.get(ParquetReadSupport$.MODULE$.SPARK_ROW_REQUESTED_SCHEMA());
    StructType sparkRequestedSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
    ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(configuration);
    this.parquetColumn = converter.convertParquetColumn(requestedSchema,
      Option.apply(sparkRequestedSchema));
    this.sparkSchema = (StructType) parquetColumn.sparkType();
    this.totalRowCount = fileReader.getFilteredRecordCount();

    // For test purpose.
    // If the last external accumulator is `NumRowGroupsAccumulator`, the row group number to read
    // will be updated to the accumulator. So we can check if the row groups are filtered or not
    // in test case.
    TaskContext taskContext = TaskContext$.MODULE$.get();
    if (taskContext != null) {
      Option<AccumulatorV2<?, ?>> accu = taskContext.taskMetrics().externalAccums().lastOption();
      if (accu.isDefined() && accu.get().getClass().getSimpleName().equals("NumRowGroupsAcc")) {
        @SuppressWarnings("unchecked")
        AccumulatorV2<Integer, Integer> intAccum = (AccumulatorV2<Integer, Integer>) accu.get();
        intAccum.add(fileReader.getRowGroups().size());
      }
    }
  }

  /**
   * Initializes the reader to read the file at `path` with `columns` projected. If columns is
   * null, all the columns are projected.
   *
   * This is exposed for testing to be able to create this reader without the rest of the Hadoop
   * split machinery. It is not intended for general use and those not support all the
   * configurations.
   */
  protected void initialize(String path, List<String> columns) throws IOException {
    Configuration config = new Configuration();
    config.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key() , false);
    config.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), false);
    config.setBoolean(SQLConf.CASE_SENSITIVE().key(), false);
    config.setBoolean(SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED().key(), false);

    this.file = new Path(path);
    long length = this.file.getFileSystem(config).getFileStatus(this.file).getLen();

    ParquetReadOptions options = HadoopReadOptions
      .builder(config, file)
      .withRange(0, length)
      .build();
    ParquetFileReader fileReader = ParquetFileReader.open(
      HadoopInputFile.fromPath(file, config), options);
    this.reader = new ParquetRowGroupReaderImpl(fileReader);
    this.fileSchema = fileReader.getFooter().getFileMetaData().getSchema();

    if (columns == null) {
      this.requestedSchema = fileSchema;
    } else {
      if (columns.size() > 0) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (String s: columns) {
          if (!fileSchema.containsField(s)) {
            throw new IOException("Can only project existing columns. Unknown field: " + s +
                    " File schema:\n" + fileSchema);
          }
          builder.addFields(fileSchema.getType(s));
        }
        this.requestedSchema = builder.named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME());
      } else {
        this.requestedSchema = ParquetSchemaConverter.EMPTY_MESSAGE();
      }
    }
    fileReader.setRequestedSchema(requestedSchema);
    this.parquetColumn = new ParquetToSparkSchemaConverter(config)
      .convertParquetColumn(requestedSchema, Option.empty());
    this.sparkSchema = (StructType) parquetColumn.sparkType();
    this.totalRowCount = fileReader.getFilteredRecordCount();
  }

  @VisibleForTesting
  protected void initialize(
      MessageType fileSchema,
      MessageType requestedSchema,
      ParquetRowGroupReader rowGroupReader,
      int totalRowCount) throws IOException {
    this.reader = rowGroupReader;
    this.fileSchema = fileSchema;
    this.requestedSchema = requestedSchema;
    Configuration config = new Configuration();
    config.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key() , false);
    config.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), false);
    config.setBoolean(SQLConf.CASE_SENSITIVE().key(), false);
    config.setBoolean(SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED().key(), false);
    this.parquetColumn = new ParquetToSparkSchemaConverter(config)
      .convertParquetColumn(requestedSchema, Option.empty());
    this.sparkSchema = (StructType) parquetColumn.sparkType();
    this.totalRowCount = totalRowCount;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
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

  @SuppressWarnings("unchecked")
  private Class<? extends ReadSupport<T>> getReadSupportClass(Configuration configuration) {
    return (Class<? extends ReadSupport<T>>) ConfigurationUtil.getClassFromConfig(configuration,
        ParquetInputFormat.READ_SUPPORT_CLASS, ReadSupport.class);
  }

  /**
   * @param readSupportClass to instantiate
   * @return the configured read support
   */
  private static <T> ReadSupport<T> getReadSupportInstance(
      Class<? extends ReadSupport<T>> readSupportClass){
    try {
      return readSupportClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException |
             NoSuchMethodException | InvocationTargetException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    }
  }

  interface ParquetRowGroupReader extends Closeable {
    /**
     * Reads the next row group from this reader. Returns null if there is no more row group.
     */
    PageReadStore readNextRowGroup() throws IOException;
  }

  private static class ParquetRowGroupReaderImpl implements ParquetRowGroupReader {
    private final ParquetFileReader reader;

    ParquetRowGroupReaderImpl(ParquetFileReader reader) {
      this.reader = reader;
    }

    @Override
    public PageReadStore readNextRowGroup() throws IOException {
      return reader.readNextFilteredRowGroup();
    }

    @Override
    public void close() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
