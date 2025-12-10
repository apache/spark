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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.apache.spark.sql.execution.datasources.PartitionedFile;

/**
 * `ParquetFooterReader` is a util class which encapsulates the helper
 * methods of reading parquet file footer
 */
public class ParquetFooterReader {

  /**
   * Build a filter for reading footer of the input Parquet file 'split'.
   * If 'skipRowGroup' is true, this will skip reading the Parquet row group metadata.
   *
   * @param file a part (i.e. "block") of a single file that should be read
   * @param hadoopConf   hadoop configuration of file
   * @param skipRowGroup If true, skip reading row groups;
   *                     if false, read row groups according to the file split range
   */
  public static ParquetMetadataConverter.MetadataFilter buildFilter(
      Configuration hadoopConf, PartitionedFile file, boolean skipRowGroup) {
    if (skipRowGroup) {
      return ParquetMetadataConverter.SKIP_ROW_GROUPS;
    } else {
      long fileStart = file.start();
      return HadoopReadOptions.builder(hadoopConf, file.toPath())
          .withRange(fileStart, fileStart + file.length())
          .build()
          .getMetadataFilter();
    }
  }

  public static ParquetMetadata readFooter(
      HadoopInputFile inputFile,
      ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    ParquetReadOptions readOptions = HadoopReadOptions
        .builder(inputFile.getConfiguration(), inputFile.getPath())
        .withMetadataFilter(filter).build();
    try (var fileReader = ParquetFileReader.open(inputFile, readOptions)) {
      return fileReader.getFooter();
    }
  }

  /**
   * Decoding Parquet files generally involves two steps:
   *  1. read and resolve the metadata (footer),
   *  2. read and decode the row groups/column chunks.
   * <p>
   * It's possible to avoid opening the file twice by resuing the SeekableInputStream.
   * When keepInputStreamOpen is true, the caller takes responsibility to close the
   * SeekableInputStream. Currently, this is only supported by parquet vectorized reader.
   *
   * @param hadoopConf hadoop configuration of file
   * @param file       a part (i.e. "block") of a single file that should be read
   * @param keepInputStreamOpen when true, keep the SeekableInputStream of file being open
   * @return if keepInputStreamOpen is true, the returned OpenedParquetFooter carries
   *         Some(SeekableInputStream), otherwise None.
   */
  public static OpenedParquetFooter openFileAndReadFooter(
      Configuration hadoopConf,
      PartitionedFile file,
      boolean keepInputStreamOpen) throws IOException {
    var readOptions = HadoopReadOptions.builder(hadoopConf, file.toPath())
        // `keepInputStreamOpen` is true only when parquet vectorized reader is used
        // on the caller side, in such a case, the footer will be resued later on
        // reading row groups, so here must read row groups metadata ahead.
        // when false, the caller uses parquet-mr to read the file, only file metadata
        // is required on planning phase, and parquet-mr will read the footer again
        // on reading row groups.
        .withMetadataFilter(buildFilter(hadoopConf, file, !keepInputStreamOpen))
        .build();
    var inputFile = HadoopInputFile.fromPath(file.toPath(), hadoopConf);
    var inputStream = inputFile.newStream();
    try (var fileReader = ParquetFileReader.open(inputFile, readOptions, inputStream)) {
      var footer = fileReader.getFooter();
      if (keepInputStreamOpen) {
        fileReader.detachFileInputStream();
        return new OpenedParquetFooter(footer, inputFile, Optional.of(inputStream));
      } else {
        return new OpenedParquetFooter(footer, inputFile, Optional.empty());
      }
    }
  }
}
