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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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

  public static final boolean SKIP_ROW_GROUPS = true;
  public static final boolean WITH_ROW_GROUPS = false;

  /**
   * Reads footer for the input Parquet file 'split'. If 'skipRowGroup' is true,
   * this will skip reading the Parquet row group metadata.
   *
   * @param file a part (i.e. "block") of a single file that should be read
   * @param configuration hadoop configuration of file
   * @param skipRowGroup If true, skip reading row groups;
   *                     if false, read row groups according to the file split range
   */
  public static ParquetMetadata readFooter(
      Configuration configuration,
      PartitionedFile file,
      boolean skipRowGroup) throws IOException {
    long fileStart = file.start();
    ParquetMetadataConverter.MetadataFilter filter;
    if (skipRowGroup) {
      filter = ParquetMetadataConverter.SKIP_ROW_GROUPS;
    } else {
      filter = HadoopReadOptions.builder(configuration, file.toPath())
          .withRange(fileStart, fileStart + file.length())
          .build()
          .getMetadataFilter();
    }
    return readFooter(configuration, file.toPath(), filter);
  }

  public static ParquetMetadata readFooter(Configuration configuration,
      Path file, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    return readFooter(HadoopInputFile.fromPath(file, configuration), filter);
  }

  public static ParquetMetadata readFooter(Configuration configuration,
      FileStatus fileStatus, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    return readFooter(HadoopInputFile.fromStatus(fileStatus, configuration), filter);
  }

  private static ParquetMetadata readFooter(HadoopInputFile inputFile,
      ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    ParquetReadOptions readOptions =
      HadoopReadOptions.builder(inputFile.getConfiguration(), inputFile.getPath())
        .withMetadataFilter(filter).build();
    // Use try-with-resources to ensure fd is closed.
    try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions)) {
      return fileReader.getFooter();
    }
  }
}
