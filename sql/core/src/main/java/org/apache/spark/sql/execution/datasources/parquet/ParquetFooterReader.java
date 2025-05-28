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
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;

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
    ParquetMetadataConverter.MetadataFilter filter =
        footerFilter(configuration, file, skipRowGroup);
    return readFooter(HadoopInputFile.fromStatus(file.fileStatus(), configuration), filter);
  }

  public static ParquetMetadataConverter.MetadataFilter footerFilter(
      Configuration configuration, PartitionedFile file, boolean skipRowGroup) {
    if (skipRowGroup) {
      return ParquetMetadataConverter.SKIP_ROW_GROUPS;
    } else {
      return HadoopReadOptions.builder(configuration, file.toPath())
          .withRange(file.start(), file.start() + file.length())
          .build()
          .getMetadataFilter();
    }
  }

  public static ParquetMetadata readFooter(HadoopInputFile inputFile,
      ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    ParquetReadOptions readOptions = HadoopReadOptions
        .builder(inputFile.getConfiguration(), inputFile.getPath())
        .usePageChecksumVerification(false) // Useless for reading footer
        .withMetadataFilter(filter).build();
    // Use try-with-resources to ensure fd is closed.
    try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions)) {
      return fileReader.getFooter();
    }
  }

  // The caller takes responsibility to close inputStream.
  public static ParquetMetadata readFooter(
      HadoopInputFile inputFile,
      SeekableInputStream inputStream,
      ParquetMetadataConverter.MetadataFilter filter)  throws IOException {
    ParquetReadOptions readOptions = HadoopReadOptions
        .builder(inputFile.getConfiguration(), inputFile.getPath())
        .usePageChecksumVerification(false) // Useless for reading footer
        .withMetadataFilter(filter).build();
    ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions, inputStream);
    return fileReader.getFooter();
  }
}
