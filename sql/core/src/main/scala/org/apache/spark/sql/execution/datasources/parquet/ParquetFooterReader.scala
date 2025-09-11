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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.SeekableInputStream

import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.Utils

object ParquetFooterReader {

  val SKIP_ROW_GROUPS = true
  val WITH_ROW_GROUPS = false

  /**
   * Build a filter for reading footer of the input Parquet file 'split'.
   * If 'skipRowGroup' is true, this will skip reading the Parquet row group metadata.
   *
   * @param hadoopConf    hadoop configuration of file
   * @param file          a part (i.e. "block") of a single file that should be read
   * @param skipRowGroup  If true, skip reading row groups;
   *                      if false, read row groups according to the file split range
   */
  def buildFilter(
      hadoopConf: Configuration,
      file: PartitionedFile,
      skipRowGroup: Boolean): ParquetMetadataConverter.MetadataFilter = {
    if (skipRowGroup) {
      ParquetMetadataConverter.SKIP_ROW_GROUPS
    } else {
      HadoopReadOptions.builder(hadoopConf, file.toPath)
        .withRange(file.start, file.start + file.length)
        .build()
        .getMetadataFilter
    }
  }

  def readFooter(
      inputFile: HadoopInputFile,
      filter: ParquetMetadataConverter.MetadataFilter): ParquetMetadata = {
    val readOptions = HadoopReadOptions.builder(inputFile.getConfiguration, inputFile.getPath)
      .withMetadataFilter(filter).build()
    Utils.tryWithResource(ParquetFileReader.open(inputFile, readOptions)) { fileReader =>
      fileReader.getFooter
    }
  }

  /**
   * Decoding Parquet files generally involves two steps:
   *  1. read and resolve the metadata (footer),
   *  2. read and decode the row groups/column chunks.
   *
   * It's possible to avoid opening the file twice by resuing the SeekableInputStream.
   * When detachFileInputStream is true, the caller takes responsibility to close the
   * SeekableInputStream. Currently, this is only supported by parquet vectorized reader.
   *
   * @param hadoopConf hadoop configuration of file
   * @param file       a part (i.e. "block") of a single file that should be read
   * @param detachFileInputStream when true, keep the SeekableInputStream of file opened
   * @return if detachFileInputStream is true, return
   *         (Some(HadoopInputFile), Soem(SeekableInputStream), ParquetMetadata),
   *         otherwise, return (None, None, ParquetMetadata).
   */
  def openFileAndReadFooter(
      hadoopConf: Configuration,
      file: PartitionedFile,
      detachFileInputStream: Boolean):
  (Option[HadoopInputFile], Option[SeekableInputStream], ParquetMetadata) = {
    val readOptions = HadoopReadOptions.builder(hadoopConf, file.toPath)
      .withMetadataFilter(buildFilter(hadoopConf, file, !detachFileInputStream))
      .build()
    val inputFile = HadoopInputFile.fromStatus(file.fileStatus, hadoopConf)
    val inputStream = inputFile.newStream()
    Utils.tryWithResource(
      ParquetFileReader.open(inputFile, readOptions, inputStream)) { fileReader =>
      val footer = fileReader.getFooter
      if (detachFileInputStream) {
        fileReader.detachFileInputStream()
        (Some(inputFile), Some(inputStream), footer)
      } else {
        (None, None, footer)
      }
    }
  }
}
