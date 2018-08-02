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
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.util.Utils

object ParquetUtils {
  /**
   * Reads the meta data block in the footer of the file
   *
   * @param configuration a hadoop configuration
   * @param file the Parquet File path
   * @param filter the filter to apply to row groups
   * @return the metadata blocks in the footer
   * @note [[ParquetFileReader]]'s readFooter was deprecated in PARQUET-1142.
   */
  def readFooter(
      configuration: Configuration,
      file: Path,
      filter: ParquetMetadataConverter.MetadataFilter): ParquetMetadata = {
    Utils.tryWithResource(createParquetReader(configuration, file, filter))(_.getFooter)
  }

  /**
   * Return a Parquet file reader. This is usually used to retrieve metadata from a Parquet file.
   *
   * @param configuration a hadoop configuration
   * @param file the Parquet File path
   * @param filter the filter to apply to row groups
   * @return the Parquet file reader
   */
  def createParquetReader(
      configuration: Configuration,
      file: Path,
      filter: ParquetMetadataConverter.MetadataFilter): ParquetFileReader = {
    ParquetFileReader.open(
      HadoopInputFile.fromPath(file, configuration),
      HadoopReadOptions.builder(configuration).withMetadataFilter(filter).build())
  }
}
