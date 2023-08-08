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

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter.{range, NO_FILTER}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ParquetMetadata}

object ParquetFileMetaUtils {

  @throws[IOException]
  def readFooterByRange(metaCacheEnabled: Boolean, configuration: Configuration,
                        file: Path, start: Long, end: Long): ParquetMetadata = {
    if (metaCacheEnabled) {
      val cachedFooter = ParquetFileMeta.readFooterFromCache(file, configuration)
      val filteredBlocks: util.List[BlockMetaData] = new util.ArrayList[BlockMetaData]
      val blocks: util.List[BlockMetaData] = cachedFooter.getBlocks
      for (block <- blocks.asScala) {
        val offset: Long = block.getStartingPos
        if (offset >= start && offset < end) filteredBlocks.add(block)
      }
      new ParquetMetadata(cachedFooter.getFileMetaData, filteredBlocks)
    }
    else {
      ParquetFooterReader.readFooter(configuration, file, range(start, end))
    }
  }

  @throws[IOException]
  def readFooterByNoFilter(metaCacheEnabled: Boolean, configuration: Configuration,
                                   file: Path): ParquetMetadata = {
    if (metaCacheEnabled) {
      ParquetFileMeta.readFooterFromCache(file, configuration)
    }
    else {
      ParquetFooterReader.readFooter(configuration, file, NO_FILTER)
    }
  }
}
