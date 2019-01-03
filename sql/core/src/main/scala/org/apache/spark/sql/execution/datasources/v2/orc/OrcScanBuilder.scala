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
package org.apache.spark.sql.execution.datasources.v2.orc

import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.orc.OrcFilters
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.Scan
import org.apache.spark.sql.types.StructType

case class OrcScanBuilder(
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType) extends FileScanBuilder(fileIndex, schema) {
  override def build(): Scan = OrcScan(fileIndex, schema, dataSchema, readSchema)

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (fileIndex.getSparkSession.sessionState.conf.orcFilterPushDown) {
      OrcFilters.createFilter(schema, filters).foreach { f =>
        OrcInputFormat.setSearchArgument(fileIndex.hadoopConf, f, schema.fieldNames)
      }
      _pushedFilters = OrcFilters.convertibleFilters(schema, filters).toArray
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters
}
