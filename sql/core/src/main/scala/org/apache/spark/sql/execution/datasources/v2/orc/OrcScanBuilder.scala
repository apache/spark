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

import scala.collection.JavaConverters._

import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.orc.OrcFilters
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.Scan
import org.apache.spark.sql.types.StructType

case class OrcScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: DataSourceOptions) extends FileScanBuilder(schema) {
  lazy val hadoopConf =
    sparkSession.sessionState.newHadoopConfWithOptions(options.asMap().asScala.toMap)

  override def build(): Scan = {
    OrcScan(sparkSession, hadoopConf, fileIndex, schema, dataSchema, readSchema)
  }

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (sparkSession.sessionState.conf.orcFilterPushDown) {
      OrcFilters.createFilter(schema, filters).foreach { f =>
        OrcInputFormat.setSearchArgument(hadoopConf, f, schema.fieldNames)
      }
      val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap
      _pushedFilters = OrcFilters.convertibleFilters(schema, dataTypeMap, filters).toArray
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters
}
