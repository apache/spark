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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.orc.OrcFilters
import org.apache.spark.sql.execution.datasources.v2.{BroadcastedHadoopConf, FileScan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OrcScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter])
  extends FileScan(sparkSession, fileIndex, readDataSchema, readPartitionSchema)
  with BroadcastedHadoopConf {

  override def isSplitable(path: Path): Boolean = true

  override def withHadoopConfRewrite(hadoopConf: Configuration): Configuration = {
    OrcFilters.createFilter(schema, pushedFilters).foreach { f =>
      OrcInputFormat.setSearchArgument(hadoopConf, f, dataSchema.fieldNames)
    }
    hadoopConf
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    OrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: OrcScan =>
      fileIndex == o.fileIndex && dataSchema == o.dataSchema &&
      readDataSchema == o.readDataSchema && readPartitionSchema == o.readPartitionSchema &&
      options == o.options && equivalentFilters(pushedFilters, o.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + pushedFilters.mkString("[", ", ", "]")
  }
}
