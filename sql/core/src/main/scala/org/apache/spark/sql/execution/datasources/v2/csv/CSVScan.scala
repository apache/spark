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
package org.apache.spark.sql.execution.datasources.v2.csv

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils}
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

case class CSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends TextBasedFileScan(sparkSession, options) {

  val columnPruning = conf.csvColumnPruning
  private def csvOptions(parameters: Map[String, String]): CSVOptions = new CSVOptions(
    parameters,
    columnPruning = columnPruning,
    conf.sessionLocalTimeZone,
    conf.columnNameOfCorruptRecord)

  private lazy val parsedOptions: CSVOptions = csvOptions(options.asScala.toMap)

  override def isSplitable(path: Path): Boolean = {
    CSVDataSource(parsedOptions).isSplitable && super.isSplitable(path)
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    if (!super.isSplitable(path)) {
      super.getFileUnSplittableReason(path)
    } else {
      "the csv datasource is set multiLine mode"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Resolve the corrupt record column once, here on the driver, also checking its type. The
    // options handed to the reader factory carry the resolved name, so every later comparison,
    // including the ones on executors, is exact.
    val rawOptions = options.asScala.toMap
    val resolvedOptions = ExprUtils.withResolvedColumnNameOfCorruptRecord(
      rawOptions, dataSchema, parsedOptions.columnNameOfCorruptRecord,
      conf.columnNameOfCorruptRecordExactMatch)
    val readOptions =
      if (resolvedOptions eq rawOptions) parsedOptions else csvOptions(resolvedOptions)

    if (readDataSchema.length == 1 &&
      readDataSchema.head.name == readOptions.columnNameOfCorruptRecord) {
      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }

    // Don't push any filter which refers to the "virtual" column which cannot present in the input.
    // Such filters will be applied later on the upper layer.
    val actualFilters =
      pushedFilters.filterNot(_.references.contains(readOptions.columnNameOfCorruptRecord))

    val hadoopConf = newReadHadoopConf()
    val broadcastedConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    CSVPartitionReaderFactory(conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, readOptions,
      actualFilters.toImmutableArraySeq)
  }

  override def equals(obj: Any): Boolean = obj match {
    case c: CSVScan => super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      equivalentFilters(pushedFilters, c.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters.toImmutableArraySeq))
  }
}
