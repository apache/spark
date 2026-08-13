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
package org.apache.spark.sql.execution.datasources.v2.fwf

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils}
import org.apache.spark.sql.catalyst.fwf.FixedWidthOptions
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.fwf.FixedWidthDataSource
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

case class FixedWidthScan(
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

  private lazy val parsedOptions: FixedWidthOptions = new FixedWidthOptions(
    options.asScala.toMap,
    conf.sessionLocalTimeZone,
    conf.columnNameOfCorruptRecord)

  override def isSplitable(path: Path): Boolean = {
    parsedOptions.skipRows.isEmpty && super.isSplitable(path)
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    if (!super.isSplitable(path)) {
      super.getFileUnSplittableReason(path)
    } else {
      "the fwf datasource has skipRows set"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)
    if (readDataSchema.length == 1 &&
      readDataSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }
    // Don't push any filter which refers to the "virtual" column which cannot present in the
    // input. Such filters will be applied later on the upper layer.
    val actualFilters =
      pushedFilters.filterNot(_.references.contains(parsedOptions.columnNameOfCorruptRecord))

    // The corrupt-record column is virtual -- it is not a field in the file -- so drop it
    // before resolving colspecs. The reader factory still receives the original schemas so
    // FailureSafeParser can fill the column in. Mirrors CSVScan / CSVPartitionReaderFactory.
    val actualDataSchema = StructType(
      dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val actualReadDataSchema = StructType(
      readDataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val colspecs = FixedWidthDataSource.resolveColspecsForRead(parsedOptions, actualDataSchema)
    val readColspecs =
      FixedWidthDataSource.effectiveColspecs(actualDataSchema, actualReadDataSchema, colspecs)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    FixedWidthPartitionReaderFactory(
      conf, broadcastedConf, dataSchema, readDataSchema, readPartitionSchema, readColspecs,
      parsedOptions, actualFilters.toImmutableArraySeq)
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: FixedWidthScan =>
      super.equals(f) && dataSchema == f.dataSchema && options == f.options &&
        equivalentFilters(pushedFilters, f.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters.toImmutableArraySeq))
  }
}
