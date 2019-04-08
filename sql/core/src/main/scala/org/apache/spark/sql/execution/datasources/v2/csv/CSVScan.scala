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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.sources.v2.reader.PartitionReaderFactory
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class CSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends TextBasedFileScan(sparkSession, fileIndex, readDataSchema, readPartitionSchema, options) {

  private lazy val parsedOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def isSplitable(path: Path): Boolean = {
    CSVDataSource(parsedOptions).isSplitable && super.isSplitable(path)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)

    if (readDataSchema.length == 1 &&
      readDataSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw new AnalysisException(
        "Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the\n" +
          "referenced columns only include the internal corrupt record column\n" +
          "(named _corrupt_record by default). For example:\n" +
          "spark.read.schema(schema).csv(file).filter($\"_corrupt_record\".isNotNull).count()\n" +
          "and spark.read.schema(schema).csv(file).select(\"_corrupt_record\").show().\n" +
          "Instead, you can cache or save the parsed results and then send the same query.\n" +
          "For example, val df = spark.read.schema(schema).csv(file).cache() and then\n" +
          "df.filter($\"_corrupt_record\".isNotNull).count()."
      )
    }

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    CSVPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions)
  }
}
