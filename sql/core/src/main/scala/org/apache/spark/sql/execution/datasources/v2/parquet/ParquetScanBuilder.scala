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

package org.apache.spark.sql.execution.datasources.v2.parquet

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.{Aggregation, Count, Filter, Max, Min}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownFilters
    with SupportsPushDownAggregates{
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters = {
    val sqlConf = sparkSession.sessionState.conf
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetSchema =
      new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(readDataSchema())
    val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
    parquetFilters.convertibleFilters(this.filters).toArray
  }

  override protected val supportsNestedSchemaPruning: Boolean = true

  private var filters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    this.filters
  }

  // Note: for Parquet, the actual filter push down happens in [[ParquetPartitionReaderFactory]].
  // It requires the Parquet physical schema to determine whether a filter is convertible.
  // All filters that can be converted to Parquet are pushed down.
  override def pushedFilters(): Array[Filter] = pushedParquetFilters

  private var pushedAggregations = Aggregation.empty

  override def pushAggregation(aggregation: Aggregation): Unit = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown ||
      aggregation.groupByColumns.nonEmpty) {
      Aggregation.empty
      return
    }

    aggregation.aggregateExpressions.foreach { _ match {
        // parquet's statistics doesn't have distinct count info
        case Seq(Max(_, _)) | Seq(Min(_, _)) | Seq(Count(_, _, false)) =>
        case _ => Aggregation.empty
      }
    }
    this.pushedAggregations = aggregation
  }

  override def pushedAggregation(): Aggregation = pushedAggregations

  override def supportsGlobalAggregatePushDownOnly(): Boolean = true

  override def supportsPushDownAggregateWithFilter(): Boolean = false

  override def getPushDownAggSchema: StructType = {
    var schema = new StructType()
    pushedAggregations.aggregateExpressions.map {
      case Seq(Max(col, _)) =>
        val field = dataSchema.fields(dataSchema.fieldNames.toList.indexOf(col))
        schema = schema.add(field.copy("max(" + field.name + ")"))
      case Seq(Min(col, _)) =>
        val field = dataSchema.fields(dataSchema.fieldNames.toList.indexOf(col))
        schema = schema.add(field.copy("min(" + field.name + ")"))
      case Seq(Count(col, _, _)) =>
        if (col.equals("1")) {
          schema = schema.add(new StructField("count(*)", LongType))
        } else {
          schema = schema.add(new StructField("count(" + col + ")", LongType))
        }
      case _ =>
    }
    schema
  }

  override def build(): Scan = {
    ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
      readPartitionSchema(), pushedParquetFilters, pushedAggregations, getPushDownAggSchema,
      options)
  }
}
