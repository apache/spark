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

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.{Aggregation, Count, FieldReference, LiteralValue, Max, Min}
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StructField, StructType, TimestampType}
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

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown ||
      aggregation.groupByColumns.nonEmpty || pushedParquetFilters.length > 0) {
      return false
    }

    aggregation.aggregateExpressions.foreach {
      case Max(col, _) => col match {
        case ref: FieldReference =>
          dataSchema.fields(dataSchema.fieldNames.toList.indexOf(ref.fieldNames.head))
            .dataType match {
            // not push down nested column and Timestamp (INT96 sort order is undefined, parquet
            // doesn't return statistics for INT96)
            case StructType(_) | ArrayType(_, _) | MapType(_, _, _) | TimestampType => return false
            case _ =>
          }
        case _ =>
          throw new SparkException("Expression $col is not currently supported.")
      }
      case Min(col, _) => col match {
        case ref: FieldReference =>
          dataSchema.fields(dataSchema.fieldNames.toList.indexOf(ref.fieldNames.head))
            .dataType match {
            // not push down nested column and Timestamp (INT96 sort order is undefined, parquet
            // doesn't return statistics for INT96)
            case StructType(_) | ArrayType(_, _) | MapType(_, _, _) | TimestampType => return false
            case _ =>
          }
        case _ =>
          throw new SparkException("Expression $col is not currently supported.")
      }
      // not push down distinct count
      case Count(_, _, false) =>
      case _ => return false
    }
    this.pushedAggregations = aggregation
    true
  }

  override def supportsGlobalAggregatePushDownOnly(): Boolean = true

  override def getPushDownAggSchema: StructType = {
    var schema = new StructType()
    pushedAggregations.aggregateExpressions.foreach {
      case Max(col, _) => col match {
        case ref: FieldReference =>
          val field = dataSchema.fields(dataSchema.fieldNames.toList.indexOf(ref.fieldNames.head))
          schema = schema.add(field.copy("max(" + field.name + ")"))
        case _ =>
          throw new SparkException("Expression $col is not currently supported.")
      }
      case Min(col, _) => col match {
        case ref: FieldReference =>
          val field = dataSchema.fields(dataSchema.fieldNames.toList.indexOf(ref.fieldNames.head))
          schema = schema.add(field.copy("min(" + field.name + ")"))
        case _ =>
          throw new SparkException("Expression $col is not currently supported.")
      }
      case Count(col, _, _) => col match {
        case _: FieldReference =>
          schema = schema.add(StructField("count(" + col + ")", LongType))
        case LiteralValue(1, _) =>
          schema = schema.add(StructField("count(*)", LongType))
        case _ =>
          throw new SparkException("Expression $col is not currently supported.")
      }
      case _ =>
        throw new SparkException("Pushed down aggregate is not supported.")
    }
    schema
  }

  override def build(): Scan = {
    ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
      readPartitionSchema(), pushedParquetFilters, pushedAggregations, getPushDownAggSchema,
      options)
  }
}
