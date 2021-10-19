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
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, Count, CountStar, Max, Min}
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownAggregates{
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters = {
    val sqlConf = sparkSession.sessionState.conf
    if (sqlConf.parquetFilterPushDown) {
      val pushDownDate = sqlConf.parquetFilterPushDownDate
      val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
      val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
      val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
      val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
      val isCaseSensitive = sqlConf.caseSensitiveAnalysis
      val parquetSchema =
        new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(readDataSchema())
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringStartWith,
        pushDownInFilterThreshold,
        isCaseSensitive,
        // The rebase mode doesn't matter here because the filters are used to determine
        // whether they is convertible.
        LegacyBehaviorPolicy.CORRECTED)
      parquetFilters.convertibleFilters(pushedDataFilters).toArray
    } else {
      Array.empty[Filter]
    }
  }

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  override protected val supportsNestedSchemaPruning: Boolean = true

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = dataFilters

  // Note: for Parquet, the actual filter push down happens in [[ParquetPartitionReaderFactory]].
  // It requires the Parquet physical schema to determine whether a filter is convertible.
  // All filters that can be converted to Parquet are pushed down.
  override def pushedFilters(): Array[Filter] = pushedParquetFilters

  override def pushAggregation(aggregation: Aggregation): Boolean = {

    def getStructFieldForCol(col: NamedReference): StructField = {
      schema.nameToField(col.fieldNames.head)
    }

    def isPartitionCol(col: NamedReference) = {
      partitionNameSet.contains(col.fieldNames.head)
    }

    def processMinOrMax(agg: AggregateFunc): Boolean = {
      val (column, aggType) = agg match {
        case max: Max => (max.column, "max")
        case min: Min => (min.column, "min")
        case _ =>
          throw new IllegalArgumentException(s"Unexpected type of AggregateFunc ${agg.describe}")
      }

      if (isPartitionCol(column)) {
        // don't push down partition column, footer doesn't have max/min for partition column
        return false
      }
      val structField = getStructFieldForCol(column)

      structField.dataType match {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        case StructType(_) | ArrayType(_, _) | MapType(_, _, _) | TimestampType =>
          false
        case _ =>
          finalSchema = finalSchema.add(structField.copy(s"$aggType(" + structField.name + ")"))
          true
      }
    }

    if (!sparkSession.sessionState.conf.parquetAggregatePushDown ||
      aggregation.groupByColumns.nonEmpty || dataFilters.length > 0) {
      // Parquet footer has max/min/count for columns
      // e.g. SELECT COUNT(col1) FROM t
      // but footer doesn't have max/min/count for a column if max/min/count
      // are combined with filter or group by
      // e.g. SELECT COUNT(col1) FROM t WHERE col2 = 8
      //      SELECT COUNT(col1) FROM t GROUP BY col2
      // Todo: 1. add support if groupby column is partition col
      //          (https://issues.apache.org/jira/browse/SPARK-36646)
      //       2. add support if filter col is partition col
      //          (https://issues.apache.org/jira/browse/SPARK-36647)
      return false
    }

    aggregation.groupByColumns.foreach { col =>
      if (col.fieldNames.length != 1) return false
      finalSchema = finalSchema.add(getStructFieldForCol(col))
    }

    aggregation.aggregateExpressions.foreach {
      case max: Max =>
        if (!processMinOrMax(max)) return false
      case min: Min =>
        if (!processMinOrMax(min)) return false
      case count: Count =>
        if (count.column.fieldNames.length != 1 || count.isDistinct) return false
        finalSchema =
          finalSchema.add(StructField(s"count(" + count.column.fieldNames.head + ")", LongType))
      case _: CountStar =>
        finalSchema = finalSchema.add(StructField("count(*)", LongType))
      case _ =>
        return false
    }
    this.pushedAggregations = Some(aggregation)
    true
  }

  override def build(): Scan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) finalSchema = readDataSchema()
    ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, finalSchema,
      readPartitionSchema(), pushedParquetFilters, options, pushedAggregations,
      partitionFilters, dataFilters)
  }
}
