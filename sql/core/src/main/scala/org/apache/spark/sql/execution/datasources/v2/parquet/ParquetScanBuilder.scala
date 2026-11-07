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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count}
import org.apache.spark.sql.connector.read.{SupportsPushDownAggregates, SupportsPushDownVariantExtractions, VariantExtraction}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetUtils, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.{FileScanBuilder, V2ColumnUtils}
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType, UserDefinedType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

case class ParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownAggregates
    with SupportsPushDownVariantExtractions {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  private var pushedVariantExtractions = Array.empty[VariantExtraction]

  override protected val supportsNestedSchemaPruning: Boolean = true

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    val sqlConf = sparkSession.sessionState.conf
    if (sqlConf.parquetFilterPushDown) {
      val pushDownDate = sqlConf.parquetFilterPushDownDate
      val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
      val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
      val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
      val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
      val isCaseSensitive = sqlConf.caseSensitiveAnalysis
      val readSchema = readDataSchema()
      val parquetSchema =
        new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(readSchema)
      // Shredded-variant predicate pushdown (SPARK-55817) is not wired here: it applies to the
      // DSv1 path only. DSv2 does rewrite variant extractions into `v.`0`` struct accesses, but
      // only in `V2ScanRelationPushDown.buildScanWithPushedVariants`, which runs *after*
      // `pushDownFilters`. So the filters reaching this method are still `variant_get(v, ...)`
      // predicates, which do not translate to a source `Filter` at all -- there is no
      // shredded-variant logical name for ParquetFilters to resolve here, and nothing would be
      // reported convertible even with a variantExtractionSchema. DSv2 reads remain correct (the
      // variant filter is applied post-scan); they just do not get row-group skipping on shredded
      // columns.
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringPredicate,
        pushDownInFilterThreshold,
        isCaseSensitive,
        // The rebase mode doesn't matter here because the filters are used to determine
        // whether they is convertible.
        RebaseSpec(LegacyBehaviorPolicy.CORRECTED),
        readSchema = Some(readSchema))
      parquetFilters.convertibleFilters(dataFilters.toImmutableArraySeq).toArray
    } else {
      Array.empty[Filter]
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    val sqlConf = sparkSession.sessionState.conf
    if (!sqlConf.parquetAggregatePushDown) {
      return false
    }
    // With field ID reading, the reader matches columns by field ID rather than by name (see
    // `ParquetReadSupport.clipParquetGroupFields`), but the footer statistics are looked up by
    // name, so the two could pick different columns.
    if (sqlConf.parquetFieldIdReadEnabled && ParquetUtils.hasFieldIds(dataSchema)) {
      return false
    }
    // The footer only has statistics for leaf columns, and the null count of a leaf of a nested
    // column is not the number of nulls of the column itself: a non-null struct can have a null
    // field, and an empty array or map has no leaf value. So COUNT of a nested column cannot be
    // answered from the footer.
    val countsNestedColumn = aggregation.aggregateExpressions.exists {
      case count: Count => V2ColumnUtils.extractV2Column(count.column).exists { colName =>
        // `colName` is the name of the resolved column, so match it exactly: `getFieldIndex` is
        // case-insensitive and could pick another column that differs only in case.
        schema.fields.find(_.name == colName).exists(f => isNestedType(f.dataType))
      }
      case _ => false
    }
    if (countsNestedColumn) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }

  private def isNestedType(dataType: DataType): Boolean = dataType match {
    case _: StructType | _: ArrayType | _: MapType | _: VariantType => true
    case udt: UserDefinedType[_] => isNestedType(udt.sqlType)
    case _ => false
  }

  // SupportsPushDownVariantExtractions API implementation
  override def supportsDeferCastError(): Boolean = true

  override def pushVariantExtractions(extractions: Array[VariantExtraction]): Array[Boolean] = {
    // Parquet supports variant pushdown for all variant extractions
    pushedVariantExtractions = extractions
    // Return true for all extractions (Parquet can handle all of them)
    Array.fill(extractions.length)(true)
  }

  override def build(): ParquetScan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }
    ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, finalSchema,
      readPartitionSchema(), pushedDataFilters, options, pushedAggregations,
      partitionFilters, dataFilters, pushedVariantExtractions)
  }
}
