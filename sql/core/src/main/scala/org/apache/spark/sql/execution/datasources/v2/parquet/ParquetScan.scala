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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, SupportsPushDownVariants, VariantAccessInfo}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

case class ParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    pushedAggregate: Option[Aggregation] = None,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    pushedVariantAccessInfo: Array[VariantAccessInfo] = Array.empty) extends FileScan
    with SupportsPushDownVariants {
  override def isSplitable(path: Path): Boolean = {
    // If aggregate is pushed down, only the file footer will be read once,
    // so file should not be split across multiple tasks.
    pushedAggregate.isEmpty
  }

  // Build transformed schema if variant pushdown is active
  private def effectiveReadDataSchema: StructType = {
    if (_pushedVariantAccess.isEmpty) {
      readDataSchema
    } else {
      // Build a mapping from column name to extracted schema
      val variantSchemaMap = _pushedVariantAccess.map(info =>
        info.columnName() -> info.extractedSchema()).toMap

      // Transform the read data schema by replacing variant columns with their extracted schemas
      StructType(readDataSchema.map { field =>
        variantSchemaMap.get(field.name) match {
          case Some(extractedSchema) => field.copy(dataType = extractedSchema)
          case None => field
        }
      })
    }
  }

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `ParquetScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) {
      effectiveReadDataSchema
    } else {
      // super.readSchema() combines readDataSchema + readPartitionSchema
      // Apply variant transformation if variant pushdown is active
      val baseSchema = super.readSchema()
      if (_pushedVariantAccess.isEmpty) {
        baseSchema
      } else {
        val variantSchemaMap = _pushedVariantAccess.map(info =>
          info.columnName() -> info.extractedSchema()).toMap
        StructType(baseSchema.map { field =>
          variantSchemaMap.get(field.name) match {
            case Some(extractedSchema) => field.copy(dataType = extractedSchema)
            case None => field
          }
        })
      }
    }
  }

  // SupportsPushDownVariants API implementation
  private var _pushedVariantAccess: Array[VariantAccessInfo] = pushedVariantAccessInfo

  override def pushVariantAccess(variantAccessInfo: Array[VariantAccessInfo]): Boolean = {
    // Parquet supports variant pushdown for all variant accesses
    if (variantAccessInfo.nonEmpty) {
      _pushedVariantAccess = variantAccessInfo
      true
    } else {
      false
    }
  }

  override def pushedVariantAccess(): Array[VariantAccessInfo] = {
    _pushedVariantAccess
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val effectiveSchema = effectiveReadDataSchema
    val readDataSchemaAsJson = effectiveSchema.json
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      conf.legacyParquetNanosAsLong)

    val broadcastedConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    ParquetPartitionReaderFactory(
      conf,
      broadcastedConf,
      dataSchema,
      effectiveSchema,
      readPartitionSchema,
      pushedFilters,
      pushedAggregate,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, conf))
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: ParquetScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
      }
      val pushedVariantEqual =
        java.util.Arrays.equals(_pushedVariantAccess.asInstanceOf[Array[Object]],
          p._pushedVariantAccess.asInstanceOf[Array[Object]])
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && pushedDownAggEqual &&
        pushedVariantEqual
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions.toImmutableArraySeq),
      seqToString(pushedAggregate.get.groupByExpressions.toImmutableArraySeq))
  } else {
    ("[]", "[]")
  }

  override def getMetaData(): Map[String, String] = {
    val variantAccessStr = if (_pushedVariantAccess.nonEmpty) {
      _pushedVariantAccess.map(info =>
        s"${info.columnName()}->${info.extractedSchema()}").mkString("[", ", ", "]")
    } else {
      "[]"
    }
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters.toImmutableArraySeq)) ++
      Map("PushedAggregation" -> pushedAggregationsStr) ++
      Map("PushedGroupBy" -> pushedGroupByStr) ++
      Map("PushedVariantAccess" -> variantAccessStr)
  }
}
