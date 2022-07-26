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

package org.apache.spark.sql.hive.execution

import java.util.Locale

import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.hive.client.HiveClientImpl

trait V1WritesHiveUtils {
  def getPartitionSpec(partition: Map[String, Option[String]]): Map[String, String] = {
    partition.map {
      case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }
  }

  def getDynamicPartitionColumns(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan): Seq[Attribute] = {
    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = getPartitionSpec(partition)

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = tableDesc.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw QueryExecutionErrors.requestedPartitionsMismatchTablePartitionsError(table, partition)
    }

    val sessionState = SparkSession.active.sessionState
    val hadoopConf = sessionState.newHadoopConf()

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }

    partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      val attr = query.resolve(name :: Nil, sessionState.analyzer.resolver).getOrElse {
        throw QueryCompilationErrors.cannotResolveAttributeError(
          name, query.output.map(_.name).mkString(", "))
      }.asInstanceOf[Attribute]
      // SPARK-28054: Hive metastore is not case preserving and keeps partition columns
      // with lower cased names. Hive will validate the column names in the partition directories
      // during `loadDynamicPartitions`. Spark needs to write partition directories with lower-cased
      // column names in order to make `loadDynamicPartitions` work.
      attr.withName(name.toLowerCase(Locale.ROOT))
    }
  }

  def getOptionsWithHiveBucketWrite(bucketSpec: Option[BucketSpec]): Map[String, String] = {
    bucketSpec
      .map(_ => Map(BucketingUtils.optionForHiveCompatibleBucketWrite -> "true"))
      .getOrElse(Map.empty)
  }
}
