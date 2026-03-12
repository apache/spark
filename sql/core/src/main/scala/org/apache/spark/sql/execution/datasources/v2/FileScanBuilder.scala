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
package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.sql.{sources, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils, PartitioningAwareFileIndex, PartitioningUtils}
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

abstract class FileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType)
  extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownCatalystFilters {
  private val partitionSchema = fileIndex.partitionSchema
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  protected val supportsNestedSchemaPruning = false
  protected var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)
  protected var partitionFilters = Seq.empty[Expression]
  protected var dataFilters = Seq.empty[Expression]
  protected var pushedDataFilters = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // [SPARK-30107] While `requiredSchema` might have pruned nested columns,
    // the actual data schema of this scan is determined in `readDataSchema`.
    // File formats that don't support nested schema pruning,
    // use `requiredSchema` as a reference and prune only top-level columns.
    this.requiredSchema = requiredSchema
  }

  protected def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val schema = if (supportsNestedSchemaPruning) requiredSchema else dataSchema
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }

  def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }

  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    val (deterministicFilters, nonDeterminsticFilters) = filters.partition(_.deterministic)
    val (partitionFilters, dataFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, deterministicFilters)
    this.partitionFilters = partitionFilters.filter { f =>
      // Python UDFs might exist because this rule is applied before ``ExtractPythonUDFs``.
      !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])
    }
    this.dataFilters = dataFilters
    val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
    for (filterExpr <- dataFilters) {
      val translated = DataSourceStrategy.translateFilter(filterExpr, true)
      if (translated.nonEmpty) {
        translatedFilters += translated.get
      }
    }
    pushedDataFilters = pushDataFilters(translatedFilters.toArray)
    dataFilters ++ nonDeterminsticFilters
  }

  override def pushedFilters: Array[Predicate] = pushedDataFilters.map(_.toV2)

  /*
   * Push down data filters to the file source, so the data filters can be evaluated there to
   * reduce the size of the data to be read. By default, data filters are not pushed down.
   * File source needs to implement this method to push down data filters.
   */
  protected def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = Array.empty[Filter]

  private def createRequiredNameSet(): Set[String] =
    requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
}
