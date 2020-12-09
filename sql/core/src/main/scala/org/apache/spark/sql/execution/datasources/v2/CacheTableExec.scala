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

import java.util.Locale

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.storage.StorageLevel

trait BaseCacheTableExec extends V2CommandExec {
  def relationName: String
  def dataFrameToCache: DataFrame
  def isLazy: Boolean
  def options: Map[String, String]

  override def run(): Seq[InternalRow] = {
    val storageLevelKey = "storagelevel"
    val storageLevelValue =
      CaseInsensitiveMap(options).get(storageLevelKey).map(_.toUpperCase(Locale.ROOT))
    val withoutStorageLevel = options.filterKeys(_.toLowerCase(Locale.ROOT) != storageLevelKey)
    if (withoutStorageLevel.nonEmpty) {
      logWarning(s"Invalid options: ${withoutStorageLevel.mkString(", ")}")
    }

    val sparkSession = sqlContext.sparkSession
    val df = dataFrameToCache
    if (storageLevelValue.nonEmpty) {
      sparkSession.sharedState.cacheManager.cacheQuery(
        df,
        Some(relationName),
        StorageLevel.fromString(storageLevelValue.get))
    } else {
      sparkSession.sharedState.cacheManager.cacheQuery(df, Some(relationName))
    }

    if (!isLazy) {
      // Performs eager caching
      df.count()
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class CacheTableExec(
    relation: LogicalPlan,
    multipartIdentifier: Seq[String],
    override val isLazy: Boolean,
    override val options: Map[String, String]) extends BaseCacheTableExec {
  override def relationName: String = multipartIdentifier.quoted

  override def dataFrameToCache: DataFrame = Dataset.ofRows(sqlContext.sparkSession, relation)
}

case class CacheTableAsSelectExec(
    tempViewName: String,
    query: LogicalPlan,
    override val isLazy: Boolean,
    override val options: Map[String, String]) extends BaseCacheTableExec {
  override def relationName: String = tempViewName

  override def dataFrameToCache: DataFrame = {
    val sparkSession = sqlContext.sparkSession
    Dataset.ofRows(sparkSession, query).createTempView(tempViewName)
    sparkSession.table(tempViewName)
  }
}
