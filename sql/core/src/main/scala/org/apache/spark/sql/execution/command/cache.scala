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

package org.apache.spark.sql.execution.command

import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LogicalPlan}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.storage.StorageLevel

case class CacheTableCommand(
    multipartIdentifier: Seq[String],
    plan: Option[LogicalPlan],
    isLazy: Boolean,
    options: Map[String, String]) extends RunnableCommand {
  require(plan.isEmpty || multipartIdentifier.length == 1,
    "Namespace name is not allowed in CACHE TABLE AS SELECT")

  override def innerChildren: Seq[QueryPlan[_]] = plan.toSeq

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = multipartIdentifier.quoted
    plan.foreach { logicalPlan =>
      Dataset.ofRows(sparkSession, logicalPlan).createTempView(tableName)
    }

    val storageLevelKey = "storagelevel"
    val storageLevelValue =
      CaseInsensitiveMap(options).get(storageLevelKey).map(_.toUpperCase(Locale.ROOT))
    val withoutStorageLevel = options.filterKeys(_.toLowerCase(Locale.ROOT) != storageLevelKey)
    if (withoutStorageLevel.nonEmpty) {
      logWarning(s"Invalid options: ${withoutStorageLevel.mkString(", ")}")
    }

    if (storageLevelValue.nonEmpty) {
      sparkSession.catalog.cacheTable(
        tableName, StorageLevel.fromString(storageLevelValue.get))
    } else {
      sparkSession.catalog.cacheTable(tableName)
    }

    if (!isLazy) {
      // Performs eager caching
      sparkSession.table(tableName).count()
    }

    Seq.empty[Row]
  }
}

case class UncacheTableCommand(
    multipartIdentifier: Seq[String],
    ifExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = multipartIdentifier.quoted
    if (!ifExists || tableExists(sparkSession, tableName)) {
      sparkSession.catalog.uncacheTable(tableName)
    }
    Seq.empty[Row]
  }

  private def tableExists(sparkSession: SparkSession, name: String): Boolean = {
    Try(sparkSession.table(name)).isSuccess
  }
}

/**
 * Clear all cached data from the in-memory cache.
 */
case object ClearCacheCommand extends RunnableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.clearCache()
    Seq.empty[Row]
  }
}
