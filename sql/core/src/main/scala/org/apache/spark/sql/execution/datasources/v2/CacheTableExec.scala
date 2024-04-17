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

import org.apache.spark.internal.LogKey.OPTIONS
import org.apache.spark.internal.MDC
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.LocalTempView
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.storage.StorageLevel

trait BaseCacheTableExec extends LeafV2CommandExec {
  def relationName: String
  def planToCache: LogicalPlan
  def dataFrameForCachedPlan: DataFrame
  def isLazy: Boolean
  def options: Map[String, String]

  override def run(): Seq[InternalRow] = {
    val storageLevelKey = "storagelevel"
    val storageLevel = CaseInsensitiveMap(options).get(storageLevelKey)
      .map(s => StorageLevel.fromString(s.toUpperCase(Locale.ROOT)))
      .getOrElse(conf.defaultCacheStorageLevel)
    val withoutStorageLevel = options
      .filter { case (k, _) => k.toLowerCase(Locale.ROOT) != storageLevelKey }
    if (withoutStorageLevel.nonEmpty) {
      logWarning(log"Invalid options: ${MDC(OPTIONS, withoutStorageLevel.mkString(", "))}")
    }

    session.sharedState.cacheManager.cacheQuery(
      session,
      planToCache,
      Some(relationName),
      storageLevel)

    if (!isLazy) {
      // Performs eager caching.
      dataFrameForCachedPlan.count()
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
  override lazy val relationName: String = multipartIdentifier.quoted

  override lazy val planToCache: LogicalPlan = relation

  override lazy val dataFrameForCachedPlan: DataFrame = {
    Dataset.ofRows(session, planToCache)
  }
}

case class CacheTableAsSelectExec(
    tempViewName: String,
    query: LogicalPlan,
    originalText: String,
    override val isLazy: Boolean,
    override val options: Map[String, String],
    referredTempFunctions: Seq[String]) extends BaseCacheTableExec {
  override lazy val relationName: String = tempViewName

  override lazy val planToCache: LogicalPlan = {
    CreateViewCommand(
      name = TableIdentifier(tempViewName),
      userSpecifiedColumns = Nil,
      comment = None,
      properties = Map.empty,
      originalText = Some(originalText),
      plan = query,
      allowExisting = false,
      replace = false,
      viewType = LocalTempView,
      isAnalyzed = true,
      referredTempFunctions = referredTempFunctions
    ).run(session)

    dataFrameForCachedPlan.logicalPlan
  }

  override lazy val dataFrameForCachedPlan: DataFrame = {
    session.table(tempViewName)
  }
}

case class UncacheTableExec(
    relation: LogicalPlan,
    cascade: Boolean) extends LeafV2CommandExec {
  override def run(): Seq[InternalRow] = {
    session.sharedState.cacheManager.uncacheQuery(session, relation, cascade)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
