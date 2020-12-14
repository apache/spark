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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

case class UncacheTableCommand(
    multipartIdentifier: Seq[String],
    ifExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = multipartIdentifier.quoted
    table(sparkSession, tableName).foreach { table =>
      val cascade = !sparkSession.sessionState.catalog.isTempView(multipartIdentifier)
      sparkSession.sharedState.cacheManager.uncacheQuery(table, cascade)
    }
    Seq.empty[Row]
  }

  private def table(sparkSession: SparkSession, name: String): Option[DataFrame] = {
    try {
      Some(sparkSession.table(name))
    } catch {
      case ex: AnalysisException if ifExists && ex.getMessage.contains("Table or view not found") =>
        None
    }
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
