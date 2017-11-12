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

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class CacheTableCommand(
    tableIdent: TableIdentifier,
    plan: Option[LogicalPlan],
    isLazy: Boolean) extends RunnableCommand {
  require(plan.isEmpty || tableIdent.database.isEmpty,
    "Database name is not allowed in CACHE TABLE AS SELECT")

  override protected def innerChildren: Seq[QueryPlan[_]] = {
    plan.toSeq
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    plan.foreach { logicalPlan =>
      Dataset.ofRows(sparkSession, logicalPlan).createTempView(tableIdent.quotedString)
    }
    sparkSession.catalog.cacheTable(tableIdent.quotedString)

    if (!isLazy) {
      // Performs eager caching
      sparkSession.table(tableIdent).count()
    }

    Seq.empty[Row]
  }
}


case class UncacheTableCommand(
    tableIdent: TableIdentifier,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableId = tableIdent.quotedString
    if (!ifExists || sparkSession.catalog.tableExists(tableId)) {
      sparkSession.catalog.uncacheTable(tableId)
    }
    Seq.empty[Row]
  }
}

/**
 * Clear all cached data from the in-memory cache.
 */
case object ClearCacheCommand extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.clearCache()
    Seq.empty[Row]
  }
}
