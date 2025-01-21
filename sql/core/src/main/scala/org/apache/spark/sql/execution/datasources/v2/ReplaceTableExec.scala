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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, StagedTable, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.Utils

case class ReplaceTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    columns: Array[Column],
    partitioning: Seq[Transform],
    tableSpec: TableSpec,
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit) extends LeafV2CommandExec {

  val tableProperties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      invalidateCache(catalog, table, ident)
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(ident)
    }
    catalog.createTable(ident, columns, partitioning.toArray, tableProperties.asJava)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class AtomicReplaceTableExec(
    catalog: StagingTableCatalog,
    identifier: Identifier,
    columns: Array[Column],
    partitioning: Seq[Transform],
    tableSpec: TableSpec,
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit) extends LeafV2CommandExec {

  val tableProperties = CatalogV2Util.convertTableProperties(tableSpec)

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(identifier)) {
      val table = catalog.loadTable(identifier)
      invalidateCache(catalog, table, identifier)
    }
    val staged = if (orCreate) {
      catalog.stageCreateOrReplace(
        identifier, columns, partitioning.toArray, tableProperties.asJava)
    } else if (catalog.tableExists(identifier)) {
      try {
        catalog.stageReplace(
          identifier, columns, partitioning.toArray, tableProperties.asJava)
      } catch {
        case e: NoSuchTableException =>
          throw QueryCompilationErrors.cannotReplaceMissingTableError(identifier, Some(e))
      }
    } else {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(identifier)
    }
    commitOrAbortStagedChanges(staged)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty

  private def commitOrAbortStagedChanges(staged: StagedTable): Unit = {
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      DataSourceV2Utils.commitStagedChanges(sparkContext, staged, metrics)
    })(catchBlock = {
      staged.abortStagedChanges()
    })
  }
}
