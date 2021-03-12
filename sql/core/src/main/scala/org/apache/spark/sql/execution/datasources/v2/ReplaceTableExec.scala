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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

case class ReplaceTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    tableSchema: StructType,
    partitioning: Seq[Transform],
    tableProperties: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit) extends V2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      invalidateCache(catalog, table, ident)
      catalog.dropTable(ident)
    } else if (!orCreate) {
      throw new CannotReplaceMissingTableException(ident)
    }
    catalog.createTable(ident, tableSchema, partitioning.toArray, tableProperties.asJava)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class AtomicReplaceTableExec(
    catalog: StagingTableCatalog,
    identifier: Identifier,
    tableSchema: StructType,
    partitioning: Seq[Transform],
    tableProperties: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit) extends V2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (catalog.tableExists(identifier)) {
      val table = catalog.loadTable(identifier)
      invalidateCache(catalog, table, identifier)
    }
    val staged = if (orCreate) {
      catalog.stageCreateOrReplace(
        identifier, tableSchema, partitioning.toArray, tableProperties.asJava)
    } else if (catalog.tableExists(identifier)) {
      try {
        catalog.stageReplace(
          identifier, tableSchema, partitioning.toArray, tableProperties.asJava)
      } catch {
        case e: NoSuchTableException =>
          throw new CannotReplaceMissingTableException(identifier, Some(e))
      }
    } else {
      throw new CannotReplaceMissingTableException(identifier)
    }
    commitOrAbortStagedChanges(staged)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty

  private def commitOrAbortStagedChanges(staged: StagedTable): Unit = {
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      staged.commitStagedChanges()
    })(catchBlock = {
      staged.abortStagedChanges()
    })
  }
}
