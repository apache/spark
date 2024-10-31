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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.storage.StorageLevel

/**
 * Physical plan node for renaming a table.
 */
case class RenameTableExec(
    catalog: TableCatalog,
    oldIdent: Identifier,
    newIdent: Identifier,
    invalidateCache: () => Option[StorageLevel],
    cacheTable: (SparkSession, LogicalPlan, Option[String], StorageLevel) => Unit)
  extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper

    val optOldStorageLevel = invalidateCache()
    catalog.invalidateTable(oldIdent)

    // If new identifier consists of a table name only, the table should be renamed in place.
    // Such behavior matches to the v1 implementation of table renaming in Spark and other DBMSs.
    val qualifiedNewIdent = if (newIdent.namespace.isEmpty) {
      Identifier.of(oldIdent.namespace, newIdent.name)
    } else newIdent
    catalog.renameTable(oldIdent, qualifiedNewIdent)

    optOldStorageLevel.foreach { oldStorageLevel =>
      val tbl = catalog.loadTable(qualifiedNewIdent)
      val newRelation = DataSourceV2Relation.create(tbl, Some(catalog), Some(qualifiedNewIdent))
      cacheTable(
        session,
        newRelation,
        Some(qualifiedNewIdent.quoted), oldStorageLevel)
    }
    Seq.empty
  }
}
