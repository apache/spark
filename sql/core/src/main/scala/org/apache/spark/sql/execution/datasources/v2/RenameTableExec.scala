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
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharScanMode
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.execution.TableCacheDescriptor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

/**
 * Physical plan node for renaming a table.
 */
private[sql] case class RenameTableExec(
    catalog: TableCatalog,
    oldIdent: Identifier,
    newIdent: Identifier,
    invalidateCache: () => Seq[TableCacheDescriptor],
    cacheTable: (SparkSession, LogicalPlan, Option[String], StorageLevel) => Unit)
  extends LeafV2CommandExec with SQLConfHelper {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val oldCaches = invalidateCache()
    catalog.invalidateTable(oldIdent)

    // If new identifier consists of a table name only, the table should be renamed in place.
    // Such behavior matches to the v1 implementation of table renaming in Spark and other DBMSs.
    val qualifiedNewIdent = if (newIdent.namespace.isEmpty) {
      Identifier.of(oldIdent.namespace, newIdent.name)
    } else newIdent
    catalog.renameTable(oldIdent, qualifiedNewIdent)

    oldCaches.foreach { cache =>
      val tbl = catalog.loadTable(qualifiedNewIdent)
      val rewritten = cache.plan.transformUp {
        case relation: DataSourceV2Relation
            if relation.catalog.contains(catalog) && relation.identifier.contains(oldIdent) =>
          val restored = relation.copy(
            table = tbl,
            catalog = Some(catalog),
            identifier = Some(qualifiedNewIdent))
          restored.copyTagsFrom(relation)
          restored.setAnalyzed()
          restored
      }
      withCharVarcharScanModeConf(cache.charVarcharScanMode) {
        cacheTable(
          session,
          rewritten,
          Some(qualifiedNewIdent.quoted), cache.storageLevel)
      }
    }
    Seq.empty
  }

  // Re-cache under the mode that produced the original plan so CHAR/VARCHAR output is legal.
  private def withCharVarcharScanModeConf[T](mode: Option[CharVarcharScanMode])(body: => T): T = {
    mode match {
      case Some(CharVarcharScanMode.SparkStandard) =>
        withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true")(body)
      case Some(CharVarcharScanMode.PreserveNative) =>
        withSQLConf(
          SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true",
          SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "false")(body)
      case None => body
    }
  }
}
