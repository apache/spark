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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, TableCatalog, ViewInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for DROP VIEW on a v2 `TableCatalog` that declares
 * [[org.apache.spark.sql.connector.catalog.TableCatalogCapability#SUPPORTS_VIEW]]. Loads the
 * target entry once to verify it is a view (a [[MetadataOnlyTable]] wrapping a [[ViewInfo]])
 * before calling [[TableCatalog#dropTable]]. Matching the v1 path's
 * `DropTableCommand(isView = true)` safety net keeps `DROP VIEW some_table` from silently
 * destroying a non-view table on a SUPPORTS_VIEW catalog.
 */
case class DropViewExec(
    catalog: TableCatalog,
    ident: Identifier,
    ifExists: Boolean,
    invalidateCache: () => Unit) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val loaded = try {
      Some(catalog.loadTable(ident))
    } catch {
      case _: NoSuchTableException => None
    }
    val nameParts =
      (catalog.name() +: ident.namespace() :+ ident.name()).toImmutableArraySeq
    loaded match {
      case Some(mot: MetadataOnlyTable) if mot.getTableInfo.isInstanceOf[ViewInfo] =>
        invalidateCache()
        catalog.dropTable(ident)
      case Some(_) =>
        throw QueryCompilationErrors.expectViewNotTableError(
          nameParts, cmd = "DROP VIEW", suggestAlternative = false, t = this)
      case None if !ifExists =>
        throw QueryCompilationErrors.noSuchTableError(nameParts)
      case None =>
      // IF EXISTS: no-op.
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
