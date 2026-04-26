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
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for dropping a table.
 *
 * For plain DROP TABLE, calls `dropTable` directly and inspects its return value; this saves
 * the upfront `tableExists` probe (1 RPC on the happy path). For DROP TABLE ... PURGE, keeps
 * the upfront `tableExists` probe so `IF EXISTS` over a missing table is a clean no-op even
 * on catalogs whose `purgeTable` is the default impl that throws unconditionally.
 *
 * On a `dropTable` returning false, falls back to `viewExists` for catalogs that also
 * implement [[ViewCatalog]] -- distinguishes "wrong type" from "missing" so a
 * `DROP TABLE someView` on a mixed catalog surfaces the dedicated `EXPECT_TABLE_NOT_VIEW`
 * error rather than a generic "table not found", matching the v1
 * `DropTableCommand(isView = false)` behavior.
 */
case class DropTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    ifExists: Boolean,
    purge: Boolean,
    invalidateCache: () => Unit) extends LeafV2CommandExec {

  override def run(): Seq[InternalRow] = {
    val dropped = if (purge) {
      // Guard `purgeTable` behind `tableExists` so the default impl (which throws
      // UNSUPPORTED_FEATURE.PURGE_TABLE unconditionally) doesn't fire for `IF EXISTS` over a
      // missing table; the IF EXISTS contract should suppress "missing" cleanly here.
      if (catalog.tableExists(ident)) catalog.purgeTable(ident) else false
    } else {
      catalog.dropTable(ident)
    }
    if (dropped) {
      invalidateCache()
    } else {
      val nameParts =
        (catalog.name() +: ident.namespace() :+ ident.name()).toImmutableArraySeq
      catalog match {
        case vc: ViewCatalog if vc.viewExists(ident) =>
          throw QueryCompilationErrors.expectTableNotViewError(
            nameParts, cmd = "DROP TABLE", suggestAlternative = false, t = this)
        case _ if !ifExists =>
          throw QueryCompilationErrors.noSuchTableError(nameParts)
        case _ =>
        // IF EXISTS: no-op.
      }
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
