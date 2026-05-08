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

import scala.collection.mutable.ArrayBuffer

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog, TableViewCatalog}
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for `SHOW TABLES AS JSON` and `SHOW TABLE EXTENDED AS JSON`.
 *
 * For a [[TableViewCatalog]] (non-extended only), listing is done via
 * [[TableViewCatalog#listTableAndViewSummaries]] so that views appear alongside tables,
 * matching the v1 `SHOW TABLES` semantics.
 */
case class ShowTablesJsonExec(
    output: Seq[Attribute],
    catalog: TableCatalog,
    namespace: Seq[String],
    pattern: String,
    isExtended: Boolean) extends V2CommandExec with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    val identifiers: Array[Identifier] = if (!isExtended) {
      catalog match {
        case mc: TableViewCatalog =>
          mc.listTableAndViewSummaries(namespace.toArray).map(_.identifier())
        case _ => catalog.listTables(namespace.toArray)
      }
    } else {
      catalog.listTables(namespace.toArray)
    }

    val filteredIdents = identifiers.filter { ident =>
      StringUtils.filterPattern(Seq(ident.name()), pattern).nonEmpty
    }

    val jsonRows = new ArrayBuffer[JObject]()
    filteredIdents.foreach { ident =>
      jsonRows += toJsonEntry(ident.name(), ident.namespace(), isTempView(ident))
    }

    // For non-session V2 catalogs that don't surface temp views via listTables() or
    // listTableAndViewSummaries(), fetch them separately. For V2SessionCatalog,
    // listTables() already includes local temp views, so we skip this to avoid duplicates.
    // For TableViewCatalog (non-extended path), views come from listTableAndViewSummaries().
    if (!CatalogV2Util.isSessionCatalog(catalog) &&
        (isExtended || !catalog.isInstanceOf[TableViewCatalog])) {
      val sessionCatalog = session.sessionState.catalog
      val db = namespace match {
        case Seq(db) => db
        case _ => ""
      }
      sessionCatalog.listTempViews(db, pattern).foreach { tempView =>
        jsonRows += toJsonEntry(
          tempView.identifier.table,
          tempView.identifier.database.toArray,
          isTemporary = true)
      }
    }

    val jsonOutput = JObject("tables" -> JArray(jsonRows.toList))
    Seq(toCatalystRow(compact(render(jsonOutput))))
  }

  private def toJsonEntry(
      name: String,
      namespace: Array[String],
      isTemporary: Boolean): JObject = {
    val nsArray = JArray(namespace.map(JString(_)).toList)
    if (isExtended) {
      JObject(
        "name" -> JString(name),
        "catalog" -> JString(catalog.name()),
        "namespace" -> nsArray,
        "type" -> JString(if (isTemporary) "VIEW" else "TABLE"),
        "isTemporary" -> JBool(isTemporary)
      )
    } else {
      JObject(
        "name" -> JString(name),
        "namespace" -> nsArray,
        "isTemporary" -> JBool(isTemporary)
      )
    }
  }

  private def isTempView(ident: Identifier): Boolean = {
    if (CatalogV2Util.isSessionCatalog(catalog)) {
      session.sessionState.catalog.isTempView((ident.namespace() :+ ident.name()).toSeq)
    } else false
  }
}
