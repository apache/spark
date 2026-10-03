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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.{CatalogExtension, Identifier, RelationCatalog, V1View, ViewCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for SHOW VIEWS on a v2 [[ViewCatalog]]. Enumerates view identifiers via
 * [[ViewCatalog#listViews]]. When the ViewCatalog is installed as the session catalog, the
 * built-in session catalog contributes temporary views and persistent views that the active
 * catalog positively resolves as delegated V1 views.
 */
case class ShowViewsExec(
    output: Seq[Attribute],
    catalog: ViewCatalog,
    namespace: Seq[String],
    pattern: Option[String],
    v1SessionCatalog: Option[SessionCatalog] = None) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new mutable.ArrayBuffer[InternalRow]()
    val seen = mutable.HashSet.empty[(String, String, Boolean)]
    val delegatesSessionNamespace = catalog match {
      case extension: CatalogExtension => extension.namespaceExists(namespace.toArray)
      case _ => false
    }

    def addView(viewNamespace: String, name: String, isTemporary: Boolean): Unit = {
      if (seen.add((viewNamespace, name, isTemporary))) {
        rows += toCatalystRow(viewNamespace, name, isTemporary)
      }
    }

    def isDelegatedV1View(ident: org.apache.spark.sql.catalyst.TableIdentifier): Boolean = {
      delegatesSessionNamespace && (catalog match {
        case relationCatalog: RelationCatalog =>
          try {
            relationCatalog.loadRelation(
              Identifier.of(ident.database.toArray, ident.table)).isInstanceOf[V1View]
          } catch {
            case _: NoSuchNamespaceException | _: NoSuchTableException => false
          }
        case _ => false
      })
    }

    var v2NamespaceMissing: Option[NoSuchNamespaceException] = None
    try {
      catalog.listViews(namespace.toArray).foreach { ident =>
        val nameMatches =
          pattern.forall(p => StringUtils.filterPattern(Seq(ident.name), p).nonEmpty)
        if (nameMatches) {
          addView(ident.namespace().quoted, ident.name(), isTemporary = false)
        }
      }
    } catch {
      case e: NoSuchNamespaceException => v2NamespaceMissing = Some(e)
    }

    var v1NamespaceAttempted = false
    var v1NamespaceMissing: Option[NoSuchNamespaceException] = None
    v1SessionCatalog.foreach { sessionCatalog =>
      sessionCatalog.listLocalTempViews(pattern.getOrElse("*")).foreach { ident =>
        addView(ident.database.toArray.quoted, ident.table, isTemporary = true)
      }
      if (namespace.length == 1) {
        v1NamespaceAttempted = true
        val database = namespace.head
        try {
          sessionCatalog.listViews(database, pattern.getOrElse("*")).foreach { ident =>
            val isTemporary = sessionCatalog.isTempView(ident)
            if (isTemporary || isDelegatedV1View(ident)) {
              addView(ident.database.toArray.quoted, ident.table, isTemporary)
            }
          }
        } catch {
          case e: NoSuchNamespaceException =>
            v1NamespaceMissing = Some(e)
            // A namespace can exist only in the custom ViewCatalog. Local temporary views still
            // belong to the session and should remain visible for that valid V2 namespace.
            sessionCatalog.listTempViews(database, pattern.getOrElse("*")).foreach { view =>
              val ident = view.identifier
              addView(ident.database.toArray.quoted, ident.table, isTemporary = true)
            }
        }
      }
    }

    if (v2NamespaceMissing.isDefined && (!delegatesSessionNamespace ||
        !v1NamespaceAttempted || v1NamespaceMissing.isDefined)) {
      throw v2NamespaceMissing.get
    }

    rows.toSeq
  }
}
