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
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for showing tables.
 */
case class ShowTablesExec(
    output: Seq[Attribute],
    catalog: TableCatalog,
    namespace: Seq[String],
    pattern: Option[String],
    asJson: Boolean = false) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val tables = catalog.listTables(namespace.toArray)
    val filteredTables = tables.filter { table =>
      pattern.map(StringUtils.filterPattern(Seq(table.name()), _).nonEmpty).getOrElse(true)
    }

    if (asJson) {
      val jsonTables = filteredTables.map { table =>
        JObject(
          "name" -> JString(table.name()),
          "namespace" -> JArray(table.namespace().map(JString(_)).toList),
          "isTemporary" -> JBool(isTempView(table, catalog))
        )
      }.toList

      val jsonOutput = JObject("tables" -> JArray(jsonTables))
      Seq(toCatalystRow(compact(render(jsonOutput))))
    } else {
      val rows = new ArrayBuffer[InternalRow]()
      filteredTables.foreach { table =>
        rows += toCatalystRow(table.namespace().quoted, table.name(), isTempView(table, catalog))
      }
      rows.toSeq
    }
  }

  private def isTempView(ident: Identifier, catalog: TableCatalog): Boolean = {
    if (CatalogV2Util.isSessionCatalog(catalog)) {
      session.sessionState.catalog.isTempView((ident.namespace() :+ ident.name()).toSeq)
    } else false
  }
}
