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

import scala.collection.mutable.ArrayBuffer

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableCatalog, TableViewCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

/**
 * The command for `SHOW TABLES AS JSON` and `SHOW TABLE EXTENDED AS JSON`.
 */
case class ShowTablesJsonCommand(
    child: LogicalPlan,
    pattern: Option[String],
    isExtended: Boolean,
    override val output: Seq[Attribute] = Seq(
      AttributeReference(
        "json_metadata",
        StringType,
        nullable = false,
        new MetadataBuilder()
          .putString("comment", "JSON metadata of the tables")
          .build())()
    )) extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val jsonOutput = child match {
      case ResolvedNamespace(catalog, ns, _) =>
        if (CatalogV2Util.isSessionCatalog(catalog)) {
          runForSessionCatalog(sparkSession, ns)
        } else {
          runForV2Catalog(sparkSession, catalog.asTableCatalog, ns)
        }
      case other =>
        throw SparkException.internalError(
          s"Unexpected child in ShowTablesJsonCommand: ${other.getClass.getSimpleName}")
    }
    Seq(Row(compact(render(jsonOutput))))
  }

  private def runForSessionCatalog(
      sparkSession: SparkSession,
      ns: Seq[String]): JObject = {
    val sessionCatalog = sparkSession.sessionState.catalog
    val db = ns.headOption.getOrElse(sessionCatalog.getCurrentDatabase)
    val tables = pattern
      .map(p => sessionCatalog.listTables(db, p))
      .getOrElse(sessionCatalog.listTables(db))

    val jsonTables = tables.map { tableIdent =>
      val isTemp = sessionCatalog.isTempView(tableIdent)
      val namespace = tableIdent.database.toList

      if (isExtended) {
        val tableType = if (isTemp) {
          "VIEW"
        } else {
          sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdent).tableType.name
        }
        JObject(
          "name" -> JString(tableIdent.table),
          "catalog" -> JString(
            sparkSession.sessionState.catalogManager.v2SessionCatalog.name()),
          "namespace" -> JArray(namespace.map(JString(_))),
          "type" -> JString(tableType),
          "isTemporary" -> JBool(isTemp)
        )
      } else {
        JObject(
          "name" -> JString(tableIdent.table),
          "namespace" -> JArray(namespace.map(JString(_))),
          "isTemporary" -> JBool(isTemp)
        )
      }
    }.toList

    JObject("tables" -> JArray(jsonTables))
  }

  private def runForV2Catalog(
      sparkSession: SparkSession,
      catalog: TableCatalog,
      ns: Seq[String]): JObject = {
    val identifiers = if (!isExtended) {
      catalog match {
        case mc: TableViewCatalog =>
          mc.listTableAndViewSummaries(ns.toArray).map(_.identifier())
        case _ => catalog.listTables(ns.toArray)
      }
    } else {
      catalog.listTables(ns.toArray)
    }

    val pat = pattern.getOrElse("*")
    val filteredIdents = identifiers.filter { ident =>
      StringUtils.filterPattern(Seq(ident.name()), pat).nonEmpty
    }

    val jsonRows = new ArrayBuffer[JObject]()
    filteredIdents.foreach { ident =>
      val nsArray = JArray(ident.namespace().map(JString(_)).toList)
      val entry = if (isExtended) {
        JObject(
          "name" -> JString(ident.name()),
          "catalog" -> JString(catalog.name()),
          "namespace" -> nsArray,
          "type" -> JString("TABLE"),
          "isTemporary" -> JBool(false)
        )
      } else {
        JObject(
          "name" -> JString(ident.name()),
          "namespace" -> nsArray,
          "isTemporary" -> JBool(false)
        )
      }
      jsonRows += entry
    }

    JObject("tables" -> JArray(jsonRows.toList))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}
