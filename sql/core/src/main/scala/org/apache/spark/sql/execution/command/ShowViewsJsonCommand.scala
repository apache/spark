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

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, ViewCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

/**
 * The command for `SHOW VIEWS AS JSON`.
 *
 * The output is a single row with a `json_metadata` column holding a JSON object of the form:
 * {{{
 *   {"views":[{"namespace":["db"],"viewName":"v1","isTemporary":false}]}
 * }}}
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW VIEWS [(FROM | IN) namespace] [[LIKE] pattern] AS JSON;
 * }}}
 */
case class ShowViewsJsonCommand(
    child: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = Seq(
      AttributeReference(
        "json_metadata",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "JSON metadata of the views").build())()))
    extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val views = child match {
      // A catalog that implements `ViewCatalog` owns its views, even when it overrides the
      // session catalog. This mirrors the routing between `ResolveSessionCatalog` and
      // `DataSourceV2Strategy` for the non-JSON `SHOW VIEWS`.
      case ResolvedNamespace(catalog: ViewCatalog, ns, _) =>
        listViewCatalogViews(catalog, ns)
      case ResolvedNamespace(catalog, ns, _) if CatalogV2Util.isSessionCatalog(catalog) =>
        listSessionCatalogViews(sparkSession, ns)
      case ResolvedNamespace(catalog, _, _) =>
        throw QueryCompilationErrors.missingCatalogViewsAbilityError(catalog)
      case other =>
        throw SparkException.internalError(
          s"Unexpected child in ShowViewsJsonCommand: ${other.getClass.getSimpleName}")
    }
    Seq(Row(compact(render(JObject("views" -> JArray(views.toList))))))
  }

  private def listSessionCatalogViews(
      sparkSession: SparkSession,
      ns: Seq[String]): Seq[JObject] = {
    val catalog = sparkSession.sessionState.catalog
    val db = databaseInSessionCatalog(ns)
    catalog.listViews(db, pattern.getOrElse("*")).map { ident =>
      toJson(ident.database.toSeq, ident.table, catalog.isTempView(ident))
    }
  }

  private def listViewCatalogViews(catalog: ViewCatalog, ns: Seq[String]): Seq[JObject] = {
    catalog.listViews(ns.toArray).filter { ident =>
      pattern.forall(p => StringUtils.filterPattern(Seq(ident.name), p).nonEmpty)
    }.map { ident =>
      // v2 catalogs have no temp views, matching `ShowViewsExec`.
      toJson(ident.namespace().toSeq, ident.name(), isTemporary = false)
    }.toSeq
  }

  private def toJson(namespace: Seq[String], viewName: String, isTemporary: Boolean): JObject = {
    JObject(
      "namespace" -> JArray(namespace.map(JString(_)).toList),
      "viewName" -> JString(viewName),
      "isTemporary" -> JBool(isTemporary))
  }

  /**
   * Resolves the single-part database name the v1 session catalog requires, rejecting the same
   * namespaces `ResolveSessionCatalog` rejects for the non-JSON `SHOW VIEWS`. `CurrentNamespace`
   * always yields at least the current database, so the empty case below does not fire through
   * the normal parser path; it stays as a guard because `ResolveSessionCatalog` keeps the same
   * guard for the identical check elsewhere.
   */
  private def databaseInSessionCatalog(ns: Seq[String]): String = ns match {
    case Seq() => throw QueryCompilationErrors.databaseFromV1SessionCatalogNotSpecifiedError()
    case Seq(db) => db
    case _ => throw QueryCompilationErrors.nestedDatabaseUnsupportedByV1SessionCatalogError(ns)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}
