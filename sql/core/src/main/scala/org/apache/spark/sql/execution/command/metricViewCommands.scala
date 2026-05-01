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

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{QueryPlanningTracker, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, SchemaUnsupported}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.metricview.util.MetricViewPlanner
import org.apache.spark.sql.types.StructType

case class CreateMetricViewCommand(
    child: LogicalPlan,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: String,
    allowExisting: Boolean,
    replace: Boolean) extends UnaryRunnableCommand {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    child match {
      case v: ResolvedIdentifier if CatalogV2Util.isSessionCatalog(v.catalog) =>
        createMetricViewInSessionCatalog(sparkSession, v)
      case _ => throw SparkException.internalError(
        s"Failed to resolve identifier for creating metric view")
    }
  }

  private def validateUserColumns(name: TableIdentifier, analyzed: LogicalPlan): Unit = {
    if (userSpecifiedColumns.nonEmpty) {
      if (userSpecifiedColumns.length > analyzed.output.length) {
        throw QueryCompilationErrors.cannotCreateViewNotEnoughColumnsError(
          name.nameParts, userSpecifiedColumns.map(_._1), analyzed)
      } else if (userSpecifiedColumns.length < analyzed.output.length) {
        throw QueryCompilationErrors.cannotCreateViewTooManyColumnsError(
          name.nameParts, userSpecifiedColumns.map(_._1), analyzed)
      }
    }
  }

  private def createMetricViewInSessionCatalog(
      sparkSession: SparkSession,
      resolved: ResolvedIdentifier): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val name = resolved.identifier.asTableIdentifier
    val analyzed = MetricViewHelper.analyzeMetricViewText(
      sparkSession, name.nameParts, originalText)
    validateUserColumns(name, analyzed)
    catalog.createTable(
      ViewHelper.prepareTable(
        sparkSession, name, Some(originalText), analyzed, userSpecifiedColumns,
        properties, SchemaUnsupported, comment,
        None, isMetricView = true),
      ignoreIfExists = allowExisting)
    Seq.empty
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

case class AlterMetricViewCommand(child: LogicalPlan, originalText: String)

object MetricViewHelper {

  /**
   * Walks the analyzed plan to collect direct table/view dependencies. Each dependency is
   * returned as a structural multi-part name (`Seq[String]`); arity is preserved per source
   * so consumers can reason about catalog / namespace / table boundaries without parsing a
   * dot-flattened string.
   *
   * Stops recursion at relation leaf nodes and persistent `View` nodes so only direct
   * (not transitive) dependencies are recorded.
   */
  private[execution] def collectTableDependencies(plan: LogicalPlan): Seq[Seq[String]] = {
    val tables = scala.collection.mutable.ArrayBuffer.empty[Seq[String]]
    def traverse(p: LogicalPlan): Unit = p match {
      case v: View if !v.isTempView =>
        tables += v.desc.identifier.nameParts
      case r: DataSourceV2Relation if r.catalog.isDefined && r.identifier.isDefined =>
        val ident = r.identifier.get
        // V2 catalogs may have multi-level namespaces; preserve the full arity rather than
        // dot-joining the namespace into a single component.
        tables += (r.catalog.get.name() +: ident.namespace().toIndexedSeq) :+ ident.name()
      case r: HiveTableRelation =>
        tables += r.tableMeta.identifier.nameParts
      case r: LogicalRelation if r.catalogTable.isDefined =>
        tables += r.catalogTable.get.identifier.nameParts
      case other =>
        other.children.foreach(traverse)
        other.expressions.foreach(_.foreach {
          case s: SubqueryExpression => traverse(s.plan)
          case _ =>
        })
    }
    traverse(plan)
    tables.distinct.toSeq
  }

  /**
   * Analyzes a metric-view YAML body so the create / alter path can capture the source plan
   * and its dependencies.
   *
   * `nameParts` is the multi-part target identifier (catalog + namespace + table). The synthetic
   * [[CatalogTable]] used as analysis context still carries a [[TableIdentifier]] (capped at
   * 3 parts: catalog + database + table); for multi-level v2 namespaces we collapse the
   * intermediate namespace components into the synthetic `database` slot. The synthetic identifier
   * is not used to resolve the view body itself, so this collapse is observationally invisible to
   * the analyzed plan; `verifyTemporaryObjectsNotExists` continues to receive the full
   * `nameParts` so error messages still render the multi-part form.
   */
  def analyzeMetricViewText(
      session: SparkSession,
      nameParts: Seq[String],
      viewText: String): LogicalPlan = {
    val analyzer = session.sessionState.analyzer
    val syntheticIdent = nameParts match {
      case Seq(table) =>
        TableIdentifier(table)
      case Seq(db, table) =>
        TableIdentifier(table, Some(db))
      case parts =>
        // 3+ parts: catalog is the head, table is the last, the middle (1..n-1) collapses
        // into the synthetic `database` slot. We dot-join the intermediate components so a
        // human inspecting the synthetic identifier can still see them.
        TableIdentifier(
          parts.last,
          Some(parts.slice(1, parts.length - 1).mkString(".")),
          Some(parts.head))
    }
    // this metadata is used for analysis check, it'll be replaced during create/update with
    // more accurate information
    val tableMeta = CatalogTable(
      identifier = syntheticIdent,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType(),
      viewOriginalText = Some(viewText),
      viewText = Some(viewText))
    val metricViewNode = MetricViewPlanner.planWrite(
      tableMeta, viewText, session.sessionState.sqlParser)
    val analyzed = analyzer.executeAndCheck(metricViewNode, new QueryPlanningTracker)
    ViewHelper.verifyTemporaryObjectsNotExists(
      isTemporary = false, nameParts, analyzed, Seq.empty)
    analyzed
  }
}
