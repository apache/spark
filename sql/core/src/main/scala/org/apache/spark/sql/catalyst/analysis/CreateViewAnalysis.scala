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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AlterView, AlterViewAsStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, CreateView, CreateViewStatement, DropView, DropViewStatement, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, Identifier, LookupCatalog, ViewCatalog, ViewChange}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.ViewHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.util.SchemaUtils

/**
 * Resolve persisted views in parsed plans and convert them to logical plans.
 */
case class CreateViewAnalysis(
    override val catalogManager: CatalogManager,
    executePlan: LogicalPlan => QueryExecution) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private val isTempView = catalogManager.v1SessionCatalog.isTempView _
  private val isTemporaryFunction = catalogManager.v1SessionCatalog.isTemporaryFunction _

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {

    case CreateViewStatement(NonSessionCatalogAndIdentifier(catalog, ident), userSpecifiedColumns,
        comment, properties, originalText, child, allowExisting, replace, PersistedView) =>
      convertCreateView(
        catalog = catalog.asViewCatalog,
        ident = ident,
        userSpecifiedColumns = userSpecifiedColumns,
        comment = comment,
        properties = properties,
        originalText = originalText,
        child = child,
        allowExisting = allowExisting,
        replace = replace)

    case alter @ AlterViewAsStatement(
        NonSessionCatalogAndIdentifier(catalog, ident), originalText, query) =>
      CatalogV2Util.loadView(catalog, ident).map { _ =>
        convertCreateView(
          catalog = catalog.asViewCatalog,
          ident = ident,
          userSpecifiedColumns = Seq.empty,
          comment = None,
          properties = Map.empty,
          originalText = Option(originalText),
          child = query,
          allowExisting = false,
          replace = true)
      }.getOrElse(alter)
  }

  /**
   * Convert [[CreateViewStatement]] or [[AlterViewAsStatement]] to logical plan [[CreateView]].
   */
  private def convertCreateView(
      catalog: ViewCatalog,
      ident: Identifier,
      userSpecifiedColumns: Seq[(String, Option[String])],
      comment: Option[String],
      properties: Map[String, String],
      originalText: Option[String],
      child: LogicalPlan,
      allowExisting: Boolean,
      replace: Boolean): LogicalPlan = {
    val qe = executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (userSpecifiedColumns.nonEmpty &&
        userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
          s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
          s"specified by CREATE VIEW (num: `${userSpecifiedColumns.length}`).")
    }

    verifyTemporaryObjectsNotExists(ident, child)

    val queryOutput = analyzedPlan.schema.fieldNames
    // Generate the query column names,
    // throw an AnalysisException if there exists duplicate column names.
    SchemaUtils.checkColumnNameDuplication(
      queryOutput, "in the view definition", SQLConf.get.resolver)

    userSpecifiedColumns.map(_._1).zip(queryOutput).foreach { case (n1, n2) =>
      if (n1 != n2) {
        throw new AnalysisException(s"Renaming columns is not supported: $n1 != $n2")
      }
    }

    if (replace) {
      // Detect cyclic view reference on CREATE OR REPLACE VIEW or ALTER VIEW AS.
      val parts = (catalog.name +: ident.asMultipartIdentifier).quoted
      ViewHelper.checkCyclicViewReference(analyzedPlan, Seq(parts), parts)
    }

    val sql = originalText.getOrElse {
      throw new AnalysisException(
        "It is not allowed to create a persisted view from the Dataset API")
    }

    val viewSchema = aliasPlan(analyzedPlan, userSpecifiedColumns).schema

    CreateView(
      catalog = catalog,
      ident = ident,
      sql = sql,
      comment = comment,
      viewSchema = viewSchema,
      properties = properties,
      allowExisting = allowExisting,
      replace = replace)
  }

  /**
   * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
   * else return the analyzed plan directly.
   */
  private def aliasPlan(
      analyzedPlan: LogicalPlan,
      userSpecifiedColumns: Seq[(String, Option[String])]): LogicalPlan = {
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }

  /**
   * Permanent views are not allowed to reference temp objects, including temp function and views.
   *
   * This function is adapted from CreateViewCommand.verifyTemporaryObjectsNotExists.
   */
  private def verifyTemporaryObjectsNotExists(
      name: Identifier,
      child: LogicalPlan): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    // This func traverses the unresolved plan `child`. Below are the reasons:
    // 1) Analyzer replaces unresolved temporary views by a SubqueryAlias with the corresponding
    // logical plan. After replacement, it is impossible to detect whether the SubqueryAlias is
    // added/generated from a temporary view.
    // 2) The temp functions are represented by multiple classes. Most are inaccessible from this
    // package (e.g., HiveGenericUDF).
    def verify(child: LogicalPlan) {
      child.collect {
        // Disallow creating permanent views based on temporary views.
        case UnresolvedRelation(nameParts) if isTempView(nameParts) =>
          throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
            s"referencing a temporary view ${nameParts.quoted}. " +
            "Please create a temp view instead by CREATE TEMP VIEW")
        case other if !other.resolved => other.expressions.flatMap(_.collect {
          // Traverse subquery plan for any unresolved relations.
          case e: SubqueryExpression => verify(e.plan)
          // Disallow creating permanent views based on temporary UDFs.
          case e: UnresolvedFunction if isTemporaryFunction(e.name) =>
            throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
              s"referencing a temporary function `${e.name}`")
        })
      }
    }
    verify(child)
  }
}
