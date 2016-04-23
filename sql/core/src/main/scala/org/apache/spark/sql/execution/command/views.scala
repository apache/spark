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

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.SQLBuilder
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}


/**
 * Create Hive view on non-hive-compatible tables by specifying schema ourselves instead of
 * depending on Hive meta-store.
 *
 * @param tableDesc the catalog table
 * @param child the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 * @param allowExisting if true, and if the view already exists, noop; if false, and if the view
 *                already exists, throws analysis exception.
 * @param replace if true, and if the view already exists, updates it; if false, and if the view
 *                already exists, throws analysis exception.
 * @param sql the original sql
 */
case class CreateViewCommand(
    tableDesc: CatalogTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    sql: String)
  extends RunnableCommand {

  // TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is
  // different from Hive and may not work for some cases like create view on self join.

  override def output: Seq[Attribute] = Seq.empty[Attribute]

  require(tableDesc.tableType == CatalogTableType.VIRTUAL_VIEW)
  require(tableDesc.viewText.isDefined)

  private val tableIdentifier = tableDesc.identifier

  if (allowExisting && replace) {
    throw new AnalysisException(
      "It is not allowed to define a view with both IF NOT EXISTS and OR REPLACE.")
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val analzyedPlan = sqlContext.executePlan(child).analyzed

    require(tableDesc.schema == Nil || tableDesc.schema.length == analzyedPlan.output.length)
    val sessionState = sqlContext.sessionState

    if (sessionState.catalog.tableExists(tableIdentifier)) {
      if (allowExisting) {
        // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
        // already exists.
      } else if (replace) {
        // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
        sessionState.catalog.alterTable(prepareTable(sqlContext, analzyedPlan))
      } else {
        // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
        // exists.
        throw new AnalysisException(s"View $tableIdentifier already exists. " +
          "If you want to update the view definition, please use ALTER VIEW AS or " +
          "CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create the view if it doesn't exist.
      sessionState.catalog.createTable(
        prepareTable(sqlContext, analzyedPlan), ignoreIfExists = false)
    }

    Seq.empty[Row]
  }

  private def prepareTable(sqlContext: SQLContext, analzyedPlan: LogicalPlan): CatalogTable = {
    val expandedText = if (sqlContext.conf.canonicalView) {
      try rebuildViewQueryString(sqlContext, analzyedPlan) catch {
        case NonFatal(e) => wrapViewTextWithSelect(analzyedPlan)
      }
    } else {
      wrapViewTextWithSelect(analzyedPlan)
    }

    val viewSchema = {
      if (tableDesc.schema.isEmpty) {
        analzyedPlan.output.map { a =>
          CatalogColumn(a.name, a.dataType.simpleString)
        }
      } else {
        analzyedPlan.output.zip(tableDesc.schema).map { case (a, col) =>
          CatalogColumn(col.name, a.dataType.simpleString, nullable = true, col.comment)
        }
      }
    }

    tableDesc.copy(schema = viewSchema, viewText = Some(expandedText))
  }

  private def wrapViewTextWithSelect(analzyedPlan: LogicalPlan): String = {
    // When user specified column names for view, we should create a project to do the renaming.
    // When no column name specified, we still need to create a project to declare the columns
    // we need, to make us more robust to top level `*`s.
    val viewOutput = {
      val columnNames = analzyedPlan.output.map(f => quote(f.name))
      if (tableDesc.schema.isEmpty) {
        columnNames.mkString(", ")
      } else {
        columnNames.zip(tableDesc.schema.map(f => quote(f.name))).map {
          case (name, alias) => s"$name AS $alias"
        }.mkString(", ")
      }
    }

    val viewText = tableDesc.viewText.get
    val viewName = quote(tableDesc.identifier.table)
    s"SELECT $viewOutput FROM ($viewText) $viewName"
  }

  private def rebuildViewQueryString(sqlContext: SQLContext, analzyedPlan: LogicalPlan): String = {
    val logicalPlan = if (tableDesc.schema.isEmpty) {
      analzyedPlan
    } else {
      val projectList = analzyedPlan.output.zip(tableDesc.schema).map {
        case (attr, col) => Alias(attr, col.name)()
      }
      sqlContext.executePlan(Project(projectList, analzyedPlan)).analyzed
    }
    new SQLBuilder(logicalPlan).toSQL
  }

  // escape backtick with double-backtick in column name and wrap it with backtick.
  private def quote(name: String) = s"`${name.replaceAll("`", "``")}`"
}
