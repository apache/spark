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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{SQLBuilder, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}


/**
 * Create or replace a view with given query plan. This command will convert the query plan to
 * canonicalized SQL string, and store it as view text in metastore, if we need to create a
 * permanent view.
 *
 * @param tableDesc the catalog table
 * @param child the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 * @param allowExisting if true, and if the view already exists, noop; if false, and if the view
 *                already exists, throws analysis exception.
 * @param replace if true, and if the view already exists, updates it; if false, and if the view
 *                already exists, throws analysis exception.
 * @param isTemporary if true, the view is created as a temporary view. Temporary views are dropped
 *                 at the end of current Spark session. Existing permanent relations with the same
 *                 name are not visible to the current session while the temporary view exists,
 *                 unless they are specified with full qualified table name with database prefix.
 */
case class CreateViewCommand(
    tableDesc: CatalogTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    isTemporary: Boolean)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  // TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is
  // different from Hive and may not work for some cases like create view on self join.

  require(tableDesc.tableType == CatalogTableType.VIEW,
    "The type of the table to created with CREATE VIEW must be 'CatalogTableType.VIEW'.")
  if (!isTemporary) {
    require(tableDesc.viewOriginalText.isDefined,
      "The table to created with CREATE VIEW must have 'viewOriginalText'.")
  }

  if (allowExisting && replace) {
    throw new AnalysisException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")
  }

  // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with 'CREATE TEMPORARY TABLE'
  if (allowExisting && isTemporary) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  if (isTemporary && tableDesc.identifier.database.isDefined) {
    val database = tableDesc.identifier.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (tableDesc.schema != Nil && tableDesc.schema.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${tableDesc.schema.length}`).")
    }
    val sessionState = sparkSession.sessionState

    if (isTemporary) {
      createTemporaryView(tableDesc.identifier, sparkSession, analyzedPlan)
    } else {
      // Adds default database for permanent table if it doesn't exist, so that tableExists()
      // only check permanent tables.
      val database = tableDesc.identifier.database.getOrElse(
        sessionState.catalog.getCurrentDatabase)
      val tableIdentifier = tableDesc.identifier.copy(database = Option(database))

      if (sessionState.catalog.tableExists(tableIdentifier)) {
        if (allowExisting) {
          // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
          // already exists.
        } else if (replace) {
          // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
          sessionState.catalog.alterTable(prepareTable(sparkSession, analyzedPlan))
        } else {
          // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
          // exists.
          throw new AnalysisException(
            s"View $tableIdentifier already exists. If you want to update the view definition, " +
              "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
        }
      } else {
        // Create the view if it doesn't exist.
        sessionState.catalog.createTable(
          prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      }
    }
    Seq.empty[Row]
  }

  private def createTemporaryView(
      table: TableIdentifier, sparkSession: SparkSession, analyzedPlan: LogicalPlan): Unit = {

    val sessionState = sparkSession.sessionState
    val catalog = sessionState.catalog

    // Projects column names to alias names
    val logicalPlan = {
      if (tableDesc.schema.isEmpty) {
        analyzedPlan
      } else {
        val projectList = analyzedPlan.output.zip(tableDesc.schema).map {
          case (attr, col) => Alias(attr, col.name)()
        }
        sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
      }
    }

    catalog.createTempView(table.table, logicalPlan, replace)
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. This comment canonicalize
   * SQL based on the analyzed plan, and also creates the proper schema for the view.
   */
  private def prepareTable(sparkSession: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    val viewSQL: String =
      if (sparkSession.sessionState.conf.canonicalView) {
        val logicalPlan =
          if (tableDesc.schema.isEmpty) {
            analyzedPlan
          } else {
            val projectList = analyzedPlan.output.zip(tableDesc.schema).map {
              case (attr, col) => Alias(attr, col.name)()
            }
            sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
          }
        new SQLBuilder(logicalPlan).toSQL
      } else {
        // When user specified column names for view, we should create a project to do the renaming.
        // When no column name specified, we still need to create a project to declare the columns
        // we need, to make us more robust to top level `*`s.
        val viewOutput = {
          val columnNames = analyzedPlan.output.map(f => quote(f.name))
          if (tableDesc.schema.isEmpty) {
            columnNames.mkString(", ")
          } else {
            columnNames.zip(tableDesc.schema.map(f => quote(f.name))).map {
              case (name, alias) => s"$name AS $alias"
            }.mkString(", ")
          }
        }

        val viewName = quote(tableDesc.identifier.table)
        s"SELECT $viewOutput FROM (${tableDesc.viewOriginalText.get}) $viewName"
      }

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val viewSchema: Seq[CatalogColumn] = {
      if (tableDesc.schema.isEmpty) {
        analyzedPlan.output.map { a =>
          CatalogColumn(a.name, a.dataType.catalogString)
        }
      } else {
        analyzedPlan.output.zip(tableDesc.schema).map { case (a, col) =>
          CatalogColumn(col.name, a.dataType.catalogString, nullable = true, col.comment)
        }
      }
    }

    tableDesc.copy(schema = viewSchema, viewText = Some(viewSQL))
  }

  /** Escape backtick with double-backtick in column name and wrap it with backtick. */
  private def quote(name: String) = s"`${name.replaceAll("`", "``")}`"
}

/**
 * Alter a view with given query plan. If the view name contains database prefix, this command will
 * alter a permanent view matching the given name, or throw an exception if view not exist. Else,
 * this command will try to alter a temporary view first, if view not exist, try permanent view
 * next, if still not exist, throw an exception.
 *
 * @param tableDesc the catalog table
 * @param query the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 */
case class AlterViewAsCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (session.sessionState.catalog.isTemporaryTable(tableDesc.identifier)) {
      session.sessionState.catalog.createTempView(
        tableDesc.identifier.table,
        analyzedPlan,
        overrideIfExists = true)
    } else {
      alterPermanentView(session, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(tableDesc.identifier)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    val viewSQL: String = new SQLBuilder(analyzedPlan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val viewSchema: Seq[CatalogColumn] = {
      analyzedPlan.output.map { a =>
        CatalogColumn(a.name, a.dataType.catalogString)
      }
    }

    val updatedViewMeta = viewMeta.copy(
      schema = viewSchema,
      viewOriginalText = tableDesc.viewOriginalText,
      viewText = Some(viewSQL))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}
