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
 * @param temporary if true, the view is created as a temporary view. Temporary views are dropped
  *                 at the end of current Spark session. It fails if a persistent table with same
  *                 table name exists. If fails if the table name contains database prefix like
  *                 "database.tablename".
 * @param sql the original sql
 */
case class CreateViewCommand(
    tableDesc: CatalogTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    temporary: Boolean,
    sql: String)
  extends RunnableCommand {

  // TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is
  // different from Hive and may not work for some cases like create view on self join.

  override def output: Seq[Attribute] = Seq.empty[Attribute]

  require(tableDesc.tableType == CatalogTableType.VIEW)
  require(tableDesc.viewText.isDefined)

  private val tableIdentifier = tableDesc.identifier

  if (allowExisting && replace) {
    throw new AnalysisException(
      "It is not allowed to define a view with both IF NOT EXISTS and OR REPLACE.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  if (temporary && tableIdentifier.database.isDefined) {
    val database = tableIdentifier.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix ${database} for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    require(tableDesc.schema == Nil || tableDesc.schema.length == analyzedPlan.output.length)
    val sessionState = sparkSession.sessionState

    def createTable(): Unit = {
      if (temporary) {
        // Creates a temp view
        sessionState.catalog.createTempTable(tableIdentifier.table, analyzedPlan,
          overrideIfExists = false)
      } else {
        sessionState.catalog.createTable(
          prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      }
    }

    def replaceTable(): Unit = {
      if (temporary) {
        // Replaces the temp view if it exists
        sessionState.catalog.createTempTable(tableIdentifier.table, analyzedPlan,
          overrideIfExists = true)
      } else {
        sessionState.catalog.alterTable(prepareTable(sparkSession, analyzedPlan))
      }
    }

    // Ensures the new view has same type (temporary or persistent) when replacing a existing table.
    def assertSameTableType(): Unit = {
      val sameType = temporary == sessionState.catalog.isTemporaryTable(tableIdentifier)
      if (!sameType) {
        def tableType(temporary: Boolean): String = {
          if (temporary) "temporary" else "not temporary"
        }

        throw new AnalysisException(s"View $tableIdentifier already exists with a different " +
          s"type, which is ${tableType(!temporary)}. Please choose a new view name " +
          s"to avoid the name conflict, or change the view type to ${tableType(!temporary)}.")
      }
    }

    if (sessionState.catalog.tableExists(tableIdentifier)) {

      // The new view and the existing table are both temporary or both persistent.
      assertSameTableType()

      if (allowExisting) {
        // Handles `CREATE [TEMPORARY] VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the
        // target view already exists.
      } else if (replace) {
        // Handles `CREATE OR REPLACE [TEMPORARY] VIEW v0 AS SELECT ...`
        replaceTable()
      } else {
        // Handles `CREATE [TEMPORARY] VIEW v0 AS SELECT ...`. Throws exception when the target
        // view already exists.
        throw new AnalysisException(s"View $tableIdentifier already exists. " +
          "If you want to update the view definition, please use ALTER VIEW AS or " +
          "CREATE OR REPLACE VIEW AS")
      }
    } else {
      // Create a table view if the table name doesn't exist.
      createTable()
    }

    Seq.empty[Row]
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
            sparkSession.executePlan(Project(projectList, analyzedPlan)).analyzed
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

        val viewText = tableDesc.viewText.get
        val viewName = quote(tableDesc.identifier.table)
        s"SELECT $viewOutput FROM ($viewText) $viewName"
      }

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(
          "Failed to analyze the canonicalized SQL. It is possible there is a bug in Spark.", e)
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
