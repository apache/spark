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

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{SQLBuilder, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.StructType


/**
 * Create Hive view on non-hive-compatible tables by specifying schema ourselves instead of
 * depending on Hive meta-store.
 *
 * @param name the name of this view.
 * @param userSpecifiedColumns the output column names and optional comments specified by users,
 *                             can be Nil if not specified.
 * @param comment the comment of this view.
 * @param properties the properties of this view.
 * @param originalText the original SQL text of this view, can be None if this view is created via
 *                     Dataset API.
 * @param child the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 * @param mode only three modes are supported here: Ignore, Overwrite and ErrorIfExists. If the
 *             view already exists, CreateViewCommand behaves based on the mode:
 *             1) Overwrite: update the view;
 *             2) Ignore: noop;
 *             3) ErrorIfExists throws analysis exception.
 * @param viewType the type of this view.
 */
case class CreateViewCommand(
    name: TableIdentifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    mode: SaveMode,
    viewType: ViewType)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  // TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is
  // different from Hive and may not work for some cases like create view on self join.

  override def output: Seq[Attribute] = Seq.empty[Attribute]

  assert(mode != SaveMode.Append,
    "CREATE or ALTER VIEW can only use ErrorIfExists, Ignore or Overwrite.")

  // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with 'CREATE TEMPORARY TABLE'
  if (mode == SaveMode.Ignore && viewType == TemporaryView) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  if (viewType == TemporaryView && name.database.isDefined) {
    val database = name.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (userSpecifiedColumns.nonEmpty &&
        userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${userSpecifiedColumns.length}`).")
    }
    val sessionState = sparkSession.sessionState

    viewType match {
      case TemporaryView =>
        createTemporaryView(sparkSession, analyzedPlan)
      case AnyTypeView if sessionState.catalog.isTemporaryTable(name) =>
        createTemporaryView(sparkSession, analyzedPlan)
      case _ =>
        // Adds default database for permanent table if it doesn't exist, so that tableExists()
        // only check permanent tables.
        val database = name.database.getOrElse(sessionState.catalog.getCurrentDatabase)
        val qualifiedName = name.copy(database = Option(database))
        if (sessionState.catalog.tableExists(qualifiedName)) {
          val tableMetadata = sessionState.catalog.getTableMetadata(qualifiedName)
          if (mode == SaveMode.Ignore) {
            // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target
            // view already exists.
          } else if (tableMetadata.tableType != CatalogTableType.VIEW) {
            throw new AnalysisException(
              "Existing table is not a view. The following is an existing table, " +
                s"not a view: $qualifiedName")
          } else if (mode == SaveMode.Overwrite) {
            // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
            sessionState.catalog.alterTable(prepareTable(sparkSession, analyzedPlan))
          } else {
            // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
            // exists.
            throw new AnalysisException(
              s"View $qualifiedName already exists. If you want to update the view definition, " +
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

  private def createTemporaryView(sparkSession: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val catalog = sparkSession.sessionState.catalog

    // Projects column names to alias names
    val logicalPlan = if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, _)) => Alias(attr, colName)()
      }
      sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }

    catalog.createTempView(name.table, logicalPlan, mode == SaveMode.Overwrite)
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. This comment canonicalize
   * SQL based on the analyzed plan, and also creates the proper schema for the view.
   */
  private def prepareTable(sparkSession: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    val aliasedPlan = if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, _)) => Alias(attr, colName)()
      }
      sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }

    val viewSQL: String = new SQLBuilder(aliasedPlan).toSQL

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: ${viewSQL}", e)
    }

    val viewSchema = if (userSpecifiedColumns.isEmpty) {
      aliasedPlan.schema
    } else {
      StructType(aliasedPlan.schema.zip(userSpecifiedColumns).map {
        case (field, (_, comment)) => comment.map(field.withComment).getOrElse(field)
      })
    }

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = viewSchema,
      properties = properties,
      viewOriginalText = originalText,
      viewText = Some(viewSQL),
      comment = comment
    )
  }
}

/**
 * The trait used to represent the type of a view.
 */
sealed trait ViewType

/**
 * Temporary means local temporary views. The views are session-scoped and automatically dropped
 * when the session terminates. Do not qualify a temporary table with a schema name. Existing
 * permanent tables or views with the same name are not visible while the temporary view exists,
 * unless they are referenced with schema-qualified names.
 */
case object TemporaryView extends ViewType

/**
 * Permanent means global permanent views. The views are global-scoped and accessible by all
 * sessions. The permanent views stays until they are explicitly dropped.
 */
case object PermanentView extends ViewType

/**
 * Any means the view type is unknown.
 */
case object AnyTypeView extends ViewType
